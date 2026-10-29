package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"inventory-service/internal/store"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader    *kafka.Reader
	Queries   *store.Queries
	Pool      *pgxpool.Pool
	DLQWriter *kafka.Writer
}

func NewConsumer(brokerAddr, topic, groupID string, queries *store.Queries, pool *pgxpool.Pool, dlqWriter *kafka.Writer) *Consumer {
	return &Consumer{
		Queries: queries,
		Pool:    pool,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{brokerAddr},
			Topic:       topic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
		}),
		DLQWriter: dlqWriter,
	}
}

// this function will keep running for eternity unless it gets stopped via context gracefully
func (c *Consumer) Start(ctx context.Context) {
	slog.Info("inventory consumer starting", "topic", c.reader.Config().Topic, "group", c.reader.Config().GroupID)

	for {
		// 1. Fetch message without committing offset
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("inventory consumer context canceled, stopping read loop", "topic", c.reader.Config().Topic)
				return
			}
			slog.Error("failed to fetch message", "topic", c.reader.Config().Topic, "error", err)
			continue
		}

		// 2. Retry loop for message processing
		maxRetries := 3
		var processErr error

		for attempt := 1; attempt <= maxRetries; attempt++ {
			processErr = c.Read(ctx, msg)
			if processErr == nil {
				break
			}

			slog.Warn("retrying inventory message processing",
				"attempt", attempt,
				"max_retries", maxRetries,
				"topic", msg.Topic,
				"offset", msg.Offset,
				"error", processErr,
			)

			select {
			case <-time.After(time.Duration(attempt*200) * time.Millisecond):
			case <-ctx.Done():
				slog.Info("context canceled during retry backoff, exiting")
				return
			}
		}

		if processErr != nil {
			slog.Error("CRITICAL: inventory message processing failed after max retries; skipping and committing offset",
				"topic", msg.Topic,
				"offset", msg.Offset,
				"key", string(msg.Key),
				"error", processErr,
			)
			c.routeToDlq(ctx, msg, fmt.Sprintf("Exhausted %d retries: %v", maxRetries, processErr))
		}

		// 3. Commit offset only after DB transaction finishes (or skipping poison pill)
		commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := c.reader.CommitMessages(commitCtx, msg); err != nil {
			slog.Error("failed to commit inventory offset", "topic", msg.Topic, "offset", msg.Offset, "error", err)
		}
		cancel()
	}
}

func (c *Consumer) Read(ctx context.Context, msg kafka.Message) error {
	var envelope EventEnvelope
	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		c.routeToDlq(ctx, msg, fmt.Sprintf("Failed to unmarshal envelope: %v", err))
		return nil
	}

	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		slog.Error("Failed to begin transaction", "error", err)
		return err
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	qtx := c.Queries.WithTx(tx)

	// this is where we do idempotency check
	if envelope.EventID == "" {
		slog.Warn("Event missing eventId, cannot guarantee idempotency", "offset", msg.Offset)
	} else {
		rowsAffected, err := qtx.InsertProcessedEvent(ctx, envelope.EventID)
		if err != nil {
			slog.Error("Failed top insert processed event", "Error:", err)
			return err
		}

		if rowsAffected == 0 {
			// because we have added on conflict do nothing with execrows, we get back rowsAffected and can just actively check
			slog.Info("idempotency check: event already processed, skipping", "event_id", envelope.EventID)
			return nil
		}
	}

	switch envelope.EventType {
	case "OrderCreated":
		var event OrderCreatedEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			return err
		}
		// ... existing decrement/reject logic, unchanged ...

		decremented := make([]OrderItemEvent, 0, len(event.Items))
		rejected := false

		var rejectReason string

		for _, item := range event.Items {
			rowsAffected, err := qtx.DecrementInventory(ctx, store.DecrementInventoryParams{
				ProductID:         item.ProductId,
				AvailableQuantity: item.Quantity,
			})
			if err != nil {
				return err
			}
			if rowsAffected == 0 {
				rejected = true
				rejectReason = fmt.Sprintf("insufficent stock for product id - %d", item.ProductId)
				break
			}
			decremented = append(decremented, item)
		}

		if rejected {
			for _, item := range decremented {
				err := qtx.IncrementInventory(ctx, store.IncrementInventoryParams{
					ProductID:         item.ProductId,
					AvailableQuantity: item.Quantity,
				})
				if err != nil {
					return err
				}
			}

			rejectedEvent := InventoryRejectedEvent{
				OrderId: event.OrderId,
				Reason:  rejectReason,
			}

			payloadBytes, err := json.Marshal(rejectedEvent)
			if err != nil {
				slog.Error("failed to marshal InventoryRejectedEvent", "error", err)
				return err
			}

			if _, err := qtx.InsertOutboxEvent(ctx, store.InsertOutboxEventParams{
				AggregateKey: strconv.Itoa(int(rejectedEvent.OrderId)),
				EventType:    "InventoryRejected",
				Payload:      payloadBytes,
			}); err != nil {
				slog.Error("Failed to insert outbox event", "error: ", err)
				return err
			}

			if err := tx.Commit(ctx); err != nil {
				slog.Error("Failed to commit rejection transaction", "error", err)
				return err
			}

			return nil

		}

		reservedEvent := InventoryReservedEvent{
			OrderId: event.OrderId,
			Items:   event.Items,
		}

		payloadBytes, err := json.Marshal(reservedEvent)
		if err != nil {
			slog.Error("failed to marshal InventoryReservedEvent", "error", err)
			return err
		}

		if _, err := qtx.InsertOutboxEvent(ctx, store.InsertOutboxEventParams{
			AggregateKey: strconv.Itoa(int(reservedEvent.OrderId)),
			EventType:    "InventoryReserved",
			Payload:      payloadBytes,
		}); err != nil {
			slog.Error("Failed to insert outbox event", "error: ", err)
			return err
		}

		if err := tx.Commit(ctx); err != nil {
			slog.Error("Failed to commit inventory reserved transaction", "Error:", err)
			return err
		}

		return nil // each case is required to return something, in these cases where we are already handling errors, just return nil

	case "OrderCancelled":
		var event OrderCancelledEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			slog.Error("failed to unmarshal OrderCancelledEvent", "error", err)
			return err
		}

		if !event.ReleaseInventory {
			slog.Info("Release inventory variable was set as false")
			return nil // nothing to release
		}

		for _, item := range event.Items {
			if err := qtx.IncrementInventory(ctx, store.IncrementInventoryParams{
				ProductID:         item.ProductId,
				AvailableQuantity: item.Quantity,
			}); err != nil {
				slog.Error("failed to release inventory", "error", err)
				return err
			}
		}

		if err := tx.Commit(ctx); err != nil {
			slog.Error("Failed to commit inventory release transaction", "Error:", err)
			return err
		}

		// doesnt publish anything because the order is cancelled and inventory is released, so just return nil
		return nil

	default:
		slog.Warn("unhandled event type", "event_type", envelope.EventType)
	}
	return nil
}

func (c *Consumer) routeToDlq(ctx context.Context, msg kafka.Message, reason string) {

	dlqPayload := DLQMessage{
		OriginalTopic: msg.Topic,
		OriginalKey:   string(msg.Key),
		OriginalValue: string(msg.Value),
		ErrorReason:   reason,
		FailedAt:      time.Now(),
	}

	payloadBytes, _ := json.Marshal(dlqPayload)

	err := c.DLQWriter.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: payloadBytes,
	})

	if err != nil {
		slog.Error("CRITICAL: Failed to write to DLQ", "Error:", err)
	} else {
		slog.Info("routed poison pill to DLQ", "key", string(msg.Key), "reason:", reason)
	}

}
