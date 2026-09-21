package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"inventory-service/internal/store"
	"log/slog"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader   *kafka.Reader
	Queries  *store.Queries
	Producer *Producer
	Pool     *pgxpool.Pool
}

func NewConsumer(brokerAddr, topic, groupID string, queries *store.Queries, producer *Producer, pool *pgxpool.Pool) *Consumer {
	return &Consumer{
		Queries:  queries,
		Producer: producer,
		Pool:     pool,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{brokerAddr},
			Topic:       topic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
		}),
	}
}

// this function will keep running for eternity unless it gets stopped via context gracefully
func (c *Consumer) Start(ctx context.Context) {
	slog.Info("consumer starting", "topic", c.reader.Config().Topic, "group", c.reader.Config().GroupID)
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			slog.Error("failed to read message", "error", err)
			continue
		}

		if err := c.Read(ctx, msg); err != nil {
			slog.Error("failed to process message", "error", err, "offset", msg.Offset)
			// deliberately not stopping the loop here — one bad message
			// shouldn't kill the whole consumer; log it and move to the next
		}
	}
}

func (c *Consumer) Read(ctx context.Context, msg kafka.Message) error {
	var envelope EventEnvelope
	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		return err
	}

	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		slog.Error("Failed to begin transaction", "error", err)
		return err
	}

	defer tx.Rollback(ctx)

	qtx := c.Queries.WithTx(tx)

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
				slog.Error("Failed to commit order transaction", "error", err)
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
			slog.Error("Failed to commit order transaction", "error", err)
			return err
		}

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
			slog.Error("Failed to commit order transaction", "error", err)
			return err
		}

		// doesnt publish anything because the order is cancelled and inventory is released, so just return nil
		return nil

	default:
		slog.Warn("unhandled event type", "event_type", envelope.EventType)
	}
	return nil
}
