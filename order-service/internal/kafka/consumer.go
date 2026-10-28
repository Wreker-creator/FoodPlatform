package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"order-service/internal/store"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader  *kafka.Reader
	Queries *store.Queries
	Pool    *pgxpool.Pool
}

func NewConsumer(brokerAddr, topic, groupID string, queries *store.Queries, pool *pgxpool.Pool) *Consumer {
	return &Consumer{
		Queries: queries,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{brokerAddr},
			Topic:       topic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
		}),
		Pool: pool,
	}
}

func (c *Consumer) Start(ctx context.Context) {
	slog.Info("consumer starting", "topic", c.reader.Config().Topic, "group", c.reader.Config().GroupID)

	for {
		// 1. Fetch message WITHOUT committing offset
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("consumer context canceled, stopping read loop", "topic", c.reader.Config().Topic)
				return
			}
			slog.Error("failed to fetch message", "error", err)
			continue
		}

		// 2. Process message with retries
		maxRetries := 3
		var processErr error

		for attempt := 1; attempt <= maxRetries; attempt++ {
			processErr = c.Read(ctx, msg)
			if processErr == nil {
				break
			}

			slog.Warn("retrying message processing",
				"attempt", attempt,
				"max_retries", maxRetries,
				"topic", msg.Topic,
				"offset", msg.Offset,
				"error", processErr,
			)

			// Simple backoff before next retry (skip sleep if context canceled)
			select {
			case <-time.After(time.Duration(attempt*200) * time.Millisecond):
			case <-ctx.Done():
				slog.Info("context canceled during retry backoff, exiting")
				return
			}
		}

		// 3. Handle persistent failure vs success
		if processErr != nil {
			// In production without a DLQ, you have two choices:
			// Option A: Skip and commit so the consumer doesn't block forever (log CRITICAL).
			// Option B: Halt the consumer loop to alert on-call (prevents skipping bad data).
			// For now, log critical failure and commit or park. Here we log error and skip.
			slog.Error("CRITICAL: message failed after max retries, skipping and committing offset",
				"topic", msg.Topic,
				"offset", msg.Offset,
				"key", string(msg.Key),
				"error", processErr,
			)
		}

		// 4. Commit offset ONLY after successful processing (or skipping poison pill)
		// Use context.Background() so an in-flight commit isn't aborted if ctx cancels
		commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := c.reader.CommitMessages(commitCtx, msg); err != nil {
			slog.Error("failed to commit message offset", "topic", msg.Topic, "offset", msg.Offset, "error", err)
		}
		cancel()
	}
}

func (c *Consumer) Read(ctx context.Context, msg kafka.Message) error {

	var envelope EventEnvelope
	slog.Info("received event envelope", "offset", msg.Offset, "key", string(msg.Key))
	err := json.Unmarshal(msg.Value, &envelope)
	if err != nil {
		slog.Error("Failed to unmarshal envelope", "Error:", err)
		return err
	}

	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		slog.Error("Failed to begin transactions", "error: ", err)
		return err
	}

	// defer tx.Rollback(ctx)
	defer func() {
		// with the introduction of graceful shutdown we are now open to handling issues like ctrl + C
		// now in such cases SIGTERM is sent through context, and tx.Rollback might receive an already closed context
		// to handle that we introduce this function
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

	case "InventoryReserved":
		var event InventoryReservedEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			slog.Error("failed to unmarshal InventoryReservedEvent", "error", err)
			return err
		}

		rowsAffect, err := qtx.UpdateOrderStatusGuarded(ctx, store.UpdateOrderStatusGuardedParams{
			ID:       event.OrderId,
			Status:   "AWAITING_PAYMENT",
			Status_2: "PENDING",
		})

		if rowsAffect == 0 {
			// this is a check to see if the order was already moved to CONFIRMED or CANCELLED
			slog.Warn("ignored stale event: order not in expected state", "order_id", event.OrderId)
			return nil
		}

		if err != nil {
			slog.Error("failed to update order status", "order_id", event.OrderId, "error", err)
			return err
		}

		if err = tx.Commit(ctx); err != nil {
			slog.Error("Failed to commit awaiting payment transaction", "Error:", err)
			return err
		}

		return nil

	case "InventoryRejected":
		var event InventoryRejectedEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			slog.Error("failed to unmarshal InventoryRejectedEvent", "error", err)
			return err
		}

		rowsAffected, err := qtx.UpdateOrderStatusGuarded(ctx, store.UpdateOrderStatusGuardedParams{
			ID:       event.OrderId,
			Status:   "CANCELLED",
			Status_2: "PENDING",
		})

		if err != nil {
			slog.Error("failed to update order status", "order_id", event.OrderId, "error", err)
			return err
		}

		if rowsAffected == 0 {
			slog.Warn("Igorning stale event: order not in expected state", "order_id", event.OrderId)
			return nil
		}

		cancelledEvent := OrderCancelledEvent{
			OrderId:          event.OrderId,
			Reason:           event.Reason,
			ReleaseInventory: false,
		}

		payloadBytes, err := json.Marshal(cancelledEvent)
		if err != nil {
			slog.Error("Failed to marshal the event", "Error:", err)
			return err
		}

		_, err = qtx.InsertOutboxEvent(ctx, store.InsertOutboxEventParams{
			AggregateKey: strconv.Itoa(int(cancelledEvent.OrderId)),
			EventType:    "OrderCancelled",
			Payload:      payloadBytes,
		})

		if err != nil {
			slog.Error("failed to insert outbox event", "error", err)
			return err
		}

		if err := tx.Commit(ctx); err != nil {
			slog.Error("Failed to commit order cancelled transaction", "Error:", err)
			return err
		}

		return nil

	case "PaymentSucceeded":
		var event PaymentSucceededEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			slog.Error("failed to unmarshal PaymentSucceededEvent", "error", err)
			return err
		}

		rowsAffected, err := qtx.UpdateOrderStatusGuarded(ctx, store.UpdateOrderStatusGuardedParams{
			ID:       event.OrderId,
			Status:   "CONFIRMED",
			Status_2: "AWAITING_PAYMENT",
		})

		if err != nil {
			slog.Error("failed to update order status", "order_id", event.OrderId, "error", err)
			return err
		}

		if rowsAffected == 0 {
			slog.Warn("Ingoring stale element, expected status incorrect", "order_id", event.OrderId)
			return nil
		}

		order, err := qtx.GetOrderByID(ctx, event.OrderId)
		if err != nil {
			slog.Error("Failed to get order by id", "error:", err)
			return err
		}

		orderConfirmedEvent := OrderConfirmedEvent{
			OrderId:    event.OrderId,
			CustomerId: order.CustomerID,
		}

		payloadBytes, err := json.Marshal(orderConfirmedEvent)
		if err != nil {
			slog.Error("Failed to marshal orderConfirmedEvent", "error: ", err)
			return err
		}

		_, err = qtx.InsertOutboxEvent(ctx, store.InsertOutboxEventParams{
			AggregateKey: strconv.Itoa(int(orderConfirmedEvent.OrderId)),
			EventType:    "OrderConfirmed",
			Payload:      payloadBytes,
		})

		if err != nil {
			slog.Error("Failed to insert outbox event, orderConfirmedEvent")
			return err
		}

		// return fmt.Errorf("False error, adding for a fake DB TIMEOUT")

		if err := tx.Commit(ctx); err != nil {
			slog.Error("Failed to commit order confirmed transaction ", "error: ", err)
			return err
		}

		return nil

	case "PaymentFailed":

		var event PaymentFailedEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			slog.Error("failed to unmarshal PaymentFailedEvent", "error", err)
			return err
		}

		rowsAffected, err := qtx.UpdateOrderStatusGuarded(ctx, store.UpdateOrderStatusGuardedParams{
			ID:       event.OrderId,
			Status:   "CANCELLED",
			Status_2: "AWAITING_PAYMENT",
		})

		if rowsAffected == 0 {
			slog.Warn("Ignoring stale element, expect state is incorrect for", "order_id", event.OrderId)
			return nil // is required because we dont want to commit another event into outbox for no reason
		}

		if err != nil {
			slog.Error("failed to update order status", "order_id", event.OrderId, "error", err)
			return err
		}

		orderItems, err := qtx.GetOrderItemsByOrderId(ctx, event.OrderId)
		if err != nil {
			slog.Error("failed to fetch order items for cancellation", "error", err)
			return err
		}
		eventItems := make([]OrderItemEvent, 0, len(orderItems))
		for _, item := range orderItems {
			eventItems = append(eventItems, OrderItemEvent{
				ProductId: item.ProductID,
				Quantity:  item.Quantity,
				// UnitPrice not needed here — Inventory only cares about quantity to release
			})
		}

		cancelledEvent := OrderCancelledEvent{
			OrderId:          event.OrderId,
			Reason:           event.Reason,
			Items:            eventItems,
			ReleaseInventory: true, // signal to Inventory service to release reserved stock
		}

		payloadBytes, err := json.Marshal(cancelledEvent)
		if err != nil {
			slog.Error("Failed to marshal order cancelled event", "error: ", err)
			return err
		}

		_, err = qtx.InsertOutboxEvent(ctx, store.InsertOutboxEventParams{
			AggregateKey: strconv.Itoa(int(cancelledEvent.OrderId)),
			EventType:    "OrderCancelled",
			Payload:      payloadBytes,
		})

		if err != nil {
			slog.Error("Failed to insert outbox event", "error: ", err)
			return err
		}

		if err := tx.Commit(ctx); err != nil {
			slog.Error("Failed to commit order cancelled transaction", "error: ", err)
			return err
		}

		return nil

	default:
		slog.Warn("unknown event type", "event_type", envelope.EventType)
	}

	return nil

}
