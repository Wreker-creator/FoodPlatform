package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"order-service/internal/store"
	"order-service/internal/telemetry" // <-- NEW: Import telemetry
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

// --- NEW: Sentinel error for metrics tracking and retry avoidance ---
// We use this so Start() knows the event was skipped intentionally and shouldn't be retried.
var ErrDuplicateEvent = errors.New("duplicate or stale event")

type Consumer struct {
	reader    *kafka.Reader
	Queries   *store.Queries
	Pool      *pgxpool.Pool
	DLQWriter *kafka.Writer
}

func NewConsumer(brokerAddr, topic, groupID string, queries *store.Queries, pool *pgxpool.Pool, dlqWriter *kafka.Writer) *Consumer {
	return &Consumer{
		Queries: queries,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{brokerAddr},
			Topic:       topic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
		}),
		Pool:      pool,
		DLQWriter: dlqWriter,
	}
}

func (c *Consumer) Start(ctx context.Context) {
	topic := c.reader.Config().Topic
	groupID := c.reader.Config().GroupID
	slog.Info("consumer starting", "topic", topic, "group", groupID)

	// --- NEW: Background ticker for Consumer Lag metric ---
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stats := c.reader.Stats()
				// Expose the internal lag gauge to Prometheus
				telemetry.ConsumerLag.WithLabelValues(topic, groupID).Set(float64(stats.Lag))
			}
		}
	}()

	for {
		// 1. Fetch message WITHOUT committing offset
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("consumer context canceled, stopping read loop", "topic", topic)
				return
			}
			slog.Error("failed to fetch message", "topic", topic, "error", err)
			continue
		}

		// --- NEW: Start timer for processing duration ---
		start := time.Now()

		// --- NEW: Extract event type for Prometheus labels ---
		var envelope EventEnvelope
		_ = json.Unmarshal(msg.Value, &envelope)
		eventType := envelope.EventType
		if eventType == "" {
			eventType = "unknown"
		}

		// 2. Process message with retries
		maxRetries := 3
		var processErr error

		for attempt := 1; attempt <= maxRetries; attempt++ {
			processErr = c.Read(ctx, msg)

			// --- NEW: Check for ErrDuplicateEvent to break out early ---
			if processErr == nil || errors.Is(processErr, ErrDuplicateEvent) {
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

		// --- NEW: Record Event Processing Duration ---
		duration := time.Since(start).Seconds()
		telemetry.EventProcessingDuration.WithLabelValues(topic).Observe(duration)

		// 3. Handle persistent failure vs success & Record Outcome Metrics
		if processErr != nil && !errors.Is(processErr, ErrDuplicateEvent) {
			slog.Error("CRITICAL: message failed after max retries, skipping and committing offset",
				"topic", msg.Topic,
				"offset", msg.Offset,
				"key", string(msg.Key),
				"error", processErr,
			)

			c.routeToDlq(ctx, msg, fmt.Sprintf("exhausted %d retries: %v", maxRetries, processErr))

			// --- NEW: Record DLQ Metric ---
			telemetry.EventsProcessedTotal.WithLabelValues(topic, eventType, "dlq").Inc()
			telemetry.DLQMessagesTotal.WithLabelValues(topic).Inc()

		} else {
			// --- NEW: Differentiate between a normal "ok" and a "duplicate" ---
			if errors.Is(processErr, ErrDuplicateEvent) {
				telemetry.EventsProcessedTotal.WithLabelValues(topic, eventType, "duplicate").Inc()
			} else {
				telemetry.EventsProcessedTotal.WithLabelValues(topic, eventType, "ok").Inc()
			}
		}

		// 4. Commit offset ONLY after successful processing (or skipping poison pill)
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
		c.routeToDlq(ctx, msg, fmt.Sprintf("Failed to unmarshal envelope:%v", err))
		return nil // CRITICAL: RETURN NIL TO COMMIT OFFSET and move past the bad message
	}

	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		slog.Error("Failed to begin transactions", "error: ", err)
		return err
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()
	qtx := c.Queries.WithTx(tx)

	// Idempotency check
	if envelope.EventID == "" {
		slog.Warn("Event missing eventId, cannot guarantee idempotency", "offset", msg.Offset)
	} else {
		rowsAffected, err := qtx.InsertProcessedEvent(ctx, envelope.EventID)
		if err != nil {
			slog.Error("Failed top insert processed event", "Error:", err)
			return err
		}

		if rowsAffected == 0 {
			slog.Info("idempotency check: event already processed, skipping", "event_id", envelope.EventID)
			return ErrDuplicateEvent // <-- CHANGED: Return our sentinel error instead of nil
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

		if err != nil {
			slog.Error("failed to update order status", "order_id", event.OrderId, "error", err)
			return err
		}

		if rowsAffect == 0 {
			slog.Warn("ignored stale event: order not in expected state", "order_id", event.OrderId)
			return ErrDuplicateEvent // <-- CHANGED: Return our sentinel error
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
			return ErrDuplicateEvent // <-- CHANGED: Return our sentinel error
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
			return ErrDuplicateEvent // <-- CHANGED: Return our sentinel error
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

		if err != nil {
			slog.Error("failed to update order status", "order_id", event.OrderId, "error", err)
			return err
		}

		if rowsAffected == 0 {
			slog.Warn("Ignoring stale element, expect state is incorrect for", "order_id", event.OrderId)
			return ErrDuplicateEvent // <-- CHANGED: Return our sentinel error
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
			})
		}

		cancelledEvent := OrderCancelledEvent{
			OrderId:          event.OrderId,
			Reason:           event.Reason,
			Items:            eventItems,
			ReleaseInventory: true,
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
