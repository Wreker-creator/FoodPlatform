package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"notification-service/internal/store"
	"notification-service/internal/telemetry"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

var (
	ErrDuplicateEvent = errors.New("duplicate or stale event")
	ErrPoisonPill     = errors.New("poison pill: unrecoverable message format")
)

type Consumer struct {
	reader    *kafka.Reader
	queries   *store.Queries
	DLQWriter *kafka.Writer
	Pool      *pgxpool.Pool
}

func NewConsumer(brokerAddr, topic, groupID string, queries *store.Queries, dlqwriter *kafka.Writer, pool *pgxpool.Pool) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{brokerAddr},
			Topic:       topic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
		}),
		queries:   queries,
		DLQWriter: dlqwriter,
		Pool:      pool,
	}
}

func (c *Consumer) Start(ctx context.Context) {
	topic := c.reader.Config().Topic
	groupId := c.reader.Config().GroupID
	slog.Info("notification consumer starting", "topic", topic, "group", groupId)

	// background concurrent ticker for consumer lag metric
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stats := c.reader.Stats()
				telemetry.ConsumerLag.WithLabelValues(topic, groupId).Set(float64(stats.Lag))
			}
		}
	}()

	for {
		// 1. Fetch message WITHOUT committing offset
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("notification consumer context canceled, stopping read loop", "topic", topic)
				return
			}
			slog.Error("failed to fetch message", "topic", topic, "error", err)
			continue
		}

		start := time.Now()

		var eventEnvelope EventEnvelope
		_ = json.Unmarshal(msg.Value, &eventEnvelope)
		eventType := eventEnvelope.EventType
		if eventType == "" {
			eventType = "unknown"
		}

		// 2. Retry loop
		maxRetries := 3
		var processErr error

		for attempt := 1; attempt <= maxRetries; attempt++ {
			processErr = c.Read(ctx, msg)

			// --- FIXED: Break immediately on success, duplicate, OR unmarshal poison pill ---
			if processErr == nil || errors.Is(processErr, ErrDuplicateEvent) || errors.Is(processErr, ErrPoisonPill) {
				break
			}

			// --- FIXED: Added 3rd label "retry" to prevent runtime cardinality panic ---
			telemetry.EventsProcessedTotal.WithLabelValues(topic, eventType, "retry").Inc()

			slog.Warn("retrying notification message processing",
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

		// Record event processing duration
		duration := time.Since(start).Seconds()
		telemetry.EventProcessingDuration.WithLabelValues(topic).Observe(duration)

		// --- FIXED: Cleanly branch across PoisonPill, Exhausted Retries, Duplicate, and OK ---
		switch {
		case errors.Is(processErr, ErrPoisonPill):
			// Already routed to DLQ inside Read(); just record the "dlq" status metric
			telemetry.EventsProcessedTotal.WithLabelValues(topic, eventType, "dlq").Inc()

		case processErr != nil && !errors.Is(processErr, ErrDuplicateEvent):
			slog.Error("CRITICAL: notification message processing failed after max retries; skipping and committing offset",
				"topic", msg.Topic,
				"offset", msg.Offset,
				"key", string(msg.Key),
				"error", processErr,
			)
			c.routeToDlq(ctx, msg, fmt.Sprintf("Exhausted %d retries: %v", maxRetries, processErr))
			telemetry.EventsProcessedTotal.WithLabelValues(topic, eventType, "dlq").Inc()

		case errors.Is(processErr, ErrDuplicateEvent):
			telemetry.EventsProcessedTotal.WithLabelValues(topic, eventType, "duplicate").Inc()

		default:
			telemetry.EventsProcessedTotal.WithLabelValues(topic, eventType, "ok").Inc()
		}

		// 3. Commit offset
		commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := c.reader.CommitMessages(commitCtx, msg); err != nil {
			slog.Error("failed to commit notification offset", "topic", msg.Topic, "offset", msg.Offset, "error", err)
		}
		cancel()
	}
}

func (c *Consumer) Read(ctx context.Context, msg kafka.Message) error {
	var envelope EventEnvelope
	slog.Info("received event message", "offset", msg.Offset, "key", string(msg.Key))

	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		c.routeToDlq(ctx, msg, fmt.Sprintf("Failed to unmarshal event envelope: %v", err))
		// --- FIXED: Return ErrPoisonPill instead of nil so metrics don't count this as "ok" ---
		return ErrPoisonPill
	}

	if envelope.EventType != "OrderConfirmed" && envelope.EventType != "OrderCancelled" {
		return nil
	}

	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		slog.Error("Failed to beign transactions", "error:", err)
		return err
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()
	qtx := c.queries.WithTx(tx)

	// idempotency check
	if envelope.EventID == "" {
		slog.Warn("Event missing eventId, cannot gurantee idempotency", "offset", msg.Offset)
	} else {
		rowsAffected, err := qtx.InsertProcessedEvent(ctx, envelope.EventID)
		if err != nil {
			slog.Error("Failed to insert processed event", "error:", err)
			return err
		}

		if rowsAffected == 0 {
			slog.Info("Idempotency check: event already processed, skipping", "event_id", envelope.EventID)
			return ErrDuplicateEvent
		}

	}

	switch envelope.EventType {
	case "OrderConfirmed":
		var event OrderConfirmedEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			slog.Error("Failed to unmarshal OrderConfirmedEvent", "error", err)
			return err
		}

		_, err := qtx.CreateNotification(ctx, store.CreateNotificationParams{
			OrderID: event.OrderId,
			Message: "Order Confirmed, on it's way to you",
		})

		if err != nil {
			slog.Error("Failed to create notification for OrderConfirmed", "error", err)
			return err
		}

		if err := tx.Commit(ctx); err != nil {
			slog.Error("Failed to commit notification for order confirmation", "Error:", err)
			return err
		}

		return nil

	case "OrderCancelled":
		var event OrderCancelledEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			// --- FIXED: Corrected log message from OrderConfirmedEvent to OrderCancelledEvent ---
			slog.Error("Failed to unmarshal OrderCancelledEvent", "error", err)
			return err
		}

		orderCancelledReason := "Your order got cancelled due to " + event.Reason

		_, err := qtx.CreateNotification(ctx, store.CreateNotificationParams{
			OrderID: event.OrderId,
			Message: orderCancelledReason,
		})

		if err != nil {
			slog.Error("Failed to create notification for OrderCancelled", "error", err)
			return err
		}

		if err := tx.Commit(ctx); err != nil {
			slog.Error("Failed to commit notification for order cancelled", "Error:", err)
			return err
		}

		return nil
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
		slog.Error("CRITICAL: failed to write to DLQ", "error", err)
	} else {
		// Increment DLQMessagesTotal right where the DLQ write succeeds so both
		// corrupt JSON envelopes and exhausted retries are counted once.
		telemetry.DLQMessagesTotal.WithLabelValues(msg.Topic).Inc()
		slog.Info("routed poison pill to DLQ", "key", string(msg.Key), "reason", reason)
	}
}
