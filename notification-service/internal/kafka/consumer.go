package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"notification-service/internal/store"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader  *kafka.Reader
	queries *store.Queries
}

func NewConsumer(brokerAddr, topic, groupID string, queries *store.Queries) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{brokerAddr},
			Topic:       topic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
		}),
		queries: queries,
	}
}

func (c *Consumer) Start(ctx context.Context) {
	slog.Info("payment consumer starting", "topic", c.reader.Config().Topic, "group", c.reader.Config().GroupID)

	for {
		// 1. Fetch message
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("payment consumer context canceled, stopping read loop", "topic", c.reader.Config().Topic)
				return
			}
			slog.Error("failed to fetch message", "topic", c.reader.Config().Topic, "error", err)
			continue
		}

		// 2. Retry loop
		maxRetries := 3
		var processErr error

		for attempt := 1; attempt <= maxRetries; attempt++ {
			processErr = c.Read(ctx, msg)
			if processErr == nil {
				break
			}

			slog.Warn("retrying payment message processing",
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
			slog.Error("CRITICAL: payment message processing failed after max retries; skipping and committing offset",
				"topic", msg.Topic,
				"offset", msg.Offset,
				"key", string(msg.Key),
				"error", processErr,
			)
		}

		// 3. Commit offset
		commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := c.reader.CommitMessages(commitCtx, msg); err != nil {
			slog.Error("failed to commit payment offset", "topic", msg.Topic, "offset", msg.Offset, "error", err)
		}
		cancel()
	}
}

func (c *Consumer) Read(ctx context.Context, msg kafka.Message) error {

	var envelope EventEnvelope
	slog.Info("Receive event message", "offset", msg.Offset, "key", string(msg.Key))

	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		slog.Error("Failed to unmarshal event envelope", "Error:", err)
		return err
	}

	switch envelope.EventType {
	case "OrderConfirmed":

		var event OrderConfirmedEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			slog.Error("Failed to unmarshal event OrderConfirmedEvent", "error:", err)
			return err
		}

		// in case of order confirmed send a message that your order has been confirmed and is on the way
		_, err := c.queries.CreateNotification(ctx, store.CreateNotificationParams{
			OrderID: event.OrderId,
			Message: "Order Confirmed, on it's way to you",
		})

		if err != nil {
			slog.Error("Failed to create notification for order confirmed event", "error:", err)
			return err
		}

		return nil // nothing to return, but we are expecting an error, so just return nil instead

	case "OrderCancelled":

		var event OrderCancelledEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			slog.Error("Failed to unmarshal event OrderConfirmedEvent", "error:", err)
			return err
		}

		orderCancelledReason := "Your order got cancelled due to " + event.Reason

		_, err := c.queries.CreateNotification(ctx, store.CreateNotificationParams{
			OrderID: event.OrderId,
			Message: orderCancelledReason,
		})

		if err != nil {
			slog.Error("Failed to Create notifications for order cancelled event", "errors:", err)
			return err
		}

		return nil

	}

	return nil

}
