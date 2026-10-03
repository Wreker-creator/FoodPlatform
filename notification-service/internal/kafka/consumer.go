package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"notification-service/internal/store"

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

	slog.Info("Consumer starting", "topic", c.reader.Config().Topic, "group", c.reader.Config().GroupID)
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			slog.Error("Failed to read messages", "error:", err)
			continue
		}

		if err := c.Read(ctx, msg); err != nil {
			slog.Error("Failed to process message", "error:", err, "offset", msg.Offset)
			// delibrately not failing this - one bad message should not kill the whole consumer
		}
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
