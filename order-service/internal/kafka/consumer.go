package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"order-service/internal/store"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader   *kafka.Reader
	Queries  *store.Queries
	Producer *Producer
}

func NewConsumer(brokerAddr, topic, groupID string, queries *store.Queries, producer *Producer) *Consumer {
	return &Consumer{
		Queries:  queries,
		Producer: producer,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{brokerAddr},
			Topic:       topic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
		}),
	}
}

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
	slog.Info("received event envelope", "offset", msg.Offset, "key", string(msg.Key))
	err := json.Unmarshal(msg.Value, &envelope)
	if err != nil {
		return err
	}

	switch envelope.EventType {

	case "InventoryReserved":
		// if inventory is reserved, we can
		var event InventoryReservedEvent
		err := json.Unmarshal(envelope.Payload, &event)
		if err != nil {
			slog.Info("Failed to extract data from the json recieved, InventoryReservedEvent", "error :", err)
			return err
		}

		if err := c.Queries.UpdateOrderStatus(ctx, store.UpdateOrderStatusParams{
			ID:     event.OrderId,
			Status: "AWAITING_PAYMENT",
		}); err != nil {
			slog.Error("Failed to update order status", "error", err)
			return err
		}

		return c.Producer.Publish(ctx, string(msg.Key), event)

	case "InventoryRejected":
		var event InventoryRejectedEvent
		err := json.Unmarshal(envelope.Payload, &event)
		if err != nil {
			slog.Info("Failed to extract data from the json recieved, InventoryRejectedEvent", "error :", err)
			return err
		}

		if err := c.Queries.UpdateOrderStatus(ctx, store.UpdateOrderStatusParams{
			ID:     event.OrderId,
			Status: "CANCELLED",
		}); err != nil {
			slog.Error("Failed to update order status", "error", err)
			return err
		}

		return c.Producer.Publish(ctx, string(msg.Key), event)

	// case "PaymentSucceeded":
	// case "PaymentFailed":
	default:
		slog.Warn("unknown event type", "event_type", envelope.EventType)
	}

	return nil

}
