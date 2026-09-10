package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"inventory-service/internal/store"
	"log/slog"

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
			rowsAffected, err := c.Queries.DecrementInventory(ctx, store.DecrementInventoryParams{
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
				err := c.Queries.IncrementInventory(ctx, store.IncrementInventoryParams{
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

			return c.Producer.PublishEvent(ctx, string(msg.Key), "InventoryRejected", rejectedEvent)
		}

		reservedEvent := InventoryReservedEvent{
			OrderId: event.OrderId,
			Items:   event.Items,
		}

		return c.Producer.PublishEvent(ctx, string(msg.Key), "InventoryReserved", reservedEvent)

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
			if err := c.Queries.IncrementInventory(ctx, store.IncrementInventoryParams{
				ProductID:         item.ProductId,
				AvailableQuantity: item.Quantity,
			}); err != nil {
				slog.Error("failed to release inventory", "error", err)
				return err
			}
		}

		// doesnt publish anything because the order is cancelled and inventory is released, so just return nil
		return nil

	default:
		slog.Warn("unhandled event type", "event_type", envelope.EventType)
	}
	return nil
}
