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
			Brokers: []string{brokerAddr},
			Topic:   topic,
			GroupID: groupID,
		}),
	}
}

// this function will keep running for eternity unless it gets stopped via context gracefully
func (c *Consumer) Start(ctx context.Context) {
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
	var event OrderCreatedEvent

	err := json.Unmarshal(msg.Value, &event)
	if err != nil {
		return err
	}

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

		return c.Producer.Publish(ctx, string(msg.Key), rejectedEvent)
	}

	reservedEvent := InventoryReservedEvent{
		OrderId: event.OrderId,
		Items:   event.Items,
	}

	return c.Producer.Publish(ctx, string(msg.Key), reservedEvent)

}
