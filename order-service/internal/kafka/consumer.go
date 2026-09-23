package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"order-service/internal/store"
	"strconv"

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

	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		slog.Error("Failed to begin transactions", "error: ", err)
		return err
	}

	defer tx.Rollback(ctx)
	qtx := c.Queries.WithTx(tx)

	switch envelope.EventType {

	case "InventoryReserved":
		var event InventoryReservedEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			slog.Error("failed to unmarshal InventoryReservedEvent", "error", err)
			return err
		}
		if err := qtx.UpdateOrderStatus(ctx, store.UpdateOrderStatusParams{
			ID:     event.OrderId,
			Status: "AWAITING_PAYMENT",
		}); err != nil {
			slog.Error("failed to update order status", "error", err)
			return err
		}

		return nil

	case "InventoryRejected":
		var event InventoryRejectedEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			slog.Error("failed to unmarshal InventoryRejectedEvent", "error", err)
			return err
		}
		if err := qtx.UpdateOrderStatus(ctx, store.UpdateOrderStatusParams{
			ID:     event.OrderId,
			Status: "CANCELLED",
		}); err != nil {
			slog.Error("failed to update order status", "error", err)
			return err
		}

		cancelledEvent := OrderCancelledEvent{
			OrderId:          event.OrderId,
			Reason:           event.Reason,
			ReleaseInventory: false,
		}

		payloadBytes, err := json.Marshal(cancelledEvent)
		if err != nil {
			slog.Error("Failed to marhsal the event", "error: ", err)
			return err
		}

		_, err = qtx.InsertOutboxEvent(ctx, store.InsertOutboxEventParams{
			AggregateKey: strconv.Itoa(int(cancelledEvent.OrderId)),
			EventType:    "OrderCancelled",
			Payload:      payloadBytes,
		})

		if err != nil {
			slog.Error("failed to insert outbox event", "error", err)
		}

		if err := tx.Commit(ctx); err != nil {
			slog.Error("Failed to commit transaction for order-service in consumer.go", "error: ", err)
			return err
		}

		return err

	case "PaymentSucceeded":
		var event PaymentSucceededEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			slog.Error("failed to unmarshal PaymentSucceededEvent", "error", err)
			return err
		}

		if err := qtx.UpdateOrderStatus(ctx, store.UpdateOrderStatusParams{
			ID:     event.OrderId,
			Status: "CONFIRMED",
		}); err != nil {
			slog.Error("failed to update order status", "error", err)
			return err
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
		}

		if err := tx.Commit(ctx); err != nil {
			slog.Error("Failed to commit transaction for order-service in consumer.go", "error: ", err)
			return err
		}

		return err

	case "PaymentFailed":

		var event PaymentFailedEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			slog.Error("failed to unmarshal PaymentFailedEvent", "error", err)
			return err
		}

		if err := qtx.UpdateOrderStatus(ctx, store.UpdateOrderStatusParams{
			ID:     event.OrderId,
			Status: "CANCELLED",
		}); err != nil {
			slog.Error("failed to update order status", "error", err)
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
		}

		if err := tx.Commit(ctx); err != nil {
			slog.Error("Failed to commit transaction for order-service in consumer.go", "error: ", err)
			return err
		}

		return err

	default:
		slog.Warn("unknown event type", "event_type", envelope.EventType)
	}

	return nil

}
