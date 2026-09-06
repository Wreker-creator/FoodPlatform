package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"payment-service/internal/store"

	"github.com/jackc/pgx/v5/pgtype"
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
		slog.Error("failed to unmarshal event envelope", "error", err)
		return err
	}

	switch envelope.EventType {

	case "InventoryReserved":
		var event InventoryReservedEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			slog.Error("failed to unmarshal event InventoryReserved", "error", err)
			return err
		}

		var totalAmount float64

		for _, item := range event.Items {
			totalAmount += float64(item.Quantity) * item.UnitPrice
		}

		totalAmountNumeric, err := toNumeric(totalAmount)
		if err != nil {
			slog.Error("Faile to convert unit price", "error", err)
			return err
		}

		createdPayment, err := c.Queries.CreatePayment(ctx, store.CreatePaymentParams{
			OrderID:     event.OrderId,
			Status:      "PENDING",
			Amount:      totalAmountNumeric,
			PaymentMode: "CARD",
		})

		if err != nil {
			slog.Error("failed to create payment", "error", err)
			return err
		}

		if totalAmount > 1000 {

			payment, err := c.Queries.UpdatePaymentStatus(ctx, store.UpdatePaymentStatusParams{
				ID:     createdPayment.ID,
				Status: "FAILED",
			})

			if err != nil {
				slog.Error("failed to update payment status", "error", err)
				return err
			}

			paymentRejectedEvent := PaymentFailedEvent{
				OrderId:   event.OrderId,
				PaymentId: payment.ID,
				Amount:    totalAmount,
				Reason:    "Amount too high",
			}

			return c.Producer.PublishEvent(ctx, string(msg.Key), "PaymentFailed", paymentRejectedEvent)
		}

		updatedPayment, err := c.Queries.UpdatePaymentStatus(ctx, store.UpdatePaymentStatusParams{
			ID:     createdPayment.ID,
			Status: "SUCCEEDED",
		})
		if err != nil {
			slog.Error("failed to update payment status", "error", err)
			return err
		}

		paymentSucceededEvent := PaymentSucceededEvent{
			OrderId:   event.OrderId,
			PaymentId: updatedPayment.ID,
			Amount:    totalAmount,
		}

		return c.Producer.PublishEvent(ctx, string(msg.Key), "PaymentSucceeded", paymentSucceededEvent)

	default:
		slog.Warn("unknown event type", "event_type", envelope.EventType)
	}

	return nil

}

func toNumeric(value float64) (pgtype.Numeric, error) {
	var n pgtype.Numeric
	err := n.Scan(fmt.Sprintf("%.2f", value))
	return n, err
}
