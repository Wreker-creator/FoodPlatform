package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"payment-service/internal/store"
	"strconv"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader   *kafka.Reader
	Queries  *store.Queries
	Producer *Producer
	Pool     *pgxpool.Pool
}

func NewConsumer(brokerAddr, topic, groupID string, queries *store.Queries, producer *Producer, pool *pgxpool.Pool) *Consumer {
	return &Consumer{
		Queries:  queries,
		Producer: producer,
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
		slog.Error("failed to unmarshal event envelope", "error", err)
		return err
	}

	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		slog.Error("Failed to begin transaction", "error", err)
		return err
	}

	defer tx.Rollback(ctx)

	qtx := c.Queries.WithTx(tx)

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

		createdPayment, err := qtx.CreatePayment(ctx, store.CreatePaymentParams{
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

			payment, err := qtx.UpdatePaymentStatus(ctx, store.UpdatePaymentStatusParams{
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

			payloadBytes, err := json.Marshal(paymentRejectedEvent)
			if err != nil {
				slog.Error("failed to marshall paymentFailedEvent")
			}

			if _, err := qtx.InsertOutboxEvent(ctx, store.InsertOutboxEventParams{
				AggregateKey: strconv.Itoa(int(paymentRejectedEvent.OrderId)),
				EventType:    "PaymentFailed",
				Payload:      payloadBytes,
			}); err != nil {
				slog.Error("Failed to insert payment Failed outbox event", "error: ", err)
				return err
			}

			return nil
		}

		updatedPayment, err := qtx.UpdatePaymentStatus(ctx, store.UpdatePaymentStatusParams{
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

		payloadBytes, err := json.Marshal(paymentSucceededEvent)
		if err != nil {
			slog.Error("failed to marshall paymentFailedEvent")
		}

		if _, err := qtx.InsertOutboxEvent(ctx, store.InsertOutboxEventParams{
			AggregateKey: strconv.Itoa(int(paymentSucceededEvent.OrderId)),
			EventType:    "PaymentSucceeded",
			Payload:      payloadBytes,
		}); err != nil {
			slog.Error("Failed to insert payment Failed outbox event", "error: ", err)
			return err
		}

		return nil

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
