package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"order-service/internal/store"

	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type Publisher struct {
	Queries *store.Queries
	writer  *kafka.Writer
	Pool    *pgxpool.Pool
}

func NewPublisher(queries *store.Queries, topic, brokerAddr string, pool *pgxpool.Pool) *Publisher {
	return &Publisher{
		Queries: queries,
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokerAddr),
			Topic:                  topic,
			AllowAutoTopicCreation: true,
		},
		Pool: pool,
	}
}

func (p *Publisher) Start(ctx context.Context) {
	slog.Info("Outbox Publisher starting")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {

		case <-ctx.Done():
			slog.Info("Publisher context cancelled, stopping publication loop")
			return

		case <-ticker.C:
			p.processBatch(ctx)
		}
	}

}

func (p *Publisher) processBatch(ctx context.Context) {

	// begin the transaction, we have added the pool because we are trying to make use of
	// FOR UPDATE SKIP LOCKED

	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		slog.Error("Failed to begin pool transcation, processBatch, publisher.go, order-service", "Error:", err)
		return
	}

	// ensure rollback the same way we have added for all the other consumers
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	qtx := p.Queries.WithTx(tx)

	events, err := qtx.GetUnpublishedEvents(ctx)
	if err != nil {
		slog.Error("Failed to fetch unpublished events, publisher.go, order-service", "Error:", err)
		return
	}

	// if no events, just commit and exit the batch
	if len(events) == 0 {
		_ = tx.Commit(ctx)
		return
	}

	// processing the batch
	for _, e := range events {

		uniqueEventId := fmt.Sprintf("order-%d", e.ID)

		err := p.PublishRaw(ctx, e.AggregateKey, e.EventType, e.Payload, uniqueEventId)
		if err != nil {
			slog.Error("Kafka publish failed, halting batch to preserve order", "Error:", err)
			break // stop processing to prevent jumping ahead in queue
		}
		err = qtx.MarkOutboxEventPublished(ctx, e.ID)
		if err != nil {
			slog.Error("Failed to mark outbox event as published, halting the entire batch to preserve order", "Error:", err)
			break
		}
	}

	// COMMIT
	if err := tx.Commit(ctx); err != nil {
		slog.Error("Failed to commit transactions to outbox", "Error:", err)
		// no return here as we have defer rollback declared on top
	}

}

func (p *Publisher) PublishRaw(ctx context.Context, key, eventType string, payload []byte, eventId string) error {
	envelope := EventEnvelope{
		EventType: eventType,
		Payload:   payload,
		EventID:   eventId,
	}

	envelopeBytes, err := json.Marshal(envelope)
	if err != nil {
		slog.Error("Failed to marshal the event envelope in publish raw for order-service", "Error-", err)
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: envelopeBytes,
	})

}
