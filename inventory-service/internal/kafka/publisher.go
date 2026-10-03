package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"inventory-service/internal/store"
	"log/slog"

	"time"

	"github.com/segmentio/kafka-go"
)

type Publisher struct {
	Queries *store.Queries
	writer  *kafka.Writer
}

func NewPublisher(queries *store.Queries, topic, brokerAddr string) *Publisher {
	return &Publisher{
		Queries: queries,
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokerAddr),
			Topic:                  topic,
			AllowAutoTopicCreation: true,
		},
	}
}

func (p *Publisher) Start(ctx context.Context) {
	slog.Info("Outbox Publisher starting")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("publisher context canceled, stopping publication loop")
			return
		case <-ticker.C:
			events, err := p.Queries.GetUnpublishedOutboxEvents(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				slog.Error("Failed to fetch unpublished outbox events", "Error: ", err)
				continue
			}
			for _, e := range events {
				if err := p.PublishRaw(ctx, e.AggregateKey, e.EventType, e.Payload); err != nil {
					continue
				}
				if err := p.Queries.MarkOutboxEventPublished(ctx, e.ID); err != nil {
					slog.Error("Failed to mark event as published", "Error: ", err, "id: ", e.ID)
					continue
				}
			}
		}
	}
}

func (p *Publisher) PublishRaw(ctx context.Context, key, eventType string, payload []byte) error {
	envelope := EventEnvelope{
		EventType: eventType,
		Payload:   payload,
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
