package kafka

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokerAddr, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokerAddr),
			Topic:                  topic,
			AllowAutoTopicCreation: true,
		},
	}
}

// func (p *Producer) Publish(ctx context.Context, key string, event any) error {
// 	payload, err := json.Marshal(event)
// 	if err != nil {
// 		return err
// 	}
// 	return p.writer.WriteMessages(ctx, kafka.Message{
// 		Key:   []byte(key),
// 		Value: []byte(payload),
// 	})
// }

func (p *Producer) PublishEvent(ctx context.Context, key string, eventType string, event any) error {

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	envelope := EventEnvelope{
		EventType: eventType,
		Payload:   payload,
	}

	envelopeBytes, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: envelopeBytes,
	})

}
