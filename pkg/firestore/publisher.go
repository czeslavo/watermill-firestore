package firestore

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PublisherConfig struct {
	ProjectID string
}

type Publisher struct {
	client *firestore.Client
	logger watermill.LoggerAdapter
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	client, err := firestore.NewClient(context.Background(), config.ProjectID)
	if err != nil {
		return nil, err
	}

	return &Publisher{
		client: client,
		logger: logger,
	}, nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	subscriptions, err := p.client.Collection("pubsub").Doc(topic).Collection("subscriptions").Documents(context.Background()).GetAll()
	if err != nil {
		return err
	}

	logger := p.logger.With(watermill.LogFields{"subscriptions_count": len(subscriptions), "topic": topic})

	logger.Debug("Publishing", nil)

	for _, msg := range messages {
		firestoreMsg := Message{
			UUID:     msg.UUID,
			Payload:  msg.Payload,
			Metadata: make(map[string]interface{}),
		}
		for k, v := range msg.Metadata {
			firestoreMsg.Metadata[k] = v
		}

		logger := logger.With(watermill.LogFields{"message_uuid": msg.UUID})

		for _, sub := range subscriptions {
			logger := logger.With(watermill.LogFields{"collection": sub.Ref.ID})

			subCol := p.client.Collection("pubsub").Doc(topic).Collection(sub.Ref.ID)

			ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
			_, _, err = subCol.Add(ctx, firestoreMsg)
			if err != nil {
				p.logger.Error("Failed to send msg", err, watermill.LogFields{})
				return err
			}

			logger.Debug("Published message", nil)
		}
	}

	return nil
}

func (p *Publisher) Close() error {
	if err := p.client.Close(); err != nil {
		if status.Code(err) == codes.Canceled {
			// client is already closed
			return nil
		}

		p.logger.Error("closing client failed", err, watermill.LogFields{})
		return err
	}

	return nil
}
