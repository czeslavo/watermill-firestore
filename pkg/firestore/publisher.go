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

	p.logger.Debug("Publishing", watermill.LogFields{"subscriptions_count": len(subscriptions), "topic": topic})

	for _, message := range messages {
		firestoreMsg := firestoreMessage{
			UUID:     message.UUID,
			Payload:  message.Payload,
			Metadata: make(map[string]interface{}),
		}
		for k, v := range message.Metadata {
			firestoreMsg.Metadata[k] = v
		}

		for _, sub := range subscriptions {
			subCol := p.client.Collection("pubsub").Doc(topic).Collection(sub.Ref.ID)

			ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
			_, _, err = subCol.Add(ctx, firestoreMsg)
			if err != nil {
				p.logger.Error("failed to send msg", err, watermill.LogFields{})
				return err
			}
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
