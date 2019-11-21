package firestore

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PublisherConfig struct {
	ProjectID                   string
	SubscriptionsGetTimeout     time.Duration
	SingleMessagePublishTimeout time.Duration
	GoogleClientOpts            []option.ClientOption
}

func (c *PublisherConfig) setDefaults() {
	if c.SubscriptionsGetTimeout == 0 {
		c.SubscriptionsGetTimeout = time.Second * 30
	}
	if c.SingleMessagePublishTimeout == 0 {
		c.SingleMessagePublishTimeout = time.Second * 60
	}
}

type Publisher struct {
	client *firestore.Client
	config PublisherConfig
	logger watermill.LoggerAdapter
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.setDefaults()

	client, err := firestore.NewClient(context.Background(), config.ProjectID, config.GoogleClientOpts...)
	if err != nil {
		return nil, err
	}

	return &Publisher{
		client: client,
		config: config,
		logger: logger,
	}, nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	subscriptions, err := p.getSubscriptions(topic)
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
			logger := logger.With(watermill.LogFields{"subscription": sub.Ref.ID})

			if err := p.publishMessage(topic, sub, firestoreMsg); err != nil {
				p.logger.Error("Failed to publish msg", err, nil)
				return err
			}
			logger.Debug("Published message", nil)
		}
	}

	return nil
}

func (p *Publisher) getSubscriptions(topic string) ([]*firestore.DocumentSnapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.config.SubscriptionsGetTimeout)
	defer cancel()
	return p.client.Collection("pubsub").Doc(topic).Collection("subscriptions").Documents(ctx).GetAll()
}

func (p *Publisher) publishMessage(topic string, sub *firestore.DocumentSnapshot, msg Message) error {
	subCol := p.client.Collection("pubsub").Doc(topic).Collection(sub.Ref.ID)

	ctx, cancel := context.WithTimeout(context.Background(), p.config.SingleMessagePublishTimeout)
	defer cancel()
	_, _, err := subCol.Add(ctx, msg)
	if err != nil {
		return err
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
