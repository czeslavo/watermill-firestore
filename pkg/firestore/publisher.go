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
	// ProjectID is an ID of a Google Cloud project with Firestore database.
	ProjectID string

	// PubSubRootCollection is a name of a collection which will be used as a root collection for the PubSub.
	// It defaults to `pubsub`.
	PubSubRootCollection string

	// MessagePublishTimeout is a timeout used for a single `Publish` call.
	MessagePublishTimeout time.Duration

	// GoogleClientOpts are options passed directly to firestore client.
	GoogleClientOpts []option.ClientOption
}

func (c *PublisherConfig) setDefaults() {
	if c.MessagePublishTimeout == 0 {
		c.MessagePublishTimeout = time.Second * 60
	}
	if c.PubSubRootCollection == "" {
		c.PubSubRootCollection = pubSubRootCollection
	}
}

type Publisher struct {
	config PublisherConfig
	logger watermill.LoggerAdapter

	client *firestore.Client
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
	ctx, cancel := context.WithTimeout(context.Background(), p.config.MessagePublishTimeout)
	defer cancel()

	subscriptions, err := p.getSubscriptions(ctx, topic)
	if err != nil {
		p.logger.Debug("Failed to get subscriptions for publishing", nil)
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

			if err := p.publishMessage(ctx, topic, sub, firestoreMsg); err != nil {
				p.logger.Error("Failed to publish msg", err, nil)
				return err
			}
			logger.Debug("Published message", nil)
		}
	}

	return nil
}

func (p *Publisher) getSubscriptions(ctx context.Context, topic string) ([]*firestore.DocumentSnapshot, error) {
	return p.client.Collection(p.config.PubSubRootCollection).Doc(topic).Collection(subscriptionsCollection).Documents(ctx).GetAll()
}

func (p *Publisher) publishMessage(ctx context.Context, topic string, sub *firestore.DocumentSnapshot, msg Message) error {
	subCol := p.client.Collection(p.config.PubSubRootCollection).Doc(topic).Collection(sub.Ref.ID)

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
