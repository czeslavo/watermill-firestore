package firestore

import (
	"context"

	"cloud.google.com/go/firestore"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type firestoreDocCreator interface {
	Create(doc *firestore.DocumentRef, data interface{}) (*firestore.WriteResult, error)
}

type transactionalDocCreator struct {
	*firestore.Transaction
}

type docCreator struct {
}

func (c *docCreator) Create(doc *firestore.DocumentRef, data interface{}) (*firestore.WriteResult, error) {
	return doc.Create(context.Background(), data)
}

type TransactionalPublisher struct {
	client  *firestore.Client
	creator firestoreDocCreator
	logger  watermill.LoggerAdapter
}

func NewTransactionalPublisher(config PublisherConfig, creator firestoreDocCreator, logger watermill.LoggerAdapter) (*TransactionalPublisher, error) {
	client, err := firestore.NewClient(context.Background(), config.ProjectID)
	if err != nil {
		return nil, err
	}

	return &TransactionalPublisher{
		client:  client,
		creator: creator,
		logger:  logger,
	}, nil
}

func (p *TransactionalPublisher) Publish(topic string, messages ...*message.Message) error {
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

			docRef := p.client.Collection("pubsub").Doc(topic).Collection(sub.Ref.ID).NewDoc()

			_, err = p.creator.Create(docRef, firestoreMsg)
			if err != nil {
				p.logger.Error("Failed to send msg", err, watermill.LogFields{})
				return err
			}

			logger.Debug("Published message", nil)
		}
	}

	return nil
}

func (p *TransactionalPublisher) Close() error {
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
