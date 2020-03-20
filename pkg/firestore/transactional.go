package firestore

import (
	"cloud.google.com/go/firestore"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type TransactionalPublisher struct {
	publisher *Publisher
	tx        *firestore.Transaction
}

func NewTransactionalPublisher(config PublisherConfig, tx *firestore.Transaction, logger watermill.LoggerAdapter) (*TransactionalPublisher, error) {
	publisher, err := NewPublisher(config, logger)
	if err != nil {
		return nil, err
	}

	return &TransactionalPublisher{publisher, tx}, nil
}

func (p *TransactionalPublisher) Publish(topic string, messages ...*message.Message) error {
	return p.publisher.PublishInTransaction(topic, p.tx, messages...)
}
