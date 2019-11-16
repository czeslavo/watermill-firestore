package firestore

import (
	"context"
	"errors"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type SubscriberConfig struct {
	GenerateSubscriptionName func(topic string) string
	ProjectID                string
}

type Subscriber struct {
	closed  bool
	closing chan struct{}

	client *firestore.Client

	config SubscriberConfig
	logger watermill.LoggerAdapter

	allSubscriptionsWaitingGroup sync.WaitGroup
}

func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	client, err := firestore.NewClient(ctx, config.ProjectID)
	if err != nil {
		return nil, err
	}

	return &Subscriber{
		closed:  false,
		closing: make(chan struct{}),
		client:  client,
		config:  config,
		logger:  logger,
	}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber is closed")
	}

	ctx, cancel := context.WithCancel(ctx)

	subscriptionName := s.config.GenerateSubscriptionName(topic)
	logFields := watermill.LogFields{
		"provider":          "firestore",
		"topic":             topic,
		"subscription_name": subscriptionName,
	}
	s.logger.Info("Subscribing to Firestore topic", logFields)

	sub, err := newSubscription(s.client, s.logger, subscriptionName, topic)
	if err != nil {
		return nil, err
	}

	receiveFinished := make(chan struct{})
	s.allSubscriptionsWaitingGroup.Add(1)
	go func() {
		sub.receive(ctx)
		close(receiveFinished)
	}()

	go func() {
		<-receiveFinished
		close(sub.output)
		s.allSubscriptionsWaitingGroup.Done()
	}()

	go func() {
		<-s.closing
		cancel()
	}()

	return sub.output, nil
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true
	close(s.closing)

	s.allSubscriptionsWaitingGroup.Wait()
	return nil
}
