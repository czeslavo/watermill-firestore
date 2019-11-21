package firestore

import (
	"context"
	"errors"
	"sync"

	"cloud.google.com/go/firestore"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	client, err := firestore.NewClient(context.Background(), config.ProjectID)
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
	logger := s.logger.With(watermill.LogFields{
		"provider":          "firestore",
		"topic":             topic,
		"subscription_name": subscriptionName,
	})

	sub, err := newSubscription(s.client, logger, subscriptionName, topic)
	if err != nil {
		return nil, err
	}

	logger.Info("Subscribed to topic", nil)

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

	if err := s.client.Close(); err != nil {
		s.logger.Error("failed closing firebase client", err, watermill.LogFields{})
		return err
	}

	return nil
}

func (s *Subscriber) SubscribeInitialize(topic string) error {
	ctx := context.Background()
	_, err := s.client.Collection("pubsub").
		Doc(topic).
		Collection("subscriptions").
		Doc(s.config.GenerateSubscriptionName(topic)).Create(ctx, firestoreSubscription{Name: topic})
	if status.Code(err) == codes.AlreadyExists {
		return nil
	} else if err != nil {
		return err
	}

	s.logger.Info("Created subscription", nil)
	return nil
}
