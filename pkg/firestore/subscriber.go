package firestore

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultPubSubRootCollection = "pubsub"
	defaultTimeout              = time.Second * 30

	subscriptionsCollection = "subscriptions"
)

type SubscriberConfig struct {
	// ProjectID is an ID of a Google Cloud project with Firestore database.
	// It defaults to os.Getenv("FIRESTORE_PROJECT_ID").
	ProjectID string

	// GenerateSubscriptionName should accept topic name and construct a subscription name basing on it.
	// It defaults to topic -> topic + "_sub".
	GenerateSubscriptionName GenerateSubscriptionNameFn

	// PubSubRootCollection is a name of a collection which will be used as a root collection for the PubSub.
	// It defaults to "pubsub".
	PubSubRootCollection string

	// Timeout is used for single Firestore operations.
	// It defaults to 30 seconds.
	Timeout time.Duration

	// GoogleClientOpts are options passed directly to firestore client.
	GoogleClientOpts []option.ClientOption

	// Marshaler marshals message from Watermill to Firestore format and vice versa.
	Marshaler Marshaler
}

func (c *SubscriberConfig) setDefaults() {
	if c.ProjectID == "" {
		c.ProjectID = os.Getenv("FIRESTORE_PROJECT_ID")
	}
	if c.PubSubRootCollection == "" {
		c.PubSubRootCollection = defaultPubSubRootCollection
	}
	if c.Timeout == 0 {
		c.Timeout = defaultTimeout
	}
	if c.GenerateSubscriptionName == nil {
		c.GenerateSubscriptionName = DefaultGenerateSubscriptionName
	}
	if c.Marshaler == nil {
		c.Marshaler = DefaultMarshaler{}
	}
}

type GenerateSubscriptionNameFn func(topicName string) string

func DefaultGenerateSubscriptionName(topicName string) string {
	return topicName + "_sub"
}

type Subscriber struct {
	config SubscriberConfig
	logger watermill.LoggerAdapter

	client *firestore.Client

	closed                       bool
	closing                      chan struct{}
	allSubscriptionsWaitingGroup sync.WaitGroup

	closedMutex sync.Mutex
}

func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	config.setDefaults()

	client, err := firestore.NewClient(context.Background(), config.ProjectID, config.GoogleClientOpts...)
	if err != nil {
		return nil, err
	}

	return &Subscriber{
		closed:  false,
		closing: make(chan struct{}),
		client:  client,
		config:  config,
		logger:  logger.With(watermill.LogFields{"provider": "firestore"}),
	}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	s.closedMutex.Lock()

	if s.closed {
		s.closedMutex.Unlock()
		return nil, errors.New("subscriber is closed")
	}

	s.closedMutex.Unlock()

	ctx, cancel := context.WithCancel(ctx)

	subscriptionName := s.config.GenerateSubscriptionName(topic)
	logger := s.logger.With(watermill.LogFields{
		"topic":        topic,
		"subscription": subscriptionName,
	})

	sub, err := newSubscription(subscriptionName, topic, s.config, s.client, logger, s.closing)
	if err != nil {
		cancel()
		return nil, err
	}

	logger.Debug("Subscribed to topic", nil)

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

func (s *Subscriber) QueueLength(topic string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()
	docs, err := s.subscriptionCollection(topic).Documents(ctx).GetAll()
	if err != nil {
		s.logger.Error("Failed to get queue length", err, watermill.LogFields{"topic": topic})
		return 0, err
	}

	return len(docs), nil
}

func (s *Subscriber) subscriptionCollection(topic string) *firestore.CollectionRef {
	return s.client.Collection(s.config.PubSubRootCollection).Doc(topic).Collection(s.config.GenerateSubscriptionName(topic))
}

func (s *Subscriber) Close() error {
	s.closedMutex.Lock()
	defer s.closedMutex.Unlock()

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
	return createFirestoreSubscriptionIfNotExists(s.client, s.config.PubSubRootCollection, topic, s.config.GenerateSubscriptionName(topic), s.logger, s.config.Timeout)
}

func createFirestoreSubscriptionIfNotExists(client *firestore.Client, rootCollection, topic, subscription string, logger watermill.LoggerAdapter, timeout time.Duration) error {
	logger = logger.With(watermill.LogFields{"topic": topic})

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err := client.Collection(rootCollection).Doc(topic).Collection(subscriptionsCollection).Doc(subscription).Create(ctx, struct{}{})
	if status.Code(err) == codes.AlreadyExists {
		logger.Trace("Subscription already exists", nil)
		return nil
	} else if err != nil {
		logger.Error("Couldn't create subscription", err, nil)
		return err
	}

	logger.Debug("Created subscription", nil)
	return nil
}
