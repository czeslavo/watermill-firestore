package firestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SubscriberConfig struct {
	GenerateSubscriptionName func(topic string) string
	ProjectID                string
}

type Subscriber struct {
	closed bool

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
		closed: false,
		client: client,
		config: config,
		logger: logger,
	}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber is closed")
	}

	subscriptionName := s.config.GenerateSubscriptionName(topic)
	logFields := watermill.LogFields{
		"provider":          "firestore",
		"topic":             topic,
		"subscription_name": subscriptionName,
	}
	s.logger.Info("Subscribing to Firestore topic", logFields)

	sub, err := s.newSubscription(subscriptionName, topic)
	if err != nil {
		panic(err)
	}
	go sub.receive()

	return sub.output, nil
}

type firestoreSubscription struct {
	Name string `firestore:"name"`
}

func (s *Subscriber) newSubscription(name, topic string) (subscription, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	transErr := s.client.RunTransaction(ctx, func(ctx context.Context, t *firestore.Transaction) error {
		q := s.client.
			Collection("pubsub").
			Doc(topic).
			Collection("subscriptions").Query.
			Where("name", "==", name)

		subDocs, err := t.Documents(q).GetAll()
		if err != nil {
			return err
		}

		if len(subDocs) <= 0 {
			s.logger.Info("Creating subscription", watermill.LogFields{})
			if err := t.Create(
				s.client.Collection("pubsub").
					Doc(topic).
					Collection("subscriptions").
					NewDoc(),
				firestoreSubscription{Name: name}); err != nil {
				return err
			}
		} else {
			s.logger.Info("subscription already exists", watermill.LogFields{})
		}

		return nil
	})
	if transErr != nil {
		return subscription{}, transErr
	}
	logger := s.logger.With(watermill.LogFields{
		"subscription_name": name,
	})
	output := make(chan *message.Message)
	return subscription{
		name:   name,
		topic:  topic,
		logger: logger,
		client: s.client,
		output: output,
	}, nil
}

type subscription struct {
	name   string
	topic  string
	logger watermill.LoggerAdapter
	client *firestore.Client

	output chan *message.Message
}

func (s *subscription) receive() {
	ctx := context.Background()

	col := s.client.Collection("pubsub").Doc(s.topic).Collection(s.name)
	subscriptionSnapshots := col.Query.Snapshots(ctx)
	defer subscriptionSnapshots.Stop()

	for {
		subscriptionSnapshot, err := subscriptionSnapshots.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			break
		}

		if subscriptionSnapshot.Size == 0 {
			continue
		}

		s.handleAddedEvents(onlyAddedEvents(subscriptionSnapshot.Changes))
	}
}

func onlyAddedEvents(changes []firestore.DocumentChange) (added []firestore.DocumentChange) {
	for _, ch := range changes {
		if ch.Kind == firestore.DocumentAdded {
			added = append(added, ch)
		}
	}
	return
}

func (s *subscription) handleAddedEvents(added []firestore.DocumentChange) {
	for _, e := range added {
		s.handleAddedEvent(e.Doc.Ref)
	}
}

func (s *subscription) handleAddedEvent(doc *firestore.DocumentRef) {
	ctx := context.Background()

	if err := s.client.RunTransaction(ctx, func(ctx context.Context, t *firestore.Transaction) error {
		d, err := t.Get(doc)
		if err != nil && status.Code(err) == codes.NotFound {
			fmt.Println("snapshot doesn't exist")
			return nil
		} else if err != nil {
			fmt.Println("error getting snapshot")
			return err
		}

		// consume if still exists
		if err := t.Delete(d.Ref, firestore.Exists); err != nil {
			s.logger.Error("deleting failed", err, watermill.LogFields{})
		}

		payload, err := json.Marshal(d.Data())
		if err != nil {
			panic(err)
		}
		s.output <- message.NewMessage("uuid", payload)

		return nil
	}, firestore.MaxAttempts(1)); err != nil {
		fmt.Printf("Transaction error: %v\n", err)
	}

}

func (p *Subscriber) Close() error {
	return nil
}
