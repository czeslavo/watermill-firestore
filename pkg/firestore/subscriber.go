package firestore

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"google.golang.org/api/iterator"
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
		s.handleAddedEvent(e.Doc)
	}
}

type firestoreMessage struct {
	UUID     string                 `firestore:"uuid"`
	Metadata map[string]interface{} `firestore:"metadata"`
	Payload  []byte                 `firestore:"payload"`
}

func (s *subscription) handleAddedEvent(doc *firestore.DocumentSnapshot) {
	ctx := context.Background()

	// delete with precondition that the document exists
	// when the precondition fails, it returns error
	_, err := doc.Ref.Delete(ctx, firestore.Exists)
	if err != nil {
		// we shouldn't handle this message since it was already handleded by someone
		s.logger.Info("deleting failed", watermill.LogFields{"error": err})
		return
	}

	msg := firestoreMessage{}
	if err := doc.DataTo(&msg); err != nil {
		panic(err)
	}
	s.logger.Info("Handling", watermill.LogFields{"uuid": msg.UUID})

	payload, err := json.Marshal(doc.Data()["uuid"])
	if err != nil {
		panic(err)
	}
	s.output <- message.NewMessage("uuid", payload)
}

func (p *Subscriber) Close() error {
	return nil
}
