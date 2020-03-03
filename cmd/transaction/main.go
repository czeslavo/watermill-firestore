// This example shows how to use transactional publisher in order to
// store data and publish events in one transaction.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/Pallinder/go-randomdata"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	watermillFirestore "github.com/czeslavo/watermill-firestore/pkg/firestore"
)

const projectID = "test"

type User struct {
	Name string `firestore:"name"`
}

type UserAdded struct {
	When time.Time `json:"when"`
	Name string    `json:"name"`
}

type UserStore struct {
	client *firestore.Client
}

func NewUserStore() *UserStore {
	client, err := firestore.NewClient(context.Background(), projectID)
	if err != nil {
		panic(err)
	}

	return &UserStore{
		client: client,
	}
}

func (r *UserStore) Add(u User, t *firestore.Transaction) error {
	if err := t.Create(r.client.Collection("users").NewDoc(), u); err != nil {
		return err
	}

	// 3/4 for success
	if rand.Intn(4) == 1 {
		return errors.New("random error")
	}
	return nil
}

func main() {
	client, err := firestore.NewClient(context.Background(), projectID)
	if err != nil {
		panic(err)
	}

	logger := watermill.NewStdLogger(true, false)

	go addUsers(client, logger)
	subscriber, err := watermillFirestore.NewSubscriber(watermillFirestore.SubscriberConfig{
		ProjectID: projectID,
	}, logger)
	if err != nil {
		panic(err)
	}
	go monitorQueueLength(subscriber, "user_added", logger)

	userAddedCh, err := subscriber.Subscribe(context.Background(), "user_added")
	if err != nil {
		panic(err)
	}
	consume(userAddedCh, logger)
}

func addUsers(client *firestore.Client, logger watermill.LoggerAdapter) {
	publisher, err := watermillFirestore.NewPublisher(watermillFirestore.PublisherConfig{
		ProjectID: projectID,
	}, logger)
	if err != nil {
		panic(err)
	}

	userStore := NewUserStore()
	for {
		if err := client.RunTransaction(context.Background(), func(ctx context.Context, t *firestore.Transaction) error {
			user := User{Name: randomdata.FirstName(randomdata.RandomGender)}
			if err := userStore.Add(user, t); err != nil {
				return err
			}

			payload, err := json.Marshal(&UserAdded{When: time.Now(), Name: user.Name})
			if err != nil {
				return err
			}

			msg := message.NewMessage(watermill.NewShortUUID(), payload)
			if err := publisher.PublishInTransaction("user_added", t, msg); err != nil {
				return err
			}

			return nil
		}); err != nil {
			logger.Debug("Transaction failed", nil)
		}
		<-time.After(time.Second * 5)
	}
}

func consume(ch <-chan *message.Message, logger watermill.LoggerAdapter) {
	for msg := range ch {
		var event UserAdded
		if err := json.Unmarshal(msg.Payload, &event); err != nil {
			panic(err)
		}

		logger.Debug("Received userAdded", watermill.LogFields{"event": event})
		<-time.After(time.Second)
		msg.Ack()
	}
}

func monitorQueueLength(sub *watermillFirestore.Subscriber, topic string, logger watermill.LoggerAdapter) {
	for {
		length, err := sub.QueueLength(topic)
		if err != nil {
			panic(err)
		}
		logger.Debug("Read queue length", watermill.LogFields{"queue_length": length})
		<-time.After(time.Second)
	}
}
