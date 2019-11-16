package firestore

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type subscription struct {
	name   string
	topic  string
	logger watermill.LoggerAdapter
	client *firestore.Client

	output chan *message.Message
}
type firestoreSubscription struct {
	Name string `firestore:"name"`
}

func newSubscription(client *firestore.Client, logger watermill.LoggerAdapter, name, topic string) (*subscription, error) {
	s := &subscription{
		name:   name,
		topic:  topic,
		logger: logger,
		client: client,
		output: make(chan *message.Message),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	if err := s.createFirestoreSubIfNotExist(ctx, name, topic); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *subscription) createFirestoreSubIfNotExist(ctx context.Context, name, topic string) error {
	return s.client.RunTransaction(ctx, func(ctx context.Context, t *firestore.Transaction) error {
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
			s.logger.Info("Subscription already exists", watermill.LogFields{})
		}

		return nil
	})
}

func (s *subscription) receive(ctx context.Context) {
	subscriptionSnapshots := s.client.
		Collection("pubsub").
		Doc(s.topic).
		Collection(s.name).
		Query.
		Snapshots(ctx)
	defer subscriptionSnapshots.Stop()

	for {
		subscriptionSnapshot, err := subscriptionSnapshots.Next()
		if err == iterator.Done {
			s.logger.Debug("Listening on subscription done", watermill.LogFields{})
			break
		} else if status.Code(err) == codes.Canceled {
			s.logger.Debug("Receive context canceled", watermill.LogFields{})
			break
		} else if err != nil {
			s.logger.Error("Error receiving", err, watermill.LogFields{})
			break
		}

		if subscriptionSnapshot.Size == 0 {
			continue
		}

		for _, e := range onlyAddedEvents(subscriptionSnapshot.Changes) {
			s.handleAddedEvent(ctx, e.Doc)
		}
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

func (s *subscription) handleAddedEvent(ctx context.Context, doc *firestore.DocumentSnapshot) {
	// delete with precondition that the document exists
	// when the precondition fails, it returns error
	// todo: should we update only to mark that we're processing it and
	// avoid lost messages in case of error/power outage during processing?
	_, err := doc.Ref.Delete(ctx, firestore.Exists)
	if err != nil {
		s.logger.Debug("Message deleted meanwhile, skipping", watermill.LogFields{})
		// we shouldn't handle this message since it was already handled by someone
		return
	}

	fsMsg := firestoreMessage{}
	if err := doc.DataTo(&fsMsg); err != nil {
		panic(err)
	}

	republish := func() {
		ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
		_, err = doc.Ref.Create(ctx, doc.Data())
		if err != nil {
			panic(err)
		}
	}

	msg := message.NewMessage(fsMsg.UUID, fsMsg.Payload)
	select {
	case <-ctx.Done():
		republish()
	case s.output <- msg:
		// message consumed, wait for ack/nack
	}

	select {
	case <-msg.Acked():
		s.logger.Debug("Message acked", watermill.LogFields{})
		return
	case <-msg.Nacked():
		s.logger.Debug("Message nacked, republishing", watermill.LogFields{})
		republish()
	case <-ctx.Done():
		s.logger.Debug("Context done, republishing", watermill.LogFields{})
		republish()
	}
}
