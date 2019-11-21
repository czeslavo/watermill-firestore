package firestore

import (
	"context"

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

	// ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	// defer cancel()
	if err := s.createFirestoreSubIfNotExist(context.Background(), name, topic); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *subscription) createFirestoreSubIfNotExist(ctx context.Context, name, topic string) error {
	_, err := s.client.Collection("pubsub").
		Doc(topic).
		Collection("subscriptions").
		Doc(name).Create(ctx, firestoreSubscription{Name: name})
	if err != nil {
		s.logger.Debug("Error creating subscription (possibly already exist)", nil)
		return nil
	}

	s.logger.Info("Created subscription", nil)
	return nil
}

func (s *subscription) messagesQuery() *firestore.CollectionRef {
	return s.client.Collection("pubsub").Doc(s.topic).Collection(s.name)
}

func (s *subscription) receive(ctx context.Context) {
	docs := s.messagesQuery().Documents(ctx)

	logger := s.logger.With(watermill.LogFields{"topic": s.topic, "collection": s.name})
	logger.Debug("Reading messages with iterator", nil)

	for {
		doc, err := docs.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			logger.Error("Failed to get event from iterator", err, nil)
			break
		}

		logger.Trace("Handling doc from iterator", nil)
		s.handleAddedEvent(ctx, doc, logger)
	}

	logger.Debug("Reading messages from sub", nil)

	subscriptionSnapshots := s.messagesQuery().Query.Snapshots(ctx)
	defer subscriptionSnapshots.Stop()

	for {
		subscriptionSnapshot, err := subscriptionSnapshots.Next()
		if err == iterator.Done {
			logger.Debug("Listening on subscription done", nil)
			break
		} else if status.Code(err) == codes.Canceled {
			logger.Debug("Receive context canceled", nil)
			break
		} else if err != nil {
			logger.Error("Error receiving", err, nil)
			break
		}

		if subscriptionSnapshot.Size == 0 {
			continue
		}

		for _, e := range onlyAddedEvents(subscriptionSnapshot.Changes) {
			s.handleAddedEvent(ctx, e.Doc, logger)
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

func (s *subscription) handleAddedEvent(ctx context.Context, doc *firestore.DocumentSnapshot, logger watermill.LoggerAdapter) {
	fsMsg := Message{}
	if err := doc.DataTo(&fsMsg); err != nil {
		panic(err)
	}

	logger = logger.With(watermill.LogFields{"message_uuid": fsMsg.UUID})

	msg := message.NewMessage(fsMsg.UUID, fsMsg.Payload)
	for k, v := range fsMsg.Metadata {
		msg.Metadata.Set(k, v.(string))
	}
	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	select {
	case <-ctx.Done():
		return
	case s.output <- msg:
		// message consumed, wait for ack/nack
	}

	select {
	case <-msg.Acked():
		_, err := doc.Ref.Delete(ctx, firestore.Exists)
		if err != nil {
			logger.Debug("Message deleted meanwhile", nil)
			return
		}
		logger.Debug("Message acked", nil)
	case <-msg.Nacked():
		logger.Debug("Message nacked", nil)
	case <-ctx.Done():
		logger.Debug("Context done", nil)
	}
}
