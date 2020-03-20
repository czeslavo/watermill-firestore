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
	config SubscriberConfig

	client *firestore.Client
	locker *MessageLocker
	logger watermill.LoggerAdapter

	closing chan struct{}
	output  chan *message.Message
}

func newSubscription(name, topic string, config SubscriberConfig, client *firestore.Client, logger watermill.LoggerAdapter, closing chan struct{}) (*subscription, error) {
	s := &subscription{
		name:    name,
		topic:   topic,
		config:  config,
		client:  client,
		locker:  NewMessageLocker(client),
		logger:  logger,
		closing: closing,
		output:  make(chan *message.Message),
	}

	if err := createFirestoreSubscriptionIfNotExists(s.client, config.PubSubRootCollection, topic, s.config.GenerateSubscriptionName(topic), s.logger, s.config.Timeout); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *subscription) receive(ctx context.Context) {
	s.readAll(ctx)
	s.watchChanges(ctx)
}

func (s *subscription) readAll(ctx context.Context) {
	s.logger.Debug("Reading messages on receive start", nil)
	docs, err := s.messagesQuery().Documents(ctx).GetAll()
	if err != nil {
		s.logger.Error("Couldn't read messages on receive start", err, nil)
	}
	for _, doc := range docs {
		s.handleAddedMessage(ctx, doc)
	}

	s.logger.Trace("Reading messages from sub", nil)
}

func (s *subscription) watchChanges(ctx context.Context) {
	subscriptionSnapshots := s.messagesQuery().Query.Snapshots(ctx)
	defer subscriptionSnapshots.Stop()
	for {
		subscriptionSnapshot, err := subscriptionSnapshots.Next()
		if err == iterator.Done {
			s.logger.Debug("Listening on subscription done", nil)
			break
		} else if status.Code(err) == codes.Canceled {
			s.logger.Debug("Received context canceled", nil)
			break
		} else if err != nil {
			s.logger.Error("Error receiving", err, nil)
			break
		}

		if subscriptionSnapshot.Size == 0 {
			continue
		}

		for _, e := range onlyAddedMessages(subscriptionSnapshot.Changes) {
			s.handleAddedMessage(ctx, e.Doc)
		}
	}
}

func (s *subscription) messagesQuery() *firestore.CollectionRef {
	return s.client.Collection(s.config.PubSubRootCollection).Doc(s.topic).Collection(s.name)
}

func onlyAddedMessages(changes []firestore.DocumentChange) (added []firestore.DocumentChange) {
	for _, ch := range changes {
		if ch.Kind == firestore.DocumentAdded {
			added = append(added, ch)
		}
	}
	return
}

func (s *subscription) handleAddedMessage(ctx context.Context, doc *firestore.DocumentSnapshot) {
	logger := s.logger.With(watermill.LogFields{"document_id": doc.Ref.ID})
	msg, err := s.config.Marshaler.Unmarshal(doc)
	if err != nil {
		logger.Error("Couldn't unmarshal message", err, nil)
		return
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	//locked, err := s.locker.Lock(ctx, msg.UUID)
	//if err != nil {
	//	logger.Trace("Message lock couldn't be acquired", watermill.LogFields{"err": err})
	//	return
	//}
	//if !locked {
	//	logger.Trace("Message lock was already acquired", nil)
	//	return
	//}
	//
	//wasAcked := false
	//defer func() {
	//	if !wasAcked {
	//		if err := s.locker.Unlock(ctx, msg.UUID); err != nil {
	//			logger.Error("Could not unlock message when not acked", err, nil)
	//		}
	//	}
	//}()

	select {
	case <-s.closing:
		logger.Trace("Channel closed when waiting for consuming message", nil)
		return
	case <-ctx.Done():
		logger.Trace("Context done when waiting for consuming message", nil)
		return
	case s.output <- msg:
		logger.Trace("Message consumed, waiting for ack/nack", nil)
		// message consumed, wait for ack/nack
	}

	select {
	case <-s.closing:
		logger.Trace("Closing when waiting for ack/nack", nil)
	case <-msg.Nacked():
		logger.Trace("Message nacked", nil)
	case <-ctx.Done():
		logger.Trace("Context done", nil)
	case <-msg.Acked():
		if err := s.removeMessage(ctx, doc, logger); err != nil {
			logger.Error("Failed to ack message", err, nil)
			return
		}
		//wasAcked = true
		logger.Trace("Message acked", nil)
	}
}

func (s *subscription) removeMessage(ctx context.Context, message *firestore.DocumentSnapshot, logger watermill.LoggerAdapter) error {
	deleteCtx, deleteCancel := context.WithTimeout(ctx, s.config.Timeout)
	defer deleteCancel()
	_, err := message.Ref.Delete(deleteCtx, firestore.Exists)
	if status.Code(err) == codes.NotFound {
		logger.Trace("Message deleted meanwhile", nil)
		return nil
	}
	if err != nil {
		return err
	}

	return nil
}
