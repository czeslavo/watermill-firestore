package firestore

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"

	"cloud.google.com/go/firestore"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PublisherConfig struct {
	// ProjectID is an ID of a Google Cloud project with Firestore database.
	// It defaults to os.Getenv("FIRESTORE_PROJECT_ID").
	ProjectID string

	// PubSubRootCollection is a name of a collection which will be used as a root collection for the PubSub.
	// It defaults to "pubsub".
	PubSubRootCollection string

	// MessagePublishTimeout is a timeout used for a single `Publish` call.
	// It defaults to 1 minute.
	MessagePublishTimeout time.Duration

	// SubscriptionsCacheValidityDuration is used for internal subscriptions cache
	// in order to reduce fetch calls to Firestore on each `Publish` method call.
	//
	// If you prefer to not cache subscriptions and fetch them each time `Publish`
	// is called, please set `DontCacheSubscriptions` to true.
	//
	// It defaults to 500 milliseconds.
	SubscriptionsCacheValidityDuration time.Duration

	// DontCacheSubscriptions should be set to true when you don't want
	// Publisher to keep an internal cache of subscribers.
	DontCacheSubscriptions bool

	// GoogleClientOpts are options passed directly to firestore client.
	GoogleClientOpts []option.ClientOption

	// Marshaler marshals message from Watermill to Firestore format and vice versa.
	Marshaler Marshaler
}

func (c *PublisherConfig) setDefaults() {
	if c.ProjectID == "" {
		c.ProjectID = os.Getenv("FIRESTORE_PROJECT_ID")
	}
	if c.MessagePublishTimeout == 0 {
		c.MessagePublishTimeout = time.Minute
	}
	if c.PubSubRootCollection == "" {
		c.PubSubRootCollection = defaultPubSubRootCollection
	}
	if c.SubscriptionsCacheValidityDuration == 0 {
		c.SubscriptionsCacheValidityDuration = time.Millisecond * 500
	}
	if c.Marshaler == nil {
		c.Marshaler = DefaultMarshaler{}
	}
}

type Publisher struct {
	config PublisherConfig
	logger watermill.LoggerAdapter

	client *firestore.Client

	subscriptionsCacheMtx *sync.RWMutex
	subscriptionsCache    map[string]subscriptionsCacheEntry
}

type subscriptionsCacheEntry struct {
	subscriptionsNames []string
	lastWrite          time.Time
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.setDefaults()

	client, err := firestore.NewClient(context.Background(), config.ProjectID, config.GoogleClientOpts...)
	if err != nil {
		return nil, err
	}

	return &Publisher{
		client:                client,
		config:                config,
		logger:                logger,
		subscriptionsCacheMtx: &sync.RWMutex{},
		subscriptionsCache:    make(map[string]subscriptionsCacheEntry),
	}, nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.config.MessagePublishTimeout)
	defer cancel()

	logger := p.logger.With(watermill.LogFields{"topic": topic})

	subscriptions, err := p.getSubscriptions(ctx, topic)
	if err != nil {
		logger.Error("Failed to get subscriptions for publishing", err, nil)
		return err
	}
	logger = logger.With(watermill.LogFields{"subscriptions_count": len(subscriptions)})

	msgsToPublish, err := p.prepareFirestoreMessages(messages)
	if err != nil {
		return errors.Wrap(err, "cannot prepare messages to publish")
	}

	logger.Trace("Publishing to topic", nil)

	for _, subscription := range subscriptions {
		logger := logger.With(watermill.LogFields{"subscription": subscription})
		logger.Trace("Publishing to subscription", nil)

		if err := p.publishInBatches(ctx, topic, subscription, msgsToPublish, logger); err != nil {
			return err
		}
	}

	logger.Debug("Published to topic", nil)
	return nil
}

func (p *Publisher) PublishInTransaction(topic string, t *firestore.Transaction, messages ...*message.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.config.MessagePublishTimeout)
	defer cancel()

	logger := p.logger.With(watermill.LogFields{"topic": topic})

	subscriptions, err := p.getSubscriptions(ctx, topic)
	if err != nil {
		logger.Error("Failed to get subscriptions for publishing", err, nil)
		return err
	}
	logger = logger.With(watermill.LogFields{"subscriptions_count": len(subscriptions)})

	marshaledMessages, err := p.prepareFirestoreMessages(messages)
	if err != nil {
		return errors.Wrap(err, "cannot prepare messages to publish")
	}

	logger.Trace("Publishing to topic", nil)

	for _, subscription := range subscriptions {
		logger := logger.With(watermill.LogFields{"subscription": subscription})
		logger.Trace("Publishing to subscription", nil)

		for _, marshaledMessage := range marshaledMessages {
			doc := p.client.Collection(p.config.PubSubRootCollection).Doc(topic).Collection(subscription).NewDoc()
			if err := t.Create(doc, marshaledMessage.Data); err != nil {
				logger.Error("Failed to add message to transaction", err, nil)
				return err
			}

			logger.Debug("Publishing message to firestore", watermill.LogFields{
				"firestore_path":   doc.Path,
				"firestore_doc_id": doc.ID,
				"message_uuid":     marshaledMessage.MessageUUID,
			})

			logger.Trace("Added message to transaction", nil)
			continue
		}
	}

	return nil
}

func (p *Publisher) getSubscriptions(ctx context.Context, topic string) ([]string, error) {
	logger := p.logger.With(watermill.LogFields{"topic": topic})

	if p.isCacheValid(topic) {
		subs := p.getSubscriptionsFromCache(topic)
		logger.Trace("Read subscriptions from cache", watermill.LogFields{"subs_count": len(subs)})
		return subs, nil
	}
	logger.Trace("Subscriptions cache is not valid", nil)

	subsDocs, err := p.client.Collection(p.config.PubSubRootCollection).Doc(topic).Collection(subscriptionsCollection).Documents(ctx).GetAll()
	if err != nil {
		return nil, err
	}

	var subs []string
	for _, subDoc := range subsDocs {
		subs = append(subs, subDoc.Ref.ID)
	}

	p.cacheSubscriptions(topic, subs)
	logger.Trace("Cached subscriptions", watermill.LogFields{"subs_count": len(subs)})

	return subs, nil
}

func (p *Publisher) getSubscriptionsFromCache(topic string) []string {
	p.subscriptionsCacheMtx.RLock()
	defer p.subscriptionsCacheMtx.RUnlock()
	return p.subscriptionsCache[topic].subscriptionsNames
}

func (p *Publisher) cacheSubscriptions(topic string, subs []string) {
	p.subscriptionsCacheMtx.Lock()
	defer p.subscriptionsCacheMtx.Unlock()
	entry := subscriptionsCacheEntry{
		lastWrite:          time.Now(),
		subscriptionsNames: subs,
	}
	p.subscriptionsCache[topic] = entry
}

func (p *Publisher) isCacheValid(topic string) bool {
	p.subscriptionsCacheMtx.RLock()
	defer p.subscriptionsCacheMtx.RUnlock()
	return time.Now().Before(p.subscriptionsCache[topic].lastWrite.Add(p.config.SubscriptionsCacheValidityDuration))
}

type marshaledMessage struct {
	MessageUUID string
	Data        interface{}
}

func (p *Publisher) prepareFirestoreMessages(messages []*message.Message) ([]marshaledMessage, error) {
	var msgsToPublish []marshaledMessage
	for _, msg := range messages {
		firestoreMsg, err := p.config.Marshaler.Marshal(msg)
		if err != nil {
			return nil, err
		}

		msgsToPublish = append(msgsToPublish, marshaledMessage{
			MessageUUID: msg.UUID,
			Data:        firestoreMsg,
		})
	}

	return msgsToPublish, nil
}

func (p *Publisher) publishInBatches(
	ctx context.Context,
	topic,
	subscription string,
	marshaledMsgs []marshaledMessage,
	logger watermill.LoggerAdapter,
) error {
	const firestoreBatchSizeLimit = 500
	for offset := 0; offset < len(marshaledMsgs); offset = offset + firestoreBatchSizeLimit {
		lastInBatch := offset + firestoreBatchSizeLimit
		if len(marshaledMsgs) < lastInBatch {
			lastInBatch = len(marshaledMsgs)
		}

		logger := logger.With(watermill.LogFields{
			"batch_start": offset,
			"batch_end":   lastInBatch,
		})
		logger.Trace("Publishing messages batch", nil)

		batch := p.client.Batch()
		for _, marshaledMsg := range marshaledMsgs[offset:lastInBatch] {
			doc := p.client.Collection(p.config.PubSubRootCollection).Doc(topic).Collection(subscription).NewDoc()
			batch = batch.Create(doc, marshaledMsg.Data)

			logger.Debug("Publishing message to firestore", watermill.LogFields{
				"firestore_path":   doc.Path,
				"firestore_doc_id": doc.ID,
				"message_uuid":     marshaledMsg.MessageUUID,
			})
		}

		if _, err := batch.Commit(ctx); err != nil {
			logger.Error("Failed to commit messages batch", err, nil)
			return err
		}
		logger.Trace("Published message", nil)
	}

	return nil
}

func (p *Publisher) Close() error {
	if err := p.client.Close(); err != nil {
		if status.Code(err) == codes.Canceled {
			// client is already closed
			p.logger.Trace("Closing when already closed", nil)
			return nil
		}

		p.logger.Error("closing client failed", err, watermill.LogFields{})
		return err
	}

	return nil
}
