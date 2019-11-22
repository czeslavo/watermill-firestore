package firestore

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PublisherConfig struct {
	// ProjectID is an ID of a Google Cloud project with Firestore database.
	ProjectID string

	// PubSubRootCollection is a name of a collection which will be used as a root collection for the PubSub.
	// It defaults to `pubsub`.
	PubSubRootCollection string

	// MessagePublishTimeout is a timeout used for a single `Publish` call.
	MessagePublishTimeout time.Duration

	// GoogleClientOpts are options passed directly to firestore client.
	GoogleClientOpts []option.ClientOption
}

func (c *PublisherConfig) setDefaults() {
	if c.MessagePublishTimeout == 0 {
		c.MessagePublishTimeout = time.Second * 60
	}
	if c.PubSubRootCollection == "" {
		c.PubSubRootCollection = pubSubRootCollection
	}
}

type Publisher struct {
	config PublisherConfig
	logger watermill.LoggerAdapter

	client *firestore.Client

	subCacheMtx        *sync.RWMutex
	subscriptionsCache map[string]subCacheEntry
}

type subCacheEntry struct {
	subNames  []string
	lastWrite time.Time
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.setDefaults()

	client, err := firestore.NewClient(context.Background(), config.ProjectID, config.GoogleClientOpts...)
	if err != nil {
		return nil, err
	}

	return &Publisher{
		client:             client,
		config:             config,
		logger:             logger,
		subCacheMtx:        &sync.RWMutex{},
		subscriptionsCache: make(map[string]subCacheEntry),
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

	// prepare messages
	var msgsToPublish []Message
	for _, msg := range messages {
		firestoreMsg := Message{
			UUID:     msg.UUID,
			Payload:  msg.Payload,
			Metadata: make(map[string]interface{}),
		}
		for k, v := range msg.Metadata {
			firestoreMsg.Metadata[k] = v
		}
		msgsToPublish = append(msgsToPublish, firestoreMsg)
	}

	logger = logger.With(watermill.LogFields{"subscriptions_count": len(subscriptions)})

	logger.Debug("Publishing to topic", nil)

	for _, sub := range subscriptions {
		logger := logger.With(watermill.LogFields{"subscription": sub})
		logger.Trace("Publishing to subscription", nil)

		// in case of single message just use single add operation
		if len(msgsToPublish) == 1 {
			if _, _, err := p.client.Collection(p.config.PubSubRootCollection).Doc(topic).Collection(sub).Add(ctx, msgsToPublish[0]); err != nil {
				return err
			}
			continue
		}

		// write in batches with max 500 msgs since it's the limit on firestore side
		for offset := 0; offset < len(msgsToPublish); offset = offset + 500 {
			lastInBatch := offset + 500
			if len(msgsToPublish) < lastInBatch {
				lastInBatch = len(msgsToPublish)
			}
			logger.Trace("Publishing batch", watermill.LogFields{"batch_start": offset, "batch_end": lastInBatch})

			batch := p.client.Batch()
			for _, msg := range msgsToPublish[offset:lastInBatch] {
				batch = batch.Create(p.client.Collection(p.config.PubSubRootCollection).Doc(topic).Collection(sub).NewDoc(), msg)
			}
			if _, err := batch.Commit(ctx); err != nil {
				logger.Error("Failed to commit messages batch", err, nil)
				return err
			}
		}
		logger.Debug("Published messages to subscription", nil)
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
	p.subCacheMtx.RLock()
	defer p.subCacheMtx.RUnlock()
	return p.subscriptionsCache[topic].subNames
}

func (p *Publisher) cacheSubscriptions(topic string, subs []string) {
	p.subCacheMtx.Lock()
	defer p.subCacheMtx.Unlock()
	entry := subCacheEntry{
		lastWrite: time.Now(),
		subNames:  subs,
	}
	p.subscriptionsCache[topic] = entry
}

func (p *Publisher) isCacheValid(topic string) bool {
	p.subCacheMtx.RLock()
	defer p.subCacheMtx.RUnlock()
	return time.Now().Before(p.subscriptionsCache[topic].lastWrite.Add(time.Millisecond * 500))
}

func (p *Publisher) publishMessage(ctx context.Context, topic, sub string, msg Message) error {
	subCol := p.client.Collection(p.config.PubSubRootCollection).Doc(topic).Collection(sub)

	_, _, err := subCol.Add(ctx, msg)
	if err != nil {
		return err
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
