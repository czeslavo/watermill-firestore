package inspector_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"cloud.google.com/go/firestore"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/czeslavo/watermill-firestore/internal/inspector"
	watermillFirestore "github.com/czeslavo/watermill-firestore/pkg/firestore"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	pubSubRootCollection = "root"
)

func TestIntrospectionClient(t *testing.T) {
	logger := watermill.NewStdLogger(true, true)
	ctx := context.Background()

	firestoreClient := getFirestoreClient(t, ctx)
	subscriber := getSubscriberWithUniqueSubscriptions(t, firestoreClient, logger)
	inspectorClient := getInspectorClient(t, firestoreClient)

	t.Run("GetTopics", func(t *testing.T) {
		t.Parallel()

		topics := prepareRandomTopics()
		for _, topic := range topics {
			triggerSubscriptionCreating(t, ctx, subscriber, topic)
		}

		topicsFromClient, err := inspectorClient.GetTopics(ctx)
		require.NoError(t, err)

		for _, topic := range topics {
			assert.Contains(t, topicsFromClient, topic)
		}
	})

	t.Run("GetTopicSubscriptions", func(t *testing.T) {
		t.Parallel()

		topic := uuid.New().String()

		subscriptionsCount := 100
		for i := 0; i < subscriptionsCount; i++ {
			triggerSubscriptionCreating(t, ctx, subscriber, topic)
		}

		subscriptions, err := inspectorClient.GetTopicSubscriptions(ctx, topic)
		require.NoError(t, err)

		assert.Len(t, subscriptions, subscriptionsCount)
	})

	t.Run("GetSubscriptionMessages", func(t *testing.T) {
		t.Parallel()

		topic := uuid.New().String()

		subscriber := getSubscriberWithFixedSubscription(t, firestoreClient, logger)
		publisher := getPublisher(t, firestoreClient, logger)

		triggerSubscriptionCreating(t, ctx, subscriber, topic)
		subscriptions, err := inspectorClient.GetTopicSubscriptions(ctx, topic)
		require.NoError(t, err)
		require.Len(t, subscriptions, 1)
		subscription := subscriptions[0]

		err = publisher.Publish(topic, someRandomMessage(t), someRandomMessage(t))
		require.NoError(t, err)

		messages, err := inspectorClient.GetSubscriptionMessages(ctx, topic, subscription)
		require.NoError(t, err)

		assert.Len(t, messages, 2)
	})

	t.Run("AckMessage", func(t *testing.T) {
		t.Parallel()

		topic := uuid.New().String()

		subscriber := getSubscriberWithFixedSubscription(t, firestoreClient, logger)
		publisher := getPublisher(t, firestoreClient, logger)

		triggerSubscriptionCreating(t, ctx, subscriber, topic)
		subscriptions, err := inspectorClient.GetTopicSubscriptions(ctx, topic)
		require.NoError(t, err)
		require.Len(t, subscriptions, 1)
		subscription := subscriptions[0]

		err = publisher.Publish(topic, someRandomMessage(t))
		require.NoError(t, err)

		messages, err := inspectorClient.GetSubscriptionMessages(ctx, topic, subscription)
		require.NoError(t, err)
		require.Len(t, messages, 1)
		msg := messages[0]

		err = inspectorClient.AckMessage(ctx, topic, subscription, msg.UUID)
		require.NoError(t, err)

		messages, err = inspectorClient.GetSubscriptionMessages(ctx, topic, subscription)
		require.NoError(t, err)
		require.Len(t, messages, 0, "after acking there should be no messages left")
	})

	t.Run("PurgeSubscription", func(t *testing.T) {
		t.Parallel()

		topic := uuid.New().String()

		subscriber := getSubscriberWithFixedSubscription(t, firestoreClient, logger)
		publisher := getPublisher(t, firestoreClient, logger)

		triggerSubscriptionCreating(t, ctx, subscriber, topic)
		subscriptions, err := inspectorClient.GetTopicSubscriptions(ctx, topic)
		require.NoError(t, err)
		require.Len(t, subscriptions, 1)
		subscription := subscriptions[0]

		err = publisher.Publish(topic, someRandomMessage(t), someRandomMessage(t))
		require.NoError(t, err)

		messages, err := inspectorClient.GetSubscriptionMessages(ctx, topic, subscription)
		require.NoError(t, err)
		require.Len(t, messages, 2)

		err = inspectorClient.PurgeSubscription(ctx, topic, subscription)
		require.NoError(t, err)

		messages, err = inspectorClient.GetSubscriptionMessages(ctx, topic, subscription)
		require.NoError(t, err)
		require.Len(t, messages, 0, "after purging there should be no messages left")
	})
}

func getFirestoreClient(t *testing.T, ctx context.Context) *firestore.Client {
	firestoreClient, err := firestore.NewClient(ctx, os.Getenv("FIRESTORE_PROJECT_ID"))
	require.NoError(t, err)
	return firestoreClient
}

func getSubscriber(
	t *testing.T,
	firestoreClient *firestore.Client,
	logger watermill.LoggerAdapter,
	generateSubscriptionName watermillFirestore.GenerateSubscriptionNameFn,
) message.Subscriber {
	subscriber, err := watermillFirestore.NewSubscriber(watermillFirestore.SubscriberConfig{
		PubSubRootCollection:     pubSubRootCollection,
		CustomFirestoreClient:    firestoreClient,
		GenerateSubscriptionName: generateSubscriptionName,
	}, logger)
	require.NoError(t, err)
	return subscriber
}

func getPublisher(t *testing.T, firestoreClient *firestore.Client, logger watermill.LoggerAdapter) message.Publisher {
	publisher, err := watermillFirestore.NewPublisher(watermillFirestore.PublisherConfig{
		PubSubRootCollection:  pubSubRootCollection,
		CustomFirestoreClient: firestoreClient,
	}, logger)
	require.NoError(t, err)
	return publisher
}

// Each call to `Subscribe` will create a new unique subscription.
func getSubscriberWithUniqueSubscriptions(t *testing.T, firestoreClient *firestore.Client, logger watermill.LoggerAdapter) message.Subscriber {
	return getSubscriber(t, firestoreClient, logger, func(topic string) string {
		// to make sure every subscription will be unique
		return fmt.Sprintf("%s_%s", topic, uuid.New().String())
	})
}

// Each call to `Subscribe` will create the same subscription.
func getSubscriberWithFixedSubscription(t *testing.T, firestoreClient *firestore.Client, logger watermill.LoggerAdapter) message.Subscriber {
	return getSubscriber(t, firestoreClient, logger, func(topic string) string {
		return fmt.Sprintf("%s_sub", topic)
	})
}

func getInspectorClient(t *testing.T, firestoreClient *firestore.Client) *inspector.Client {
	introClient, err := inspector.NewClient(firestoreClient, pubSubRootCollection)
	require.NoError(t, err)
	return introClient
}

func prepareRandomTopics() []string {
	var topics []string
	for i := 0; i < 100; i++ {
		topics = append(topics, uuid.New().String())
	}
	return topics
}

func triggerSubscriptionCreating(t *testing.T, ctx context.Context, subscriber message.Subscriber, topic string) {
	_, err := subscriber.Subscribe(ctx, topic)
	require.NoError(t, err)
}

func someRandomMessage(t *testing.T) *message.Message {
	msg := struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}{
		Key:   uuid.New().String(),
		Value: uuid.New().String(),
	}
	payload, err := json.Marshal(msg)
	require.NoError(t, err)

	return message.NewMessage(uuid.New().String(), payload)
}
