package firestore_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	stdFirestore "cloud.google.com/go/firestore"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/czeslavo/watermill-firestore/pkg/firestore"
	"github.com/stretchr/testify/require"
)

func TestTransactionalPublisher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	projectID := os.Getenv("FIRESTORE_PROJECT_ID")
	client, err := stdFirestore.NewClient(ctx, projectID)
	require.NoError(t, err)

	logger := watermill.NewStdLogger(true, true)
	topic := "transactional_publisher_test_" + watermill.NewShortUUID()

	subscriber, err := firestore.NewSubscriber(firestore.SubscriberConfig{
		ProjectID: projectID,
	}, logger)
	msgs, err := subscriber.Subscribe(ctx, topic)
	require.NoError(t, err)

	msg := message.NewMessage(watermill.NewUUID(), message.Payload("payload"))
	msg.Metadata = message.Metadata{"key": "value"}

	err = client.RunTransaction(ctx, func(ctx context.Context, tx *stdFirestore.Transaction) error {
		publisher, err := firestore.NewTransactionalPublisher(firestore.PublisherConfig{
			ProjectID: projectID,
		}, tx, logger)
		require.NoError(t, err)

		err = publisher.Publish(topic, msg)
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, err)

	receivedMsg := <-msgs
	require.NotNil(t, receivedMsg)
	assert.Equal(t, msg.UUID, receivedMsg.UUID)
	assert.Equal(t, msg.Payload, receivedMsg.Payload)
	assert.Equal(t, msg.Metadata, receivedMsg.Metadata)
}
