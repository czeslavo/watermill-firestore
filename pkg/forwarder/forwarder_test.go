package forwarder_test

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/czeslavo/watermill-firestore/pkg/firestore"
	"github.com/czeslavo/watermill-firestore/pkg/forwarder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Publish to Firestore, receive from GoChannel.
func TestForwarder_firestore_to_gochannel(t *testing.T) {
	logger := watermill.NewStdLogger(true, true)

	channelPubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)
	defer func() {
		require.NoError(t, channelPubSub.Close())
	}()

	ctx, cancelCtx := context.WithTimeout(context.Background(), time.Second*5)
	defer cancelCtx()
	publisher, closeForwarder := setupForwarder(t, ctx, channelPubSub, logger)
	defer closeForwarder()

	gochannelTopic := "gochannel_topic_" + watermill.NewShortUUID()
	msgs, err := channelPubSub.Subscribe(ctx, gochannelTopic)
	require.NoError(t, err)

	sentMsg := message.NewMessage(watermill.NewUUID(), message.Payload("message payload"))
	sentMsg.Metadata = message.Metadata{"key": "value"}
	err = publisher.Publish(gochannelTopic, sentMsg)
	require.NoError(t, err)

	receivedMsg := <-msgs
	require.NotNil(t, receivedMsg)
	receivedMsg.Ack()

	assert.Equal(t, sentMsg.UUID, receivedMsg.UUID)
	assert.Equal(t, sentMsg.Payload, receivedMsg.Payload)
	assert.Equal(t, sentMsg.Metadata, receivedMsg.Metadata)
}

func setupForwarder(t *testing.T, ctx context.Context, forwardingPublisher message.Publisher, logger watermill.LoggerAdapter) (message.Publisher, func()) {
	firestoreSubscriber, err := firestore.NewSubscriber(firestore.SubscriberConfig{}, logger)
	require.NoError(t, err)

	forwarderConfig := forwarder.Config{ForwarderTopic: "forwarder_topic"}
	f, err := forwarder.NewForwarder(forwardingPublisher, firestoreSubscriber, logger, forwarderConfig)
	require.NoError(t, err)

	go func() {
		require.NoError(t, f.Run(ctx))
	}()
	<-f.Running()

	firestorePublisher, err := firestore.NewPublisher(firestore.PublisherConfig{}, logger)
	require.NoError(t, err)

	wrappedPublisher := forwarder.NewPublisher(firestorePublisher, forwarderConfig)

	return wrappedPublisher, func() {
		require.NoError(t, firestoreSubscriber.Close())
		require.NoError(t, f.Close())
	}
}
