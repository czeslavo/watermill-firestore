package firestore_test

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"
)

func TestSubscriber_multiple_subscriptions_in_consumer_group(t *testing.T) {
	t.Skip()
	pub, sub := createPubSub(t)

	topic := watermill.NewULID()

	ctx := context.Background()
	msgsCh := spawnSubscriptions(t, ctx, sub, topic, 10)

	msg := message.NewMessage(watermill.NewULID(), nil)
	err := pub.Publish(topic, msg)
	require.NoError(t, err)

	count := countMsgsWithTimeout(msgsCh, 5*time.Second)

	require.Equal(t, 1, count)
}

func spawnSubscriptions(t *testing.T, ctx context.Context, subscriber message.Subscriber, topic string, n int) <-chan *message.Message {
	output := make(chan *message.Message)
	for i := 0; i < n; i++ {
		msgsCh, err := subscriber.Subscribe(ctx, topic)
		require.NoError(t, err)

		go func() {
			for {
				select {
				case msg := <-msgsCh:
					output <- msg
				case <-ctx.Done():
					t.Log("context of subscription cancelled")
				}
			}
		}()

	}

	return output
}

func countMsgsWithTimeout(msgsCh <-chan *message.Message, timeout time.Duration) int {
	count := 0

ReceiveMsgsLoop:
	for {
		select {
		case <-msgsCh:
			count += 1
		case <-time.After(timeout):
			break ReceiveMsgsLoop
		}
	}

	return count
}
