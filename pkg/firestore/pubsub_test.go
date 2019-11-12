package firestore_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"

	"github.com/czeslavo/watermill-firestore/pkg/firestore"
)

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPubSubWithSubscriptionName(t, "topic")
}

func createPubSubWithSubscriptionName(t *testing.T, topic string) (message.Publisher, message.Subscriber) {
	pub, err := firestore.NewPublisher()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := firestore.NewSubscriber()
	if err != nil {
		t.Fatal(err)
	}

	return pub, sub
}

func TestPublishSubscribe(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
			Persistent:          true,
		},
		createPubSub,
		createPubSubWithSubscriptionName,
	)
}
