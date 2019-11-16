package firestore

import (
	"context"
	"os"
	"testing"
	"time"

	fs "cloud.google.com/go/firestore"
	"github.com/stretchr/testify/require"
	"github.com/ThreeDotsLabs/watermill"
)

func TestSubscription(t *testing.T) {
	client, err := fs.NewClient(context.Background(), os.Getenv("FIRESTORE_PROJECT_ID"))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, err = client.Collection("pubsub").Doc("topicName").Collection("subName").Add(ctx, firestoreMessage{})
	require.NoError(t, err)

	logger := watermill.NewStdLogger(true, true)

	s, err := newSubscription(client, logger, "subName", "topicName")
	require.NoError(t, err)


	go func() {
		 s.receive(ctx)
	}()

	msg := <- s.output
	require.NotNil(t, msg)

	_, _, err = client.Collection("pubsub").Doc("topicName").Collection("subName").Add(ctx, firestoreMessage{})
	require.NoError(t, err)

	select {
	case <- s.output:
		t.Fail()
	case <- time.After(time.Second * 2):
		// cannot receive message if the recent one hasn't been acked/nacked
	}

	msg.Ack()

	select {
	case msg := <- s.output:
		require.NotNil(t, msg)
		msg.Nack()
	case <- time.After(time.Second * 2):
		t.Fail()
	}

	// message nacked
	// should sent back to firestore

	msg = <- s.output
	require.NotNil(t, msg)
}

