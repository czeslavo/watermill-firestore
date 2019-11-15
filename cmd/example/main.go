package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/czeslavo/watermill-firestore/pkg/firestore"
)

func main() {
	logger := watermill.NewStdLogger(false, false)

	subscriber, err := firestore.NewSubscriber(
		firestore.SubscriberConfig{
			GenerateSubscriptionName: func(topic string) string {
				return topic + "_sub"
			},
			ProjectID: os.Getenv("FIRESTORE_PROJECT_ID"),
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	output, err := subscriber.Subscribe(context.Background(), "topic")
	if err != nil {
		panic(err)
	}

	read(output)
}

func read(output <-chan *message.Message) {
	t0 := time.Now()
	for _ = range output {
		t1 := time.Now()
		fmt.Printf("%f/sec\n", 1./t1.Sub(t0).Seconds())
		t0 = t1
	}
}
