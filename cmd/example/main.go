package main

import (
	"context"
	"fmt"

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
			ProjectID: "test",
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
	for m := range output {
		fmt.Println("Received", m)
	}
}
