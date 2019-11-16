package main

import (
	"context"
	"os"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/czeslavo/watermill-firestore/pkg/firestore"
)

func main() {
	logger := watermill.NewStdLogger(true, false)

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

	output1, err := subscriber.Subscribe(context.Background(), "topic")
	if err != nil {
		panic(err)
	}

	output2, err := subscriber.Subscribe(context.Background(), "topic")
	if err != nil {
		panic(err)
	}

	go read(output1)
	go read(output2)

	<-time.After(time.Second * 20)
	subscriber.Close()
}

func read(output <-chan *message.Message) {
	for {
		select {
		case msg := <-output:
			if msg == nil {
				return
			}
			msg.Ack()
		}
	}
}
