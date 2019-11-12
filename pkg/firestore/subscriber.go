package firestore

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Subscriber struct {
}

func NewSubscriber() (*Subscriber, error) {
	return &Subscriber{}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	return make(chan *message.Message), nil
}

func (p *Subscriber) Close() error {
	return nil
}
