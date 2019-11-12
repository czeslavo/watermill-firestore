package firestore

import "github.com/ThreeDotsLabs/watermill/message"

type Publisher struct {
}

func NewPublisher() (*Publisher, error) {
	return &Publisher{}, nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	return nil
}

func (p *Publisher) Close() error {
	return nil
}
