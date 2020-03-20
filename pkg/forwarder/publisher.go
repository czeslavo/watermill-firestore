package forwarder

import (
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/pkg/errors"
)

type Publisher struct {
	wrappedPublisher message.Publisher
	config           Config
}

func NewPublisher(publisher message.Publisher, config Config) *Publisher {
	return &Publisher{
		wrappedPublisher: publisher,
		config:           config,
	}
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	for _, msg := range messages {
		envelopedMsg, err := wrapMessageInEnvelope(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot wrap message, target topic: '%s', uuid: '%s'", topic, msg.UUID)
		}

		if err := p.wrappedPublisher.Publish(p.config.ForwarderTopic, envelopedMsg); err != nil {
			return errors.Wrapf(err, "cannot publish message to forwarder topic: '%s', message uuid: '%s'", p.config.ForwarderTopic, msg.UUID)
		}
	}

	return nil
}

func (p *Publisher) Close() error {
	return p.wrappedPublisher.Close()
}
