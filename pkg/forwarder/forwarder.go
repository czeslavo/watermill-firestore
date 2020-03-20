package forwarder

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type Forwarder struct {
	router    *message.Router
	publisher message.Publisher
}

func NewForwarder(publisher message.Publisher, subscriber message.Subscriber, logger watermill.LoggerAdapter, config Config) (*Forwarder, error) {
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, err
	}

	f := &Forwarder{router, publisher}

	router.AddNoPublisherHandler(
		"events_forwarder",
		config.ForwarderTopic,
		subscriber,
		f.forwardMessage,
	)

	return f, nil
}

func (f *Forwarder) Run(ctx context.Context) error {
	return f.router.Run(ctx)
}

func (f *Forwarder) Close() error {
	return f.router.Close()
}

func (f *Forwarder) Running() chan struct{} {
	return f.router.Running()
}

func (f *Forwarder) forwardMessage(msg *message.Message) error {
	destTopic, unwrappedMsg, err := unwrapMessageFromEnvelope(msg)
	if err != nil {
		return errors.Wrap(err, "cannot unwrap message from envelope")
	}

	if err := f.publisher.Publish(destTopic, unwrappedMsg); err != nil {
		return errors.Wrap(err, "cannot publish message")
	}

	return nil
}
