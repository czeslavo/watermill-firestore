package introspection

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/firestore"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type Client struct {
	firestoreClient      *firestore.Client
	pubSubRootCollection string
}

func NewClient(firestoreClient *firestore.Client, pubSubRootCollection string) (*Client, error) {
	if firestoreClient == nil {
		return nil, errors.New("empty firestore client")
	}
	if pubSubRootCollection == "" {
		return nil, errors.New("empty pubsub root collection")
	}

	return &Client{firestoreClient, pubSubRootCollection}, nil
}

func (c *Client) GetTopics(ctx context.Context) ([]string, error) {
	topicsDocs, err := c.rootCollection().Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf("failed to get topics: %s", err)
	}

	var topics []string
	for _, topicDoc := range topicsDocs {
		topics = append(topics, topicDoc.Ref.ID)
	}

	return topics, nil
}

func (c *Client) GetTopicSubscriptions(ctx context.Context, topic string) ([]string, error) {
	subscriptionsDocs, err := c.rootCollection().Doc(topic).Collection("subscriptions").Documents(ctx).GetAll()
	if err != nil {
		return nil, err
	}

	var subscriptions []string
	for _, topicDoc := range subscriptionsDocs {
		subscriptions = append(subscriptions, topicDoc.Ref.ID)
	}

	return subscriptions, nil
}

func (c *Client) PollSubscriptionMessages(ctx context.Context, topic, subscription string) (message.Messages, error) {
	docs, err := c.rootCollection().Doc(topic).Collection(subscription).Documents(ctx).GetAll()
	if err != nil {
		return nil, errors.Wrap(err, "could not get messages documents")
	}

	var messages message.Messages
	for _, doc := range docs {
		j, err := json.Marshal(doc.Data())
		if err != nil {
			return nil, errors.Wrap(err, "could not marshal message to json")
		}

		messages = append(messages, message.NewMessage(doc.Ref.ID, j))
	}

	return messages, nil
}

func (c *Client) AckMessage(ctx context.Context, topic, subscription, uuid string) error {
	_, err := c.rootCollection().Doc(topic).Collection(subscription).Doc(uuid).Delete(ctx, firestore.Exists)
	if status.Code(err) == codes.NotFound {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "could not delete document")
	}

	return nil
}

func (c *Client) PurgeSubscription(ctx context.Context, topic, subscription string) error {
	docs, err := c.rootCollection().Doc(topic).Collection(subscription).Documents(ctx).GetAll()
	if err != nil {
		return errors.Wrap(err, "could not get all messages documents")
	}

	for _, doc := range docs {
		_, err := doc.Ref.Delete(ctx, firestore.Exists)
		if status.Code(err) == codes.NotFound {
			continue
		}
		if err != nil {
			return errors.Wrap(err, "could not delete document")
		}
	}

	return nil
}

func (c *Client) rootCollection() *firestore.CollectionRef {
	return c.firestoreClient.Collection(c.pubSubRootCollection)
}
