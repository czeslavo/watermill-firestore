package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"cloud.google.com/go/firestore"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/czeslavo/watermill-firestore/internal/inspector"
	"github.com/urfave/cli/v2"
)

func main() {
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	firestoreClient, err := firestore.NewClient(ctx, os.Getenv("FIRESTORE_PROJECT_ID"))
	if err != nil {
		panic(err)
	}

	// It's going to be initialized in `Before` function of an app.
	var inspectClient *inspector.Client

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "root-collection",
				Aliases:  []string{"root"},
				Required: true,
				Usage:    "Pub/Sub root collection.",
			},
		},
		Before: func(c *cli.Context) error {
			rootCollection := c.String("root-collection")
			inspectClient, err = inspector.NewClient(firestoreClient, rootCollection)
			return err
		},
		Commands: []*cli.Command{
			{
				Name:    "list",
				Aliases: []string{"l"},
				Subcommands: []*cli.Command{
					{
						Name:    "topics",
						Aliases: []string{"t"},
						Usage:   "List all topics.",
						Action: func(c *cli.Context) error {
							topics, err := inspectClient.GetTopics(c.Context)
							if err != nil {
								return err
							}

							fmt.Println(renderTopics(topics))
							return nil
						},
					},
					{
						Name:      "subscriptions",
						Aliases:   []string{"s"},
						Usage:     "List all subscriptions on the topic.",
						ArgsUsage: "[topic]",
						Action: func(c *cli.Context) error {
							topic := c.Args().First()
							if topic == "" {
								return errors.New("topic has to be provided")
							}

							subscriptions, err := inspectClient.GetTopicSubscriptions(c.Context, topic)
							if err != nil {
								return err
							}

							fmt.Println(renderSubscriptions(subscriptions))
							return nil
						},
					},
					{
						Name:      "messages",
						Aliases:   []string{"m"},
						Usage:     "List all unacked messages from the subscription.",
						ArgsUsage: "[topic] [subscription]",
						Action: func(c *cli.Context) error {
							topic := c.Args().First()
							if topic == "" {
								return errors.New("topic has to be provided")
							}
							subscription := c.Args().Get(1)
							if subscription == "" {
								return errors.New("subscription has to be provided")
							}

							messages, err := inspectClient.GetSubscriptionMessages(c.Context, topic, subscription)
							if err != nil {
								return err
							}

							fmt.Println(renderMessages(messages))
							return nil
						},
					},
				},
			},
			{
				Name:      "ack",
				Aliases:   []string{"a"},
				Usage:     "Ack a message.",
				ArgsUsage: "[topic] [subscription] [message_uuid]",
				Action: func(c *cli.Context) error {
					topic := c.Args().First()
					if topic == "" {
						return errors.New("topic has to be provided")
					}
					subscription := c.Args().Get(1)
					if subscription == "" {
						return errors.New("subscription has to be provided")
					}
					messageUUID := c.Args().Get(2)
					if messageUUID == "" {
						return errors.New("message_uuid has to be provided")
					}

					if err := inspectClient.AckMessage(c.Context, topic, subscription, messageUUID); err != nil {
						return err
					}

					fmt.Println("Acked!")
					return nil
				},
			},
			{
				Name:    "purge",
				Aliases: []string{"p"},
				Usage:   "Purge all messages from a subscription.",
				Action: func(c *cli.Context) error {
					topic := c.Args().First()
					if topic == "" {
						return errors.New("topic has to be provided")
					}
					subscription := c.Args().Get(1)
					if subscription == "" {
						return errors.New("subscription has to be provided")
					}

					if err := inspectClient.PurgeSubscription(c.Context, topic, subscription); err != nil {
						return err
					}

					fmt.Println("Purged!")
					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func renderTopics(topics []string) string {
	if len(topics) == 0 {
		return "No topics."
	}

	lines := []string{
		"Topics:",
	}

	for _, topic := range topics {
		lines = append(lines, fmt.Sprintf("\t* %s", topic))
	}

	return strings.Join(lines, "\n")
}

func renderSubscriptions(subscriptions []string) string {

	if len(subscriptions) == 0 {
		return "No subscriptions."
	}

	lines := []string{
		"Subscriptions:",
	}

	for _, sub := range subscriptions {
		lines = append(lines, fmt.Sprintf("\t* %s", sub))
	}

	return strings.Join(lines, "\n")
}

func renderMessages(messages message.Messages) string {
	if len(messages) == 0 {
		return "No messages."
	}

	lines := []string{
		"Messages:",
	}

	for _, msg := range messages {
		lines = append(lines, fmt.Sprintf("\t* %s: %s", msg.UUID, string(msg.Payload)))
	}

	return strings.Join(lines, "\n")
}
