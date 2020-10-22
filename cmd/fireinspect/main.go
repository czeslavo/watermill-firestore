package main

import (
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
	app := cli.NewApp()
	app.Name = "fireinspect"
	app.Usage = "Tiny tool helpful when Firestore Pub/Sub needs to be inspected."
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:     "rootCollection",
			Usage:    "Pub/Sub root collection.",
			Required: true,
			EnvVars:  []string{"FIRESTORE_PUBSUB_ROOT_COLLECTION"},
		},
		&cli.StringFlag{
			Name:     "projectID",
			Usage:    "Firestore project ID.",
			Required: true,
			EnvVars:  []string{"FIRESTORE_PROJECT_ID"},
		},
	}
	app.Commands = []*cli.Command{
		{
			Name:    "list",
			Aliases: []string{"l"},
			Subcommands: []*cli.Command{
				{
					Name:    "topics",
					Aliases: []string{"t"},
					Usage:   "List all topics.",
					Action: func(c *cli.Context) error {
						inspectClient := mustInspectorClient(c)
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
						inspectClient := mustInspectorClient(c)
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
						inspectClient := mustInspectorClient(c)
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
				inspectClient := mustInspectorClient(c)
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
				inspectClient := mustInspectorClient(c)
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
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func mustInspectorClient(c *cli.Context) *inspector.Client {
	rootCollection := c.String("rootCollection")
	projectID := c.String("projectID")

	firestoreClient, err := firestore.NewClient(c.Context, projectID)
	if err != nil {
		panic(err)
	}

	client, err := inspector.NewClient(firestoreClient, rootCollection)
	if err != nil {
		panic(err)
	}

	return client
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
