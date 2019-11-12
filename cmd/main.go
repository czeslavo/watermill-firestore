package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type firestoreMessage struct {
	UUID    string `firestore:"uuid"`
	Payload []byte `firestore:"payload"`
}

type subscription struct {
	Name string `firestore:"name"`
}

// structure in firestore should be:

// pubsub (col) -> topic1 -> subscriptions (col)  -> name:svc-fares
//                                                -> name:svc-other
//                        -> svc-fares (col) -> event1
//										     -> event2
//              -> topic2
//              -> topic3

// we should publish on each subscription for this topic
func publish(c *firestore.Client, topic string) {
	for {
		// find all subscription for particular topic
		subscriptions, err := c.Collection("pubsub").Doc(topic).Collection("subscriptions").Documents(context.Background()).GetAll()
		if err != nil {
			panic(fmt.Sprintln("couldn't get all active subscriptions", err))
		}
		fmt.Printf("%d active subscriptions on topic %s\n", len(subscriptions), topic)

		for _, sub := range subscriptions {
			subName, err := sub.DataAt("name")
			if err != nil {
				fmt.Println("error getting subscription name", err)
			}
			subNameStr, ok := subName.(string)
			if !ok {
				continue
			}
			subCol := c.Collection("pubsub").Doc(topic).Collection(subNameStr)

			_, _, err = subCol.Add(context.Background(), firestoreMessage{
				uuid.Must(uuid.NewUUID()).String(),
				[]byte("testowy-message"),
			})
			if err != nil {
				fmt.Printf("Error adding doc: %v\n", err)
			}
		}

		<-time.After(time.Millisecond * 500)
	}
}

func main() {
	ctx := context.Background()
	client, err := firestore.NewClient(ctx, os.Getenv("FIRESTORE_PROJECT_ID"))
	if err != nil {
		panic(err)
	}

	topic := "topic"

	// logic for creating subscription if it doesn't exist
	// should be in transaction?
	subDocs, err := client.Collection("pubsub").
		Doc(topic).
		Collection("subscriptions").
		Where("name", "==", "testowa").
		Documents(context.Background()).
		GetAll()
	if err != nil {
		panic(err)
	}

	if len(subDocs) <= 0 {
		if _, _, err := client.Collection("pubsub").
			Doc(topic).
			Collection("subscriptions").
			Add(context.Background(), subscription{Name: "testowa"}); err != nil {
			panic(err)
		}

	}

	col := client.Collection("pubsub").Doc(topic).Collection("testowa")
	query := col.Query.Snapshots(ctx)
	defer query.Stop()
	go publish(client, topic)

	for {
		// get update on topic
		fmt.Println("Waiting for next update...")
		snapshots, err := query.Next()
		if err == iterator.Done {
			fmt.Println("Listening on topic done")
			break
		} else if err != nil {
			fmt.Printf("Error listening on topic: %v\n", err)
			break
		}

		fmt.Println("Update received")
		if snapshots.Size == 0 {
			fmt.Println("Zero documents in snapshot")
			continue
		}

		for {
			snapshot, err := snapshots.Documents.Next()
			if err != nil {
				if err == iterator.Done {
					break
				}
				fmt.Printf("Error reading snapshots: %v\n", err)
				break
			}

			fmt.Println("iterating over snapshots")
			if err := client.RunTransaction(ctx, func(ctx context.Context, t *firestore.Transaction) error {
				s, err := t.Get(snapshot.Ref)
				if err != nil && status.Code(err) == codes.NotFound {
					fmt.Println("snapshot doesn't exist")
					return nil
				} else if err != nil {
					fmt.Println("error getting snapshot")
					return err
				}

				// consume if still exists
				if err := t.Delete(s.Ref, firestore.Exists); err != nil {
					fmt.Printf("Consumed message: %+v\n", s.Data())
					return err
				}
				fmt.Printf("Consumed message: %+v\n", s.Data())

				return nil
			}); err != nil {
				fmt.Printf("Transaction error: %v\n", err)
			}
		}
	}

}
