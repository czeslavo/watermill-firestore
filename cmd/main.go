package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/google/uuid"
)

type firestoreMessage struct {
	UUID     string                 `firestore:"uuid"`
	Metadata map[string]interface{} `firestore:"metadata"`
	Payload  []byte                 `firestore:"payload"`
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
				nil,
				[]byte("testowy-message"),
			})
			if err != nil {
				fmt.Printf("Error adding doc: %v\n", err)
			}
		}

		<-time.After(time.Second * 10)
	}
}

func main() {
	ctx := context.Background()
	client, err := firestore.NewClient(ctx, os.Getenv("FIRESTORE_PROJECT_ID"))
	if err != nil {
		panic(err)
	}

	topic := "topic"

	// create subscription if it doesn't exist
	transErr := client.RunTransaction(ctx, func(ctx context.Context, t *firestore.Transaction) error {
		q := client.Collection("pubsub").
			Doc(topic).
			Collection("subscriptions").Query.
			Where("name", "==", "testowa")

		subDocs, err := t.Documents(q).GetAll()
		if err != nil {
			return err
		}

		if len(subDocs) <= 0 {
			if err := t.Create(
				client.Collection("pubsub").
					Doc(topic).
					Collection("subscriptions").NewDoc(),
				subscription{Name: "testowa"}); err != nil {
				return err
			}
		}

		return nil
	})
	if transErr != nil {
		panic(transErr)
	}

	col := client.Collection("pubsub").Doc(topic).Collection("testowa")
	query := col.Query.
		Snapshots(ctx)
	defer query.Stop()
	publish(client, topic)

	// for {
	// 	// get update on topic
	// 	fmt.Println("Waiting for next update...")
	// 	querySnapshot, err := query.Next()
	// 	if err == iterator.Done {
	// 		fmt.Println("Listening on topic done")
	// 		break
	// 	} else if err != nil {
	// 		fmt.Printf("Error listening on topic: %v\n", err)
	// 		break
	// 	}

	// 	fmt.Println("Update received")
	// 	if querySnapshot.Size == 0 {
	// 		fmt.Println("Zero documents in snapshot")
	// 		continue
	// 	}
	// 	for _, ch := range querySnapshot.Changes {
	// 		// handle only if document was added
	// 		if ch.Kind == firestore.DocumentAdded {
	// 			fmt.Println("documents exists", ch.Doc.Exists())

	// 			if err := client.RunTransaction(ctx, func(ctx context.Context, t *firestore.Transaction) error {
	// 				s, err := t.Get(ch.Doc.Ref)
	// 				if err != nil && status.Code(err) == codes.NotFound {
	// 					fmt.Println("snapshot doesn't exist")
	// 					return nil
	// 				} else if err != nil {
	// 					fmt.Println("error getting snapshot")
	// 					return err
	// 				}

	// 				// consume if still exists
	// 				if err := t.Delete(s.Ref, firestore.Exists); err != nil {
	// 					fmt.Printf("Consumed message: %+v\n", s.Data())
	// 					return err
	// 				}
	// 				fmt.Printf("Consumed message: %+v\n", s.Data())

	// 				return nil
	// 			}, firestore.MaxAttempts(1)); err != nil {
	// 				fmt.Printf("Transaction error: %v\n", err)
	// 			}
	// 		}
	// 	}
	// }

}
