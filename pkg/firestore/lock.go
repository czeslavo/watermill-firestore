package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MessageLocker struct {
	client *firestore.Client
}

func NewMessageLocker(client *firestore.Client) *MessageLocker {
	return &MessageLocker{client}
}

func (l *MessageLocker) Lock(ctx context.Context, key string) (func(ctx context.Context) error, error) {
	transactionErr := l.client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		_, err := tx.Get(l.lockDoc(key))
		if err != nil && status.Code(err) != codes.NotFound {
			fmt.Println("could not check if lock exists")
			return errors.Wrap(err, "could not check if lock exists")
		}

		err = tx.Create(l.lockDoc(key), struct{}{})
		if err != nil {
			fmt.Println("could not acquire lock")
			return errors.Wrap(err, "could not acquire lock")
		}

		return nil
	}, firestore.MaxAttempts(1))
	if transactionErr != nil {
		fmt.Printf("could not acquire lock: transaction, %s\n", transactionErr)
		return nil, errors.Wrap(transactionErr, "could not acquire lock")
	}

	// lock acquired
	fmt.Println("lock acquired")

	return func(ctx context.Context) error {
		return l.Unlock(ctx, key)
	}, nil
}

func (l *MessageLocker) Unlock(ctx context.Context, key string) error {
	_, err := l.lockDoc(key).Delete(ctx)
	return err
}

func (l *MessageLocker) lockDoc(key string) *firestore.DocumentRef {
	return l.client.Doc(fmt.Sprintf("locks/%s", key))
}
