package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

type MessageLocker struct {
	client *firestore.Client
}

func NewMessageLocker(client *firestore.Client) *MessageLocker {
	return &MessageLocker{client}
}

func (l *MessageLocker) Lock(ctx context.Context, key string) (func(ctx context.Context) error, error) {
	start := time.Now()
	_, err := l.lockDoc(key).Get(ctx)
	if err != nil && status.Code(err) != codes.NotFound {
		return nil, errors.Wrap(err, "could not acquire lock")
	}
	if err == nil {
		return nil, errors.New("lock already acquired")
	}

	_, err = l.lockDoc(key).Create(ctx, struct{}{})
	if err != nil {
		return nil, errors.Wrap(err, "lock probably has been created in the meantime")
	}

	// lock acquired
	fmt.Printf("lock acquired in %s\n", time.Now().Sub(start))

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
