package firestore

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type lock struct {
	ExpirationTime time.Time `firestore:"expiration_time"`
}

type MessageLocker struct {
	client *firestore.Client
}

func NewMessageLocker(client *firestore.Client) *MessageLocker {
	return &MessageLocker{client}
}

func (l *MessageLocker) Lock(ctx context.Context, key string, expireAfter time.Duration) (bool, error) {
	start := time.Now()
	_, err := l.lockDoc(key).Create(ctx, lock{
		ExpirationTime: time.Now().Add(expireAfter),
	})
	if status.Code(err) == codes.AlreadyExists {
		// lock has been created in the meantime
		doc, err := l.lockDoc(key).Get(ctx)
		if err != nil {
			return false, errors.Wrap(err, "failed to get lock when it exist")
		}
		lock := &lock{}
		if err := doc.DataTo(lock); err != nil {
			return false, errors.Wrap(err, "could not unmarshal lock")
		}
		if lock.ExpirationTime.After(time.Now()) {
			return false, nil
		}

		lock.ExpirationTime = time.Now().Add(expireAfter)

		if _, err := l.lockDoc(key).Set(ctx, lock); err != nil {
			return false, errors.Wrap(err, "could not make lock longer")
		}

		// acquired lock by extending expiration time
		return true, nil
	}
	if err != nil {
		return false, errors.Wrap(err, "could not create lock document")
	}

	// lock acquired
	fmt.Printf("lock acquired in %s\n", time.Now().Sub(start))
	return true, nil
}

func (l *MessageLocker) Unlock(ctx context.Context, key string) error {
	_, err := l.lockDoc(key).Delete(ctx)
	return err
}

func (l *MessageLocker) lockDoc(key string) *firestore.DocumentRef {
	return l.client.Doc(fmt.Sprintf("locks/%s", key))
}
