package firestore_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"

	"cloud.google.com/go/firestore"
	"github.com/google/uuid"

	watermillFirestore "github.com/czeslavo/watermill-firestore/pkg/firestore"
	"github.com/stretchr/testify/require"
)

func TestLock(t *testing.T) {
	client, err := firestore.NewClient(context.Background(), os.Getenv("FIRESTORE_PROJECT_ID"))
	require.NoError(t, err)

	locker := watermillFirestore.NewMessageLocker(client)

	docPath := watermill.NewULID()
	ctx := context.Background()

	// first consumer tries to acquire lock successfully
	locked, err := locker.Lock(ctx, docPath)
	require.NoError(t, err)
	require.True(t, locked)

	// second consumer fails to acquire lock
	locked, err = locker.Lock(ctx, docPath)
	require.NoError(t, err)
	require.False(t, locked)

	// first consumer unlocks
	locker.Unlock(ctx, docPath)

	// lock is acquirable again
	locked, err = locker.Lock(ctx, docPath)
	require.NoError(t, err)
	require.True(t, locked)
}

func generateUUIDs(n int) []string {
	var uuids []string
	for i := 0; i < n; i++ {
		uuids = append(uuids, uuid.New().String())
	}
	return uuids
}

func TestLock_parallel(t *testing.T) {
	t.Skip()
	client, err := firestore.NewClient(context.Background(), os.Getenv("FIRESTORE_PROJECT_ID"))
	require.NoError(t, err)

	locker := watermillFirestore.NewMessageLocker(client)
	workQueueLength := 1000
	workersCount := 2
	beginWorkCh := make(chan struct{})
	var acquiredLocksChs []chan int

	uuids := generateUUIDs(workQueueLength)

	for i := 0; i < workersCount; i++ {
		acquiredLocksChs = append(acquiredLocksChs, lockingWorker(t, locker, uuids, beginWorkCh))
	}

	close(beginWorkCh)

	acquiredLocksSum := 0
	for _, acquiredLocksCh := range acquiredLocksChs {
		acquiredLocksSum += <-acquiredLocksCh
	}

	require.Equal(t, workQueueLength, acquiredLocksSum)
}

func lockingWorker(t *testing.T, locker *watermillFirestore.MessageLocker, uuids []string, beginWorkCh chan struct{}) chan int {
	t.Helper()

	acquiredLocksCh := make(chan int)
	go func() {
		acquiredLocksCounter := 0
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
		defer cancel()

		// wait so all workers will start at the same time
		<-beginWorkCh

		for _, uuid := range uuids {
			_, err := locker.Lock(ctx, uuid)
			if err != nil {
				t.Logf("not acquired: %s", err)
				continue
			}
			acquiredLocksCounter++
		}

		acquiredLocksCh <- acquiredLocksCounter
	}()

	return acquiredLocksCh
}
