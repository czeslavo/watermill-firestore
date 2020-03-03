package firestore_test

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/google/uuid"
	"os"
	"testing"
	"time"

	watermillFirestore "github.com/czeslavo/watermill-firestore/pkg/firestore"
	"github.com/stretchr/testify/require"
)

func TestLock(t *testing.T) {
	client, err := firestore.NewClient(context.Background(), os.Getenv("FIRESTORE_PROJECT_ID"))
	require.NoError(t, err)

	locker := watermillFirestore.NewMessageLocker(client)

	docPath := "first_to_lock"
	ctx := context.Background()

	// first consumer tries to acquire lock successfully
	unlock, err := locker.Lock(ctx, docPath)
	require.NoError(t, err)
	defer unlock(ctx)

	// second consumer fails to acquire lock
	_, err = locker.Lock(ctx, docPath)
	require.Error(t, err)

	// first consumer unlocks
	unlock(ctx)

	// lock is acquirable again
	unlock, err = locker.Lock(ctx, docPath)
	require.NoError(t, err)
	unlock(ctx)
}

func generateUUIDs(n int) []string {
	var uuids []string
	for i := 0; i < n; i++ {
		uuids = append(uuids, uuid.New().String())
	}
	return uuids
}

func TestLock_parallel(t *testing.T) {
	client, err := firestore.NewClient(context.Background(), os.Getenv("FIRESTORE_PROJECT_ID"))
	require.NoError(t, err)

	locker := watermillFirestore.NewMessageLocker(client)
	workQueueLength := 1000
	workersCount := 2
	beginWorkCh := make(chan struct{})
	var acquiredLocksChs []chan int

	uuids := generateUUIDs(workQueueLength)
	//unlockAll(t, locker, uuids)

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

func unlockAll(t *testing.T, locker *watermillFirestore.MessageLocker, uuids []string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	for _, uuid := range uuids {
		t.Log(uuid)
		err := locker.Unlock(ctx, uuid)
		require.NoError(t, err)
	}
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
				continue
			}
			acquiredLocksCounter++
		}

		acquiredLocksCh <- acquiredLocksCounter
	}()

	return acquiredLocksCh
}
