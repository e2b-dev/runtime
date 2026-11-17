package redis

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	storage_redis "github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/storage/redis"
)

// setupReservationStorageWithoutSubManager wires the reservation store so that
// PubSub messages are never delivered in-process. Used to exercise the safety-net path.
func setupReservationStorageWithoutSubManager(t *testing.T) (*ReservationStorage, goredis.UniversalClient) {
	t.Helper()

	client := redis_utils.SetupInstance(t)

	storageInstance := newTestSandboxStorage(t, client)
	t.Cleanup(func() { storageInstance.Close(context.WithoutCancel(t.Context())) })

	storage := NewReservationStorage(client, storageInstance.Notifier())

	return storage, client
}

func TestWaitForStart_WokenByFinishStartPublish(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestReservationStorage(t)
	teamID := uuid.New()
	sbxID := "pubsub-finish"

	reserved, err := storage.Reserve(t.Context(), teamID, sbxID, 10)
	require.NoError(t, err)
	require.True(t, reserved)

	reserved, err = storage.Reserve(t.Context(), teamID, sbxID, 10)
	require.NoError(t, err)
	require.False(t, reserved)

	waiterDone := make(chan struct{})
	var got []byte
	var waitErr error
	go func() {
		got, waitErr = storage.WaitForStart(t.Context(), teamID, sbxID)
		close(waiterDone)
	}()

	waitForSubscribers(t, storage, 1)

	start := time.Now()
	require.NoError(t, storage.FinishStart(t.Context(), teamID, sbxID, testResult))

	select {
	case <-waiterDone:
		elapsed := time.Since(start)
		require.NoError(t, waitErr)
		assert.Equal(t, testResult, got)
		assert.Less(t, elapsed, 500*time.Millisecond,
			"waiter should wake via PubSub, not the fallback ticker")
	case <-time.After(3 * time.Second):
		require.Fail(t, "waiter did not wake in time")
	}
}

func TestWaitForStart_WokenByReleasePublish(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestReservationStorage(t)
	teamID := uuid.New()
	sbxID := "pubsub-release"

	reserved, err := storage.Reserve(t.Context(), teamID, sbxID, 10)
	require.NoError(t, err)
	require.True(t, reserved)

	reserved, err = storage.Reserve(t.Context(), teamID, sbxID, 10)
	require.NoError(t, err)
	require.False(t, reserved)

	waiterErr := make(chan error, 1)
	go func() {
		_, err := storage.WaitForStart(t.Context(), teamID, sbxID)
		waiterErr <- err
	}()
	waitForSubscribers(t, storage, 1)

	start := time.Now()
	require.NoError(t, storage.Release(t.Context(), teamID, sbxID))

	select {
	case err := <-waiterErr:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no longer pending")
		assert.Less(t, time.Since(start), 500*time.Millisecond,
			"release should wake the waiter via PubSub")
	case <-time.After(3 * time.Second):
		require.Fail(t, "waiter did not wake in time")
	}
}

func TestWaitForStart_FallbackTickerWhenPubSubMissed(t *testing.T) {
	t.Parallel()

	storage, _ := setupReservationStorageWithoutSubManager(t)
	teamID := uuid.New()
	sbxID := "pubsub-fallback"

	reserved, err := storage.Reserve(t.Context(), teamID, sbxID, 10)
	require.NoError(t, err)
	require.True(t, reserved)

	reserved, err = storage.Reserve(t.Context(), teamID, sbxID, 10)
	require.NoError(t, err)
	require.False(t, reserved)

	// Set the result directly via the finishStartScript path; with the
	// subManager not running, no fan-out occurs. The waiter must rely on
	// the fallback ticker.
	teamIDStr := teamID.String()
	err = finishStartScript.Run(t.Context(), storage.redisClient,
		[]string{getPendingSetKey(teamIDStr), getResultKey(teamIDStr, sbxID)},
		sbxID, testResult, int(resultTTL.Seconds()),
	).Err()
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, err := storage.WaitForStart(t.Context(), teamID, sbxID)
		done <- err
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(fallbackPollInterval + 2*time.Second):
		require.Fail(t, "fallback ticker did not resolve the wait")
	}
}

// TestWaitForStart_ResultLandedBeforeSubscribe covers the race where the
// producer finishes BEFORE the waiter calls WaitForStart. The initial
// post-subscribe GET must catch the already-present result rather than
// blocking until a wakeup that will never come.
func TestWaitForStart_ResultLandedBeforeSubscribe(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestReservationStorage(t)
	teamID := uuid.New()
	sbxID := "pubsub-race"

	_, err := storage.Reserve(t.Context(), teamID, sbxID, 10)
	require.NoError(t, err)

	_, err = storage.Reserve(t.Context(), teamID, sbxID, 10)
	require.NoError(t, err)

	// Finish BEFORE the waiter ever subscribes.
	require.NoError(t, storage.FinishStart(t.Context(), teamID, sbxID, testResult))
	// Give the in-process publisher worker a moment to drain.
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	var got []byte
	var waitErr error
	start := time.Now()
	go func() {
		got, waitErr = storage.WaitForStart(t.Context(), teamID, sbxID)
		close(done)
	}()

	select {
	case <-done:
		require.NoError(t, waitErr)
		assert.Equal(t, testResult, got)
		assert.Less(t, time.Since(start), 200*time.Millisecond,
			"initial post-subscribe check must catch a pre-existing result")
	case <-time.After(3 * time.Second):
		require.Fail(t, "waiter did not return")
	}
}

// TestWaitForStart_ContextCancellation asserts ctx.Done is the only path
// out under normal load; cancellation must unblock immediately.
func TestWaitForStart_ContextCancellation(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestReservationStorage(t)
	teamID := uuid.New()
	sbxID := "pubsub-cancel"

	_, err := storage.Reserve(t.Context(), teamID, sbxID, 10)
	require.NoError(t, err)

	reserved, err := storage.Reserve(t.Context(), teamID, sbxID, 10)
	require.NoError(t, err)
	require.False(t, reserved)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = storage.WaitForStart(ctx, teamID, sbxID)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, 500*time.Millisecond, "cancellation should be immediate")
}

// TestWaitForStart_MultipleWaitersOnePublish confirms the fan-out wakes
// every concurrent waiter from a single producer publish.
func TestWaitForStart_MultipleWaitersOnePublish(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestReservationStorage(t)
	teamID := uuid.New()
	sbxID := "pubsub-multi"
	const numWaiters = 10

	_, err := storage.Reserve(t.Context(), teamID, sbxID, 50)
	require.NoError(t, err)

	for range numWaiters {
		reserved, err := storage.Reserve(t.Context(), teamID, sbxID, 50)
		require.NoError(t, err)
		require.False(t, reserved)
	}

	var wg sync.WaitGroup
	errs := make([]error, numWaiters)
	completions := make([]time.Duration, numWaiters)
	for i := range numWaiters {
		wg.Go(func() {
			start := time.Now()
			_, errs[i] = storage.WaitForStart(t.Context(), teamID, sbxID)
			completions[i] = time.Since(start)
		})
	}

	waitForSubscribers(t, storage, numWaiters)
	require.NoError(t, storage.FinishStart(t.Context(), teamID, sbxID, testResult))

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		for i := range numWaiters {
			require.NoError(t, errs[i])
			assert.Less(t, completions[i], 600*time.Millisecond,
				"waiter %d should be woken by PubSub", i)
		}
	case <-time.After(3 * time.Second):
		require.Fail(t, "not all waiters completed")
	}
}

func TestWaitForStart_RedisOpsBounded(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)

	counter := &cmdCounter{}
	client.AddHook(counter)

	storageInstance := newTestSandboxStorage(t, client)
	go storageInstance.Start(t.Context())
	t.Cleanup(func() { storageInstance.Close(context.WithoutCancel(t.Context())) })

	storage := NewReservationStorage(client, storageInstance.Notifier())

	teamID := uuid.New()
	sbxID := "pubsub-bounded"

	_, err := storage.Reserve(t.Context(), teamID, sbxID, 10)
	require.NoError(t, err)

	_, err = storage.Reserve(t.Context(), teamID, sbxID, 10)
	require.NoError(t, err)

	waiterDone := make(chan error, 1)
	go func() {
		_, err := storage.WaitForStart(t.Context(), teamID, sbxID)
		waiterDone <- err
	}()

	time.Sleep(50 * time.Millisecond)
	counter.Reset() // start counting from after the waiter has subscribed

	time.Sleep(1500 * time.Millisecond)
	require.NoError(t, storage.FinishStart(t.Context(), teamID, sbxID, testResult))

	select {
	case err := <-waiterDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		require.Fail(t, "waiter did not return")
	}

	// Allow at most a handful of read ops: initial GET (+ ZSCORE legacy
	// safety net), maybe one fallback tick if it fired just before the
	// publish landed, and the post-wakeup GET. 10 is a generous ceiling
	// for the new design; the old design would blow past 50.
	got := counter.Reads()
	assert.LessOrEqual(t, got, 10,
		"expected bounded reads; got %d (regression: polling is back)", got)
}

// cmdCounter is a redis.Hook that counts read-side operations on the
// client. We only care about the waiter's reads, not the producer's
// writes, so we only count GET and ZSCORE.
type cmdCounter struct {
	mu  sync.Mutex
	get int
	zs  int
}

func (c *cmdCounter) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.get = 0
	c.zs = 0
}

func (c *cmdCounter) Reads() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.get + c.zs
}

func (c *cmdCounter) DialHook(next goredis.DialHook) goredis.DialHook {
	return next
}

func (c *cmdCounter) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		c.mu.Lock()
		switch cmd.Name() {
		case "get":
			c.get++
		case "zscore":
			c.zs++
		}
		c.mu.Unlock()

		return next(ctx, cmd)
	}
}

func (c *cmdCounter) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		c.mu.Lock()
		for _, cmd := range cmds {
			switch cmd.Name() {
			case "get":
				c.get++
			case "zscore":
				c.zs++
			}
		}
		c.mu.Unlock()

		return next(ctx, cmds)
	}
}

// Compile-time assertion that the storage Notifier satisfies the local
// Notifier seam. If this breaks, the orchestrator wiring needs to change too.
var _ Notifier = (*storage_redis.Notifier)(nil)
