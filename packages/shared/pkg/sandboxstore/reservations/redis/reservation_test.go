package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"golang.org/x/sync/errgroup"

	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
	storage_redis "github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/storage/redis"
)

const (
	testSandboxID = "test-sandbox-id"
)

var (
	testTeamID = uuid.New()
	testResult = []byte("result")
)

func setupTestReservationStorage(t *testing.T) (*ReservationStorage, goredis.UniversalClient) {
	t.Helper()
	client := redis_utils.SetupInstance(t)

	storageInstance := newTestSandboxStorage(t, client)
	go storageInstance.Start(t.Context())
	t.Cleanup(func() { storageInstance.Close(context.WithoutCancel(t.Context())) })

	notifier := storageInstance.Notifier()
	waitForNotifierLive(t, notifier)

	storage := NewReservationStorage(client, &subscribeCountingNotifier{Notifier: notifier})

	return storage, client
}

// waitForNotifierLive blocks until a publish reaches an in-process subscriber.
// Storage.Start subscribes to Redis asynchronously; without this, a test's
// first publish can land before the subscription and be dropped.
func waitForNotifierLive(t *testing.T, notifier Notifier) {
	t.Helper()

	probeKey := "notifier-probe:" + uuid.NewString()
	ch, cleanup := notifier.Subscribe(probeKey)
	defer cleanup()

	require.Eventually(t, func() bool {
		notifier.Publish(t.Context(), probeKey)

		select {
		case <-ch:
			return true
		case <-time.After(50 * time.Millisecond):
			return false
		}
	}, 10*time.Second, time.Millisecond, "storage notifier never delivered a publish")
}

// subscribeCountingNotifier counts Subscribe calls so a test can wait for its
// waiters to register before publishing.
type subscribeCountingNotifier struct {
	Notifier

	subscribed atomic.Int64
}

func (n *subscribeCountingNotifier) Subscribe(routingKey string) (<-chan struct{}, func()) {
	ch, cleanup := n.Notifier.Subscribe(routingKey)
	n.subscribed.Add(1)

	return ch, cleanup
}

// waitForSubscribers blocks until at least count waiters have subscribed on a
// storage built by setupTestReservationStorage.
func waitForSubscribers(t *testing.T, storage *ReservationStorage, count int64) {
	t.Helper()

	notifier, ok := storage.notifier.(*subscribeCountingNotifier)
	require.True(t, ok, "storage was not built by setupTestReservationStorage")

	require.Eventually(t, func() bool {
		return notifier.subscribed.Load() >= count
	}, 5*time.Second, time.Millisecond, "waiters did not subscribe")
}

func newTestSandboxStorage(t *testing.T, client goredis.UniversalClient) *storage_redis.Storage {
	t.Helper()

	storageInstance, err := storage_redis.NewStorage(client, noop.NewMeterProvider())
	require.NoError(t, err)

	return storageInstance
}

func TestReservation(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	reserved, err := storage.Reserve(t.Context(), testTeamID, testSandboxID, 1)
	require.NoError(t, err)
	assert.True(t, reserved)
}

func TestReservation_Exceeded(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()
	_, err := storage.Reserve(t.Context(), teamID, testSandboxID, 1)
	require.NoError(t, err)
	_, err = storage.Reserve(t.Context(), teamID, "sandbox-2", 1)
	require.ErrorAs(t, err, new(&sandboxtypes.LimitExceededError{}))
}

func TestReservation_SameSandbox(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()
	_, err := storage.Reserve(t.Context(), teamID, testSandboxID, 1)
	require.NoError(t, err)

	reserved, err := storage.Reserve(t.Context(), teamID, testSandboxID, 1)
	require.NoError(t, err)
	assert.False(t, reserved)
}

func TestReservation_Release(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()
	_, err := storage.Reserve(t.Context(), teamID, testSandboxID, 1)
	require.NoError(t, err)
	err = storage.Release(t.Context(), teamID, testSandboxID)
	require.NoError(t, err)

	_, err = storage.Reserve(t.Context(), teamID, testSandboxID, 1)
	assert.NoError(t, err)
}

func TestWaitForStart_ReleasedStartIsResultMissing(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()
	reserved, err := storage.Reserve(t.Context(), teamID, testSandboxID, 1)
	require.NoError(t, err)
	require.True(t, reserved)
	require.NoError(t, storage.Release(t.Context(), teamID, testSandboxID))

	_, err = storage.WaitForStart(t.Context(), teamID, testSandboxID)
	require.Error(t, err)
	assert.Equal(t, "sandbox "+testSandboxID+" is no longer pending and has no result", err.Error())
	missing, ok := errors.AsType[*sandboxtypes.StartResultMissingError](err)
	require.True(t, ok)
	assert.Equal(t, testSandboxID, missing.SandboxID)
}

func TestReservation_MultipleWaiters(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()
	reserved, err := storage.Reserve(t.Context(), teamID, testSandboxID, 10)
	require.NoError(t, err)
	require.True(t, reserved)

	// Multiple calls should all join the pending start
	reserved, err = storage.Reserve(t.Context(), teamID, testSandboxID, 10)
	require.NoError(t, err)
	require.False(t, reserved)

	reserved, err = storage.Reserve(t.Context(), teamID, testSandboxID, 10)
	require.NoError(t, err)
	require.False(t, reserved)

	// Finish the start operation
	require.NoError(t, storage.FinishStart(t.Context(), teamID, testSandboxID, testResult))

	// All waiters should get the result
	result1, err := storage.WaitForStart(t.Context(), teamID, testSandboxID)
	require.NoError(t, err)
	assert.Equal(t, testResult, result1)

	result2, err := storage.WaitForStart(t.Context(), teamID, testSandboxID)
	require.NoError(t, err)
	assert.Equal(t, testResult, result2)
}

func TestReservation_Remove(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()
	reserved, err := storage.Reserve(t.Context(), teamID, testSandboxID, 1)
	require.NoError(t, err)
	require.True(t, reserved)

	require.NoError(t, storage.FinishStart(t.Context(), teamID, testSandboxID, testResult))

	// Remove the reservation
	err = storage.Release(t.Context(), teamID, testSandboxID)
	require.NoError(t, err)

	// Should be able to reserve again
	reserved, err = storage.Reserve(t.Context(), teamID, testSandboxID, 1)
	require.NoError(t, err)
	require.True(t, reserved)
}

func TestReservation_MultipleTeams(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	team1 := uuid.New()
	team2 := uuid.New()
	sandbox1 := "sandbox-1"
	sandbox2 := "sandbox-2"

	// Reserve for team1
	_, err := storage.Reserve(t.Context(), team1, sandbox1, 1)
	require.NoError(t, err)

	// Should not affect team2's limit
	_, err = storage.Reserve(t.Context(), team2, sandbox2, 1)
	require.NoError(t, err)

	// team1 should be at limit
	_, err = storage.Reserve(t.Context(), team1, "sandbox-3", 1)
	require.ErrorAs(t, err, new(&sandboxtypes.LimitExceededError{}))

	// team2 should also be at limit
	_, err = storage.Reserve(t.Context(), team2, "sandbox-4", 1)
	require.ErrorAs(t, err, new(&sandboxtypes.LimitExceededError{}))
}

func TestReservation_FailedStart(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()
	sbxID := "failed-sandbox"

	// Reserve sandbox
	reserved, err := storage.Reserve(t.Context(), teamID, sbxID, 10)
	require.NoError(t, err)
	require.True(t, reserved)

	// Finish with a failure result — this should auto-release
	require.NoError(t, storage.FinishStart(t.Context(), teamID, sbxID, []byte("start failed")))

	// After failed start, should be able to reserve again
	reserved, err = storage.Reserve(t.Context(), teamID, sbxID, 10)
	require.NoError(t, err)
	require.True(t, reserved)
}

func TestReservation_FailedStartWithWaiters(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()
	sbxID := "failed-with-waiters"
	numWaiters := 10
	failure := []byte("start failed")

	// First reservation
	reserved, err := storage.Reserve(t.Context(), teamID, sbxID, 100)
	require.NoError(t, err)
	require.True(t, reserved)

	var wg errgroup.Group

	// Multiple waiters
	for range numWaiters {
		wg.Go(func() error {
			reserved, err := storage.Reserve(t.Context(), teamID, sbxID, 100)
			if err != nil {
				return err
			}
			if reserved {
				return errors.New("waiter should not own the start")
			}

			return nil
		})
	}

	err = wg.Wait()
	require.NoError(t, err)

	// Finish with a failure result
	require.NoError(t, storage.FinishStart(t.Context(), teamID, sbxID, failure))

	// All waiters should receive the failure result
	var wg2 sync.WaitGroup
	var failureCount atomic.Int32

	for range numWaiters {
		wg2.Go(func() {
			result, err := storage.WaitForStart(t.Context(), teamID, sbxID)
			if err == nil && string(result) == string(failure) {
				failureCount.Add(1)
			}
		})
	}

	wg2.Wait()
	assert.Equal(t, int32(numWaiters), failureCount.Load())
}

func TestReservation_ConcurrentReservations(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()
	concurrency := 100
	limit := 50

	var wg sync.WaitGroup
	var successCount atomic.Int32
	var limitExceededCount atomic.Int32

	for i := range concurrency {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sandboxID := fmt.Sprintf("sandbox-%d", idx)
			_, err := storage.Reserve(t.Context(), teamID, sandboxID, limit)
			if err == nil {
				successCount.Add(1)
			} else {
				if _, ok := errors.AsType[*sandboxtypes.LimitExceededError](err); ok {
					limitExceededCount.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()

	// Should have exactly 50 successful reservations and 50 limit exceeded errors
	assert.Equal(t, int32(limit), successCount.Load())
	assert.Equal(t, int32(concurrency)-int32(limit), limitExceededCount.Load())
}

func TestReservation_ConcurrentSameSandbox(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()
	sbxID := "concurrent-sandbox"
	concurrency := 50

	var wg errgroup.Group
	var reservedCount atomic.Int32
	var pendingCount atomic.Int32

	// Multiple goroutines try to reserve the same sandbox
	for range concurrency {
		wg.Go(func() error {
			reserved, err := storage.Reserve(t.Context(), teamID, sbxID, 10)
			if err != nil {
				return err
			}

			if reserved {
				reservedCount.Add(1)
			} else {
				pendingCount.Add(1)
			}

			return nil
		})
	}

	err := wg.Wait()
	require.NoError(t, err)

	// Only one should own the start, all others should join it
	assert.Equal(t, int32(1), reservedCount.Load())
	assert.Equal(t, int32(concurrency-1), pendingCount.Load())
}

func TestReservation_ConcurrentWaitAndFinish(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()
	sbxID := "wait-finish-sandbox"
	numWaiters := 20

	// First goroutine reserves
	reserved, err := storage.Reserve(t.Context(), teamID, sbxID, 1)
	require.NoError(t, err)
	require.True(t, reserved)

	var wg errgroup.Group

	// Multiple waiters
	for range numWaiters {
		wg.Go(func() error {
			reserved, err := storage.Reserve(t.Context(), teamID, sbxID, 1)
			if err != nil {
				return err
			}
			if reserved {
				return errors.New("waiter should not own the start")
			}

			return nil
		})
	}

	err = wg.Wait()
	require.NoError(t, err)

	// Finish the start operation
	require.NoError(t, storage.FinishStart(t.Context(), teamID, sbxID, testResult))

	// All waiters should receive the result
	var wg2 sync.WaitGroup
	var successCount atomic.Int32

	for range numWaiters {
		wg2.Go(func() {
			result, err := storage.WaitForStart(t.Context(), teamID, sbxID)
			if err == nil && string(result) == string(testResult) {
				successCount.Add(1)
			}
		})
	}

	wg2.Wait()
	assert.Equal(t, int32(numWaiters), successCount.Load())
}

func TestReservation_RaceConditionStressTest(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()
	numOperations := 2000
	numSandboxes := 100
	limit := 5

	var wg sync.WaitGroup
	var operationCount atomic.Int32

	// Mix of reserve, remove, and finish operations
	for i := range numOperations {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sbxID := fmt.Sprintf("sandbox-%d", idx%numSandboxes)

			switch idx % 3 {
			case 0:
				// Reserve
				reserved, err := storage.Reserve(t.Context(), teamID, sbxID, limit)
				if err == nil {
					operationCount.Add(1)
					if reserved {
						// Immediately finish
						go func() {
							time.Sleep(time.Millisecond)
							_ = storage.FinishStart(context.WithoutCancel(t.Context()), teamID, sbxID, testResult)
						}()
					}
				} else {
					var limitExceededError *sandboxtypes.LimitExceededError
					if errors.As(err, &limitExceededError) || errors.Is(err, sandboxtypes.ErrAlreadyExists) {
						operationCount.Add(1)
					}
				}
			case 1:
				// Remove
				_ = storage.Release(t.Context(), teamID, sbxID)
				operationCount.Add(1)
			case 2:
				// Reserve again
				_, _ = storage.Reserve(t.Context(), teamID, sbxID, limit)
				operationCount.Add(1)
			}
		}(i)
	}

	wg.Wait()
	assert.Equal(t, int32(numOperations), operationCount.Load())
}

func TestReservation_ResultKeyTTL(t *testing.T) {
	t.Parallel()
	storage, client := setupTestReservationStorage(t)

	teamID := uuid.New()
	sbxID := "ttl-test"

	reserved, err := storage.Reserve(t.Context(), teamID, sbxID, 10)
	require.NoError(t, err)
	require.True(t, reserved)

	require.NoError(t, storage.FinishStart(t.Context(), teamID, sbxID, testResult))

	// Result key should exist with a TTL
	resultKeyStr := getResultKey(teamID.String(), sbxID)
	ttl, err := client.TTL(t.Context(), resultKeyStr).Result()
	require.NoError(t, err)
	assert.Greater(t, ttl, time.Duration(0))
	assert.LessOrEqual(t, ttl, resultTTL)
}

func TestReservation_StalePendingCleanup(t *testing.T) {
	t.Parallel()
	_, client := setupTestReservationStorage(t)

	teamID := uuid.New()
	pendingSetKey := getPendingSetKey(teamID.String())

	// Simulate a crashed API instance by manually inserting a stale pending entry
	// with a timestamp well in the past (3 minutes ago, beyond the 90-second StaleTTL)
	staleTimestamp := float64(time.Now().Add(-3 * time.Minute).Unix())
	err := client.ZAdd(t.Context(), pendingSetKey, goredis.Z{
		Score:  staleTimestamp,
		Member: "orphaned-sandbox",
	}).Err()
	require.NoError(t, err)

	// Verify it's there
	count, err := client.ZCard(t.Context(), pendingSetKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// Create a new storage instance (simulating a fresh/restarted API)
	storageInstance := newTestSandboxStorage(t, client)
	go storageInstance.Start(t.Context())
	t.Cleanup(func() { storageInstance.Close(context.WithoutCancel(t.Context())) })
	storage := NewReservationStorage(client, storageInstance.Notifier())

	// Reserve with limit=1 — this should succeed because the stale entry
	// gets cleaned up by the reserveScript before counting
	reserved, err := storage.Reserve(t.Context(), teamID, "new-sandbox", 1)
	require.NoError(t, err)
	require.True(t, reserved)

	// The stale entry should be gone
	count, err = client.ZCard(t.Context(), pendingSetKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), count) // only the new entry remains

	// Verify the orphaned sandbox is no longer in the set
	score := client.ZScore(t.Context(), pendingSetKey, "orphaned-sandbox")
	assert.ErrorIs(t, score.Err(), goredis.Nil)
}
