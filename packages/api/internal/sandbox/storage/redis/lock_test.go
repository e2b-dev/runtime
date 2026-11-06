package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsm/redislock"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

const (
	testLockTimeout         = 30 * time.Minute
	testWaitTimeout         = time.Second
	testPollInterval        = 10 * time.Millisecond
	testPubSubWakeupTimeout = 135 * time.Millisecond
)

// testPubSubWakeupTimeout bounds the waiter's Obtain in the test asserting
// the pub/sub wake-up path. It has to satisfy two constraints:
//   - long enough for the register/release/notify/re-acquire choreography
//     (several Redis round-trips plus goroutine scheduling) on small, loaded
//     CI runners - the previous 25ms flaked regularly on 4vcpu machines;
//   - strictly below the earliest possible backoff retry,
//     lockRetryMinInterval*(1-lockRetryJitter), so the waiter can only
//     succeed via the pub/sub notification: if lock holders stop publishing
//     release notifications, the test still fails instead of passing via
//     the polling fallback.
func TestStorageLocker_ObtainAfterReleaseNotification(t *testing.T) {
	t.Parallel()

	locker, subManager := setupTestLocker(t, true)

	key := getSandboxKey(uuid.NewString(), "lock-notification")
	lockKey := redis_utils.GetLockKey(key)
	routingKey := getLockRoutingKey(lockKey)

	lock, err := locker.Obtain(t.Context(), lockKey, testLockTimeout)
	require.NoError(t, err)

	waiterDone := make(chan error, 1)
	go func() {
		waiterLock, obtainErr := locker.Obtain(t.Context(), lockKey, testLockTimeout)
		if obtainErr != nil {
			waiterDone <- obtainErr

			return
		}

		waiterDone <- waiterLock.Release(context.WithoutCancel(t.Context()))
	}()

	waitForLockWaiter(t, subManager, routingKey, waiterDone)

	require.NoError(t, lock.Release(context.WithoutCancel(t.Context())))
	requireNoErrorFromChannel(t, waiterDone)
}

func TestStorageLocker_ObtainTimesOutWhenHeld(t *testing.T) {
	t.Parallel()

	locker, _ := setupTestLocker(t, true)
	lockKey := redis_utils.GetLockKey(getSandboxKey(uuid.NewString(), "lock-timeout"))

	lock, err := locker.Obtain(t.Context(), lockKey, testLockTimeout)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = lock.Release(context.WithoutCancel(t.Context()))
	})

	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()

	_, err = locker.Obtain(ctx, lockKey, testLockTimeout)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestStorageLocker_ObtainUsesProvidedContext(t *testing.T) {
	t.Parallel()

	locker, subManager := setupTestLocker(t, true)
	lockKey := redis_utils.GetLockKey(getSandboxKey(uuid.NewString(), "lock-parent-deadline"))
	routingKey := getLockRoutingKey(lockKey)

	lock, err := locker.Obtain(t.Context(), lockKey, testLockTimeout)
	require.NoError(t, err)

	waiterDone := make(chan error, 1)
	go func() {
		waiterLock, obtainErr := locker.Obtain(t.Context(), lockKey, testPubSubWakeupTimeout)
		if obtainErr != nil {
			waiterDone <- obtainErr

			return
		}

		waiterDone <- waiterLock.Release(context.WithoutCancel(t.Context()))
	}()

	waitForLockWaiter(t, subManager, routingKey, waiterDone)

	require.NoError(t, lock.Release(context.WithoutCancel(t.Context())))
	requireNoErrorFromChannel(t, waiterDone)
}

func TestStorageLocker_ObtainReturnsContextCancellation(t *testing.T) {
	t.Parallel()

	locker, _ := setupTestLocker(t, true)
	lockKey := redis_utils.GetLockKey(getSandboxKey(uuid.NewString(), "lock-context-cancel"))

	lock, err := locker.Obtain(t.Context(), lockKey, testLockTimeout)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = lock.Release(context.WithoutCancel(t.Context()))
	})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = locker.Obtain(ctx, lockKey, testLockTimeout)
	require.ErrorIs(t, err, context.Canceled)
}

func TestStorageLocker_ObtainFallsBackWhenNotificationMissed(t *testing.T) {
	t.Parallel()

	// Do not start the subscription manager. This simulates a missed PubSub
	// notification and verifies the exponential fallback still makes progress.
	locker, subManager := setupTestLocker(t, false)

	lockKey := redis_utils.GetLockKey(getSandboxKey(uuid.NewString(), "lock-fallback"))
	routingKey := getLockRoutingKey(lockKey)

	lock, err := locker.Obtain(t.Context(), lockKey, testLockTimeout)
	require.NoError(t, err)

	waiterDone := make(chan error, 1)
	go func() {
		waiterLock, obtainErr := locker.Obtain(t.Context(), lockKey, testLockTimeout)
		if obtainErr != nil {
			waiterDone <- obtainErr

			return
		}

		waiterDone <- waiterLock.Release(context.WithoutCancel(t.Context()))
	}()

	waitForLockWaiter(t, subManager, routingKey, waiterDone)
	require.NoError(t, lock.Lock.Release(context.WithoutCancel(t.Context())))
	requireNoErrorFromChannel(t, waiterDone)
}

func TestStorageLock_ReleaseUsesProvidedContext(t *testing.T) {
	t.Parallel()

	locker, _ := setupTestLocker(t, false)
	lockKey := redis_utils.GetLockKey(getSandboxKey(uuid.NewString(), "lock-canceled-release"))
	routingKey := getLockRoutingKey(lockKey)

	pubsub := locker.redisClient.Subscribe(t.Context(), globalStorageNotifyChannel)
	t.Cleanup(func() { require.NoError(t, pubsub.Close()) })
	_, err := pubsub.Receive(t.Context())
	require.NoError(t, err)

	lock, err := locker.Obtain(t.Context(), lockKey, testLockTimeout)
	require.NoError(t, err)

	canceledCtx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, lock.Release(canceledCtx), context.Canceled)
	require.NoError(t, lock.Release(context.WithoutCancel(t.Context())))
	requirePubSubPayload(t, pubsub, routingKey)
}

func TestStorageLock_ReleaseReturnsErrorWhenLockAlreadyReleased(t *testing.T) {
	t.Parallel()

	locker, _ := setupTestLocker(t, true)
	lockKey := redis_utils.GetLockKey(getSandboxKey(uuid.NewString(), "lock-double-release"))

	lock, err := locker.Obtain(t.Context(), lockKey, testLockTimeout)
	require.NoError(t, err)
	require.NoError(t, lock.Release(context.WithoutCancel(t.Context())))
	require.ErrorIs(t, lock.Release(context.WithoutCancel(t.Context())), redislock.ErrLockNotHeld)
}

func TestStorageLocker_IgnoresUnrelatedNotification(t *testing.T) {
	t.Parallel()

	locker, subManager := setupTestLocker(t, true)
	lockKey := redis_utils.GetLockKey(getSandboxKey(uuid.NewString(), "lock-unrelated-notification"))
	routingKey := getLockRoutingKey(lockKey)

	lock, err := locker.Obtain(t.Context(), lockKey, testLockTimeout)
	require.NoError(t, err)

	waiterDone := make(chan error, 1)
	go func() {
		waiterLock, obtainErr := locker.Obtain(t.Context(), lockKey, testLockTimeout)
		if obtainErr != nil {
			waiterDone <- obtainErr

			return
		}

		waiterDone <- waiterLock.Release(context.WithoutCancel(t.Context()))
	}()

	waitForLockWaiter(t, subManager, routingKey, waiterDone)
	subManager.dispatch(getLockRoutingKey(redis_utils.GetLockKey("unrelated")))

	var unexpectedErr error
	require.Never(t, func() bool {
		select {
		case unexpectedErr = <-waiterDone:
			return true
		default:
			return false
		}
	}, 5*testPollInterval, testPollInterval, "waiter completed after unrelated notification: %v", unexpectedErr)

	require.NoError(t, lock.Release(context.WithoutCancel(t.Context())))
	requireNoErrorFromChannel(t, waiterDone)
}

func TestStorageLocker_AttemptsImmediatelyBeforeWaiting(t *testing.T) {
	t.Parallel()

	t.Run("initial success", func(t *testing.T) {
		t.Parallel()

		client := newRecordingLockClient(nil)
		locker, subManager := newScriptedStorageLocker(client, time.Hour)

		lock, err := locker.Obtain(t.Context(), "initial-success", testLockTimeout)

		require.NoError(t, err)
		require.NotNil(t, lock)
		require.Equal(t, 1, client.callCount())
		require.Empty(t, subManager.waiters)
	})

	t.Run("initial attempt", func(t *testing.T) {
		t.Parallel()

		initialErr := errors.New("initial lock error")
		client := newRecordingLockClient(initialErr)
		locker, _ := newScriptedStorageLocker(client, time.Hour)

		_, err := locker.Obtain(t.Context(), "initial", testLockTimeout)

		require.ErrorIs(t, err, initialErr)
		require.Equal(t, 1, client.callCount())
	})

	t.Run("post-subscribe recheck", func(t *testing.T) {
		t.Parallel()

		recheckErr := errors.New("recheck lock error")
		client := newRecordingLockClient(redislock.ErrNotObtained, recheckErr)
		locker, subManager := newScriptedStorageLocker(client, time.Hour)

		_, err := locker.Obtain(t.Context(), "recheck", testLockTimeout)

		require.ErrorIs(t, err, recheckErr)
		require.Equal(t, 2, client.callCount())
		require.Empty(t, subManager.waiters)
	})

	t.Run("post-subscribe recheck success", func(t *testing.T) {
		t.Parallel()

		client := newRecordingLockClient(redislock.ErrNotObtained, nil)
		locker, subManager := newScriptedStorageLocker(client, time.Hour)

		lock, err := locker.Obtain(t.Context(), "recheck-success", testLockTimeout)

		require.NoError(t, err)
		require.NotNil(t, lock)
		require.Equal(t, 2, client.callCount())
		require.Empty(t, subManager.waiters)
	})
}

func TestStorageLocker_NotificationRetryIsDelayedAndCoalesced(t *testing.T) {
	t.Parallel()

	// The scripted delay is long against what the test spends before each
	// window it opens inside the delay, so a loaded runner cannot push a
	// window into the retry; the windows and thresholds scale with it.
	const delay = 400 * time.Millisecond
	client := newRecordingLockClient(redislock.ErrNotObtained, redislock.ErrNotObtained)
	var samples atomic.Int32
	locker, subManager := newScriptedStorageLockerWithDelay(client, func() time.Duration {
		samples.Add(1)

		return delay
	})
	lockKey := "coalesced-notification"
	routingKey := getLockRoutingKey(lockKey)

	ctx, cancel := context.WithCancel(t.Context())
	result := obtainErrorAsync(locker, ctx, lockKey)
	waitForLockWaiter(t, subManager, routingKey, result)
	require.Equal(t, 2, client.callCount())

	notifiedAt := time.Now()
	subManager.dispatch(routingKey)
	require.Eventually(t, func() bool {
		return samples.Load() == 1 || client.callCount() >= 3
	}, testWaitTimeout, time.Millisecond)

	// Notifications that arrive during the pending delay must neither reset
	// its deadline nor create another acquisition attempt.
	for range 5 {
		time.Sleep(5 * time.Millisecond)
		subManager.dispatch(routingKey)
	}
	require.Never(t, func() bool { return client.callCount() >= 3 }, delay/3, time.Millisecond)
	require.Eventually(t, func() bool { return client.callCount() == 3 }, 2*delay, time.Millisecond)

	calls := client.callTimes()
	require.GreaterOrEqual(t, calls[2].Sub(notifiedAt), 3*delay/4)
	require.Equal(t, int32(1), samples.Load())

	// Any stale buffered notification was drained before the failed attempt;
	// it cannot trigger a back-to-back retry. The window ends before the
	// fallback's earliest retry, lockRetryMinInterval less its jitter.
	require.Never(t, func() bool { return client.callCount() > 3 }, 60*time.Millisecond, time.Millisecond)
	require.Equal(t, int32(1), samples.Load())

	// A notification after the stale-signal drain is a new signal. It
	// preempts the fallback, but still receives a fresh full delay.
	notifiedAgainAt := time.Now()
	subManager.dispatch(routingKey)
	require.Eventually(t, func() bool { return samples.Load() == 2 }, testWaitTimeout, time.Millisecond)
	require.Never(t, func() bool { return client.callCount() >= 4 }, delay/3, time.Millisecond)
	require.Eventually(t, func() bool { return client.callCount() == 4 }, 2*delay, time.Millisecond)
	require.GreaterOrEqual(t, client.callTimes()[3].Sub(notifiedAgainAt), 3*delay/4)

	cancel()
	requireErrorFromChannel(t, result, context.Canceled)
}

func TestStorageLocker_NotificationDelaysAreIndependent(t *testing.T) {
	t.Parallel()

	clientA := newRecordingLockClient(redislock.ErrNotObtained, redislock.ErrNotObtained)
	clientB := newRecordingLockClient(redislock.ErrNotObtained, redislock.ErrNotObtained)
	var samplesA, samplesB atomic.Int32
	subManager := newSubscriptionManager(nil, globalStorageNotifyChannel)
	lockerA := newScriptedStorageLockerWithManager(clientA, subManager, func() time.Duration {
		samplesA.Add(1)

		return 30 * time.Millisecond
	})
	lockerB := newScriptedStorageLockerWithManager(clientB, subManager, func() time.Duration {
		samplesB.Add(1)

		return 90 * time.Millisecond
	})
	lockKey := "independent-notifications"
	routingKey := getLockRoutingKey(lockKey)

	ctx, cancel := context.WithCancel(t.Context())
	resultA := obtainErrorAsync(lockerA, ctx, lockKey)
	resultB := obtainErrorAsync(lockerB, ctx, lockKey)
	waitForRecordedCalls(t, clientA, 2)
	waitForRecordedCalls(t, clientB, 2)
	waitForRegisteredWaiters(t, subManager, routingKey, 2)

	notifiedAt := time.Now()
	subManager.dispatch(routingKey)
	waitForRecordedCalls(t, clientA, 3)
	waitForRecordedCalls(t, clientB, 3)

	delayA := clientA.callTimes()[2].Sub(notifiedAt)
	delayB := clientB.callTimes()[2].Sub(notifiedAt)
	require.GreaterOrEqual(t, delayA, 20*time.Millisecond)
	require.GreaterOrEqual(t, delayB, 70*time.Millisecond)
	require.Less(t, delayA, delayB)
	require.Equal(t, int32(1), samplesA.Load())
	require.Equal(t, int32(1), samplesB.Load())

	cancel()
	requireErrorFromChannel(t, resultA, context.Canceled)
	requireErrorFromChannel(t, resultB, context.Canceled)
}

func TestStorageLocker_NotificationRetryPreservesFallbackBase(t *testing.T) {
	t.Parallel()

	client := newRecordingLockClient(redislock.ErrNotObtained, redislock.ErrNotObtained)
	locker, subManager := newScriptedStorageLocker(client, 5*time.Millisecond)
	lockKey := "notification-fallback-base"
	routingKey := getLockRoutingKey(lockKey)

	ctx, cancel := context.WithCancel(t.Context())
	result := obtainErrorAsync(locker, ctx, lockKey)
	waitForLockWaiter(t, subManager, routingKey, result)
	subManager.dispatch(routingKey)
	waitForRecordedCalls(t, client, 3)
	waitForRecordedCalls(t, client, 4)

	calls := client.callTimes()
	// The failed notification attempt rearms the original 200ms fallback.
	// Advancing to 400ms here would have a minimum jittered delay of 300ms.
	require.Less(t, calls[3].Sub(calls[2]), 300*time.Millisecond)

	cancel()
	requireErrorFromChannel(t, result, context.Canceled)
}

func TestStorageLocker_FallbackRetainsDoublingAndCap(t *testing.T) {
	t.Parallel()

	client := newRecordingLockClient(redislock.ErrNotObtained, redislock.ErrNotObtained)
	locker, _ := newScriptedStorageLocker(client, time.Hour)

	ctx, cancel := context.WithCancel(t.Context())
	result := obtainErrorAsync(locker, ctx, "fallback-backoff")
	waitForRecordedCalls(t, client, 6)
	cancel()
	requireErrorFromChannel(t, result, context.Canceled)

	calls := client.callTimes()
	bases := []time.Duration{
		lockRetryMinInterval,
		2 * lockRetryMinInterval,
		4 * lockRetryMinInterval,
		lockRetryMaxInterval,
	}
	for i, base := range bases {
		interval := calls[i+2].Sub(calls[i+1])
		minDelay := time.Duration(float64(base)*(1-lockRetryJitter)) - 25*time.Millisecond
		maxDelay := time.Duration(float64(base)*(1+lockRetryJitter)) + 250*time.Millisecond
		require.GreaterOrEqual(t, interval, minDelay, "fallback interval %d", i)
		require.LessOrEqual(t, interval, maxDelay, "fallback interval %d", i)
	}
}

func TestStorageLocker_NotificationDelayHonorsCancellationAndDeadline(t *testing.T) {
	t.Parallel()

	t.Run("cancellation", func(t *testing.T) {
		t.Parallel()

		client := newRecordingLockClient(redislock.ErrNotObtained, redislock.ErrNotObtained)
		locker, subManager := newScriptedStorageLocker(client, time.Hour)
		lockKey := "notification-cancellation"

		ctx, cancel := context.WithCancel(t.Context())
		result := obtainErrorAsync(locker, ctx, lockKey)
		waitForLockWaiter(t, subManager, getLockRoutingKey(lockKey), result)
		subManager.dispatch(getLockRoutingKey(lockKey))
		cancel()

		requireErrorFromChannel(t, result, context.Canceled)
		require.Equal(t, 2, client.callCount())
	})

	t.Run("original deadline", func(t *testing.T) {
		t.Parallel()

		client := newRecordingLockClient(redislock.ErrNotObtained, redislock.ErrNotObtained)
		locker, subManager := newScriptedStorageLocker(client, time.Hour)
		lockKey := "notification-deadline"

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		result := obtainErrorAsync(locker, ctx, lockKey)
		waitForLockWaiter(t, subManager, getLockRoutingKey(lockKey), result)
		subManager.dispatch(getLockRoutingKey(lockKey))

		requireErrorFromChannel(t, result, context.DeadlineExceeded)
		require.Equal(t, 2, client.callCount())
	})
}

func TestStorageLocker_PropagatesPostNotificationError(t *testing.T) {
	t.Parallel()

	lockErr := errors.New("notification lock error")
	client := newRecordingLockClient(redislock.ErrNotObtained, redislock.ErrNotObtained, lockErr)
	locker, subManager := newScriptedStorageLocker(client, 5*time.Millisecond)
	lockKey := "post-notification-error"
	result := obtainErrorAsync(locker, t.Context(), lockKey)
	waitForLockWaiter(t, subManager, getLockRoutingKey(lockKey), result)

	subManager.dispatch(getLockRoutingKey(lockKey))
	requireErrorFromChannel(t, result, lockErr)
	require.Equal(t, 3, client.callCount())
}

func TestStorageLocker_IgnoresUnrelatedNotificationWithoutRedis(t *testing.T) {
	t.Parallel()

	client := newRecordingLockClient(redislock.ErrNotObtained, redislock.ErrNotObtained)
	var samples atomic.Int32
	locker, subManager := newScriptedStorageLockerWithDelay(client, func() time.Duration {
		samples.Add(1)

		return 5 * time.Millisecond
	})
	lockKey := "related-notification"

	ctx, cancel := context.WithCancel(t.Context())
	result := obtainErrorAsync(locker, ctx, lockKey)
	waitForLockWaiter(t, subManager, getLockRoutingKey(lockKey), result)
	subManager.dispatch(getLockRoutingKey("unrelated-notification"))

	require.Never(t, func() bool {
		return samples.Load() != 0 || client.callCount() != 2
	}, 50*time.Millisecond, time.Millisecond)

	cancel()
	requireErrorFromChannel(t, result, context.Canceled)
}

func TestStorageLocker_ConcurrentWaitersRemainExclusive(t *testing.T) {
	t.Parallel()

	const waiterCount = 32

	locker, subManager := setupTestLocker(t, true)
	lockKey := redis_utils.GetLockKey(getSandboxKey(uuid.NewString(), "concurrent-waiters"))
	routingKey := getLockRoutingKey(lockKey)
	incumbent, err := locker.Obtain(t.Context(), lockKey, testLockTimeout)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	results := make(chan error, waiterCount)
	entries := make([]atomic.Int32, waiterCount)
	var active, maxActive atomic.Int32
	for i := range waiterCount {
		go func() {
			lock, obtainErr := locker.Obtain(ctx, lockKey, testLockTimeout)
			if obtainErr != nil {
				results <- obtainErr

				return
			}

			entries[i].Add(1)
			current := active.Add(1)
			for {
				observed := maxActive.Load()
				if current <= observed || maxActive.CompareAndSwap(observed, current) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			active.Add(-1)
			results <- lock.Release(context.WithoutCancel(ctx))
		}()
	}

	waitForRegisteredWaiters(t, subManager, routingKey, waiterCount)
	require.NoError(t, incumbent.Release(context.WithoutCancel(t.Context())))
	for range waiterCount {
		select {
		case resultErr := <-results:
			require.NoError(t, resultErr)
		case <-ctx.Done():
			require.FailNow(t, "concurrent lock waiters did not complete", "err: %v", ctx.Err())
		}
	}

	require.Equal(t, int32(1), maxActive.Load())
	require.Zero(t, active.Load())
	for i := range waiterCount {
		require.Equal(t, int32(1), entries[i].Load(), "waiter %d critical-section entries", i)
	}
}

func TestObtainBothOrderAndRelease(t *testing.T) {
	t.Parallel()

	locker, subManager, recorder := setupRecordingTestLocker(t)
	teamID, sandboxID := uuid.NewString(), uuid.NewString()
	first := redis_utils.GetLockKey(recordKey(formTeam, teamID, sandboxID))
	second := redis_utils.GetLockKey(recordKey(formSandbox, teamID, sandboxID))

	holder, err := locker.Obtain(t.Context(), second, testLockTimeout)
	require.NoError(t, err)

	var release func(context.Context) error
	done := make(chan error, 1)
	go func() {
		var obtainErr error
		release, obtainErr = locker.ObtainBoth(t.Context(), first, second, testLockTimeout)
		done <- obtainErr
	}()

	waitForLockWaiter(t, subManager, getLockRoutingKey(second), done)
	require.True(t, lockHeld(t, locker, first))

	require.NoError(t, holder.Release(context.WithoutCancel(t.Context())))
	requireNoErrorFromChannel(t, done)
	require.True(t, lockHeld(t, locker, second))

	require.NoError(t, release(context.WithoutCancel(t.Context())))
	require.False(t, lockHeld(t, locker, first))
	require.False(t, lockHeld(t, locker, second))
	// The holder's release, then the pair's: the second lock before the first.
	require.Equal(t, []string{getLockRoutingKey(second), getLockRoutingKey(second), getLockRoutingKey(first)}, recorder.published())
}

func TestObtainBothSecondFailsReleasesFirst(t *testing.T) {
	t.Parallel()

	locker, _, recorder := setupRecordingTestLocker(t)
	teamID, sandboxID := uuid.NewString(), uuid.NewString()
	first := redis_utils.GetLockKey(recordKey(formTeam, teamID, sandboxID))
	second := redis_utils.GetLockKey(recordKey(formSandbox, teamID, sandboxID))

	holder, err := locker.Obtain(t.Context(), second, testLockTimeout)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = holder.Release(context.WithoutCancel(t.Context()))
	})

	// The deadline bounds the wait, not the lock TTL: a first lock freed by
	// its TTL would pass the check below without a release.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	release, err := locker.ObtainBoth(ctx, first, second, testLockTimeout)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, release)
	require.False(t, lockHeld(t, locker, first))
	require.True(t, lockHeld(t, locker, second))
	require.Equal(t, []string{getLockRoutingKey(first)}, recorder.published())
}

func TestObtainBothReleaseAttemptsBoth(t *testing.T) {
	t.Parallel()

	locker, _, recorder := setupRecordingTestLocker(t)
	teamID, sandboxID := uuid.NewString(), uuid.NewString()
	first := redis_utils.GetLockKey(recordKey(formTeam, teamID, sandboxID))
	second := redis_utils.GetLockKey(recordKey(formSandbox, teamID, sandboxID))

	release, err := locker.ObtainBoth(t.Context(), first, second, testLockTimeout)
	require.NoError(t, err)

	// The second lock is gone, as after its TTL.
	require.NoError(t, locker.redisClient.Del(t.Context(), second).Err())

	require.ErrorIs(t, release(context.WithoutCancel(t.Context())), redislock.ErrLockNotHeld)
	require.False(t, lockHeld(t, locker, first))
	require.Equal(t, []string{getLockRoutingKey(first)}, recorder.published())
}

// Once both locks are held, the first lease, which counted down while the
// second was awaited, is renewed to the full timeout.
func TestObtainBothRenewsFirstLease(t *testing.T) {
	t.Parallel()

	locker, subManager, _ := setupRecordingTestLocker(t)
	teamID, sandboxID := uuid.NewString(), uuid.NewString()
	first := redis_utils.GetLockKey(recordKey(formTeam, teamID, sandboxID))
	second := redis_utils.GetLockKey(recordKey(formSandbox, teamID, sandboxID))

	holder, err := locker.Obtain(t.Context(), second, testLockTimeout)
	require.NoError(t, err)

	var release func(context.Context) error
	done := make(chan error, 1)
	go func() {
		var obtainErr error
		release, obtainErr = locker.ObtainBoth(t.Context(), first, second, testLockTimeout)
		done <- obtainErr
	}()

	// The first lease has nearly run out by the time the second is freed.
	waitForLockWaiter(t, subManager, getLockRoutingKey(second), done)
	require.NoError(t, locker.redisClient.PExpire(t.Context(), first, time.Minute).Err())

	require.NoError(t, holder.Release(context.WithoutCancel(t.Context())))
	requireNoErrorFromChannel(t, done)
	ttl, err := locker.redisClient.PTTL(t.Context(), first).Result()
	require.NoError(t, err)
	require.Greater(t, ttl, testLockTimeout-testWaitTimeout, "the first lease was not renewed")
	require.NoError(t, release(context.WithoutCancel(t.Context())))
}

// A first lease that ran out while the second lock was awaited fails the
// call: both locks are given back and an error returned, not the crash
// sentinel, so the caller does not write under a lease another holder may
// have taken meanwhile.
func TestObtainBothFailsWhenFirstLeaseLost(t *testing.T) {
	t.Parallel()

	locker, subManager, _ := setupRecordingTestLocker(t)
	teamID, sandboxID := uuid.NewString(), uuid.NewString()
	first := redis_utils.GetLockKey(recordKey(formTeam, teamID, sandboxID))
	second := redis_utils.GetLockKey(recordKey(formSandbox, teamID, sandboxID))

	holder, err := locker.Obtain(t.Context(), second, testLockTimeout)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, obtainErr := locker.ObtainBoth(t.Context(), first, second, testLockTimeout)
		done <- obtainErr
	}()

	waitForLockWaiter(t, subManager, getLockRoutingKey(second), done)
	require.NoError(t, locker.redisClient.PExpire(t.Context(), first, time.Millisecond).Err())
	require.Eventually(t, func() bool {
		n, err := locker.redisClient.Exists(context.WithoutCancel(t.Context()), first).Result()

		return err == nil && n == 0
	}, testWaitTimeout, testPollInterval, "the first lease did not run out")

	require.NoError(t, holder.Release(context.WithoutCancel(t.Context())))
	select {
	case err = <-done:
	case <-time.After(testWaitTimeout):
		require.FailNow(t, "ObtainBoth did not return")
	}
	require.ErrorIs(t, err, redislock.ErrNotObtained)
	require.False(t, crashed(err))
	require.False(t, lockHeld(t, locker, first))
	require.False(t, lockHeld(t, locker, second))
}

func TestLockNotificationRetryDelayStaysWithinConfiguredRange(t *testing.T) {
	t.Parallel()

	for range 10_000 {
		delay := sampleLockNotificationRetryDelay()
		require.GreaterOrEqual(t, delay, lockNotificationRetryMinDelay)
		require.Less(t, delay, lockNotificationRetryMaxDelay)
	}
}

func TestJitterBackoffStaysWithinConfiguredRange(t *testing.T) {
	t.Parallel()

	base := 200 * time.Millisecond
	minBackoff := time.Duration(float64(base) * (1 - lockRetryJitter))
	maxBackoff := time.Duration(float64(base) * (1 + lockRetryJitter))

	for range 100 {
		backoff := jitterBackoff(base)
		require.GreaterOrEqual(t, backoff, minBackoff)
		require.LessOrEqual(t, backoff, maxBackoff)
	}
}

func setupTestLocker(t *testing.T, startSubManager bool) (*storageLocker, *subscriptionManager) {
	t.Helper()

	redisClient := redis_utils.SetupInstance(t)
	subManager := newSubscriptionManager(redisClient, globalStorageNotifyChannel)
	pub, err := newPublisher(redisClient, globalStorageNotifyChannel, noop.NewMeterProvider().Meter(meterScope))
	require.NoError(t, err)

	// The publisher always runs: lock release tests assert PubSub payloads
	// arrive, even when the in-process subscription manager is intentionally
	// disabled to exercise the timer fallback.
	go pub.run(t.Context())
	t.Cleanup(func() { pub.close(context.WithoutCancel(t.Context())) })

	if startSubManager {
		go subManager.start(t.Context())
		t.Cleanup(subManager.close)
	}

	return newStorageLocker(redisClient, subManager, pub), subManager
}

// recordingNotifier records the routing keys released locks publish, in call
// order, and forwards them so waiters still wake. The publisher drains its
// queue with a worker pool, so the wire order of two back-to-back publishes
// is not observable.
type recordingNotifier struct {
	inner notifier

	mu   sync.Mutex
	keys []string
}

func (n *recordingNotifier) Publish(ctx context.Context, routingKey string) {
	n.mu.Lock()
	n.keys = append(n.keys, routingKey)
	n.mu.Unlock()

	n.inner.Publish(ctx, routingKey)
}

func (n *recordingNotifier) published() []string {
	n.mu.Lock()
	defer n.mu.Unlock()

	return append([]string(nil), n.keys...)
}

func setupRecordingTestLocker(t *testing.T) (*storageLocker, *subscriptionManager, *recordingNotifier) {
	t.Helper()

	locker, subManager := setupTestLocker(t, true)
	recorder := &recordingNotifier{inner: locker.notifier}
	locker.notifier = recorder

	return locker, subManager, recorder
}

func lockHeld(t *testing.T, locker *storageLocker, lockKey string) bool {
	t.Helper()

	held, err := locker.redisClient.Exists(t.Context(), lockKey).Result()
	require.NoError(t, err)

	return held == 1
}

type recordingLockClient struct {
	mu      sync.Mutex
	results []error
	calls   []time.Time
}

func newRecordingLockClient(results ...error) *recordingLockClient {
	return &recordingLockClient{results: results}
}

func (c *recordingLockClient) Obtain(context.Context, string, time.Duration, *redislock.Options) (*redislock.Lock, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	call := len(c.calls)
	c.calls = append(c.calls, time.Now())
	if call < len(c.results) {
		return nil, c.results[call]
	}

	return nil, redislock.ErrNotObtained
}

func (c *recordingLockClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.calls)
}

func (c *recordingLockClient) callTimes() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]time.Time(nil), c.calls...)
}

func newScriptedStorageLocker(client lockObtainer, delay time.Duration) (*storageLocker, *subscriptionManager) {
	return newScriptedStorageLockerWithDelay(client, func() time.Duration { return delay })
}

func newScriptedStorageLockerWithDelay(client lockObtainer, delay func() time.Duration) (*storageLocker, *subscriptionManager) {
	subManager := newSubscriptionManager(nil, globalStorageNotifyChannel)

	return newScriptedStorageLockerWithManager(client, subManager, delay), subManager
}

func newScriptedStorageLockerWithManager(client lockObtainer, subManager *subscriptionManager, delay func() time.Duration) *storageLocker {
	return &storageLocker{
		client: client,
		option: &redislock.Options{
			RetryStrategy: redislock.NoRetry(),
		},
		subManager:             subManager,
		notificationRetryDelay: delay,
	}
}

func obtainErrorAsync(locker *storageLocker, ctx context.Context, lockKey string) <-chan error {
	result := make(chan error, 1)
	go func() {
		_, err := locker.Obtain(ctx, lockKey, testLockTimeout)
		result <- err
	}()

	return result
}

func waitForRecordedCalls(t *testing.T, client *recordingLockClient, expected int) {
	t.Helper()

	require.Eventually(t, func() bool {
		return client.callCount() >= expected
	}, 5*time.Second, time.Millisecond, "lock client did not receive %d calls", expected)
}

func waitForRegisteredWaiters(t *testing.T, subManager *subscriptionManager, routingKey string, expected int) {
	t.Helper()

	require.Eventually(t, func() bool {
		subManager.mu.RLock()
		defer subManager.mu.RUnlock()

		return len(subManager.waiters[routingKey]) == expected
	}, 5*time.Second, time.Millisecond, "expected %d registered lock waiters", expected)
}

func waitForLockWaiter(t *testing.T, subManager *subscriptionManager, routingKey string, waiterDone <-chan error) {
	t.Helper()

	require.Eventually(t, func() bool {
		select {
		case err := <-waiterDone:
			require.FailNow(t, "lock waiter finished before registering", "err: %v", err)
		default:
		}

		subManager.mu.RLock()
		ready := len(subManager.waiters[routingKey]) > 0
		subManager.mu.RUnlock()

		return ready
	}, testWaitTimeout, testPollInterval, "lock waiter was not registered")
}

func requireNoErrorFromChannel(t *testing.T, ch <-chan error) {
	t.Helper()

	var err error
	require.Eventually(t, func() bool {
		select {
		case err = <-ch:
			return true
		default:
			return false
		}
	}, testWaitTimeout, testPollInterval, "operation did not complete")
	require.NoError(t, err)
}

func requireErrorFromChannel(t *testing.T, ch <-chan error, expected error) {
	t.Helper()

	select {
	case err := <-ch:
		require.ErrorIs(t, err, expected)
	case <-time.After(5 * time.Second):
		require.FailNow(t, fmt.Sprintf("operation did not return %v", expected))
	}
}

// requirePubSubPayload waits for expected on the subscription, draining every
// message already queued on each poll: the channel is shared by the package's
// parallel tests, whose lock releases can queue more notifications than one
// per poll interval within the wait.
func requirePubSubPayload(t *testing.T, pubsub *goredis.PubSub, expected string) {
	t.Helper()

	messages := pubsub.Channel()
	require.Eventually(t, func() bool {
		for {
			select {
			case msg := <-messages:
				if msg.Payload == expected {
					return true
				}
			default:
				return false
			}
		}
	}, testWaitTimeout, testPollInterval, "expected PubSub payload was not received")
}
