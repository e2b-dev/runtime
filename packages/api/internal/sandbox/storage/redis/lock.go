package redis

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
)

const (
	lockNotificationRetryMinDelay = 5 * time.Millisecond
	lockNotificationRetryMaxDelay = 25 * time.Millisecond
)

type lockObtainer interface {
	Obtain(ctx context.Context, key string, ttl time.Duration, opt *redislock.Options) (*redislock.Lock, error)
}

type storageLocker struct {
	redisClient            redis.UniversalClient
	client                 lockObtainer
	option                 *redislock.Options
	subManager             *subscriptionManager
	notifier               notifier
	notificationRetryDelay func() time.Duration
}

func newStorageLocker(redisClient redis.UniversalClient, subManager *subscriptionManager, n notifier) *storageLocker {
	return &storageLocker{
		redisClient: redisClient,
		client:      redislock.New(redisClient),
		option: &redislock.Options{
			RetryStrategy: redislock.NoRetry(),
		},
		subManager:             subManager,
		notifier:               n,
		notificationRetryDelay: sampleLockNotificationRetryDelay,
	}
}

type storageLock struct {
	*redislock.Lock

	notifier notifier
}

func (l *storageLock) Release(ctx context.Context) error {
	if err := l.Lock.Release(ctx); err != nil {
		return err
	}

	// Hand off to the shared publisher
	l.notifier.Publish(ctx, getLockRoutingKey(l.Key()))

	return nil
}

func (l *storageLocker) Obtain(ctx context.Context, lockKey string, timeout time.Duration) (*storageLock, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	lock, err := l.tryLock(ctx, lockKey, timeout)
	if err == nil {
		return lock, nil
	}
	if !errors.Is(err, redislock.ErrNotObtained) {
		return nil, err
	}

	ch, cleanup := l.subManager.subscribe(getLockRoutingKey(lockKey))
	defer cleanup()

	// Recheck after subscribing so a release between the first attempt and
	// registration cannot be missed.
	lock, err = l.tryLock(ctx, lockKey, timeout)
	if err == nil {
		return lock, nil
	}
	if !errors.Is(err, redislock.ErrNotObtained) {
		return nil, err
	}

	backoff := lockRetryMinInterval
	timer := time.NewTimer(jitterBackoff(backoff))
	defer stopAndDrainLockTimer(timer)

	notificationRetryPending := false
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
			if notificationRetryPending {
				continue
			}

			stopAndDrainLockTimer(timer)
			timer.Reset(l.notificationRetryDelay())
			notificationRetryPending = true
		case <-timer.C:
			retryFromNotification := notificationRetryPending
			notificationRetryPending = false
			if retryFromNotification {
				drainLockNotification(ch)
			} else {
				backoff = min(backoff*2, lockRetryMaxInterval)
			}

			// Cancellation can become ready at the same time as the timer. Avoid
			// starting another Redis operation after the caller's deadline.
			if err := ctx.Err(); err != nil {
				return nil, err
			}

			lock, err = l.tryLock(ctx, lockKey, timeout)
			if err == nil {
				return lock, nil
			}
			if !errors.Is(err, redislock.ErrNotObtained) {
				return nil, err
			}

			// Notification retries leave the current fallback base unchanged.
			// Only a fallback timer firing advances the exponential backoff.
			timer.Reset(jitterBackoff(backoff))
		}
	}
}

func (l *storageLocker) tryLock(ctx context.Context, lockKey string, timeout time.Duration) (*storageLock, error) {
	lock, err := l.client.Obtain(ctx, lockKey, timeout, l.option)
	if err != nil {
		return nil, err
	}

	return &storageLock{Lock: lock, notifier: l.notifier}, nil
}

func jitterBackoff(backoff time.Duration) time.Duration {
	factor := 1 + lockRetryJitter*(2*rand.Float64()-1)

	return time.Duration(float64(backoff) * factor)
}

func sampleLockNotificationRetryDelay() time.Duration {
	return lockNotificationRetryMinDelay + time.Duration(rand.Int64N(int64(lockNotificationRetryMaxDelay-lockNotificationRetryMinDelay)))
}

func stopAndDrainLockTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}

	select {
	case <-timer.C:
	default:
	}
}

func drainLockNotification(ch <-chan struct{}) {
	select {
	case <-ch:
	default:
	}
}
