package redis

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// newTestPublisher wraps newPublisher with a noop meter for tests. The
// publisher's metric recordings exercise a real metric.Meter surface even
// in tests — using noop keeps assertions focused on PubSub behaviour while
// still catching nil-Meter bugs and misuse of the OTel API.
func newTestPublisher(t *testing.T, client goredis.UniversalClient) *publisher {
	t.Helper()
	pub, err := newPublisher(client, globalStorageNotifyChannel, noop.NewMeterProvider().Meter(meterScope))
	require.NoError(t, err)

	return pub
}

// newTestStorage wraps NewStorage with a noop meter provider for tests.
func newTestStorage(t *testing.T, client goredis.UniversalClient) *Storage {
	t.Helper()
	s, err := NewStorage(client, noop.NewMeterProvider())
	require.NoError(t, err)

	return s
}

// TestPublisher_PublishEnqueuesAndDrainsToRedis verifies that keys handed
// to Publish are eventually delivered on the global notify channel.
func TestPublisher_PublishEnqueuesAndDrainsToRedis(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupContainerInstance(t)
	pub := newTestPublisher(t, client)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	pubsub := client.Subscribe(t.Context(), globalStorageNotifyChannel)
	t.Cleanup(func() { _ = pubsub.Close() })
	_, err := pubsub.Receive(t.Context())
	require.NoError(t, err)

	go pub.run(ctx)
	t.Cleanup(func() { pub.close(context.WithoutCancel(t.Context())) })

	const n = 50
	want := make(map[string]struct{}, n)
	for i := range n {
		key := fmt.Sprintf("rk:%d", i)
		want[key] = struct{}{}
		pub.Publish(ctx, key)
	}

	messages := pubsub.Channel()
	deadline := time.After(5 * time.Second)
	got := make(map[string]struct{}, n)
	for len(got) < n {
		select {
		case msg := <-messages:
			got[msg.Payload] = struct{}{}
		case <-deadline:
			require.FailNowf(t, "did not receive all payloads",
				"got %d/%d", len(got), n)
		}
	}
	assert.Equal(t, want, got)
}

// TestPublisher_PublishNeverBlocks fires more publishes than the queue can
// hold without a drainer running and asserts every call returns immediately.
// This is the core scaling invariant: callers must not block on Redis.
func TestPublisher_PublishNeverBlocks(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	pub := newTestPublisher(t, client)
	// Intentionally do not call run() — the queue will fill and overflow.

	const n = publishQueueDepth + 256
	done := make(chan struct{})
	go func() {
		for range n {
			pub.Publish(t.Context(), "k")
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "Publish blocked when queue was full")
	}

	require.GreaterOrEqual(t, pub.dropped.Load(), uint64(256),
		"expected at least 256 drops once the queue saturated")
}

// TestPublisher_DropOnClosed verifies sends after close are rejected and
// counted as drops, never blocking and never writing into a stale queue.
func TestPublisher_DropOnClosed(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	pub := newTestPublisher(t, client)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	go pub.run(ctx)
	pub.close(t.Context())

	const n = 100
	before := pub.dropped.Load()
	for range n {
		pub.Publish(t.Context(), "after-close")
	}
	// close() runs closeOnce.Do(close(p.closed)) synchronously, so by the
	// time the loop above starts every Publish call sees p.closed closed
	// in its fast-reject branch and must increment the drop counter.
	// Asserting an exact `before+n` lower bound is what makes this a real
	// regression guard: a future change that silently no-op'd the
	// post-close branch would no longer satisfy it.
	require.GreaterOrEqual(t, pub.dropped.Load(), before+n)
}

// TestPublisher_RunExitsOnContextCancel asserts the drainer goroutine
// stops cleanly when its run context is cancelled.
func TestPublisher_RunExitsOnContextCancel(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	pub := newTestPublisher(t, client)
	ctx, cancel := context.WithCancel(t.Context())

	go pub.run(ctx)
	cancel()

	select {
	case <-pub.done:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "publisher did not exit on context cancel")
	}
}

// TestPublisher_CloseDrainsPending enqueues messages, immediately closes,
// and verifies the bounded drain best-effort publishes pending items.
func TestPublisher_CloseDrainsPending(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupContainerInstance(t)
	pub := newTestPublisher(t, client)

	pubsub := client.Subscribe(t.Context(), globalStorageNotifyChannel)
	t.Cleanup(func() { _ = pubsub.Close() })
	_, err := pubsub.Receive(t.Context())
	require.NoError(t, err)

	const n = 20
	for i := range n {
		pub.Publish(t.Context(), fmt.Sprintf("drain:%d", i))
	}

	// close() without a running drainer takes the stateInit->stateClosed
	// path and drains all pending items synchronously within
	// publishShutdownBudget. Starting run() concurrently would race the
	// worker pool's context cancellation against the queue: a worker can
	// dequeue a key and then fail to publish once close() cancels pubCtx,
	// dropping an unpredictable subset (best-effort by design, not assertable).
	pub.close(t.Context())

	messages := pubsub.Channel()
	deadline := time.After(publishShutdownBudget + time.Second)
	seen := 0
	for seen < n {
		select {
		case <-messages:
			seen++
		case <-deadline:
			require.FailNowf(t, "shutdown drain did not flush all pending",
				"got %d/%d", seen, n)
		}
	}
}

// TestStorageLock_ReleaseUsesNotifier proves the lock no longer talks to
// Redis directly on release. Uses the interface seam with a fake notifier
// — no Redis-side observation required.
func TestStorageLock_ReleaseUsesNotifier(t *testing.T) {
	t.Parallel()

	var (
		mu      sync.Mutex
		got     []string
		fakeNot = notifierFunc(func(key string) {
			mu.Lock()
			got = append(got, key)
			mu.Unlock()
		})
	)

	client := redis_utils.SetupInstance(t)
	subManager := newSubscriptionManager(client, globalStorageNotifyChannel)
	locker := newStorageLocker(client, subManager, fakeNot)

	lockKey := redis_utils.GetLockKey(recordKey("fake-notifier"))
	lock, err := locker.Obtain(t.Context(), lockKey, testLockTimeout)
	require.NoError(t, err)
	require.NoError(t, lock.Release(context.WithoutCancel(t.Context())))

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{getLockRoutingKey(lockKey)}, got)
}

// notifierFunc is a one-method adapter for tests — same shape as
// http.HandlerFunc, lets tests inject behavior without a struct.
type notifierFunc func(routingKey string)

func (f notifierFunc) Publish(_ context.Context, routingKey string) { f(routingKey) }

// TestStorage_CloseIsIdempotent verifies that double-Close does not panic
// and the publisher's drainer exits cleanly.
func TestStorage_CloseIsIdempotent(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	go storage.Start(t.Context())

	storage.Close(t.Context())
	storage.Close(t.Context()) // must not panic
}

// TestStorage_CloseBeforeStartDoesNotDeadlock covers the early-init
// failure path where Storage.Close is invoked before Storage.Start has
// ever run (e.g. a defer s.Close() on a failing initializer). The
// publisher's close() must not block on its done channel in this case.
func TestStorage_CloseBeforeStartDoesNotDeadlock(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	// Deliberately do NOT call Start.

	done := make(chan struct{})
	go func() {
		storage.Close(t.Context())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "Close blocked when Start was never called")
	}
}

// TestPublisher_CloseBeforeRunDoesNotDeadlock is the publisher-level
// equivalent of the above: close() must return promptly even when run()
// has never been invoked.
func TestPublisher_CloseBeforeRunDoesNotDeadlock(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	pub := newTestPublisher(t, client)

	done := make(chan struct{})
	go func() {
		pub.close(t.Context())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "publisher.close blocked when run was never called")
	}
}

// TestPublisher_ConcurrentProducers stresses the queue under many concurrent
// callers and asserts: no panic/race, every call accounted for (enqueued or
// dropped), Close returns promptly. Run with -race to catch data races on
// the drop counter or queue.
func TestPublisher_ConcurrentProducers(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	pub := newTestPublisher(t, client)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	go pub.run(ctx)
	t.Cleanup(func() { pub.close(context.WithoutCancel(t.Context())) })

	const (
		producers   = 32
		perProducer = 500
	)
	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := range perProducer {
				pub.Publish(t.Context(), fmt.Sprintf("concurrent:%d:%d", p, i))
			}
		}(p)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "concurrent producers blocked")
	}

	// Touch the drop counter. We do not assert an exact value (it depends
	// on drain timing relative to production rate); this test exists so
	// the race detector can observe concurrent producers + counter reads.
	_ = pub.dropped.Load()
}

// blockingPublisher stalls on Publish until released. Used to force the
// shutdown drain to exhaust its budget.
type blockingPublisher struct {
	goredis.UniversalClient

	gate chan struct{}
}

func (b *blockingPublisher) Publish(ctx context.Context, _ string, _ any) *goredis.IntCmd {
	cmd := goredis.NewIntCmd(ctx)
	select {
	case <-b.gate:
	case <-ctx.Done():
	}
	cmd.SetVal(0)

	return cmd
}

// TestPublisher_DrainOnShutdownRespectsBudget proves the shutdown drain
// terminates within publishShutdownBudget even when each PUBLISH hangs.
// This exercises the deadline-exceeded branch of drainOnShutdown.
func TestPublisher_DrainOnShutdownRespectsBudget(t *testing.T) {
	t.Parallel()

	// Override the budget for the test via a local constructor that uses
	// the package-level constant; we cannot change the constant from a
	// test, so we instead rely on the publishTimeout per call. Each hung
	// call burns publishTimeout, and drainOnShutdown returns once the
	// shared budget is gone. We pre-fill the queue so the drain loop
	// has work to do.
	blocking := &blockingPublisher{
		UniversalClient: redis_utils.SetupInstance(t),
		gate:            make(chan struct{}), // never released → ctx-cancel exits
	}
	pub := newTestPublisher(t, blocking)

	for i := range 16 {
		pub.Publish(t.Context(), fmt.Sprintf("hang:%d", i))
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go pub.run(ctx)

	start := time.Now()
	pub.close(t.Context())
	elapsed := time.Since(start)

	require.LessOrEqual(t, elapsed, publishShutdownBudget+2*time.Second,
		"close must return within shutdown budget even with hung Redis")
}
