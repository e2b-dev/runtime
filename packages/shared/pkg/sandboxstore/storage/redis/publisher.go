package redis

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	publishQueueDepth = 8192

	// publishTimeout bounds a single PUBLISH round-trip
	publishTimeout        = 5 * time.Second
	publishShutdownBudget = 10 * time.Second

	// publishDropLogFrequency rate-limits drop warnings: only every Nth
	// drop produces a log line, with the running total attached.
	publishDropLogFrequency = 64

	// publishWorkerCount is the size of the goroutine pool draining the
	// publish queue. Each worker is mostly blocked in Redis RTT, so the
	// effective steady-state throughput is ~N / RTT.
	publishWorkerCount = 32
)

const (
	stateInit uint32 = iota
	stateRunning
	stateClosed
)

// Backpressure policy: drop and count. Subscribers tolerate dropped
// notifications by design (see Obtain and waitForTransition fallback
// timers).
type publisher struct {
	redisClient redis.UniversalClient
	channel     string

	queue  chan string
	closed chan struct{} // signals the drainer to stop and reject new sends
	done   chan struct{} // closed when the drainer has fully exited

	closeOnce sync.Once
	mu        sync.RWMutex
	closing   bool
	state     atomic.Uint32 // lifecycle: stateInit → stateRunning | stateClosed
	dropped   atomic.Uint64

	metrics publisherMetrics
}

type publisherMetrics struct {
	published        metric.Int64Counter
	publishedSuccess metric.MeasurementOption
	publishedFailure metric.MeasurementOption

	dropped          metric.Int64Counter
	droppedQueueFull metric.MeasurementOption
	droppedClosed    metric.MeasurementOption

	publishDuration        metric.Int64Histogram
	publishDurationSuccess metric.MeasurementOption
	publishDurationFailure metric.MeasurementOption
}

const (
	dropReasonAttr      = "reason"
	dropReasonQueueFull = "queue_full"
	dropReasonClosed    = "closed"
)

// notifier is the narrow message-only seam consumed by storageLock.
// Defining it where it is consumed (lock.go-side) lets storageLock be
// faked in tests without standing up Redis.
type notifier interface {
	Publish(ctx context.Context, routingKey string)
}

func newPublisher(redisClient redis.UniversalClient, channel string, meter metric.Meter) (*publisher, error) {
	p := &publisher{
		redisClient: redisClient,
		channel:     channel,
		queue:       make(chan string, publishQueueDepth),
		closed:      make(chan struct{}),
		done:        make(chan struct{}),
	}

	if err := p.initMetrics(meter); err != nil {
		return nil, fmt.Errorf("failed to init publisher metrics: %w", err)
	}

	return p, nil
}

func (p *publisher) initMetrics(meter metric.Meter) error {
	published, err := telemetry.GetCounter(meter, telemetry.ApiRedisStoragePublisherPublished)
	if err != nil {
		return fmt.Errorf("publisher published counter: %w", err)
	}

	dropped, err := telemetry.GetCounter(meter, telemetry.ApiRedisStoragePublisherDropped)
	if err != nil {
		return fmt.Errorf("publisher dropped counter: %w", err)
	}

	publishDuration, err := telemetry.GetHistogram(meter, telemetry.ApiRedisStoragePublisherPublishDuration)
	if err != nil {
		return fmt.Errorf("publisher publish duration histogram: %w", err)
	}

	_, err = telemetry.GetObservableUpDownCounter(meter, telemetry.ApiRedisStoragePublisherQueueDepth, func(_ context.Context, observer metric.Int64Observer) error {
		observer.Observe(int64(len(p.queue)))

		return nil
	})
	if err != nil {
		return fmt.Errorf("publisher queue depth gauge: %w", err)
	}

	p.metrics = publisherMetrics{
		published:              published,
		publishedSuccess:       metric.WithAttributeSet(attribute.NewSet(telemetry.Success)),
		publishedFailure:       metric.WithAttributeSet(attribute.NewSet(telemetry.Failure)),
		dropped:                dropped,
		droppedQueueFull:       metric.WithAttributeSet(attribute.NewSet(attribute.String(dropReasonAttr, dropReasonQueueFull))),
		droppedClosed:          metric.WithAttributeSet(attribute.NewSet(attribute.String(dropReasonAttr, dropReasonClosed))),
		publishDuration:        publishDuration,
		publishDurationSuccess: metric.WithAttributeSet(attribute.NewSet(telemetry.Success)),
		publishDurationFailure: metric.WithAttributeSet(attribute.NewSet(telemetry.Failure)),
	}

	return nil
}

// Publish enqueues a routing key for asynchronous PUBLISH. Never blocks.
// Drops silently (with rate-limited warn) when the queue is full or the
// publisher has been closed.
func (p *publisher) Publish(ctx context.Context, routingKey string) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closing {
		p.drop(ctx, routingKey, dropReasonClosed, p.metrics.droppedClosed)

		return
	}

	select {
	case p.queue <- routingKey:
	default:
		p.drop(ctx, routingKey, dropReasonQueueFull, p.metrics.droppedQueueFull)
	}
}

func (p *publisher) drop(ctx context.Context, routingKey, reason string, attrs metric.MeasurementOption) {
	p.metrics.dropped.Add(ctx, 1, attrs)

	n := p.dropped.Add(1)
	if n%publishDropLogFrequency == 1 {
		logger.L().Warn(ctx,
			"Dropping storage notification: publish queue saturated or closed",
			zap.String("routing_key", routingKey),
			zap.String("reason", reason),
			zap.Uint64("total_drops", n),
		)
	}
}

// run starts the worker pool and blocks until the pool exits.
// On exit it performs a bounded best-effort drain of pending items so a
// graceful shutdown does not lose every in-flight notification.
func (p *publisher) run(ctx context.Context) {
	if !p.state.CompareAndSwap(stateInit, stateRunning) {
		// close() raced ahead and transitioned us to stateClosed; it has
		// already closed p.done so callers blocked in close() unblock.
		return
	}
	defer close(p.done)

	pubCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Bridge close() into pubCtx so in-flight publishOne calls abort when
	// the publisher is closed, not just when the parent is cancelled.
	go func() {
		select {
		case <-p.closed:
			cancel()
		case <-pubCtx.Done():
		}
	}()

	var wg sync.WaitGroup
	for range publishWorkerCount {
		wg.Go(func() {
			p.workerLoop(pubCtx)
		})
	}
	wg.Wait()

	p.drainOnShutdown(ctx)
}

// workerLoop is the per-worker drain loop. Multiple workers contend on
// the same queue; Go channel receives are serialized internally, so each
// key is delivered to exactly one worker.
func (p *publisher) workerLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case key := <-p.queue:
			p.publishOne(ctx, key)
		}
	}
}

// drainOnShutdown opportunistically publishes any keys still in the queue,
// using a single shared deadline so a hung Redis cannot block teardown.
func (p *publisher) drainOnShutdown(ctx context.Context) {
	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishShutdownBudget)
	defer cancel()

	for {
		select {
		case key := <-p.queue:
			p.publishOne(drainCtx, key)
			if drainCtx.Err() != nil {
				return
			}
		default:
			return
		}
	}
}

func (p *publisher) publishOne(ctx context.Context, routingKey string) {
	pubCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()

	start := time.Now()
	err := p.redisClient.Publish(pubCtx, p.channel, routingKey).Err()
	elapsed := time.Since(start).Milliseconds()

	if err != nil {
		p.metrics.published.Add(pubCtx, 1, p.metrics.publishedFailure)
		p.metrics.publishDuration.Record(pubCtx, elapsed, p.metrics.publishDurationFailure)
		logger.L().Warn(pubCtx, "Failed to publish storage notification",
			zap.String("routing_key", routingKey),
			zap.Error(err),
		)

		return
	}

	p.metrics.published.Add(pubCtx, 1, p.metrics.publishedSuccess)
	p.metrics.publishDuration.Record(pubCtx, elapsed, p.metrics.publishDurationSuccess)
}

func (p *publisher) close(ctx context.Context) {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closing = true
		close(p.closed)
		p.mu.Unlock()

		if p.state.CompareAndSwap(stateInit, stateClosed) {
			// run() either has not been scheduled yet or will never be
			// invoked. Drain whatever is queued on this goroutine
			p.drainOnShutdown(ctx)
			close(p.done)
		}
	})
	<-p.done
}
