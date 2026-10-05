package batcher

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	defaultQueueSize    = 8 * 1024
	defaultMaxBatchSize = 64 * 1024
	defaultMaxDelay     = 100 * time.Millisecond
)

var (
	ErrBatcherAlreadyStarted = errors.New("batcher already started")
	ErrBatcherNotStarted     = errors.New("batcher not started")
	ErrFuncNotSet            = errors.New("Batcher.Func must be set")
	ErrBatcherQueueFull      = errors.New("batcher queue is full")
)

var (
	meter = otel.Meter("github.com/e2b-dev/infra/packages/clickhouse/pkg/batcher")

	mItemsDropped      = utils.Must(meter.Int64Counter("batcher.items.dropped", metric.WithDescription("Number of items dropped because the batcher queue was full"), metric.WithUnit("{item}")))
	mFlushes           = utils.Must(meter.Int64Counter("batcher.flushes", metric.WithDescription("Completed flush attempts by result")))
	mMaxBatchSize      = utils.Must(meter.Int64Gauge("batcher.config.max_batch_size", metric.WithUnit("{item}")))
	mMaxDelay          = utils.Must(meter.Int64Gauge("batcher.config.max_delay", metric.WithUnit("ms")))
	mQueueSize         = utils.Must(meter.Int64Gauge("batcher.config.queue_size", metric.WithUnit("{item}")))
	mQueueLen          = utils.Must(meter.Int64Gauge("batcher.queue.length", metric.WithDescription("Current number of items waiting in the batcher queue"), metric.WithUnit("{item}")))
	mFlushBatchSize    = utils.Must(meter.Int64Histogram("batcher.flush.batch_size", metric.WithDescription("Number of items per flushed batch"), metric.WithUnit("{item}")))
	mFlushWaitDuration = utils.Must(meter.Int64Histogram("batcher.flush.wait_duration", metric.WithDescription("Time from first item enqueued in a batch to when the batch is flushed"), metric.WithUnit("ms")))
	mFlushDuration     = utils.Must(meter.Int64Histogram("batcher.flush.duration", metric.WithDescription("Time spent executing BatcherFunc per flush"), metric.WithUnit("ms")))
)

// Batcher groups items in batches and calls Func on them.
//
// See also BytesBatcher.
type Batcher[T any] struct {
	// Func is called by Batcher when batch is ready to be processed.
	Func BatcherFunc[T]

	// Maximum batch size that will be passed to BatcherFunc.
	MaxBatchSize int

	// Maximum delay between the first item being enqueued in a batch and the
	// BatcherFunc call for that batch.
	MaxDelay time.Duration

	// Maximum unprocessed items' queue size.
	QueueSize int

	// ErrorHandler is called when BatcherFunc returns an error.
	// If not set, errors from BatcherFunc will be silently dropped.
	ErrorHandler func(error)

	// Synchronization primitives.
	mu      sync.RWMutex
	ch      chan T
	doneCh  chan struct{}
	started bool

	flags *featureflags.Client
	name  string
	attrs metric.MeasurementOption
}

// BatcherFunc is called by Batcher when batch is ready to be processed.
//
// BatcherFunc must process the given batch before returning.
// It must not hold references to the batch after returning.
type BatcherFunc[T any] func(ctx context.Context, batch []T) error

type BatcherOptions struct {
	// FeatureFlags optionally overrides batch size and delay at startup and every
	// 30 seconds between flushes. Nil keeps static options. QueueSize is startup-only.
	FeatureFlags *featureflags.Client

	// Name is added as the "batcher" attribute on all metrics, allowing different
	// batcher instances to be identified in dashboards (e.g. "sandbox-events", "billing-export").
	Name string

	// MaxBatchSize is the maximum number of items collected into a single batch
	// before being flushed by the BatcherFunc.
	MaxBatchSize int

	// MaxDelay is the maximum time between the first item being enqueued in a
	// batch and the BatcherFunc call for that batch.
	MaxDelay time.Duration

	// QueueSize is the size of the channel buffer used to queue incoming items.
	// Items pushed when the queue is full are dropped (Push returns ErrBatcherQueueFull).
	QueueSize int

	// ErrorHandler is called when BatcherFunc returns an error.
	// If not set, errors from BatcherFunc will be silently dropped.
	ErrorHandler func(error)
}

// NewBatcher creates a new Batcher with the given parameters.
func NewBatcher[T any](fn BatcherFunc[T], cfg BatcherOptions) (*Batcher[T], error) {
	b := &Batcher[T]{
		Func:         fn,
		flags:        cfg.FeatureFlags,
		name:         cfg.Name,
		MaxBatchSize: cfg.MaxBatchSize,
		MaxDelay:     cfg.MaxDelay,
		QueueSize:    cfg.QueueSize,
		ErrorHandler: cfg.ErrorHandler,

		attrs: metric.WithAttributeSet(attribute.NewSet(attribute.String("batcher", cfg.Name))),
	}

	if b.Func == nil {
		return nil, ErrFuncNotSet
	}
	if b.ErrorHandler == nil {
		b.ErrorHandler = func(error) {}
	}
	if b.MaxBatchSize <= 0 {
		b.MaxBatchSize = defaultMaxBatchSize
		if b.flags != nil {
			b.MaxBatchSize = featureflags.ClickhouseBatcherMaxBatchSize.Fallback()
		}
	}
	if b.MaxDelay <= 0 {
		b.MaxDelay = defaultMaxDelay
		if b.flags != nil {
			b.MaxDelay = time.Duration(featureflags.ClickhouseBatcherMaxDelay.Fallback()) * time.Millisecond
		}
	}
	if b.QueueSize <= 0 {
		b.QueueSize = defaultQueueSize
	}

	return b, nil
}

// Start begins batch processing.
func (b *Batcher[T]) Start(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.started {
		return ErrBatcherAlreadyStarted
	}

	b.ch = make(chan T, b.QueueSize)
	b.doneCh = make(chan struct{})
	b.started = true

	go func() {
		b.processBatches(ctx)
		close(b.doneCh)
	}()

	return nil
}

// Stop stops batch processing and flushes remaining items.
func (b *Batcher[T]) Stop() error {
	b.mu.Lock()
	if !b.started {
		b.mu.Unlock()

		return ErrBatcherNotStarted
	}

	b.started = false
	close(b.ch)
	b.mu.Unlock()

	<-b.doneCh

	return nil
}

// Push enqueues an item for batching.
// Returns ErrBatcherQueueFull immediately if the queue is full.
func (b *Batcher[T]) Push(item T) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.started {
		return ErrBatcherNotStarted
	}

	select {
	case b.ch <- item:
		mQueueLen.Record(context.Background(), int64(len(b.ch)), b.attrs)

		return nil
	default:
		mItemsDropped.Add(context.Background(), 1, b.attrs)

		return ErrBatcherQueueFull
	}
}

// QueueLen returns the number of items pending in the queue.
func (b *Batcher[T]) QueueLen() int {
	return len(b.ch)
}

func (b *Batcher[T]) processBatches(ctx context.Context) {
	var (
		batch          []T
		batchStartTime time.Time
	)

	// Runtime limits belong to this goroutine; public fields remain startup options.
	maxBatchSize, maxDelay := b.limits(ctx, b.MaxBatchSize, b.MaxDelay)
	recordConfig := func() {
		mMaxBatchSize.Record(ctx, int64(maxBatchSize), b.attrs)
		mMaxDelay.Record(ctx, maxDelay.Milliseconds(), b.attrs)
		mQueueSize.Record(ctx, int64(b.QueueSize), b.attrs)
	}
	recordConfig()
	var refreshC <-chan time.Time
	if b.flags != nil {
		refresh := time.NewTicker(30 * time.Second)
		defer refresh.Stop()
		refreshC = refresh.C
	}
	ticker := time.NewTicker(maxDelay)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		mFlushWaitDuration.Record(ctx, time.Since(batchStartTime).Milliseconds(), b.attrs)
		start := time.Now()
		result := "success"
		if err := b.Func(ctx, batch); err != nil {
			result = "error"
			b.ErrorHandler(err)
		}

		mFlushes.Add(ctx, 1, b.attrs, metric.WithAttributes(attribute.String("result", result)))
		mFlushBatchSize.Record(ctx, int64(len(batch)), b.attrs)
		mFlushDuration.Record(ctx, time.Since(start).Milliseconds(), b.attrs)
		mQueueLen.Record(ctx, int64(len(b.ch)), b.attrs)

		batch = batch[:0]
		ticker.Reset(maxDelay)
	}

	for {
		select {
		case item, ok := <-b.ch:
			if !ok {
				flush()

				return
			}

			if len(batch) == 0 {
				batchStartTime = time.Now()
			}

			batch = append(batch, item)
			if len(batch) >= maxBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-refreshC:
			previousDelay := maxDelay
			maxBatchSize, maxDelay = b.limits(ctx, maxBatchSize, maxDelay)
			if maxDelay != previousDelay {
				remaining := maxDelay
				if len(batch) > 0 {
					remaining -= time.Since(batchStartTime)
				}
				if remaining <= 0 {
					flush()
				} else {
					ticker.Reset(remaining)
				}
			}
			if len(batch) >= maxBatchSize {
				flush()
			}
			recordConfig()
		}
	}
}

// limits retains the last valid values when a flag update is invalid.
func (b *Batcher[T]) limits(ctx context.Context, size int, delay time.Duration) (int, time.Duration) {
	if b.flags == nil {
		return size, delay
	}
	target := featureflags.BatcherContext(b.name)
	if value := b.flags.IntFlag(ctx, featureflags.ClickhouseBatcherMaxBatchSize, target); value > 0 {
		size = value
	}
	if ms := b.flags.IntFlag(ctx, featureflags.ClickhouseBatcherMaxDelay, target); ms > 0 && int64(ms) <= math.MaxInt64/int64(time.Millisecond) {
		delay = time.Duration(ms) * time.Millisecond
	}

	return size, delay
}
