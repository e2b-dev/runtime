package cacheindex

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	defaultFlushInterval = 30 * time.Second
	defaultMaxPending    = 50_000
	flushTimeout         = 5 * time.Second
	// idRecheckInterval paces re-reading the marker: the cleaner creates it on
	// its first run with the index on, which may come long after this process
	// starts, and a recreated cache gets a new one.
	idRecheckInterval = 10 * time.Minute
)

var (
	meter = otel.Meter("github.com/e2b-dev/infra/packages/shared/pkg/storage/cacheindex")

	touches = utils.Must(meter.Int64Counter("orchestrator.cache_index.touches",
		metric.WithDescription("Build uses handed to the cache index writer, by outcome (queued, or dropped because the pending set is full)")))
	flushes = utils.Must(meter.Int64Counter("orchestrator.cache_index.flushes",
		metric.WithDescription("Cache index flushes, by outcome: ok, error (batch dropped), or disabled (no Redis, no cache ID, or flag off)")))
	flushedBuilds = utils.Must(meter.Int64Counter("orchestrator.cache_index.flushed_builds",
		metric.WithDescription("Builds whose last use was written to the cache index")))
)

func outcome(v string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("outcome", v))
}

type WriterConfig struct {
	// Root is the cache root holding the marker file.
	Root string
	// Enabled is asked at every flush; a false answer discards the batch.
	Enabled func(context.Context) bool
	// FlushInterval is the mean time between flushes (jittered ±20 %).
	FlushInterval time.Duration
	// MaxPending caps the distinct builds held between flushes; further new
	// builds are dropped until the next flush.
	MaxPending int
	// Now replaces the clock; nil means time.Now.
	Now func() time.Time
}

// Writer batches build uses in memory and writes them to the index
// periodically. Touch never blocks on I/O; a failed flush is retried with the
// next one. A use that is lost anyway only makes its build a candidate, which
// the cleaner's on-disk check keeps if it is in use.
type Writer struct {
	client redis.UniversalClient
	cfg    WriterConfig
	now    func() time.Time

	mu      sync.Mutex
	pending map[string]int64

	// Owned by the flush loop.
	cacheID     string
	lastIDCheck time.Time

	stop chan struct{}
	done chan struct{}
}

func NewWriter(client redis.UniversalClient, cfg WriterConfig) *Writer {
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = defaultFlushInterval
	}
	if cfg.MaxPending <= 0 {
		cfg.MaxPending = defaultMaxPending
	}
	if cfg.Enabled == nil {
		cfg.Enabled = func(context.Context) bool { return true }
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}

	return &Writer{
		client:  client,
		cfg:     cfg,
		now:     now,
		pending: make(map[string]int64),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// Touch records that buildID was used now.
func (w *Writer) Touch(buildID string) {
	nowSec := w.now().Unix()

	w.mu.Lock()
	_, known := w.pending[buildID]
	if !known && len(w.pending) >= w.cfg.MaxPending {
		w.mu.Unlock()
		touches.Add(context.Background(), 1, outcome("dropped"))

		return
	}
	w.pending[buildID] = nowSec
	w.mu.Unlock()

	if !known {
		touches.Add(context.Background(), 1, outcome("queued"))
	}
}

// Start runs the flush loop until Close.
func (w *Writer) Start(ctx context.Context) {
	go func() {
		defer close(w.done)
		for {
			timer := time.NewTimer(jitter(w.cfg.FlushInterval))
			select {
			case <-w.stop:
				timer.Stop()

				return
			case <-ctx.Done():
				timer.Stop()

				return
			case <-timer.C:
				w.flush(ctx)
			}
		}
	}()
}

// Close stops the loop and flushes once more within ctx. Start must have been called.
// It returns at ctx's deadline even if Redis stalls, abandoning that flush, and
// with ctx already done it only stops the loop and returns nil.
func (w *Writer) Close(ctx context.Context) error {
	close(w.stop)
	// A forced stop: dropping the last batch costs at most a slightly older score.
	select {
	case <-ctx.Done():
		return nil
	default:
	}
	select {
	case <-w.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	flushed := make(chan struct{})
	go func() {
		defer close(flushed)
		w.flush(ctx)
	}()
	select {
	case <-flushed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func jitter(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
}

func (w *Writer) flush(ctx context.Context) {
	w.mu.Lock()
	batch := w.pending
	w.pending = make(map[string]int64, len(batch))
	w.mu.Unlock()

	if len(batch) == 0 {
		return
	}

	// The flag first: with writes off, no marker read touches the share.
	if !w.cfg.Enabled(ctx) {
		flushes.Add(ctx, 1, outcome("disabled"))

		return
	}
	id := w.id(ctx)
	if id == "" {
		flushes.Add(ctx, 1, outcome("disabled"))

		return
	}

	shards := make(map[int][]redis.Z)
	for build, ts := range batch {
		s := shardOf(build)
		shards[s] = append(shards[s], redis.Z{Score: float64(ts), Member: build})
	}

	ctx, cancel := context.WithTimeout(ctx, flushTimeout)
	defer cancel()

	pipe := w.client.Pipeline()
	for s, members := range shards {
		// GT: a use only ever moves a build's score forward.
		pipe.ZAddGT(ctx, lruKey(id, s), members...)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		flushes.Add(ctx, 1, outcome("error"))
		logger.L().Debug(ctx, "cache index flush failed; retrying with the next flush",
			zap.Int("builds", len(batch)), zap.Error(err))
		w.requeue(ctx, batch)

		return
	}

	flushes.Add(ctx, 1, outcome("ok"))
	flushedBuilds.Add(ctx, int64(len(batch)))
}

// requeue puts a failed batch back under the pending cap; a newer use of the
// same build wins.
func (w *Writer) requeue(ctx context.Context, batch map[string]int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for build, ts := range batch {
		if cur, ok := w.pending[build]; ok {
			w.pending[build] = max(cur, ts)

			continue
		}
		if len(w.pending) >= w.cfg.MaxPending {
			touches.Add(ctx, 1, outcome("dropped"))

			continue
		}
		w.pending[build] = ts
	}
}

// id returns the cache ID, re-reading the marker at most once per
// idRecheckInterval so a recreated cache's new ID is picked up. "" means
// unknown; a marker that turns unreadable keeps the last known ID.
func (w *Writer) id(ctx context.Context) string {
	now := w.now()
	if !w.lastIDCheck.IsZero() && now.Sub(w.lastIDCheck) < idRecheckInterval {
		return w.cacheID
	}
	w.lastIDCheck = now

	id, err := ReadID(w.cfg.Root)
	if err != nil {
		logger.L().Debug(ctx, "cache index marker not readable", zap.String("root", w.cfg.Root), zap.Error(err))

		return w.cacheID
	}
	w.cacheID = id

	return id
}
