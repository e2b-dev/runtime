package sharedriver

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const backlogReadTimeout = 5 * time.Second

// ActiveJobStates are the states of a job River has not finished with.
var ActiveJobStates = []string{"available", "pending", "retryable", "running", "scheduled"}

const defaultQueueClass = "default"

type BacklogConfig struct {
	// Schema is the schema River's tables live in.
	Schema string
	// Kinds are the job kinds the service works. Each is reported at zero
	// when it has no jobs, so an empty backlog still returns series and no
	// series means the read stopped. A kind outside the list is reported only
	// while it has jobs.
	Kinds []string
	// QueueClass groups queues into a bounded label, or "default" when nil.
	QueueClass func(queue string) string
	// QueueClasses are the classes QueueClass returns, each reported at zero.
	QueueClasses []string
	// Interval is how often the backlog is read.
	Interval time.Duration
}

// Backlog reports a River outbox's unfinished and discarded jobs from a
// snapshot it reads every Interval, so an export never waits on the database.
// Every replica reads the same rows: aggregate its gauges with max, not sum.
// outbox.snapshot_age says how old the snapshot an export reported is.
type Backlog struct {
	pool   *pgxpool.Pool
	config BacklogConfig
	log    logger.Logger

	jobs           metric.Int64ObservableGauge
	discarded      metric.Int64ObservableGauge
	oldestReady    metric.Float64ObservableGauge
	oldestRunning  metric.Float64ObservableGauge
	snapshotAge    metric.Float64ObservableGauge
	snapshotFailed metric.Int64Counter

	startedAt time.Time

	mu       sync.RWMutex
	snapshot backlogSnapshot

	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	stopped     sync.WaitGroup
}

type backlogKey struct {
	kind       string
	queueClass string
	state      string
}

type backlogSeries struct {
	jobs          int64
	oldestReady   *time.Time
	oldestRunning *time.Time
}

type backlogSnapshot struct {
	collectedAt time.Time
	series      map[backlogKey]backlogSeries
	discarded   map[string]int64
}

func NewBacklog(pool *pgxpool.Pool, tel *telemetry.Client, l logger.Logger, config BacklogConfig) (*Backlog, error) {
	if tel == nil {
		tel = telemetry.NewNoopClient()
	}
	if l == nil {
		l = logger.NewNopLogger()
	}
	if config.QueueClass == nil {
		config.QueueClass = func(string) string { return defaultQueueClass }
	}
	if len(config.QueueClasses) == 0 {
		config.QueueClasses = []string{defaultQueueClass}
	}

	meter := tel.MeterProvider.Meter(instrumentationScope)
	backlog := &Backlog{
		pool:   pool,
		config: config,
		log:    l,
		jobs: utils.Must(meter.Int64ObservableGauge("outbox.jobs",
			metric.WithDescription("Jobs River has not finished, by kind, queue class and state."))),
		discarded: utils.Must(meter.Int64ObservableGauge("outbox.discarded",
			metric.WithDescription("Discarded jobs River keeps until an operator retries or deletes them, by kind."))),
		oldestReady: utils.Must(meter.Float64ObservableGauge("outbox.oldest_available_age", metric.WithUnit("s"),
			metric.WithDescription("How long the oldest job ready to run has waited for a worker, by kind and queue class."))),
		oldestRunning: utils.Must(meter.Float64ObservableGauge("outbox.oldest_running_age", metric.WithUnit("s"),
			metric.WithDescription("How long the oldest running job has run, by kind and queue class."))),
		snapshotAge: utils.Must(meter.Float64ObservableGauge("outbox.snapshot_age", metric.WithUnit("s"),
			metric.WithDescription("How old the backlog snapshot this export reports is."))),
		snapshotFailed: utils.Must(meter.Int64Counter("outbox.snapshot_failures",
			metric.WithDescription("Backlog reads that failed."))),
		startedAt: time.Now(),
		snapshot:  backlogSnapshot{series: map[backlogKey]backlogSeries{}, discarded: map[string]int64{}},
	}
	if _, err := meter.RegisterCallback(backlog.observe,
		backlog.jobs, backlog.discarded, backlog.oldestReady, backlog.oldestRunning, backlog.snapshotAge,
	); err != nil {
		return nil, fmt.Errorf("register the outbox backlog callback: %w", err)
	}

	return backlog, nil
}

// Start reads the backlog now and then every Interval until Stop.
func (b *Backlog) Start(ctx context.Context) {
	b.lifecycleMu.Lock()
	defer b.lifecycleMu.Unlock()
	if b.cancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	b.cancel = cancel
	b.stopped.Go(func() {
		b.collect(ctx)
		ticker := time.NewTicker(b.config.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				b.collect(ctx)
			}
		}
	})
}

func (b *Backlog) Stop() {
	b.lifecycleMu.Lock()
	cancel := b.cancel
	b.lifecycleMu.Unlock()
	if cancel == nil {
		return
	}

	cancel()
	b.stopped.Wait()
}

func (b *Backlog) observe(_ context.Context, observer metric.Observer) error {
	b.mu.RLock()
	snapshot := b.snapshot
	b.mu.RUnlock()

	kinds := slices.Clone(b.config.Kinds)
	for key := range snapshot.series {
		kinds = append(kinds, key.kind)
	}
	for kind := range snapshot.discarded {
		kinds = append(kinds, kind)
	}
	slices.Sort(kinds)

	for _, kind := range slices.Compact(kinds) {
		observer.ObserveInt64(b.discarded, snapshot.discarded[kind], metric.WithAttributes(attribute.String("job.kind", kind)))
		for _, class := range b.config.QueueClasses {
			for _, state := range ActiveJobStates {
				observer.ObserveInt64(b.jobs, snapshot.series[backlogKey{kind: kind, queueClass: class, state: state}].jobs,
					metric.WithAttributes(
						attribute.String("job.kind", kind),
						attribute.String("queue.class", class),
						attribute.String("state", state),
					))
			}
			attributes := metric.WithAttributes(attribute.String("job.kind", kind), attribute.String("queue.class", class))
			observer.ObserveFloat64(b.oldestReady,
				ageOf(snapshot.series[backlogKey{kind: kind, queueClass: class, state: "available"}].oldestReady), attributes)
			observer.ObserveFloat64(b.oldestRunning,
				ageOf(snapshot.series[backlogKey{kind: kind, queueClass: class, state: "running"}].oldestRunning), attributes)
		}
	}

	collectedAt := snapshot.collectedAt
	if collectedAt.IsZero() {
		collectedAt = b.startedAt
	}
	observer.ObserveFloat64(b.snapshotAge, time.Since(collectedAt).Seconds())

	return nil
}

func ageOf(since *time.Time) float64 {
	if since == nil {
		return 0
	}

	return time.Since(*since).Seconds()
}

func (b *Backlog) collect(ctx context.Context) {
	snapshot, err := b.read(ctx)
	if err != nil {
		b.snapshotFailed.Add(ctx, 1)
		b.log.Warn(ctx, "outbox backlog snapshot failed", zap.Error(err))

		return
	}

	b.mu.Lock()
	b.snapshot = snapshot
	b.mu.Unlock()
}

func (b *Backlog) read(ctx context.Context) (backlogSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, backlogReadTimeout)
	defer cancel()

	rows, err := b.pool.Query(ctx, fmt.Sprintf(`
		SELECT
			kind,
			queue,
			state::text,
			count(*)::bigint,
			min(scheduled_at) FILTER (WHERE state = 'available'),
			min(attempted_at) FILTER (WHERE state = 'running')
		FROM %[1]s.river_job
		WHERE state IN (SELECT unnest($1::text[])::%[1]s.river_job_state)
		GROUP BY kind, queue, state`, pgx.Identifier{b.config.Schema}.Sanitize()),
		append(slices.Clone(ActiveJobStates), "discarded"))
	if err != nil {
		return backlogSnapshot{}, fmt.Errorf("read the outbox backlog: %w", err)
	}
	defer rows.Close()

	snapshot := backlogSnapshot{
		collectedAt: time.Now(),
		series:      map[backlogKey]backlogSeries{},
		discarded:   map[string]int64{},
	}
	for rows.Next() {
		var (
			kind, queue, state         string
			jobs                       int64
			oldestReady, oldestRunning *time.Time
		)
		if err := rows.Scan(&kind, &queue, &state, &jobs, &oldestReady, &oldestRunning); err != nil {
			return backlogSnapshot{}, fmt.Errorf("read the outbox backlog: %w", err)
		}
		if state == "discarded" {
			snapshot.discarded[kind] += jobs

			continue
		}

		key := backlogKey{kind: kind, queueClass: b.config.QueueClass(queue), state: state}
		series := snapshot.series[key]
		series.jobs += jobs
		series.oldestReady = oldest(series.oldestReady, oldestReady)
		series.oldestRunning = oldest(series.oldestRunning, oldestRunning)
		snapshot.series[key] = series
	}
	if err := rows.Err(); err != nil {
		return backlogSnapshot{}, fmt.Errorf("read the outbox backlog: %w", err)
	}

	return snapshot, nil
}

func oldest(current, candidate *time.Time) *time.Time {
	if current == nil || (candidate != nil && candidate.Before(*current)) {
		return candidate
	}

	return current
}
