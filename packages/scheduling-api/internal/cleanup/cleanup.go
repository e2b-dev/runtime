// Package cleanup ends expired starts and purges old tombstones in the
// background. One replica at a time does the work: the one that holds a
// session advisory lock on a dedicated connection.
package cleanup

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	queries "github.com/e2b-dev/infra/packages/scheduling-api/internal/db/queries/gen"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/store"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// lockKey names the session advisory lock of the cleanup leader in the
// scheduling database. Every replica must use the same key.
const lockKey int64 = 0x7363_6865_645f_636c

const (
	applicationName = "scheduling-api-cleanup"
	connectTimeout  = 5 * time.Second
	checkTimeout    = time.Second
	releaseTimeout  = 5 * time.Second
)

type Config struct {
	// The wait after a round before the next one. Each round also has a work
	// budget of Interval, half for start expiry and half for the purge.
	Interval time.Duration
	// The most starts or tombstones that one statement handles.
	BatchSize int32
	// The pause between two full batches in a step.
	Yield time.Duration
	// The idle_session_timeout of the lock connection. PostgreSQL ends the
	// session, and so releases the lock, when the leader stops using it, for
	// example after its host failed. At least 3 Intervals, because the leader
	// uses the connection at least once in that time.
	IdleSessionTimeout time.Duration
}

var DefaultConfig = Config{
	Interval:           10 * time.Second,
	BatchSize:          1000,
	Yield:              10 * time.Millisecond,
	IdleSessionTimeout: 60 * time.Second,
}

// Worker is not safe for concurrent use: only Run uses it.
type Worker struct {
	store      *store.Store
	connConfig *pgx.ConnConfig
	config     Config

	leader    *pgx.Conn
	leaderPID int32
	after     uuid.UUID

	rounds atomic.Int64
}

var (
	meter = otel.Meter("github.com/e2b-dev/infra/packages/scheduling-api/internal/cleanup")

	expiredStarts = utils.Must(meter.Int64Counter("api.scheduling.cleanup.expired_starts",
		metric.WithDescription("Expired starts that the cleanup ended."), metric.WithUnit("{start}")))
	failedStarts = utils.Must(meter.Int64Counter("api.scheduling.cleanup.failed_starts",
		metric.WithDescription("Attempts to end an expired start that failed. The start is tried again in a later round."), metric.WithUnit("{start}")))
	purgedTombstones = utils.Must(meter.Int64Counter("api.scheduling.cleanup.purged_tombstones",
		metric.WithDescription("Tombstones that the cleanup deleted."), metric.WithUnit("{tombstone}")))
	roundFailures = utils.Must(meter.Int64Counter("api.scheduling.cleanup.round.failures",
		metric.WithDescription("Cleanup rounds with at least one failure."), metric.WithUnit("{round}")))
	roundDuration = utils.Must(meter.Float64Histogram("api.scheduling.cleanup.round.duration",
		metric.WithDescription("Duration of the cleanup rounds of the leader."), metric.WithUnit("s")))

	leaders atomic.Int64
	_       = utils.Must(meter.Int64ObservableGauge("api.scheduling.cleanup.leader",
		metric.WithDescription("Cleanup workers of this process that hold the leader lock: 1 on the leader replica, else 0."), metric.WithUnit("1"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(leaders.Load())

			return nil
		})))
)

type roundStats struct {
	expired, failed int
	purged          int64
	errs            []error
}

// New returns a worker that does the work through st and holds the lock on
// its own connection, made from connConfig. That connection is not part of
// the pool of st, and always has the application_name and
// idle_session_timeout of the worker.
func New(st *store.Store, connConfig *pgx.ConnConfig, config Config) (*Worker, error) {
	if config.Interval <= 0 || config.BatchSize <= 0 || config.Yield < 0 ||
		config.IdleSessionTimeout < 3*config.Interval || config.IdleSessionTimeout%time.Millisecond != 0 {
		return nil, fmt.Errorf("invalid cleanup configuration %+v", config)
	}

	connConfig = connConfig.Copy()
	if connConfig.RuntimeParams == nil {
		connConfig.RuntimeParams = map[string]string{}
	}
	connConfig.RuntimeParams["application_name"] = applicationName
	connConfig.RuntimeParams["idle_session_timeout"] = fmt.Sprintf("%dms", config.IdleSessionTimeout.Milliseconds())

	return &Worker{store: st, connConfig: connConfig, config: config}, nil
}

func record(ctx context.Context, stats roundStats, duration time.Duration) {
	expiredStarts.Add(ctx, int64(stats.expired))
	failedStarts.Add(ctx, int64(stats.failed))
	purgedTombstones.Add(ctx, stats.purged)
	if len(stats.errs) > 0 {
		roundFailures.Add(ctx, 1)
	}
	roundDuration.Record(ctx, duration.Seconds())
}

// Run does a round at once and then Interval after the end of each round,
// until ctx is cancelled. It releases the lock and closes its connection
// before it returns.
func (w *Worker) Run(ctx context.Context) {
	defer w.resign(ctx)

	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		w.round(ctx)
		timer.Reset(w.config.Interval)
	}
}

// round runs the two steps, each within half of the work budget. Running out
// of a budget ends the step without an error.
func (w *Worker) round(ctx context.Context) roundStats {
	var stats roundStats
	if !w.lead(ctx) {
		return stats
	}
	w.rounds.Add(1)
	started := time.Now()
	defer func() { record(ctx, stats, time.Since(started)) }()

	expireCtx, cancel := context.WithTimeout(ctx, w.config.Interval/2)
	w.expire(ctx, expireCtx, &stats)
	cancel()
	if ctx.Err() != nil {
		return stats
	}
	purgeCtx, cancel := context.WithTimeout(ctx, w.config.Interval/2)
	w.purge(ctx, purgeCtx, &stats)
	cancel()
	if ctx.Err() != nil {
		return stats
	}

	fields := []zap.Field{
		zap.Int("expired_starts", stats.expired),
		zap.Int("failed_starts", stats.failed),
		zap.Int64("purged_tombstones", stats.purged),
		zap.Duration("duration", time.Since(started)),
	}
	switch {
	case len(stats.errs) > 0:
		logger.L().Error(ctx, "scheduling cleanup round failed", append(fields, zap.Error(errors.Join(stats.errs...)))...)
	case stats.expired > 0 || stats.purged > 0:
		logger.L().Info(ctx, "scheduling cleanup round", fields...)
	}

	return stats
}

func (w *Worker) expire(parent, ctx context.Context, stats *roundStats) {
	for w.holdsLock(parent, ctx) {
		result, err := w.store.ExpireStarts(ctx, w.after, w.config.BatchSize)
		stats.expired += result.Ended
		stats.failed += result.Failed
		w.after = result.Next
		failed := err != nil && (result.Failed > 0 || ctx.Err() == nil)
		if failed {
			stats.errs = append(stats.errs, err)
		}
		if (failed && result.Failed == 0) || ctx.Err() != nil || result.Next == uuid.Nil || !w.yield(ctx) {
			return
		}
	}
}

func (w *Worker) purge(parent, ctx context.Context, stats *roundStats) {
	for w.holdsLock(parent, ctx) {
		deleted, err := w.store.PurgeTombstones(ctx, w.config.BatchSize)
		stats.purged += deleted
		if err != nil {
			if ctx.Err() == nil {
				stats.errs = append(stats.errs, err)
			}

			return
		}
		if deleted < int64(w.config.BatchSize) || !w.yield(ctx) {
			return
		}
	}
}

func (w *Worker) yield(ctx context.Context) bool {
	timer := time.NewTimer(w.config.Yield)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// lead reports whether this worker leads, and tries to take the lock when it
// does not. The steps check the lock before each batch. A worker that does
// not get the lock keeps no connection.
func (w *Worker) lead(ctx context.Context) bool {
	if w.leader != nil {
		return true
	}

	connectCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	conn, err := pgx.ConnectConfig(connectCtx, w.connConfig)
	if err != nil {
		if ctx.Err() == nil {
			logger.L().Warn(ctx, "scheduling cleanup cannot connect to the database", zap.Error(err))
		}

		return false
	}
	locked, err := queries.New(conn).TryCleanupLock(connectCtx, lockKey)
	if err != nil || !locked.Locked {
		if err != nil && ctx.Err() == nil {
			logger.L().Warn(ctx, "scheduling cleanup cannot try the leader lock", zap.Error(err))
		}
		closeCtx, cancelClose := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
		defer cancelClose()
		_ = conn.Close(closeCtx)

		return false
	}

	w.leader, w.leaderPID, w.after = conn, locked.Pid, uuid.Nil
	leaders.Add(1)
	logger.L().Info(ctx, "scheduling cleanup leadership acquired", zap.Int32("backend_pid", locked.Pid))

	return true
}

// holdsLock reports whether the leader connection still runs on the backend
// that took the lock, and drops the connection when it does not. It checks
// only while the step budget remains. The check itself runs under ctx, not
// the budget: pgx closes a connection whose query is cancelled, and that
// would release the lock.
func (w *Worker) holdsLock(ctx, step context.Context) bool {
	if w.leader == nil || step.Err() != nil {
		return false
	}

	checkCtx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	pid, err := queries.New(w.leader).CleanupBackendPID(checkCtx)
	if err == nil && pid == w.leaderPID {
		return true
	}
	if ctx.Err() != nil {
		return false
	}
	logger.L().Warn(ctx, "scheduling cleanup leadership lost", zap.Error(err))
	w.closeLeader(ctx)

	return false
}

func (w *Worker) resign(ctx context.Context) {
	if w.leader == nil {
		return
	}
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()
	if _, err := queries.New(w.leader).ReleaseCleanupLock(releaseCtx, lockKey); err != nil {
		logger.L().Warn(releaseCtx, "scheduling cleanup cannot release the leader lock; closing the connection releases it", zap.Error(err))
	} else {
		logger.L().Info(releaseCtx, "scheduling cleanup leadership released")
	}
	w.closeLeader(releaseCtx)
}

func (w *Worker) closeLeader(ctx context.Context) {
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()
	_ = w.leader.Close(closeCtx)
	w.leader, w.leaderPID, w.after = nil, 0, uuid.Nil
	leaders.Add(-1)
}
