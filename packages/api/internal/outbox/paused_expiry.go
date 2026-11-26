package outbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	expirePausedSandboxesKind = "expire_paused_sandboxes"
	expirePausedSandboxesID   = "expire-paused-sandboxes"
	// Must stay under rescueStuckJobsAfter, or a slow sweep is retried
	// underneath itself.
	expirePausedSandboxesBudget = 4 * time.Minute
	// A sweep stops taking new sandboxes once this has passed, and the next
	// run resumes after the last one it took. One sandbox can take up to the
	// 25s snapshot delete timeout, which has to fit before the budget ends.
	expirePausedSandboxesSoftBudget = 3*time.Minute + 15*time.Second
)

// PausedExpiryOutcome is what the sweep did with one expired paused sandbox.
type PausedExpiryOutcome string

const (
	PausedExpiryDeleted          PausedExpiryOutcome = "deleted"
	PausedExpiryWouldDelete      PausedExpiryOutcome = "would_delete"
	PausedExpirySkippedLive      PausedExpiryOutcome = "skipped_live"
	PausedExpirySkippedRefreshed PausedExpiryOutcome = "skipped_refreshed"
	PausedExpiryFailed           PausedExpiryOutcome = "failed"
)

// pausedSandboxExpirer is the API side of the sweep: the handlers' store,
// which deletes a paused sandbox the way DELETE /sandboxes/{id} does.
type pausedSandboxExpirer interface {
	// ExpiredPausedSandboxes returns one page of the live snapshots whose
	// expiry passed by asOf, after the cursor, oldest first.
	ExpiredPausedSandboxes(ctx context.Context, asOf time.Time, after queries.ListExpiredPausedSnapshotsRow, limit int32) ([]queries.ListExpiredPausedSnapshotsRow, error)
	// PausedSandboxLive reports whether a run of the sandbox is in the store,
	// without claiming it.
	PausedSandboxLive(ctx context.Context, teamID uuid.UUID, sandboxID string) (bool, error)
	// ExpirePausedSandbox soft-deletes the sandbox's snapshot if no run of it
	// is live and its expiry has still passed.
	ExpirePausedSandbox(ctx context.Context, teamID uuid.UUID, sandboxID string) (PausedExpiryOutcome, error)
	// LoadSweepCursor returns where an unfinished sweep stopped, nil when the
	// last one reached the end.
	LoadSweepCursor(ctx context.Context) (*queries.ListExpiredPausedSnapshotsRow, error)
	// SaveSweepCursor stores where the sweep stopped, or clears it when nil.
	// Any API replica may run the next sweep, so it is shared.
	SaveSweepCursor(ctx context.Context, cursor *queries.ListExpiredPausedSnapshotsRow) error
}

type expirePausedSandboxesArgs struct{}

func (expirePausedSandboxesArgs) Kind() string { return expirePausedSandboxesKind }

func (expirePausedSandboxesArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: river.QueueDefault, UniqueOpts: uniqueInFlight}
}

func expirePausedSandboxes(every time.Duration) *river.PeriodicJob {
	return river.NewPeriodicJob(
		river.PeriodicInterval(every),
		func() (river.JobArgs, *river.InsertOpts) { return expirePausedSandboxesArgs{}, nil },
		&river.PeriodicJobOpts{ID: expirePausedSandboxesID, RunOnStart: true},
	)
}

// uniqueInFlight leaves terminal states out: River's default counts a
// completed job as a conflict, deduplicating a periodic kind against its own
// success.
var uniqueInFlight = river.UniqueOpts{
	ByState: []rivertype.JobState{
		rivertype.JobStateAvailable,
		rivertype.JobStatePending,
		rivertype.JobStateRunning,
		rivertype.JobStateRetryable,
		rivertype.JobStateScheduled,
	},
}

type pausedExpiryWorker struct {
	river.WorkerDefaults[expirePausedSandboxesArgs]

	expirer   pausedSandboxExpirer
	flags     *featureflags.Client
	batchSize int32
	// concurrency caps how many sandboxes of a page are handled at once.
	concurrency int
	swept       metric.Int64Counter
	runDuration metric.Int64Histogram
	log         logger.Logger
	// softBudget and now are fields so tests can run a sweep out of time.
	softBudget time.Duration
	now        func() time.Time
}

// expiryTuning is what one run uses: the worker's configured values, with any
// valid field of the paused-sandbox-expiry-tuning flag on top.
type expiryTuning struct {
	batchSize   int32
	concurrency int
	budget      time.Duration
}

// Bounds of the tuning flag's fields. maxTuningBudget is the soft budget that
// still leaves room for one last delete before the hard timeout.
const (
	maxTuningBatchSize   = 1000
	maxTuningConcurrency = 64
	minTuningBudget      = 10 * time.Second
	maxTuningBudget      = expirePausedSandboxesSoftBudget
)

func (w *pausedExpiryWorker) tuning(ctx context.Context) expiryTuning {
	return parseExpiryTuning(w.flags.JSONFlag(ctx, featureflags.PausedSandboxExpiryTuning), expiryTuning{
		batchSize:   w.batchSize,
		concurrency: max(w.concurrency, 1),
		budget:      w.softBudget,
	})
}

// parseExpiryTuning keeps a default for every field the flag leaves out or
// sets out of range, so a bad value never stops the sweep.
func parseExpiryTuning(value ldvalue.Value, defaults expiryTuning) expiryTuning {
	tuning := defaults
	if v := value.GetByKey("batchSize"); v.IsInt() && v.IntValue() >= 1 && v.IntValue() <= maxTuningBatchSize {
		tuning.batchSize = int32(v.IntValue())
	}
	if v := value.GetByKey("concurrency"); v.IsInt() && v.IntValue() >= 1 && v.IntValue() <= maxTuningConcurrency {
		tuning.concurrency = v.IntValue()
	}
	if v := value.GetByKey("budgetSeconds"); v.IsInt() {
		budget := time.Duration(v.IntValue()) * time.Second
		if budget >= minTuningBudget && budget <= maxTuningBudget {
			tuning.budget = budget
		}
	}

	return tuning
}

// Sweep results, the result attribute of the run duration.
const (
	sweepReachedEnd = "reached_end"
	sweepOutOfTime  = "out_of_time"
	sweepStopped    = "stopped"
	sweepFailed     = "failed"
)

// Work walks the snapshots that had expired when it started, a page at a time,
// from where the last unfinished sweep stopped. Sandboxes it leaves in place
// (live, dry run, disabled, failed) are visited again once a sweep reaches the
// end and starts over, so they cannot hold back the ones behind them. Only a
// failed page read fails the job; running out of time does not, so the next
// periodic run always starts on schedule.
func (w *pausedExpiryWorker) Work(ctx context.Context, _ *river.Job[expirePausedSandboxesArgs]) error {
	start := w.now()
	cursor, err := w.expirer.LoadSweepCursor(ctx)
	if err != nil {
		w.log.Warn(ctx, "failed to load the paused expiry sweep cursor, starting from the oldest", zap.Error(err))
	}
	after := queries.ListExpiredPausedSnapshotsRow{ExpiresAt: time.Unix(0, 0)}
	if cursor != nil {
		after = *cursor
	}

	reachedEnd, err := w.sweep(ctx, start, w.tuning(ctx), &after)
	// A stop that cancelled the run mid-delete must reach River as one, so it
	// hands the job back instead of counting it as done.
	if err == nil {
		err = ctx.Err()
	}

	saved := &after
	if reachedEnd {
		saved = nil
	}
	if err := w.expirer.SaveSweepCursor(context.WithoutCancel(ctx), saved); err != nil {
		w.log.Warn(ctx, "failed to save the paused expiry sweep cursor", zap.Error(err))
	}

	result := sweepOutOfTime
	switch {
	case ctx.Err() != nil:
		result = sweepStopped
	case err != nil:
		result = sweepFailed
	case reachedEnd:
		result = sweepReachedEnd
	}
	w.runDuration.Record(context.WithoutCancel(ctx), w.now().Sub(start).Milliseconds(), metric.WithAttributes(attribute.String("result", result)))

	return err
}

// sweep moves after past each sandbox it takes, and reports whether it read
// to the end of the expired snapshots. Every step is bounded by the soft
// budget: a page read gets only the time left, and a page stops taking
// sandboxes once it is spent, so a run ends before River's hard timeout even
// when the database or the deletes are slow.
func (w *pausedExpiryWorker) sweep(ctx context.Context, start time.Time, tuning expiryTuning, after *queries.ListExpiredPausedSnapshotsRow) (bool, error) {
	for {
		left := tuning.budget - w.now().Sub(start)
		if left <= 0 {
			return false, nil
		}
		readCtx, cancel := context.WithTimeout(ctx, left)
		page, err := w.expirer.ExpiredPausedSandboxes(readCtx, start, *after, tuning.batchSize)
		cancel()
		if err != nil {
			if ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
				return false, nil
			}

			return false, fmt.Errorf("list expired paused sandboxes: %w", err)
		}

		taken, err := w.expirePage(ctx, start, tuning, page)
		if taken > 0 {
			*after = page[taken-1]
		}
		if err != nil {
			return false, err
		}
		if taken < len(page) {
			return false, nil
		}

		if len(page) < int(tuning.batchSize) {
			return true, nil
		}
	}
}

// expirePage hands the page's sandboxes out in order to at most concurrency
// workers and waits for all of them, so every sandbox up to the last one taken
// has been handled when it returns. It stops taking sandboxes once the soft
// budget is spent or ctx ends, and reports how many it took.
func (w *pausedExpiryWorker) expirePage(ctx context.Context, start time.Time, tuning expiryTuning, page []queries.ListExpiredPausedSnapshotsRow) (int, error) {
	slots := make(chan struct{}, max(tuning.concurrency, 1))
	var wg sync.WaitGroup
	taken := 0
	var stopErr error
	for _, expired := range page {
		if stopErr = ctx.Err(); stopErr != nil {
			break
		}
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			stopErr = ctx.Err()
		}
		if stopErr != nil {
			break
		}
		// Checked after a slot is free: waiting for one can take a whole delete.
		if w.now().Sub(start) >= tuning.budget {
			<-slots

			break
		}
		taken++
		wg.Go(func() {
			defer func() { <-slots }()
			w.expire(ctx, expired)
		})
	}
	wg.Wait()

	return taken, stopErr
}

func (w *pausedExpiryWorker) expire(ctx context.Context, expired queries.ListExpiredPausedSnapshotsRow) {
	mode := w.flags.StringFlag(ctx, featureflags.PausedSandboxExpiryMode, featureflags.TeamContext(expired.TeamID.String()))

	var outcome PausedExpiryOutcome
	var err error
	switch mode {
	case featureflags.PausedSandboxExpiryEnabled:
		outcome, err = w.expirer.ExpirePausedSandbox(ctx, expired.TeamID, expired.SandboxID)
	case featureflags.PausedSandboxExpiryDryRun:
		var live bool
		live, err = w.expirer.PausedSandboxLive(ctx, expired.TeamID, expired.SandboxID)
		outcome = PausedExpiryWouldDelete
		if live {
			outcome = PausedExpirySkippedLive
		}
	default:
		return
	}

	fields := []zap.Field{
		logger.WithTeamID(expired.TeamID.String()),
		logger.WithSandboxID(expired.SandboxID),
		logger.Time("expires_at", expired.ExpiresAt),
		zap.String("mode", mode),
	}
	switch {
	case err != nil:
		outcome = PausedExpiryFailed
		w.log.Error(ctx, "failed to expire a paused sandbox", append(fields, zap.Error(err))...)
	case outcome == PausedExpiryDeleted:
		w.log.Info(ctx, "deleted an expired paused sandbox", fields...)
	case outcome == PausedExpiryWouldDelete:
		w.log.Info(ctx, "would delete an expired paused sandbox (dry run)", fields...)
	}

	w.swept.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", string(outcome))))
}

func (*pausedExpiryWorker) Timeout(*river.Job[expirePausedSandboxesArgs]) time.Duration {
	return expirePausedSandboxesBudget
}
