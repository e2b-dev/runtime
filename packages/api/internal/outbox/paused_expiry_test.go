package outbox

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	dbmodule "github.com/e2b-dev/infra/packages/api/internal/db"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// expirerStub holds expired paused sandboxes in sweep order and records what
// the worker asks of each.
type expirerStub struct {
	mu       sync.Mutex
	expired  []queries.ListExpiredPausedSnapshotsRow
	live     map[string]bool
	outcomes map[string]PausedExpiryOutcome
	failing  map[string]bool
	listErr  error
	// cursor is the shared sweep cursor; loadErr fails reading it.
	cursor  *queries.ListExpiredPausedSnapshotsRow
	loadErr error
	// onExpire runs for every sandbox the worker takes, before it is handled.
	onExpire func()
	// slowRead makes a page read wait until its context ends.
	slowRead bool
	// holdExpire makes a delete wait until its context ends, after closing
	// expiring (once).
	holdExpire bool
	expiring   chan struct{}
	signalOnce sync.Once

	pages   int
	expires []string
	probes  []string
}

func (s *expirerStub) LoadSweepCursor(context.Context) (*queries.ListExpiredPausedSnapshotsRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.cursor, s.loadErr
}

func (s *expirerStub) SaveSweepCursor(_ context.Context, cursor *queries.ListExpiredPausedSnapshotsRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursor = cursor

	return nil
}

func (s *expirerStub) savedCursor() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cursor == nil {
		return ""
	}

	return s.cursor.SandboxID
}

func (s *expirerStub) ExpiredPausedSandboxes(ctx context.Context, _ time.Time, after queries.ListExpiredPausedSnapshotsRow, limit int32) ([]queries.ListExpiredPausedSnapshotsRow, error) {
	if s.slowRead {
		<-ctx.Done()

		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pages++
	if s.listErr != nil {
		return nil, s.listErr
	}

	var page []queries.ListExpiredPausedSnapshotsRow
	for _, row := range s.expired {
		if row.ExpiresAt.Before(after.ExpiresAt) || (row.ExpiresAt.Equal(after.ExpiresAt) && row.SandboxID <= after.SandboxID) {
			continue
		}
		if len(page) == int(limit) {
			break
		}
		page = append(page, row)
	}

	return page, nil
}

func (s *expirerStub) PausedSandboxLive(_ context.Context, _ uuid.UUID, sandboxID string) (bool, error) {
	if s.onExpire != nil {
		s.onExpire()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.probes = append(s.probes, sandboxID)
	if s.failing[sandboxID] {
		return false, errors.New("store unavailable")
	}

	return s.live[sandboxID], nil
}

func (s *expirerStub) ExpirePausedSandbox(ctx context.Context, _ uuid.UUID, sandboxID string) (PausedExpiryOutcome, error) {
	if s.onExpire != nil {
		s.onExpire()
	}
	if s.holdExpire {
		s.signalOnce.Do(func() { close(s.expiring) })
		<-ctx.Done()

		return "", ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expires = append(s.expires, sandboxID)
	if s.failing[sandboxID] {
		return "", errors.New("database unavailable")
	}
	if outcome, ok := s.outcomes[sandboxID]; ok {
		return outcome, nil
	}

	return PausedExpiryDeleted, nil
}

func expiredRows(teamID uuid.UUID, ids ...string) []queries.ListExpiredPausedSnapshotsRow {
	start := time.Now().Add(-time.Hour)
	rows := make([]queries.ListExpiredPausedSnapshotsRow, len(ids))
	for i, id := range ids {
		rows[i] = queries.ListExpiredPausedSnapshotsRow{SandboxID: id, TeamID: teamID, ExpiresAt: start.Add(time.Duration(i) * time.Second)}
	}

	return rows
}

// modeFlags serves mode to every team but those in perTeam.
func modeFlags(t *testing.T, mode string, perTeam map[uuid.UUID]string) *featureflags.Client {
	t.Helper()

	return tunedFlags(t, mode, perTeam, ldvalue.Null())
}

// tunedFlags is modeFlags with the tuning flag set to tuning.
func tunedFlags(t *testing.T, mode string, perTeam map[uuid.UUID]string, tuning ldvalue.Value) *featureflags.Client {
	t.Helper()

	modes := []string{featureflags.PausedSandboxExpiryDisabled, featureflags.PausedSandboxExpiryDryRun, featureflags.PausedSandboxExpiryEnabled}
	values := make([]ldvalue.Value, len(modes))
	for i, m := range modes {
		values[i] = ldvalue.String(m)
	}
	td := ldtestdata.DataSource()
	flag := td.Flag(featureflags.PausedSandboxExpiryMode.Key()).Variations(values...).FallthroughVariationIndex(slices.Index(modes, mode))
	for teamID, teamMode := range perTeam {
		flag = flag.VariationIndexForKey(featureflags.TeamKind, teamID.String(), slices.Index(modes, teamMode))
	}
	td.Update(flag)
	if !tuning.IsNull() {
		td.Update(td.Flag(featureflags.PausedSandboxExpiryTuning.Key()).ValueForAll(tuning))
	}

	client, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close(context.WithoutCancel(t.Context())) })

	return client
}

type sweepRun struct {
	err      error
	outcomes map[string]int64
	// results counts the recorded run durations by result.
	results map[string]uint64
}

func runSweep(t *testing.T, ctx context.Context, stub *expirerStub, flags *featureflags.Client, batch int32) sweepRun {
	t.Helper()

	return runSweepWith(t, ctx, stub, flags, batch, expirePausedSandboxesSoftBudget, time.Now)
}

// runSweepWith handles one sandbox at a time, so tests can check the order.
func runSweepWith(t *testing.T, ctx context.Context, stub *expirerStub, flags *featureflags.Client, batch int32, softBudget time.Duration, now func() time.Time) sweepRun {
	t.Helper()

	return runSweepConcurrent(t, ctx, stub, flags, batch, 1, softBudget, now)
}

func runSweepConcurrent(t *testing.T, ctx context.Context, stub *expirerStub, flags *featureflags.Client, batch int32, concurrency int, softBudget time.Duration, now func() time.Time) sweepRun {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/outbox")
	swept, err := telemetry.GetCounter(meter, telemetry.ApiSandboxPausedExpirySwept)
	require.NoError(t, err)
	runDuration, err := telemetry.GetHistogram(meter, telemetry.ApiSandboxPausedExpiryRunDuration)
	require.NoError(t, err)

	worker := &pausedExpiryWorker{
		expirer: stub, flags: flags, batchSize: batch, concurrency: concurrency,
		swept: swept, runDuration: runDuration, log: logger.NewNopLogger(), softBudget: softBudget, now: now,
	}
	run := sweepRun{err: worker.Work(ctx, &river.Job[expirePausedSandboxesArgs]{}), outcomes: map[string]int64{}, results: map[string]uint64{}}

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.WithoutCancel(ctx), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case string(telemetry.ApiSandboxPausedExpirySwept):
				for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
					outcome, _ := dp.Attributes.Value("outcome")
					run.outcomes[outcome.AsString()] += dp.Value
				}
			case string(telemetry.ApiSandboxPausedExpiryRunDuration):
				for _, dp := range m.Data.(metricdata.Histogram[int64]).DataPoints {
					result, _ := dp.Attributes.Value("result")
					run.results[result.AsString()] += dp.Count
				}
			}
		}
	}

	return run
}

func TestPausedExpirySweep(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()

	t.Run("enabled expires every expired sandbox across pages", func(t *testing.T) {
		t.Parallel()

		stub := &expirerStub{
			expired:  expiredRows(teamID, "a", "b", "c", "d", "e"),
			outcomes: map[string]PausedExpiryOutcome{"b": PausedExpirySkippedLive, "d": PausedExpirySkippedRefreshed},
			failing:  map[string]bool{"c": true},
		}

		run := runSweep(t, t.Context(), stub, modeFlags(t, featureflags.PausedSandboxExpiryEnabled, nil), 2)
		require.NoError(t, run.err, "a sandbox that fails is counted and left for the next run")
		assert.Equal(t, []string{"a", "b", "c", "d", "e"}, stub.expires)
		assert.Equal(t, 3, stub.pages, "two full pages and a short one")
		assert.Empty(t, stub.probes)
		assert.Equal(t, map[string]int64{"deleted": 2, "skipped_live": 1, "skipped_refreshed": 1, "failed": 1}, run.outcomes)
	})

	t.Run("a sweep that ends on a full page reads one more", func(t *testing.T) {
		t.Parallel()

		stub := &expirerStub{expired: expiredRows(teamID, "a", "b")}

		run := runSweep(t, t.Context(), stub, modeFlags(t, featureflags.PausedSandboxExpiryEnabled, nil), 2)
		require.NoError(t, run.err)
		assert.Equal(t, 2, stub.pages)
		assert.Equal(t, []string{"a", "b"}, stub.expires)
	})

	t.Run("dry run deletes nothing and counts what it would", func(t *testing.T) {
		t.Parallel()

		stub := &expirerStub{expired: expiredRows(teamID, "a", "b", "c"), live: map[string]bool{"b": true}, failing: map[string]bool{"c": true}}

		run := runSweep(t, t.Context(), stub, modeFlags(t, featureflags.PausedSandboxExpiryDryRun, nil), 100)
		require.NoError(t, run.err)
		assert.Empty(t, stub.expires)
		assert.Equal(t, []string{"a", "b", "c"}, stub.probes)
		assert.Equal(t, map[string]int64{"would_delete": 1, "skipped_live": 1, "failed": 1}, run.outcomes)
	})

	t.Run("disabled touches nothing and counts nothing", func(t *testing.T) {
		t.Parallel()

		stub := &expirerStub{expired: expiredRows(teamID, "a", "b")}

		run := runSweep(t, t.Context(), stub, modeFlags(t, featureflags.PausedSandboxExpiryDisabled, nil), 100)
		require.NoError(t, run.err)
		assert.Empty(t, stub.expires)
		assert.Empty(t, stub.probes)
		assert.Empty(t, run.outcomes)
	})

	t.Run("an unknown mode reads as disabled", func(t *testing.T) {
		t.Parallel()

		td := ldtestdata.DataSource()
		td.Update(td.Flag(featureflags.PausedSandboxExpiryMode.Key()).ValueForAll(ldvalue.String("on")))
		flags, err := featureflags.NewClientWithDatasource(td)
		require.NoError(t, err)
		t.Cleanup(func() { _ = flags.Close(context.WithoutCancel(t.Context())) })
		stub := &expirerStub{expired: expiredRows(teamID, "a")}

		run := runSweep(t, t.Context(), stub, flags, 100)
		require.NoError(t, run.err)
		assert.Empty(t, stub.expires)
		assert.Empty(t, stub.probes)
	})

	t.Run("the mode is the team's own", func(t *testing.T) {
		t.Parallel()

		enabledTeam, dryRunTeam := uuid.New(), uuid.New()
		rows := append(expiredRows(enabledTeam, "enabled"), expiredRows(dryRunTeam, "dry")...)
		rows = append(rows, expiredRows(uuid.New(), "other")...)
		slices.SortFunc(rows, func(a, b queries.ListExpiredPausedSnapshotsRow) int { return strings.Compare(a.SandboxID, b.SandboxID) })
		stub := &expirerStub{expired: rows}

		flags := modeFlags(t, featureflags.PausedSandboxExpiryDisabled, map[uuid.UUID]string{
			enabledTeam: featureflags.PausedSandboxExpiryEnabled,
			dryRunTeam:  featureflags.PausedSandboxExpiryDryRun,
		})
		run := runSweep(t, t.Context(), stub, flags, 100)
		require.NoError(t, run.err)
		assert.Equal(t, []string{"enabled"}, stub.expires)
		assert.Equal(t, []string{"dry"}, stub.probes)
	})

	t.Run("a failed page read fails the job for River to retry", func(t *testing.T) {
		t.Parallel()

		stub := &expirerStub{listErr: errors.New("database unavailable")}

		run := runSweep(t, t.Context(), stub, modeFlags(t, featureflags.PausedSandboxExpiryEnabled, nil), 100)
		require.ErrorContains(t, run.err, "database unavailable")
	})

	t.Run("a sweep out of time stops and fails", func(t *testing.T) {
		t.Parallel()

		stub := &expirerStub{expired: expiredRows(teamID, "a", "b")}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		run := runSweep(t, ctx, stub, modeFlags(t, featureflags.PausedSandboxExpiryEnabled, nil), 100)
		require.ErrorIs(t, run.err, context.Canceled)
		assert.Empty(t, stub.expires)
	})
}

func TestPausedExpirySweepResumesWhereItStopped(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	flags := modeFlags(t, featureflags.PausedSandboxExpiryEnabled, nil)

	t.Run("a sweep out of time stops after the last sandbox it took and the next one resumes there", func(t *testing.T) {
		t.Parallel()

		stub := &expirerStub{expired: expiredRows(teamID, "a", "b", "c", "d", "e")}
		clock := time.Now()
		stub.onExpire = func() { clock = clock.Add(time.Minute) }
		now := func() time.Time { return clock }

		first := runSweepWith(t, t.Context(), stub, flags, 2, 150*time.Second, now)
		require.NoError(t, first.err, "running out of time is not a failure")
		assert.Equal(t, []string{"a", "b", "c"}, stub.expires)
		assert.Equal(t, "c", stub.savedCursor())

		second := runSweepWith(t, t.Context(), stub, flags, 2, 150*time.Second, now)
		require.NoError(t, second.err)
		assert.Equal(t, []string{"a", "b", "c", "d", "e"}, stub.expires, "the next sweep resumes after c")
		assert.Empty(t, stub.savedCursor(), "a sweep that reads to the end starts the next one from the oldest")
	})

	t.Run("sandboxes left in place cannot hold back the ones behind them", func(t *testing.T) {
		t.Parallel()

		// Each run has time for two sandboxes, and a, b and c are never deleted.
		stub := &expirerStub{
			expired: expiredRows(teamID, "a", "b", "c", "d"),
			failing: map[string]bool{"a": true, "b": true, "c": true},
		}
		clock := time.Now()
		stub.onExpire = func() { clock = clock.Add(time.Minute) }
		now := func() time.Time { return clock }

		runSweepWith(t, t.Context(), stub, flags, 100, 90*time.Second, now)
		runSweepWith(t, t.Context(), stub, flags, 100, 90*time.Second, now)
		assert.Equal(t, []string{"a", "b", "c", "d"}, stub.expires)
	})

	t.Run("a failed page read keeps the cursor for the retry", func(t *testing.T) {
		t.Parallel()

		saved := expiredRows(teamID, "b")[0]
		stub := &expirerStub{cursor: &saved, listErr: errors.New("database unavailable")}

		run := runSweep(t, t.Context(), stub, flags, 100)
		require.ErrorContains(t, run.err, "database unavailable")
		assert.Equal(t, "b", stub.savedCursor())
	})

	t.Run("an unreadable cursor starts from the oldest", func(t *testing.T) {
		t.Parallel()

		stub := &expirerStub{expired: expiredRows(teamID, "a", "b"), loadErr: errors.New("redis unavailable")}

		run := runSweep(t, t.Context(), stub, flags, 100)
		require.NoError(t, run.err)
		assert.Equal(t, []string{"a", "b"}, stub.expires)
	})
}

func TestStartingTheOutboxRunsTheExpirySweep(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	stub := &expirerStub{expired: expiredRows(uuid.New(), "a")}
	outboxRiver, err := New(Dependencies{
		Pool:         db.SqlcClient.Pool(),
		Teams:        db.AuthDb,
		PausedExpiry: stub,
		FeatureFlags: modeFlags(t, featureflags.PausedSandboxExpiryEnabled, nil),
		Logger:       logger.NewNopLogger(),
		Telemetry:    telemetry.NewNoopClient(),
		Config:       Config{MaxWorkers: 1, BacklogInterval: time.Second, PausedExpiryInterval: time.Hour, PausedExpiryBatchSize: 100},
	})
	require.NoError(t, err)

	completed, cancelSubscription := outboxRiver.client.Subscribe(river.EventKindJobCompleted)
	defer cancelSubscription()

	require.NoError(t, outboxRiver.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, outboxRiver.Stop(context.WithoutCancel(t.Context()))) })

	select {
	case event := <-completed:
		require.Equal(t, expirePausedSandboxesKind, event.Job.Kind)
	case <-time.After(30 * time.Second):
		require.Fail(t, "the sweep did not run on start")
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	assert.Equal(t, []string{"a"}, stub.expires)
}

func TestPausedExpiryGivesUpBeforeItIsRescued(t *testing.T) {
	t.Parallel()

	assert.Less(t, (&pausedExpiryWorker{}).Timeout(nil), rescueStuckJobsAfter)
}

func TestPausedExpiryRefusesToOverlapItselfButNotItsHistory(t *testing.T) {
	t.Parallel()

	states := expirePausedSandboxesArgs{}.InsertOpts().UniqueOpts.ByState
	for _, inFlight := range []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateRunning, rivertype.JobStateRetryable, rivertype.JobStateScheduled} {
		assert.Contains(t, states, inFlight)
	}
	for _, done := range []rivertype.JobState{rivertype.JobStateCompleted, rivertype.JobStateCancelled, rivertype.JobStateDiscarded} {
		assert.NotContains(t, states, done)
	}
}

func TestPausedExpirySweepEndsOnTime(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	flags := modeFlags(t, featureflags.PausedSandboxExpiryEnabled, nil)

	t.Run("a slow page read ends the run at its budget instead of failing it", func(t *testing.T) {
		t.Parallel()

		saved := expiredRows(teamID, "b")[0]
		stub := &expirerStub{slowRead: true, cursor: &saved}
		// Bounded, so a read that ignored the budget fails here instead of hanging.
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		began := time.Now()
		run := runSweepWith(t, ctx, stub, flags, 100, 100*time.Millisecond, time.Now)
		require.NoError(t, run.err, "running out of time must not send the job into retry backoff")
		assert.Less(t, time.Since(began), 5*time.Second, "the read gets only the time left in the budget")
		assert.Equal(t, "b", stub.savedCursor(), "the next run resumes where this one stood")
		assert.Equal(t, map[string]uint64{sweepOutOfTime: 1}, run.results)
	})

	t.Run("a run that reads to the end records it", func(t *testing.T) {
		t.Parallel()

		run := runSweep(t, t.Context(), &expirerStub{expired: expiredRows(teamID, "a", "b")}, flags, 100)
		require.NoError(t, run.err)
		assert.Equal(t, map[string]uint64{sweepReachedEnd: 1}, run.results)
	})

	t.Run("a failed page read records a failed run", func(t *testing.T) {
		t.Parallel()

		run := runSweep(t, t.Context(), &expirerStub{listErr: errors.New("database unavailable")}, flags, 100)
		require.Error(t, run.err)
		assert.Equal(t, map[string]uint64{sweepFailed: 1}, run.results)
	})

	t.Run("a run stopped mid-sweep keeps the cursor of the last sandbox it took", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var taken atomic.Int32
		stub := &expirerStub{expired: expiredRows(teamID, "a", "b", "c", "d")}
		stub.onExpire = func() {
			if taken.Add(1) == 2 {
				cancel()
			}
		}

		run := runSweepWith(t, ctx, stub, flags, 100, expirePausedSandboxesSoftBudget, time.Now)
		require.ErrorIs(t, run.err, context.Canceled, "River hands a stopped job straight back only when it sees the cancel")
		assert.Equal(t, []string{"a", "b"}, stub.expires)
		assert.Equal(t, "b", stub.savedCursor())
		assert.Equal(t, map[string]uint64{sweepStopped: 1}, run.results)
	})
}

func TestPausedExpirySweepHandlesAPageConcurrently(t *testing.T) {
	t.Parallel()

	ids := make([]string, 20)
	for i := range ids {
		ids[i] = fmt.Sprintf("sbx-%02d", i)
	}
	stub := &expirerStub{expired: expiredRows(uuid.New(), ids...)}
	var inFlight, most atomic.Int32
	stub.onExpire = func() {
		now := inFlight.Add(1)
		for {
			seen := most.Load()
			if now <= seen || most.CompareAndSwap(seen, now) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
	}

	run := runSweepConcurrent(t, t.Context(), stub, modeFlags(t, featureflags.PausedSandboxExpiryEnabled, nil), 7, 4, expirePausedSandboxesSoftBudget, time.Now)
	require.NoError(t, run.err)
	assert.ElementsMatch(t, ids, stub.expires, "every sandbox is handled exactly once across pages")
	assert.LessOrEqual(t, most.Load(), int32(4), "never more than the concurrency at once")
	assert.Greater(t, most.Load(), int32(1), "sandboxes of a page are handled side by side")
	assert.Empty(t, stub.savedCursor())
	assert.Equal(t, map[string]uint64{sweepReachedEnd: 1}, run.results)
}

func TestStoppingTheOutboxHandsARunningSweepBack(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	stub := &expirerStub{expired: expiredRows(uuid.New(), "a"), holdExpire: true, expiring: make(chan struct{})}
	outboxRiver, err := New(Dependencies{
		Pool:         db.SqlcClient.Pool(),
		Teams:        db.AuthDb,
		PausedExpiry: stub,
		FeatureFlags: modeFlags(t, featureflags.PausedSandboxExpiryEnabled, nil),
		Logger:       logger.NewNopLogger(),
		Telemetry:    telemetry.NewNoopClient(),
		Config: Config{
			MaxWorkers: 1, BacklogInterval: time.Second,
			PausedExpiryInterval: time.Hour, PausedExpiryBatchSize: 100, PausedExpiryConcurrency: 1,
			SoftStopTimeout: 200 * time.Millisecond,
		},
	})
	require.NoError(t, err)
	require.NoError(t, outboxRiver.Start(t.Context()))

	select {
	case <-stub.expiring:
	case <-time.After(30 * time.Second):
		require.Fail(t, "the sweep did not start")
	}

	// Bounded, so a stop that waited for the sweep fails here instead of hanging.
	stopCtx, cancelStop := context.WithTimeout(context.WithoutCancel(t.Context()), 20*time.Second)
	defer cancelStop()
	began := time.Now()
	require.NoError(t, outboxRiver.Stop(stopCtx), "the soft stop cancels the sweep instead of waiting for it")
	assert.Less(t, time.Since(began), 10*time.Second)

	var state string
	var attempt int
	require.NoError(t, db.SqlcClient.Pool().QueryRow(context.WithoutCancel(t.Context()),
		"SELECT state, attempt FROM "+dbmodule.RiverSchema+".river_job WHERE kind = $1", expirePausedSandboxesKind).Scan(&state, &attempt))
	assert.Equal(t, string(rivertype.JobStateAvailable), state, "another replica can pick it up at once, not after the 45-minute rescue")
	assert.Equal(t, 0, attempt, "the interrupted run is not counted as an attempt")
}

func TestParseExpiryTuning(t *testing.T) {
	t.Parallel()

	defaults := expiryTuning{batchSize: 100, concurrency: 8, budget: expirePausedSandboxesSoftBudget}
	for _, tt := range []struct {
		name  string
		value ldvalue.Value
		want  expiryTuning
	}{
		{name: "no flag keeps the configured values", value: ldvalue.Null(), want: defaults},
		{
			name:  "every field set",
			value: ldvalue.FromJSONMarshal(map[string]any{"batchSize": 500, "concurrency": 16, "budgetSeconds": 60}),
			want:  expiryTuning{batchSize: 500, concurrency: 16, budget: time.Minute},
		},
		{
			name:  "a missing field keeps its default",
			value: ldvalue.FromJSONMarshal(map[string]any{"concurrency": 2}),
			want:  expiryTuning{batchSize: 100, concurrency: 2, budget: expirePausedSandboxesSoftBudget},
		},
		{
			name:  "out-of-range fields keep their defaults",
			value: ldvalue.FromJSONMarshal(map[string]any{"batchSize": 0, "concurrency": 65, "budgetSeconds": 196}),
			want:  defaults,
		},
		{
			name:  "a budget too short to delete anything keeps the default",
			value: ldvalue.FromJSONMarshal(map[string]any{"budgetSeconds": 9}),
			want:  defaults,
		},
		{
			name:  "non-numeric fields keep their defaults",
			value: ldvalue.FromJSONMarshal(map[string]any{"batchSize": "500", "concurrency": true}),
			want:  defaults,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, parseExpiryTuning(tt.value, defaults))
		})
	}
}

func TestPausedExpirySweepFollowsTheTuningFlag(t *testing.T) {
	t.Parallel()

	ids := make([]string, 7)
	for i := range ids {
		ids[i] = fmt.Sprintf("sbx-%02d", i)
	}
	stub := &expirerStub{expired: expiredRows(uuid.New(), ids...)}
	var inFlight, most atomic.Int32
	stub.onExpire = func() {
		now := inFlight.Add(1)
		for {
			seen := most.Load()
			if now <= seen || most.CompareAndSwap(seen, now) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		inFlight.Add(-1)
	}
	flags := tunedFlags(t, featureflags.PausedSandboxExpiryEnabled, nil, ldvalue.FromJSONMarshal(map[string]any{"batchSize": 3, "concurrency": 1}))

	// Configured with pages of 100 and 4 at once; the flag says 3 and 1.
	run := runSweepConcurrent(t, t.Context(), stub, flags, 100, 4, expirePausedSandboxesSoftBudget, time.Now)
	require.NoError(t, run.err)
	assert.Equal(t, ids, stub.expires, "one at a time, in order")
	assert.Equal(t, int32(1), most.Load())
	assert.Equal(t, 3, stub.pages, "pages of 3, 3 and 1")
}

func TestOutboxStopsJobsBeforeTheyAreRescued(t *testing.T) {
	t.Parallel()

	assert.Positive(t, softStopTimeout)
	assert.Less(t, softStopTimeout, 75*time.Second, "shorter than the API's outbox stop deadline")
}
