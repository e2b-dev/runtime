package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	redisreservations "github.com/e2b-dev/infra/packages/api/internal/sandbox/reservations/redis"
	sandboxredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// running is the state filter the snapshot tests list with.
var running = []sandbox.State{sandbox.StateRunning}

// fakeTeamRead is a team read the test controls: it counts calls and keeps
// the states it was last asked for, can block until released or take a fixed
// (fake) time, and returns items or, without them, a fixed number of running
// sandboxes.
type fakeTeamRead struct {
	calls   atomic.Int32
	states  atomic.Pointer[[]sandbox.State]
	items   []sandbox.Sandbox
	records int
	err     error
	delay   time.Duration
	release chan struct{}
	gotCtx  chan context.Context
}

func (f *fakeTeamRead) load(ctx context.Context, states []sandbox.State) ([]sandbox.Sandbox, error) {
	f.calls.Add(1)
	f.states.Store(&states)
	if f.gotCtx != nil {
		f.gotCtx <- ctx
	}
	if f.release != nil {
		<-f.release
	}
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	if f.err != nil {
		return nil, f.err
	}
	if f.items != nil {
		return f.items, nil
	}

	items := make([]sandbox.Sandbox, f.records)
	for i := range items {
		items[i].State = sandbox.StateRunning
	}

	return items, nil
}

func (f *fakeTeamRead) lastStates() []sandbox.State {
	if p := f.states.Load(); p != nil {
		return *p
	}

	return nil
}

func snapshotCount(s *teamListSnapshots) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.entries)
}

func TestTeamListSnapshots(t *testing.T) {
	t.Parallel()

	t.Run("disabled reads every time", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			var s teamListSnapshots
			read := &fakeTeamRead{records: 10}
			team := uuid.New()

			for range 3 {
				r, err := s.read(t.Context(), team, 0, running, read.load)
				require.NoError(t, err)
				assert.Len(t, r.items, 10)
				assert.Equal(t, listSnapshotDisabled, r.outcome)
			}
			assert.Equal(t, int32(3), read.calls.Load())
		})
	})

	t.Run("disabled is the caller's own read of its states", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			var s teamListSnapshots
			stored := []sandbox.Sandbox{{SandboxID: "a", State: sandbox.StatePausing}, {SandboxID: "b", State: sandbox.StatePausing}}
			read := &fakeTeamRead{items: stored}
			states := []sandbox.State{sandbox.StatePausing}

			r, err := s.read(t.Context(), uuid.New(), 0, states, read.load)
			require.NoError(t, err)
			assert.Equal(t, states, read.lastStates(), "the store is asked for the caller's states, not the shared union")
			require.Len(t, r.items, len(stored))
			assert.Same(t, &stored[0], &r.items[0], "the store's slice is returned as is, not filtered into a copy")
		})
	})

	t.Run("a snapshot is read in the shared states and filtered into a copy", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			var s teamListSnapshots
			stored := []sandbox.Sandbox{{SandboxID: "a", State: sandbox.StateRunning}, {SandboxID: "b", State: sandbox.StatePausing}, {SandboxID: "c", State: sandbox.StateRunning}}
			read := &fakeTeamRead{items: stored}
			team := uuid.New()

			r, err := s.read(t.Context(), team, 5*time.Second, running, read.load)
			require.NoError(t, err)
			assert.Equal(t, sharedListStates(), read.lastStates(), "the shared read loads every state a list may ask for")
			require.Len(t, r.items, 2)
			assert.Equal(t, []string{"a", "c"}, []string{r.items[0].SandboxID, r.items[1].SandboxID})
			assert.NotSame(t, &stored[0], &r.items[0], "a caller's list is a copy: the snapshot is shared")

			r, err = s.read(t.Context(), team, 5*time.Second, []sandbox.State{sandbox.StatePausing}, read.load)
			require.NoError(t, err)
			assert.Equal(t, listSnapshotHit, r.outcome)
			require.Len(t, r.items, 1)
			assert.Equal(t, "b", r.items[0].SandboxID)

			r, err = s.read(t.Context(), team, 5*time.Second, nil, read.load)
			require.NoError(t, err)
			require.Len(t, r.items, 3, "no states means every state held, as the store reads it")
			assert.NotSame(t, &stored[0], &r.items[0])
			assert.Equal(t, int32(1), read.calls.Load(), "one shared read serves every state filter")
		})
	})

	t.Run("concurrent callers share one read", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			var s teamListSnapshots
			read := &fakeTeamRead{records: 10, release: make(chan struct{})}
			team := uuid.New()

			var wg sync.WaitGroup
			results := make([][]sandbox.Sandbox, 5)
			outcomes := make([]listSnapshotOutcome, 5)
			for i := range results {
				wg.Go(func() {
					r, err := s.read(t.Context(), team, 5*time.Second, running, read.load)
					assert.NoError(t, err)
					results[i], outcomes[i] = r.items, r.outcome
				})
			}
			synctest.Wait()
			assert.Equal(t, int32(1), read.calls.Load(), "callers arriving during a read must join it")

			close(read.release)
			wg.Wait()
			for _, got := range results {
				assert.Len(t, got, 10)
			}
			assert.Equal(t, int32(1), read.calls.Load())
			slices.Sort(outcomes)
			assert.Equal(t, []listSnapshotOutcome{listSnapshotRead, listSnapshotShared, listSnapshotShared, listSnapshotShared, listSnapshotShared}, outcomes,
				"one caller reports the read, the others that they joined it")
		})
	})

	t.Run("a snapshot serves until its lifetime ends", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			var s teamListSnapshots
			read := &fakeTeamRead{records: 10}
			team := uuid.New()

			r, err := s.read(t.Context(), team, time.Second, running, read.load)
			require.NoError(t, err)
			assert.Equal(t, listSnapshotRead, r.outcome)
			assert.Equal(t, time.Duration(0), r.age, "a read's age is the store read's duration")

			time.Sleep(900 * time.Millisecond)
			e, outcome, age := s.entry(t.Context(), team, time.Second, read.load)
			time.Sleep(200 * time.Millisecond) // the snapshot turns 1.1 s old between the lock and the result
			r, err = awaitEntry(t.Context(), e, outcome, age, running)
			require.NoError(t, err)
			assert.Equal(t, listSnapshotHit, r.outcome)
			assert.Equal(t, 900*time.Millisecond, r.age, "a hit's age is how far it lags the store")
			assert.Less(t, r.age, time.Second, "a hit's recorded age is the one judged within the allowance")
			assert.Equal(t, int32(1), read.calls.Load())

			// 1.2 s after the read started.
			time.Sleep(100 * time.Millisecond)
			r, err = s.read(t.Context(), team, time.Second, running, read.load)
			require.NoError(t, err)
			assert.Equal(t, listSnapshotRead, r.outcome)
			assert.Equal(t, int32(2), read.calls.Load())
		})
	})

	t.Run("age counts from the start of the read", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			var s teamListSnapshots
			read := &fakeTeamRead{records: 10, delay: 800 * time.Millisecond}
			team := uuid.New()

			r, err := s.read(t.Context(), team, time.Second, running, read.load)
			require.NoError(t, err)
			assert.Equal(t, 800*time.Millisecond, r.age, "a read's age is the read's own duration")

			time.Sleep(100 * time.Millisecond) // 0.9 s after the read started, 0.1 s after it ended
			r, err = s.read(t.Context(), team, time.Second, running, read.load)
			require.NoError(t, err)
			assert.Equal(t, listSnapshotHit, r.outcome)
			assert.Equal(t, 900*time.Millisecond, r.age)

			time.Sleep(200 * time.Millisecond) // 1.1 s after the start, 0.3 s after the end
			r, err = s.read(t.Context(), team, time.Second, running, read.load)
			require.NoError(t, err)
			assert.Equal(t, listSnapshotRead, r.outcome, "the lifetime counts from the start of the read, not its end")
		})
	})

	t.Run("a replaced snapshot's timer does not drop its replacement", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			var s teamListSnapshots
			read := &fakeTeamRead{records: 10}
			team := uuid.New()

			_, err := s.read(t.Context(), team, 5*time.Second, running, read.load)
			require.NoError(t, err)

			time.Sleep(4500 * time.Millisecond)
			r, err := s.read(t.Context(), team, time.Second, running, read.load)
			require.NoError(t, err)
			require.Equal(t, listSnapshotRead, r.outcome, "the 4.5 s old snapshot is replaced under a 1 s allowance")

			time.Sleep(700 * time.Millisecond) // the first snapshot's own timer was due at 5.0 s
			r, err = s.read(t.Context(), team, time.Second, running, read.load)
			require.NoError(t, err)
			assert.Equal(t, listSnapshotHit, r.outcome, "the replacement, 0.7 s old, is still served")
			assert.Equal(t, int32(2), read.calls.Load())
		})
	})

	t.Run("a lowered allowance applies to a live snapshot", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			var s teamListSnapshots
			read := &fakeTeamRead{records: 10}
			team := uuid.New()

			_, err := s.read(t.Context(), team, 5*time.Second, running, read.load)
			require.NoError(t, err)

			time.Sleep(2 * time.Second)
			r, err := s.read(t.Context(), team, time.Second, running, read.load)
			require.NoError(t, err)
			assert.Equal(t, listSnapshotRead, r.outcome, "a snapshot older than the allowance now in force is not served")
			assert.Equal(t, int32(2), read.calls.Load())
		})
	})

	t.Run("an expired snapshot is released", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			var s teamListSnapshots
			read := &fakeTeamRead{records: 10}

			_, err := s.read(t.Context(), uuid.New(), time.Second, running, read.load)
			require.NoError(t, err)
			require.Equal(t, 1, snapshotCount(&s))

			time.Sleep(2 * time.Second)
			synctest.Wait()
			assert.Equal(t, 0, snapshotCount(&s), "an expired snapshot must not stay in memory")
		})
	})

	t.Run("a failed read is not cached", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			var s teamListSnapshots
			team := uuid.New()
			boom := errors.New("redis down")

			failing := &fakeTeamRead{err: boom}
			_, err := s.read(t.Context(), team, 5*time.Second, running, failing.load)
			require.ErrorIs(t, err, boom)

			healthy := &fakeTeamRead{records: 10}
			r, err := s.read(t.Context(), team, 5*time.Second, running, healthy.load)
			require.NoError(t, err)
			assert.Len(t, r.items, 10)
			assert.Equal(t, listSnapshotRead, r.outcome)
		})
	})

	t.Run("teams do not share snapshots", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			var s teamListSnapshots
			read := &fakeTeamRead{records: 10}

			_, err := s.read(t.Context(), uuid.New(), 5*time.Second, running, read.load)
			require.NoError(t, err)
			_, err = s.read(t.Context(), uuid.New(), 5*time.Second, running, read.load)
			require.NoError(t, err)
			assert.Equal(t, int32(2), read.calls.Load())
		})
	})

	t.Run("a cancelled caller neither waits nor cancels the shared read", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			var s teamListSnapshots
			read := &fakeTeamRead{records: 10, release: make(chan struct{}), gotCtx: make(chan context.Context, 1)}
			team := uuid.New()

			leaderCtx, cancelLeader := context.WithCancel(t.Context())
			leaderErr := make(chan error, 1)
			go func() {
				_, err := s.read(leaderCtx, team, 5*time.Second, running, read.load)
				leaderErr <- err
			}()
			loadCtx := <-read.gotCtx

			type result struct {
				items []sandbox.Sandbox
				err   error
			}
			follower := make(chan result, 1)
			go func() {
				r, err := s.read(t.Context(), team, 5*time.Second, running, read.load)
				follower <- result{items: r.items, err: err}
			}()
			synctest.Wait()

			cancelLeader()
			require.ErrorIs(t, <-leaderErr, context.Canceled, "the cancelled caller returns at once")
			require.NoError(t, loadCtx.Err(), "the shared read outlives the caller that started it")

			close(read.release)
			got := <-follower
			require.NoError(t, got.err)
			assert.Len(t, got.items, 10)
			assert.Equal(t, int32(1), read.calls.Load())
		})
	})

	t.Run("a live snapshot serves a caller whose context has ended", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			var s teamListSnapshots
			read := &fakeTeamRead{records: 10}
			team := uuid.New()

			_, err := s.read(t.Context(), team, 5*time.Second, running, read.load)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			// Repeated: a select over two ready channels picks at random.
			for range 32 {
				r, err := s.read(ctx, team, 5*time.Second, running, read.load)
				require.NoError(t, err, "a result in hand is served, not traded for the context's error")
				assert.Equal(t, listSnapshotHit, r.outcome)
				assert.Len(t, r.items, 10)
			}
			assert.Equal(t, int32(1), read.calls.Load())
		})
	})

	t.Run("a hung read is bounded", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			var s teamListSnapshots
			start := time.Now()

			_, err := s.read(t.Context(), uuid.New(), 5*time.Second, running, func(ctx context.Context, _ []sandbox.State) ([]sandbox.Sandbox, error) {
				<-ctx.Done()

				return nil, ctx.Err()
			})
			require.ErrorIs(t, err, context.DeadlineExceeded)
			assert.Equal(t, cfg.RequestTimeout, time.Since(start), "a shared read gets the budget a request's own read has")
		})
	})

	t.Run("a panicking read fails its callers, not the process", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			var s teamListSnapshots

			_, err := s.read(t.Context(), uuid.New(), 5*time.Second, running, func(context.Context, []sandbox.State) ([]sandbox.Sandbox, error) {
				panic("decode failed")
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "decode failed")
		})
	})
}

// sweepCountHook counts the store sweeps of one team: each begins with an
// SSCAN of the team's index at cursor 0.
type sweepCountHook struct {
	teamID string
	sweeps *atomic.Int32
}

func (h sweepCountHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h sweepCountHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h sweepCountHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "sscan" && strings.Contains(fmt.Sprint(cmd.Args()[1]), h.teamID) && fmt.Sprint(cmd.Args()[2]) == "0" {
			h.sweeps.Add(1)
		}

		return next(ctx, cmd)
	}
}

// listFixture is an Orchestrator over a real Redis store with a lag allowance,
// counting the store sweeps and the reported outcomes of one team's lists.
type listFixture struct {
	o      *Orchestrator
	redis  redis.UniversalClient
	teamID uuid.UUID
	reader *sdkmetric.ManualReader
	sweeps atomic.Int32
}

// newListFixture sets the lag flag to maxLagMs; zero leaves it undefined, so
// that fixture runs on the flag's fallback, as an unflagged deploy does.
func newListFixture(t *testing.T, maxLagMs int) *listFixture {
	t.Helper()

	if maxLagMs > 0 {
		return newListFixtureWithFlag(t, ldvalue.Int(maxLagMs))
	}

	return newListFixtureWithFlag(t, ldvalue.Null())
}

// newListFixtureWithFlag sets the lag flag to value; a null value leaves the
// flag undefined.
func newListFixtureWithFlag(t *testing.T, value ldvalue.Value) *listFixture {
	t.Helper()

	f := &listFixture{teamID: uuid.New(), reader: sdkmetric.NewManualReader()}

	redisClient := redis_utils.SetupInstance(t)
	redisClient.AddHook(sweepCountHook{teamID: f.teamID.String(), sweeps: &f.sweeps})
	f.redis = redisClient

	storage, err := sandboxredis.NewStorage(redisClient, noop.NewMeterProvider())
	require.NoError(t, err)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })

	td := ldtestdata.DataSource()
	if !value.IsNull() {
		td.Update(td.Flag(featureflags.SandboxListMaxLagMsFlag.Key()).ValueForAll(value))
	}
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(f.reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator")
	counter, err := telemetry.GetCounter(meter, telemetry.ApiSandboxListReads)
	require.NoError(t, err)
	age, err := telemetry.GetHistogram(meter, telemetry.ApiSandboxListAge)
	require.NoError(t, err)

	f.o = &Orchestrator{
		sandboxStore: sandbox.NewStore(
			storage,
			redisreservations.NewReservationStorage(redisClient, storage.Notifier()),
			sandbox.Callbacks{AsyncNewlyCreatedSandbox: func(context.Context, sandbox.Sandbox, sandbox.CreationMetadata) {}},
		),
		featureFlagsClient: ff,
		listReadsCounter:   counter,
		listAgeHistogram:   age,
	}

	return f
}

func (f *listFixture) add(t *testing.T, state sandbox.State) string {
	t.Helper()

	sbx := sandbox.Sandbox{
		SandboxID:         "sbx-" + uuid.NewString()[:8],
		TemplateID:        "template",
		BaseTemplateID:    "template",
		ExecutionID:       uuid.NewString(),
		TeamID:            f.teamID,
		BuildID:           uuid.New(),
		MaxInstanceLength: time.Hour,
		StartTime:         time.Now(),
		EndTime:           time.Now().Add(time.Hour),
		State:             state,
	}
	require.NoError(t, f.o.sandboxStore.Add(t.Context(), sbx, nil))

	return sbx.SandboxID
}

func (f *listFixture) list(t *testing.T, states ...sandbox.State) []string {
	t.Helper()

	items, err := f.o.ListSandboxes(t.Context(), f.teamID, states)
	require.NoError(t, err)

	ids := make([]string, 0, len(items))
	for _, sbx := range items {
		ids = append(ids, sbx.SandboxID)
	}
	slices.Sort(ids)

	return ids
}

// metrics returns, per outcome, the list reads counted and the ages recorded.
func (f *listFixture) metrics(t *testing.T) (reads map[string]int64, ages map[string]uint64) {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, f.reader.Collect(t.Context(), &rm))

	outcome := func(set attribute.Set) string {
		v, _ := set.Value("outcome")

		return v.AsString()
	}
	reads, ages = map[string]int64{}, map[string]uint64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case string(telemetry.ApiSandboxListReads):
				sum, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok)
				for _, dp := range sum.DataPoints {
					_, hasTeam := dp.Attributes.Value("team.id")
					assert.True(t, hasTeam, "the counter is the per-team signal")
					reads[outcome(dp.Attributes)] += dp.Value
				}
			case string(telemetry.ApiSandboxListAge):
				hist, ok := m.Data.(metricdata.Histogram[int64])
				require.True(t, ok)
				for _, dp := range hist.DataPoints {
					// Per outcome only: with the flag set fleet-wide, a per-team
					// histogram would multiply the counter's series by its buckets.
					_, hasTeam := dp.Attributes.Value("team.id")
					assert.False(t, hasTeam, "the age histogram must not carry the team")
					ages[outcome(dp.Attributes)] += dp.Count
				}
			}
		}
	}

	return reads, ages
}

func TestListSandboxes(t *testing.T) {
	t.Parallel()

	t.Run("one store read serves every state filter within the lifetime", func(t *testing.T) {
		t.Parallel()

		// An hour's allowance: no CI stall between the three lists can expire it.
		f := newListFixture(t, 3_600_000)
		running := []string{f.add(t, sandbox.StateRunning), f.add(t, sandbox.StateRunning), f.add(t, sandbox.StateRunning)}
		pausing := f.add(t, sandbox.StatePausing)
		slices.Sort(running)
		all := slices.Sorted(slices.Values(append(slices.Clone(running), pausing)))

		assert.Equal(t, running, f.list(t, sandbox.StateRunning), "a running-only list leaves the pausing sandbox out")
		assert.Equal(t, all, f.list(t, sandbox.StateRunning, sandbox.StatePausing))
		assert.Equal(t, all, f.list(t, sandbox.StateRunning, sandbox.StatePausing))

		assert.Equal(t, int32(1), f.sweeps.Load(), "three lists, one store sweep")
		reads, ages := f.metrics(t)
		assert.Equal(t, map[string]int64{"read": 1, "hit": 2}, reads)
		assert.Equal(t, map[string]uint64{"read": 1, "hit": 2}, ages, "every served list records its age")
	})

	t.Run("without a lag allowance every request reads the store", func(t *testing.T) {
		t.Parallel()

		f := newListFixture(t, 0) // the flag's fallback, not an explicit zero
		id := f.add(t, sandbox.StateRunning)

		assert.Equal(t, []string{id}, f.list(t, sandbox.StateRunning))
		assert.Equal(t, []string{id}, f.list(t, sandbox.StateRunning))

		assert.Equal(t, int32(2), f.sweeps.Load())
		reads, ages := f.metrics(t)
		assert.Equal(t, map[string]int64{"disabled": 2}, reads)
		assert.Empty(t, ages, "no age series until a team's flag is set")
	})

	t.Run("a flag that fails to evaluate keeps every request on its own read", func(t *testing.T) {
		t.Parallel()

		f := newListFixtureWithFlag(t, ldvalue.String("soon"))
		id := f.add(t, sandbox.StateRunning)

		assert.Equal(t, []string{id}, f.list(t, sandbox.StateRunning))
		assert.Equal(t, []string{id}, f.list(t, sandbox.StateRunning))

		assert.Equal(t, int32(2), f.sweeps.Load())
		reads, _ := f.metrics(t)
		assert.Equal(t, map[string]int64{"disabled": 2}, reads)
	})

	t.Run("a failed read is counted but records no age", func(t *testing.T) {
		t.Parallel()

		f := newListFixture(t, 3_600_000)
		require.NoError(t, f.redis.Close())

		_, err := f.o.ListSandboxes(t.Context(), f.teamID, []sandbox.State{sandbox.StateRunning})
		require.Error(t, err)

		reads, ages := f.metrics(t)
		assert.Equal(t, map[string]int64{"read": 1}, reads)
		assert.Empty(t, ages)
	})
}

// An environment that never created the flag must not log on every list
// request: the fallback is the chosen value there.
//
//nolint:paralleltest // ReplaceGlobals mutates process state
func TestListSandboxesUndefinedFlagIsQuiet(t *testing.T) {
	f := newListFixture(t, 0)
	f.add(t, sandbox.StateRunning)

	// Observed around the list alone, and judged on the flag's own lines: the
	// fixture's background work may log inside the window.
	core, logs := observer.New(zapcore.WarnLevel)
	restore := logger.ReplaceGlobals(t.Context(), logger.NewTracedLoggerFromCore(core))
	flagLines := func() []observer.LoggedEntry {
		return logs.FilterField(zap.String("flag", featureflags.SandboxListMaxLagMsFlag.Key())).All()
	}
	// Positive control: the plain evaluation warns on the undefined key, and
	// the filter must see that line, or it proves nothing below.
	f.o.featureFlagsClient.IntFlag(t.Context(), featureflags.SandboxListMaxLagMsFlag, featureflags.TeamContext(f.teamID.String()))
	require.Len(t, flagLines(), 1, "the filter must match the flag client's warning")
	logs.TakeAll()

	f.list(t, sandbox.StateRunning)
	restore()

	for _, entry := range flagLines() {
		assert.Failf(t, "the undefined flag was logged", "%s: %s %v", entry.Level, entry.Message, entry.ContextMap())
	}
}
