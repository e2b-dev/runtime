package evictor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldlog"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/api/internal/pause"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func TestEvictSandbox_ReasonByAction(t *testing.T) {
	t.Parallel()

	// Filesystem-only cases carry an FC release that qualifies for the
	// feature (e2b >= 0.2.0); runOn overrides it to pin the degrade.
	// Offline flags client: resolution falls back to the built-in
	// FirecrackerVersionMap, so each declared version resolves within its
	// own line — exactly the degrade semantics under test.
	flags, err := featureflags.NewClientWithLogLevel("", "", "", ldlog.Error)
	require.NoError(t, err)

	counter, err := telemetry.GetCounter(noop.NewMeterProvider().Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator/evictor"), telemetry.ApiEvictorFsOnlyAutoPause)
	require.NoError(t, err)

	runOn := func(state sandbox.State, autoPause, autoPauseFilesystemOnly bool, fcVersion string) sandbox.RemoveOpts {
		var got sandbox.RemoveOpts
		called := false
		e := &Evictor{
			featureFlags:           flags,
			fsOnlyAutoPauseCounter: counter,
			degradedCounter:        counter,
			removeSandbox: func(
				_ context.Context,
				_ uuid.UUID,
				_ string,
				opts sandbox.RemoveOpts,
			) error {
				got = opts
				called = true

				return nil
			},
		}

		e.evictSandbox(t.Context(), sandbox.Sandbox{
			SandboxID:               "sbx",
			TeamID:                  uuid.New(),
			ExecutionID:             "exec-1",
			State:                   state,
			AutoPause:               autoPause,
			AutoPauseFilesystemOnly: autoPauseFilesystemOnly,
			FirecrackerVersion:      fcVersion,
			EndTime:                 time.Now(),
		})

		require.True(t, called)

		return got
	}
	run := func(autoPause, autoPauseFilesystemOnly bool) sandbox.RemoveOpts {
		return runOn(sandbox.StateRunning, autoPause, autoPauseFilesystemOnly, "v1.14-0.2.0")
	}

	t.Run("kill carries timeout reason", func(t *testing.T) {
		t.Parallel()

		got := run(false, false)

		assert.Equal(t, sandbox.StateActionKill, got.Action)
		assert.True(t, got.Eviction)
		assert.Equal(t, sandbox.KillReasonTimeout, got.Reason)
	})

	t.Run("removal is pinned to the scanned execution", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "exec-1", run(false, false).ExpectExecutionID)
		assert.Equal(t, "exec-1", run(true, false).ExpectExecutionID)
	})

	t.Run("kill ignores the auto-pause snapshot kind", func(t *testing.T) {
		t.Parallel()

		// AutoPauseFilesystemOnly is meaningless without AutoPause; a kill must
		// never carry it.
		got := run(false, true)

		assert.Equal(t, sandbox.StateActionKill, got.Action)
		assert.False(t, got.FilesystemOnly)
	})

	t.Run("auto-pause carries no kill reason", func(t *testing.T) {
		t.Parallel()

		got := run(true, false)

		assert.Equal(t, sandbox.StateActionPause, got.Action)
		assert.Empty(t, got.Reason)
	})

	t.Run("memory auto-pause is not filesystem-only", func(t *testing.T) {
		t.Parallel()

		got := run(true, false)

		assert.Equal(t, sandbox.StateActionPause, got.Action)
		assert.False(t, got.FilesystemOnly)
	})

	t.Run("filesystem-only auto-pause requests a filesystem-only snapshot", func(t *testing.T) {
		t.Parallel()

		got := run(true, true)

		assert.Equal(t, sandbox.StateActionPause, got.Action)
		assert.True(t, got.FilesystemOnly)
	})

	// No version gate: the fs-only policy is honored on every FC version,
	// legacy and unparsable included — producing a filesystem-only snapshot
	// needs no version-gated capability.
	t.Run("filesystem-only auto-pause is honored on a legacy release", func(t *testing.T) {
		t.Parallel()

		got := runOn(sandbox.StateRunning, true, true, "v1.14.1_431f1fc")

		assert.Equal(t, sandbox.StateActionPause, got.Action)
		assert.True(t, got.FilesystemOnly)
	})

	t.Run("filesystem-only auto-pause is honored on an unparsable version", func(t *testing.T) {
		t.Parallel()

		got := runOn(sandbox.StateRunning, true, true, "")

		assert.Equal(t, sandbox.StateActionPause, got.Action)
		assert.True(t, got.FilesystemOnly)
	})

	t.Run("auto-pause leftover in killing is killed not paused", func(t *testing.T) {
		t.Parallel()

		got := runOn(sandbox.StateKilling, true, true, "v1.14-0.2.0")

		assert.Equal(t, sandbox.StateActionKill, got.Action)
		assert.True(t, got.Eviction)
		assert.Equal(t, sandbox.KillReasonTimeout, got.Reason)
		assert.False(t, got.FilesystemOnly)
	})

	t.Run("auto-pause leftover that can still pause is paused", func(t *testing.T) {
		t.Parallel()

		for _, state := range []sandbox.State{sandbox.StatePausing, sandbox.StateSnapshotting} {
			t.Run(string(state), func(t *testing.T) {
				t.Parallel()

				got := runOn(state, true, true, "v1.14-0.2.0")

				assert.Equal(t, sandbox.StateActionPause, got.Action)
				assert.True(t, got.FilesystemOnly)
			})
		}
	})
}

func TestCanTake(t *testing.T) {
	t.Parallel()

	assert.True(t, canTake(sandbox.StateRunning, sandbox.StateActionPause))
	assert.True(t, canTake(sandbox.StatePausing, sandbox.StateActionPause))
	assert.True(t, canTake(sandbox.StateSnapshotting, sandbox.StateActionPause))
	assert.False(t, canTake(sandbox.StateKilling, sandbox.StateActionPause))
	assert.True(t, canTake(sandbox.StateKilling, sandbox.StateActionKill))
}

func TestIsStaleDecision(t *testing.T) {
	t.Parallel()

	t.Run("state moved since the scan", func(t *testing.T) {
		t.Parallel()

		err := &sandbox.InvalidStateTransitionError{
			CurrentState: sandbox.StateKilling,
			TargetState:  sandbox.StatePausing,
		}
		assert.True(t, isStaleDecision(err, sandbox.StateRunning))
		assert.True(t, isKnownEvictionError(err, sandbox.StateRunning))
	})

	t.Run("refusal from the scanned state stays a failure", func(t *testing.T) {
		t.Parallel()

		// An unknown state is refused from the same state it was scanned in.
		// Nothing moved; the record is broken.
		err := &sandbox.InvalidStateTransitionError{
			CurrentState: "",
			TargetState:  sandbox.StateKilling,
		}
		assert.False(t, isStaleDecision(err, ""))
		assert.False(t, isKnownEvictionError(err, ""))
	})

	t.Run("other errors are not stale decisions", func(t *testing.T) {
		t.Parallel()

		assert.False(t, isStaleDecision(sandbox.ErrNotFound, sandbox.StateRunning))
	})
}

func TestIsGone(t *testing.T) {
	t.Parallel()

	assert.True(t, isGone(sandbox.ErrNotFound))
	assert.True(t, isGone(sandbox.ErrExecutionMismatch))
	assert.True(t, isKnownEvictionError(sandbox.ErrExecutionMismatch, sandbox.StateRunning))
	assert.False(t, isGone(sandbox.ErrEvictionNotNeeded))
}

// Every error that leaves the sandbox running for a later sweep is a skip
// with its own reason; only a genuine failure logs as one. A draining
// replica's refusal reaches the sweep wrapped, and must not read as a
// failure on every tick of the shutdown.
func TestPauseSkipReason(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		err  error
		want pause.SkipReason
		skip bool
	}{
		"node refused":       {err: fmt.Errorf("failed to auto pause sandbox: %w", sandbox.PauseQueueExhaustedError{}), want: pause.SkipReasonAdmissionRefused, skip: true},
		"replica draining":   {err: fmt.Errorf("remove: %w", sandbox.ErrDraining), want: pause.SkipReasonDraining, skip: true},
		"eviction in flight": {err: sandbox.ErrEvictionInProgress, want: pause.SkipReasonNotEvictable, skip: true},
		"sandbox gone":       {err: sandbox.ErrNotFound, want: pause.SkipReasonNotFound, skip: true},
		"state moved":        {err: &sandbox.InvalidStateTransitionError{CurrentState: sandbox.StateKilling, TargetState: sandbox.StatePausing}, want: pause.SkipReasonStateChanged, skip: true},
		"real failure":       {err: errors.New("node exploded")},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, ok := pauseSkipReason(tc.err, sandbox.StateRunning)
			assert.Equal(t, tc.skip, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

// With no retry budget a node's refusal degrades on the spot: the very next
// request asks for a filesystem-only snapshot, which the node cannot refuse
// for a pending memory parent, and the degrade is counted once it lands.
// A record's retry window keeps every replica's sweep off a held sandbox.
func TestRefusalHeld(t *testing.T) {
	t.Parallel()

	now := time.Now()
	sbx := sandbox.Sandbox{SandboxID: "sbx-held"}
	assert.False(t, refusalHeld(sbx, now), "a record with no retry window is swept")
	sbx.RefusedUntil = now.Add(10 * time.Second)
	assert.True(t, refusalHeld(sbx, now), "inside the window the sweep skips it")
	assert.True(t, refusalHeld(sbx, now.Add(10*time.Second-time.Millisecond)))
	assert.False(t, refusalHeld(sbx, now.Add(10*time.Second)), "at the window's end the sweep retries")
}

// The degrade is decided only on a refusal in the same sweep, against the
// budget counted from the first refusal of the current episode.
func TestEvictSandbox_DegradeDecision(t *testing.T) {
	t.Parallel()

	refusal := fmt.Errorf("failed to auto pause sandbox: %w", sandbox.PauseQueueExhaustedError{})

	// run sweeps one auto-pause sandbox; memoryErr is the node's answer to the
	// memory pause, fsOnlyErr to a filesystem-only one. refusedFor is how long
	// ago the current episode's first refusal was stamped (zero: never), and
	// stale moves that stamp's retry window hours into the past.
	type sweep struct {
		budgetMs   *int
		refusedFor time.Duration
		stale      bool
		memoryErr  error
		fsOnlyErr  error
	}
	run := func(t *testing.T, sw sweep) ([]bool, map[string]int64) {
		t.Helper()

		reader := sdkmetric.NewManualReader()
		meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator/evictor")
		fsOnly, err := telemetry.GetCounter(meter, telemetry.ApiEvictorFsOnlyAutoPause)
		require.NoError(t, err)
		degraded, err := telemetry.GetCounter(meter, telemetry.ApiEvictorAutoPauseDegraded)
		require.NoError(t, err)

		var flags *featureflags.Client
		if sw.budgetMs == nil {
			flags, err = featureflags.NewClientWithLogLevel("", "", "", ldlog.Error)
		} else {
			td := ldtestdata.DataSource()
			td.Update(td.Flag(featureflags.AutoPauseOverstayBudgetMs.Key()).ValueForAll(ldvalue.Int(*sw.budgetMs)))
			flags, err = featureflags.NewClientWithDatasource(td)
		}
		require.NoError(t, err)
		t.Cleanup(func() { _ = flags.Close(context.WithoutCancel(t.Context())) })

		var requests []bool
		e := &Evictor{
			featureFlags:           flags,
			fsOnlyAutoPauseCounter: fsOnly,
			degradedCounter:        degraded,
			removeSandbox: func(_ context.Context, _ uuid.UUID, _ string, opts sandbox.RemoveOpts) error {
				requests = append(requests, opts.FilesystemOnly)
				if opts.FilesystemOnly {
					return sw.fsOnlyErr
				}

				return sw.memoryErr
			},
		}
		now := time.Now()
		sbx := sandbox.Sandbox{State: sandbox.StateRunning, SandboxID: "sbx", TeamID: uuid.New(), AutoPause: true, EndTime: now.Add(-time.Hour)}
		if sw.refusedFor > 0 {
			sbx.RefusedSince = now.Add(-sw.refusedFor)
			sbx.RefusedUntil = sbx.RefusedSince.Add(10 * time.Second)
			if sw.stale {
				sbx.RefusedSince = now.Add(-3 * time.Hour)
				sbx.RefusedUntil = sbx.RefusedSince.Add(10 * time.Second)
			}
		}
		e.evictSandbox(t.Context(), sbx)

		return requests, counterByCause(t, reader, telemetry.ApiEvictorAutoPauseDegraded)
	}
	ms := func(v int) *int { return &v }

	t.Run("zero budget: the first refusal degrades in the same sweep", func(t *testing.T) {
		t.Parallel()

		requests, degraded := run(t, sweep{budgetMs: ms(0), memoryErr: refusal})

		assert.Equal(t, []bool{false, true}, requests)
		assert.Equal(t, map[string]int64{"admission_refused": 1}, degraded)
	})

	t.Run("positive budget: a refusal inside it is retried later", func(t *testing.T) {
		t.Parallel()

		requests, degraded := run(t, sweep{budgetMs: ms(60000), refusedFor: 5 * time.Second, memoryErr: refusal})

		assert.Equal(t, []bool{false}, requests, "the only request this sweep was the memory pause")
		assert.Empty(t, degraded)
	})

	t.Run("positive budget: a refusal past it degrades in the same sweep", func(t *testing.T) {
		t.Parallel()

		requests, degraded := run(t, sweep{budgetMs: ms(1000), refusedFor: 5 * time.Second, memoryErr: refusal})

		assert.Equal(t, []bool{false, true}, requests)
		assert.Equal(t, map[string]int64{"overstay_budget": 1}, degraded)
	})

	t.Run("a stale stamp from an old episode does not count against a fresh refusal", func(t *testing.T) {
		t.Parallel()

		requests, degraded := run(t, sweep{budgetMs: ms(1000), refusedFor: 5 * time.Second, stale: true, memoryErr: refusal})

		assert.Equal(t, []bool{false}, requests, "a new episode starts now, so the budget has not run out")
		assert.Empty(t, degraded)
	})

	t.Run("a stale stamp and no refusal this sweep degrades nothing", func(t *testing.T) {
		t.Parallel()

		requests, degraded := run(t, sweep{budgetMs: ms(1000), refusedFor: 5 * time.Second, stale: true})

		assert.Equal(t, []bool{false}, requests, "the node took the memory snapshot")
		assert.Empty(t, degraded)
	})

	t.Run("an exhausted budget and no refusal this sweep degrades nothing", func(t *testing.T) {
		t.Parallel()

		requests, degraded := run(t, sweep{budgetMs: ms(1000), refusedFor: 5 * time.Second})

		assert.Equal(t, []bool{false}, requests)
		assert.Empty(t, degraded)
	})

	t.Run("negative budget never degrades", func(t *testing.T) {
		t.Parallel()

		requests, degraded := run(t, sweep{budgetMs: ms(-1), refusedFor: 3 * time.Hour, memoryErr: refusal})

		assert.Equal(t, []bool{false}, requests)
		assert.Empty(t, degraded)
	})

	t.Run("the default budget degrades nothing without a refusal", func(t *testing.T) {
		t.Parallel()

		requests, degraded := run(t, sweep{})

		assert.Equal(t, []bool{false}, requests)
		assert.Empty(t, degraded)
	})

	t.Run("the default budget retries a fresh refusal", func(t *testing.T) {
		t.Parallel()

		requests, degraded := run(t, sweep{memoryErr: refusal})

		assert.Equal(t, []bool{false}, requests)
		assert.Empty(t, degraded)
	})

	t.Run("a degraded pause that fails is requested but not counted", func(t *testing.T) {
		t.Parallel()

		requests, degraded := run(t, sweep{budgetMs: ms(0), memoryErr: refusal, fsOnlyErr: errors.New("node unreachable")})

		assert.Equal(t, []bool{false, true}, requests)
		assert.Empty(t, degraded, "nothing landed, so nothing is counted")
	})
}

// counterByCause returns the counter's datapoints keyed by their cause attribute.
func counterByCause(t *testing.T, reader *sdkmetric.ManualReader, name telemetry.CounterType) map[string]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(name) {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				cause, _ := dp.Attributes.Value("cause")
				out[cause.Emit()] += dp.Value
			}
		}
	}

	return out
}
