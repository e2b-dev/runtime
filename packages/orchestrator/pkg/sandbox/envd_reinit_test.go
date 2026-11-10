//go:build linux

package sandbox

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// reinitCuts reads reinit.cut by its by attribute. The counter carries nothing per
// sandbox, so the tests that move it read deltas and run serially.
func reinitCuts(t *testing.T) map[string]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(t.Context(), &rm))

	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.EnvdReinitCut) {
				continue
			}
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				by, _ := dp.Attributes.Value("by")
				out[by.AsString()] += dp.Value
			}
		}
	}

	return out
}

// cutDelta is how many reinit.cut increments by each cause since before.
func cutDelta(t *testing.T, before map[string]int64) map[string]int64 {
	t.Helper()

	out := map[string]int64{}
	for by, n := range reinitCuts(t) {
		if d := n - before[by]; d != 0 {
			out[by] = d
		}
	}

	return out
}

// releaser returns a channel and a function closing it once, registered to run at cleanup
// so a handler blocked on it never holds the test server open.
func releaser(t *testing.T) (chan struct{}, func()) {
	t.Helper()

	ch := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release)

	return ch, release
}

// newReinitSandbox is a live-looking sandbox for the re-init: a Firecracker exit it can
// watch, envd-init-held-request at held, an envd-wait budget of budget, and the pre-pause
// freeze on.
func newReinitSandbox(t *testing.T, url string, held bool, budget time.Duration) (*Sandbox, string, *utils.ErrorOnce) {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.EnvdInitHeldRequestFlag.Key()).VariationForAll(held))
	td.Update(td.Flag(featureflags.EnvdTimeoutMilliseconds.Key()).ValueForAll(ldvalue.Int(int(budget.Milliseconds()))))
	td.Update(td.Flag(featureflags.FreezeUserCgroupFlag.Key()).VariationForAll(true))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	s, version := newHeldSandbox(t, url, "0.10.0", held)
	s.featureFlags = ff
	exit := utils.NewErrorOnce()
	s.process = &fc.Process{Exit: exit}
	t.Cleanup(func() {
		if r := s.envdReinit.Load(); r != nil {
			r.cancel(errors.New("test over"))
		}
	})

	return s, version, exit
}

// reinitFailureLogs counts the re-init failure warnings of the sandbox with baked version.
func reinitFailureLogs(version string) int {
	n := 0
	for _, e := range testLogObserver.FilterMessage("envd re-init after in-place resume failed (guest clock may lag)").All() {
		if e.ContextMap()["sandbox.id"] == "sbx-"+version {
			n++
		}
	}

	return n
}

// cutReturnsPromptly runs a Pause's cut bounded far below every case's budget, so a cut
// that waits on envd instead of cancelling fails here.
func cutReturnsPromptly(t *testing.T, s *Sandbox) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, _, err := s.cutEnvdReinit(ctx)
	require.NoError(t, err)
}

// pauseFreezesAfterTheCut runs a Pause's cut and then its pre-pause reclaim, in Pause's
// order, and requires the re-init to have returned before the reclaim asks envd to freeze.
func pauseFreezesAfterTheCut(t *testing.T, s *Sandbox, e *heldEnvd) {
	t.Helper()

	cutReturnsPromptly(t, s)
	select {
	case <-s.envdReinit.Load().done:
	default:
		t.Fatal("the cut returned before the re-init did")
	}
	s.bestEffortReclaim(t.Context(), true)
	assert.Contains(t, e.paths(), "/freeze", "the freeze is requested once the re-init has returned")
}

// TestReinitCut is serial: reinit.cut carries no per-test key, so each case reads a delta.
func TestReinitCut(t *testing.T) { //nolint:paralleltest // reads deltas of a counter with no per-test key
	t.Run("an unanswered held /init is abandoned", func(t *testing.T) { //nolint:paralleltest // see TestReinitCut
		e := newHeldEnvd(t)
		arrived := make(chan struct{})
		abandoned := make(chan struct{})
		release, _ := releaser(t)
		e.init = func(_ http.ResponseWriter, _ int, read func() envd.PostInitJSONBody) {
			read()
			close(arrived)
			<-release
			close(abandoned)
		}
		s, version, _ := newReinitSandbox(t, e.srv.URL, true, time.Minute)
		before := reinitCuts(t)

		s.startEnvdReinit(t.Context())
		<-arrived
		pauseFreezesAfterTheCut(t, s, e)

		assert.Equal(t, map[string]int64{"pause": 1}, cutDelta(t, before))
		assert.Equal(t, 1, countInits(e), "the cut re-init wrote nothing more")
		assert.Zero(t, reinitFailureLogs(version), "a cut is no failure")
		select {
		case <-abandoned:
			t.Fatal("the handler finished before the cut released it: the cut waited on envd")
		default:
		}
	})

	t.Run("a stop marked while the /init fails writes no re-send", func(t *testing.T) { //nolint:paralleltest // see TestReinitCut
		e := newHeldEnvd(t)
		var s *Sandbox
		e.init = func(w http.ResponseWriter, _ int, read func() envd.PostInitJSONBody) {
			read()
			// The stop lands as this attempt fails: whichever the loop sees first, the
			// cancellation or the dropped connection, it writes nothing after it.
			s.envdReinit.Load().stop()
			if conn, _, err := http.NewResponseController(w).Hijack(); err == nil {
				conn.Close()
			}
		}
		s, _, _ = newReinitSandbox(t, e.srv.URL, true, time.Minute)
		before := reinitCuts(t)

		s.startEnvdReinit(t.Context())
		<-s.envdReinit.Load().done

		assert.Equal(t, 1, countInits(e), "no re-send after the stop")
		assert.Equal(t, map[string]int64{"pause": 1}, cutDelta(t, before))
	})

	t.Run("a held re-init stopped before it decided writes nothing", func(t *testing.T) { //nolint:paralleltest // see TestReinitCut
		e := newHeldEnvd(t)
		s, _, _ := newReinitSandbox(t, e.srv.URL, true, 10*time.Second)
		before := reinitCuts(t)

		initCtx, cancel := context.WithCancelCause(t.Context())
		r := newEnvdReinit(cancel)
		s.envdReinit.Store(r)
		_, cut, err := s.cutEnvdReinit(t.Context())
		require.NoError(t, err, "an undecided re-init is not waited for")
		assert.True(t, cut, "but it is stopped: a held one then writes nothing")
		s.bestEffortEnvdReinit(t.Context(), initCtx, r)

		assert.Empty(t, e.paths())
		assert.Equal(t, map[string]int64{"pause": 1}, cutDelta(t, before))
	})

	t.Run("a re-init replaced before it decided is counted as replaced", func(t *testing.T) { //nolint:paralleltest // see TestReinitCut
		e := newHeldEnvd(t)
		s, version, _ := newReinitSandbox(t, e.srv.URL, false, 10*time.Second)
		before := reinitCuts(t)

		initCtx, cancel := context.WithCancelCause(t.Context())
		r := newEnvdReinit(cancel)
		s.envdReinit.Store(r)
		s.startEnvdReinit(t.Context())
		<-s.envdReinit.Load().done
		s.bestEffortEnvdReinit(t.Context(), initCtx, r)

		assert.Equal(t, 1, countInits(e), "only the fresh re-init was served")
		assert.Equal(t, map[string]int64{"replace": 1}, cutDelta(t, before))
		assert.Zero(t, reinitFailureLogs(version), "a replace is no failure")
	})

	t.Run("a held /init the pause cancelled, then replaced before it returned, is counted as paused", func(t *testing.T) { //nolint:paralleltest // see TestReinitCut
		e := newHeldEnvd(t)
		arrived := make(chan struct{})
		release, _ := releaser(t)
		e.init = func(_ http.ResponseWriter, _ int, read func() envd.PostInitJSONBody) {
			read()
			close(arrived)
			<-release
		}
		s, _, _ := newReinitSandbox(t, e.srv.URL, true, time.Minute)
		before := reinitCuts(t)

		// The handle's cancel is held back, so the replace lands before the cancelled
		// re-init can return and record how it ended.
		initCtx, cancel := context.WithCancelCause(context.WithoutCancel(t.Context()))
		var heldBack atomic.Pointer[error]
		r := newEnvdReinit(func(err error) {
			if errors.Is(err, errEnvdReinitCut) {
				heldBack.Store(&err)

				return
			}
			cancel(err)
		})
		s.envdReinit.Store(r)
		go s.bestEffortEnvdReinit(t.Context(), initCtx, r)
		<-arrived
		wait, stopped := r.stop()
		require.True(t, wait && stopped, "a held re-init in flight is cancelled")
		require.True(t, r.replace())
		cancel(*heldBack.Load())
		<-r.done

		assert.Equal(t, map[string]int64{"pause": 1}, cutDelta(t, before))
	})

	t.Run("a held /init in flight is cancelled by the fresh re-init that replaces it", func(t *testing.T) { //nolint:paralleltest // see TestReinitCut
		e := newHeldEnvd(t)
		arrived := make(chan struct{}, 2)
		release, _ := releaser(t)
		e.init = func(_ http.ResponseWriter, _ int, read func() envd.PostInitJSONBody) {
			read()
			arrived <- struct{}{}
			<-release
		}
		s, _, _ := newReinitSandbox(t, e.srv.URL, true, 10*time.Second)
		before := reinitCuts(t)

		s.startEnvdReinit(t.Context())
		r := s.envdReinit.Load()
		<-arrived
		// A fresh start no Pause stopped r for: the success path of an overlapping
		// checkpoint.
		s.startEnvdReinit(t.Context())
		require.NotSame(t, r, s.envdReinit.Load())
		select {
		case <-r.done:
		case <-time.After(2 * time.Second):
			t.Fatal("the replaced re-init kept its /init in flight")
		}
		// The fresh re-init's /init must have been read before the test's cleanup cancels
		// it, or the double's read fails after the test is over.
		<-arrived

		assert.False(t, r.mayWrite())
		assert.Equal(t, map[string]int64{"replace": 1}, cutDelta(t, before))
	})

	t.Run("a legacy re-init is left as it is", func(t *testing.T) { //nolint:paralleltest // see TestReinitCut
		e := newHeldEnvd(t)
		arrived := make(chan struct{})
		release, releaseInit := releaser(t)
		served := e.init
		e.init = func(w http.ResponseWriter, n int, read func() envd.PostInitJSONBody) {
			close(arrived)
			<-release
			served(w, n, read)
		}
		s, _, _ := newReinitSandbox(t, e.srv.URL, false, 10*time.Second)
		s.internalConfig.EnvdInitRequestTimeout = 10 * time.Second
		before := reinitCuts(t)

		s.startEnvdReinit(t.Context())
		<-arrived
		cutReturnsPromptly(t, s)
		releaseInit()
		<-s.envdReinit.Load().done

		assert.Equal(t, []string{"/init"}, e.paths(), "the legacy re-init was served")
		assert.Empty(t, cutDelta(t, before))
	})

	t.Run("a Firecracker exit during the held /init is counted as a kill", func(t *testing.T) { //nolint:paralleltest // see TestReinitCut
		e := newHeldEnvd(t)
		arrived := make(chan struct{})
		release, _ := releaser(t)
		e.init = func(_ http.ResponseWriter, _ int, read func() envd.PostInitJSONBody) {
			read()
			close(arrived)
			<-release
		}
		s, _, exit := newReinitSandbox(t, e.srv.URL, true, time.Minute)
		before := reinitCuts(t)

		s.startEnvdReinit(t.Context())
		<-arrived
		require.NoError(t, exit.SetError(nil))
		<-s.envdReinit.Load().done

		assert.Equal(t, map[string]int64{"kill": 1}, cutDelta(t, before))
	})

	t.Run("the budget ends an unanswered held /init", func(t *testing.T) { //nolint:paralleltest // see TestReinitCut
		const budget = 2 * time.Second
		e := newHeldEnvd(t)
		arrived := make(chan struct{})
		release, _ := releaser(t)
		e.init = func(_ http.ResponseWriter, _ int, read func() envd.PostInitJSONBody) {
			read()
			close(arrived)
			<-release
		}
		s, version, _ := newReinitSandbox(t, e.srv.URL, true, budget)
		before := reinitCuts(t)

		s.startEnvdReinit(t.Context())
		// The budget runs from the re-init's own first flag read, not from here, so only
		// the re-init ending before its /init arrived says the setup outran it. A select
		// over two closed channels picks either, so a test goroutine scheduled late enough
		// to see both closed re-checks the arrival before it calls the setup outrun.
		done := s.envdReinit.Load().done
		select {
		case <-arrived:
		case <-done:
			select {
			case <-arrived:
			default:
				t.Fatal("the setup outran the budget: the re-init ended before its /init arrived")
			}
		}
		<-done

		assert.Empty(t, cutDelta(t, before), "a budget expiry is not a cut")
		assert.Equal(t, 1, countInits(e), "the held re-init held its one /init to the budget")
		assert.Equal(t, 1, reinitFailureLogs(version), "a budget expiry is the warning it always was")
	})
}

// TestReinitReplaceIsOnce: a handle is replaced once, so overlapping cleanups that cut the
// same handle start one fresh re-init between them; one left to run on the legacy loop is
// never replaced.
func TestReinitReplaceIsOnce(t *testing.T) {
	t.Parallel()

	r := newEnvdReinit(func(error) {})
	assert.True(t, r.replace(), "an undecided handle is replaced")
	assert.False(t, r.replace(), "a replaced handle has its replacement")
	assert.False(t, r.begin(false), "a replaced handle writes nothing on the legacy loop")

	var cancelled error
	held := newEnvdReinit(func(err error) { cancelled = err })
	require.True(t, held.begin(true))
	assert.True(t, held.replace(), "a held handle in flight is replaced")
	require.ErrorIs(t, cancelled, errEnvdReinitCut, "and cancelled")
	assert.False(t, held.mayWrite(), "and writes nothing more")
	_, cutBy := held.finish()
	assert.Equal(t, "replace", cutBy)

	var stoppedThenReplaced error
	stopped := newEnvdReinit(func(err error) { stoppedThenReplaced = err })
	require.True(t, stopped.begin(true))
	stopped.stop()
	assert.True(t, stopped.replace(), "a stopped handle in flight is replaced")
	require.ErrorIs(t, stoppedThenReplaced, errEnvdReinitCut)
	_, cutBy = stopped.finish()
	assert.Equal(t, "pause", cutBy, "the stop's cause stands")

	// On an undecided handle the first mark names the cause, whichever it is.
	for _, tc := range []struct {
		name         string
		replaceFirst bool
		choosesHeld  bool
		want         string
	}{
		{name: "replace, then stop, chooses held", replaceFirst: true, choosesHeld: true, want: "replace"},
		{name: "replace, then stop, chooses legacy", replaceFirst: true, want: "replace"},
		{name: "stop, then replace, chooses held", want: "pause"},
		{name: "stop, then replace, chooses legacy", want: "pause"},
	} {
		undecided := newEnvdReinit(func(error) {})
		if tc.replaceFirst {
			require.True(t, undecided.replace(), tc.name)
			_, stopped := undecided.stop()
			assert.True(t, stopped, tc.name)
		} else {
			_, stopped := undecided.stop()
			require.True(t, stopped, tc.name)
			assert.True(t, undecided.replace(), "%s: a stopped undecided handle is replaced", tc.name)
		}
		assert.False(t, undecided.begin(tc.choosesHeld), "%s: refused on either loop", tc.name)
		_, cutBy = undecided.finish()
		assert.Equal(t, tc.want, cutBy, tc.name)
	}

	legacy := newEnvdReinit(func(error) {})
	require.True(t, legacy.begin(false))
	assert.False(t, legacy.replace(), "a legacy handle is left to run")
	assert.True(t, legacy.mayWrite())
}

// TestStartEnvdReinitReplacesItsPredecessor: every fresh re-init replaces the handle
// before it, from whichever path starts it. A predecessor still undecided then writes
// nothing on either loop; one that chose the legacy loop runs beside the fresh one, as
// before.
func TestStartEnvdReinitReplacesItsPredecessor(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// held is the loop the flag picks for both re-inits; legacyBefore has the
		// predecessor choose the legacy loop before the fresh one starts.
		held         bool
		legacyBefore bool
		wantInits    int
	}{
		{name: "an undecided predecessor on the legacy loop", held: false, wantInits: 1},
		{name: "an undecided predecessor on the held loop", held: true, wantInits: 1},
		{name: "a predecessor already on the legacy loop", held: false, legacyBefore: true, wantInits: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newHeldEnvd(t)
			s, _, _ := newReinitSandbox(t, e.srv.URL, tc.held, 10*time.Second)
			oldCtx, cancelOld := context.WithCancelCause(context.WithoutCancel(t.Context()))
			t.Cleanup(func() { cancelOld(errors.New("test over")) })
			old := newEnvdReinit(cancelOld)
			if tc.legacyBefore {
				require.True(t, old.begin(false))
			}
			s.envdReinit.Store(old)

			s.startEnvdReinit(t.Context())
			fresh := s.envdReinit.Load()
			require.NotSame(t, old, fresh)
			<-fresh.done
			require.Equal(t, 1, countInits(e), "the fresh re-init sent its /init")

			go s.bestEffortEnvdReinit(t.Context(), oldCtx, old)
			<-old.done
			assert.Equal(t, !tc.legacyBefore, old.replaced)
			assert.Equal(t, tc.wantInits, countInits(e))
		})
	}
}

// countInits is how many /init requests the double received.
func countInits(e *heldEnvd) int {
	return len(slices.DeleteFunc(e.paths(), func(p string) bool { return p != "/init" }))
}

// TestReinitStopWaitsOnlyForAHeldReinitInFlight pins which handles a Pause waits on, and
// which it reports as stopped: an undecided one too, though it is not waited on.
func TestReinitStopWaitsOnlyForAHeldReinitInFlight(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		decided  bool
		held     bool
		returned bool
		wait     bool
		stopped  bool
	}{
		{name: "undecided", wait: false, stopped: true},
		{name: "legacy", decided: true, held: false, wait: false, stopped: false},
		{name: "held, in flight", decided: true, held: true, wait: true, stopped: true},
		{name: "held, returned", decided: true, held: true, returned: true, wait: false, stopped: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var cancelled error
			r := newEnvdReinit(func(err error) { cancelled = err })
			if tc.decided {
				require.True(t, r.begin(tc.held))
			}
			if tc.returned {
				r.finish()
			}

			wait, stopped := r.stop()
			assert.Equal(t, tc.wait, wait)
			assert.Equal(t, tc.stopped, stopped)
			if tc.wait {
				require.ErrorIs(t, cancelled, errEnvdReinitCut, "a waited-on re-init is cancelled")
			} else {
				require.NoError(t, cancelled, "a re-init not waited on is not cancelled")
			}
			assert.False(t, r.held && r.mayWrite(), "nothing held writes after the stop")
		})
	}
}

// TestPauseCutRestartsTheReinitOnAnEarlyFailure: a Pause that cut a held re-init and then
// fails with the sandbox live and its VM pause not known to have landed starts a fresh
// re-init from its cleanup, since the cut one carried the previous checkpoint's clock
// re-sync; so does one that stopped a re-init before it chose its loop. Once the VM pause
// landed, or when no held or undecided re-init was stopped, or the sandbox was killed, it
// starts none.
// A stopped re-init that then chooses the legacy loop runs to completion, so the cleanup
// starts none beside it; one that then chooses the held loop is refused and is replaced.
// One still undecided when the cleanup runs is replaced, and writes nothing on either loop
// once it chooses: only the fresh re-init sends an /init.
// A cut whose ctx ended returns its error with the restart registered.
func TestPauseCutRestartsTheReinitOnAnEarlyFailure(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		noHandle  bool
		undecided bool
		legacy    bool
		// chooses is the loop an undecided re-init picks after the cut, "" for none;
		// choosesAfter is the loop one still undecided at the cleanup picks after it.
		chooses      string
		choosesAfter string
		ctxEnded     bool
		pauseLanded  bool
		killed       bool
		wantRestart  bool
	}{
		{name: "fails before the VM pause landed", wantRestart: true},
		{name: "ctx ends during the cut", ctxEnded: true, wantRestart: true},
		{name: "stops a re-init that has not chosen its loop", undecided: true, wantRestart: true},
		{name: "stops a re-init that then chooses the legacy loop", undecided: true, chooses: "legacy"},
		{name: "stops a re-init that then chooses the held loop", undecided: true, chooses: "held", wantRestart: true},
		{name: "replaces a re-init still undecided at the cleanup, which then chooses the legacy loop", choosesAfter: "legacy", wantRestart: true},
		{name: "replaces a re-init still undecided at the cleanup, which then chooses the held loop", choosesAfter: "held", wantRestart: true},
		{name: "fails after the VM pause landed", pauseLanded: true},
		{name: "nothing to cut", noHandle: true},
		{name: "a legacy re-init is not counted as stopped", legacy: true},
		{name: "killed", killed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newHeldEnvd(t)
			s, _, _ := newReinitSandbox(t, e.srv.URL, tc.choosesAfter != "legacy", 10*time.Second)
			var r *envdReinit
			// oldCtx carries the old re-init's goroutine when the case runs a real one after
			// the cleanup.
			oldCtx, cancelOld := context.WithCancelCause(context.WithoutCancel(t.Context()))
			t.Cleanup(func() { cancelOld(errors.New("test over")) })
			if tc.choosesAfter != "" {
				r = newEnvdReinit(cancelOld)
				s.envdReinit.Store(r)
			} else if !tc.noHandle {
				// A re-init that returns once cancelled, or, under ctxEnded, never returns. It
				// is held and in flight unless the case makes it legacy or leaves its loop
				// unchosen, and then the stop does not cancel it.
				var once sync.Once
				r = newEnvdReinit(func(error) {
					if !tc.ctxEnded {
						once.Do(func() { go func() { r.finish(); r.publishReturned() }() })
					}
				})
				if !tc.undecided {
					require.True(t, r.begin(!tc.legacy))
				}
				s.envdReinit.Store(r)
			}

			ctx, cancel := context.WithCancel(t.Context())
			if tc.ctxEnded {
				cancel()
			}
			defer cancel()
			cleanup := NewCleanup()
			pauseLanded := false
			err := s.cutEnvdReinitForPause(ctx, cleanup, &pauseLanded)
			if tc.ctxEnded {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}

			switch tc.chooses {
			case "legacy":
				require.True(t, r.begin(false), "a stop leaves a legacy re-init to run")
			case "held":
				require.False(t, r.begin(true), "a stop refuses a held re-init")
			}

			pauseLanded = tc.pauseLanded
			if tc.killed {
				s.SetStopReason(StopReasonKilled)
			}
			require.NoError(t, cleanup.Run(t.Context()))

			fresh := s.envdReinit.Load()
			if !tc.wantRestart {
				assert.Same(t, r, fresh, "no re-init was started")
				assert.Empty(t, e.paths())

				return
			}
			require.NotSame(t, r, fresh, "a fresh re-init replaced the cut one")
			<-fresh.done
			assert.Equal(t, 1, countInits(e), "the fresh re-init sent its /init")

			if tc.choosesAfter != "" {
				if tc.choosesAfter == "held" {
					// The stop mark alone refuses a held begin; cleared, only the replace does.
					r.mu.Lock()
					r.stopped = false
					r.mu.Unlock()
				}
				// The old re-init reaches its gate only now, on the loop the flag picks.
				go s.bestEffortEnvdReinit(t.Context(), oldCtx, r)
				<-r.done
				assert.True(t, r.replaced, "the cleanup replaced the handle it cut")
				assert.Equal(t, 1, countInits(e), "the replaced re-init wrote nothing on the %s loop", tc.choosesAfter)
				assert.Same(t, fresh, s.envdReinit.Load(), "the fresh re-init stays the sandbox's")
			}
		})
	}
}

// TestStartEnvdReinitUnderAPauseCutPublishesNothing stages the interleaving a second cut
// at the freeze could not close: a Pause cuts the published re-init, and another path
// starts a fresh one after that cut and before the Pause freezes. The start publishes
// nothing and the cut handle stays in place, so nothing can write an /init across the
// freeze; once the Pause's freeze landed, or the Pause gave up before it, a start
// publishes again. Each cut holds on its own, so two overlapping Pauses keep the hold
// until both released it.
func TestStartEnvdReinitUnderAPauseCutPublishesNothing(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// freezeLands has the Pause's freeze land after the refused start; otherwise the
		// Pause gives up before it and its cleanup restarts the re-init it cut.
		freezeLands bool
	}{
		{name: "the freeze lands", freezeLands: true},
		{name: "the Pause gives up before the freeze"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newHeldEnvd(t)
			arrived := make(chan struct{})
			release, _ := releaser(t)
			served := e.init
			e.init = func(w http.ResponseWriter, n int, read func() envd.PostInitJSONBody) {
				if n > 1 {
					served(w, n, read)

					return
				}
				read()
				close(arrived)
				<-release
			}
			s, _, _ := newReinitSandbox(t, e.srv.URL, true, time.Minute)

			require.True(t, s.startEnvdReinit(t.Context()))
			r := s.envdReinit.Load()
			<-arrived

			cleanup := NewCleanup()
			pauseLanded := false
			require.NoError(t, s.cutEnvdReinitForPause(t.Context(), cleanup, &pauseLanded))
			<-r.done

			// The interleaving: a start between the cut and the freeze.
			assert.False(t, s.startEnvdReinit(t.Context()), "a start under the cut publishes nothing")
			assert.Same(t, r, s.envdReinit.Load(), "the cut handle stays in place")
			assert.Equal(t, 1, countInits(e), "nothing wrote an /init after the cut")

			if tc.freezeLands {
				pauseLanded = true
				s.releaseEnvdReinit()
				require.True(t, s.startEnvdReinit(t.Context()), "once the freeze landed, a start publishes")
			} else {
				require.NoError(t, cleanup.Run(t.Context()))
			}
			fresh := s.envdReinit.Load()
			require.NotSame(t, r, fresh, "a fresh re-init is published once the hold is released")
			<-fresh.done
			assert.Equal(t, 2, countInits(e), "only the fresh re-init wrote its /init")
			if tc.freezeLands {
				require.NoError(t, cleanup.Run(t.Context()), "a landed Pause's cleanup releases nothing")
			}
			assert.True(t, s.startEnvdReinit(t.Context()), "no hold is left")
			<-s.envdReinit.Load().done
		})
	}

	t.Run("overlapping cuts each hold", func(t *testing.T) {
		t.Parallel()

		e := newHeldEnvd(t)
		s, _, _ := newReinitSandbox(t, e.srv.URL, true, 10*time.Second)

		for range 2 {
			r, cut, err := s.cutEnvdReinit(t.Context())
			require.NoError(t, err)
			assert.Nil(t, r)
			assert.False(t, cut)
		}
		assert.False(t, s.startEnvdReinit(t.Context()), "two Pauses hold")
		s.releaseEnvdReinit()
		assert.False(t, s.startEnvdReinit(t.Context()), "one Pause still holds")
		s.releaseEnvdReinit()
		require.True(t, s.startEnvdReinit(t.Context()), "no Pause holds")
		<-s.envdReinit.Load().done
		assert.Equal(t, 1, countInits(e))
	})
}
