//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func fphTestConfig() featureflags.PeriodicHintingConfig {
	return featureflags.PeriodicHintingConfig{
		Interval:          20 * time.Millisecond,
		Timeout:           50 * time.Millisecond,
		QuietAfterStart:   10 * time.Second,
		SilentRuns:        3,
		UnresponsiveRetry: 30 * time.Millisecond,
		Stop:              testStopCfg(),
	}
}

func fixedCfg(cfg featureflags.PeriodicHintingConfig) func() featureflags.PeriodicHintingConfig {
	return func() featureflags.PeriodicHintingConfig { return cfg }
}

func openGates() hintGates {
	return hintGates{
		fcSupported: func() bool { return true },
		balloon:     func(context.Context) (fc.BalloonCaps, error) { return fc.BalloonCaps{Hinting: true}, nil },
		poll:        10 * time.Millisecond,
		slow:        30 * time.Millisecond,
		admission:   &hintAdmission{},
	}
}

func testAdmission() *hintAdmission { return &hintAdmission{} }

// The production gates carry the host-wide admission, and hintOnce takes the
// bound from the gates unconditionally, so it cannot be dropped silently.
func TestPeriodicHintGates_HostAdmission(t *testing.T) {
	t.Parallel()
	s, _ := newFPHTestSandboxAt(time.Now())
	s.Metadata.Config = NewConfig(Config{FirecrackerConfig: fc.Config{FirecrackerVersion: "v1.14-0.2.1"}})
	g := s.periodicHintGates()
	assert.Same(t, hostHintAdmission, g.admission)
	assert.True(t, g.fcSupported())
	assert.Equal(t, hintDisabledPoll, g.poll)
	assert.Equal(t, hintGateSlowPoll, g.slow)
}

// Without a LaunchDarkly backend a disabled config is final: the entry point
// returns without starting a loop or touching the process.
func TestRunPeriodicHinting_StaticDisabledReturns(t *testing.T) {
	t.Parallel()
	ff, err := featureflags.NewClient("", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })
	require.False(t, ff.Live(), "the test environment has no LaunchDarkly key")
	s, o := newFPHTestSandboxAt(time.Now())
	s.Metadata.Config = NewConfig(Config{FirecrackerConfig: fc.Config{FirecrackerVersion: "v1.14-0.2.1"}})
	s.featureFlags = ff
	done := make(chan struct{})
	go func() { defer close(done); s.runPeriodicHinting(t.Context()) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a static client with hinting disabled must return at once")
	}
	assert.Empty(t, o.list())
}

// newFPHTestSandboxAt is newFPHTestSandbox with the memory backend and start
// time the loop's tick decision reads.
func newFPHTestSandboxAt(startedAt time.Time) (*Sandbox, *outcomes) {
	s := &Sandbox{
		Metadata:  &Metadata{Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "fph-test"}},
		Resources: &Resources{memory: uffd.NewNoopMemory(1<<30, 2<<20)},
	}
	s.SetStartedAt(startedAt)
	o := &outcomes{}
	s.fphObserve = o.hook
	s.fphObserveFreed = o.hookFreed
	s.fphObserveFaults = o.hookFaults

	return s, o
}

// toggleCfg serves the test config, or a disabled one while off is set.
func toggleCfg(off *atomic.Bool) func() featureflags.PeriodicHintingConfig {
	return func() featureflags.PeriodicHintingConfig {
		cfg := fphTestConfig()
		if off.Load() {
			cfg.Interval = 0
		}

		return cfg
	}
}

func TestHintSkipReason(t *testing.T) {
	t.Parallel()
	cfg := fphTestConfig()
	now := time.Unix(1_000_000, 0)
	old := now.Add(-time.Minute)

	cases := []struct {
		name string
		in   hintDecisionInput
		want string
	}{
		{"steady state runs", hintDecisionInput{now: now, startedAt: old, lastCheckpointAt: old}, ""},
		{"no start time recorded still runs", hintDecisionInput{now: now}, ""},
		{"checkpoint in flight wins", hintDecisionInput{now: now, startedAt: old, checkpointActive: true, memSealPending: true}, "checkpoint-in-flight"},
		{"memory seal pending", hintDecisionInput{now: now, startedAt: old, memSealPending: true}, "memory-seal-pending"},
		{"rootfs seal pending", hintDecisionInput{now: now, startedAt: old, rootfsSealPend: true}, "rootfs-seal-pending"},
		{"just started", hintDecisionInput{now: now, startedAt: now.Add(-time.Second)}, "quiet-after-start"},
		{"just checkpointed", hintDecisionInput{now: now, startedAt: old, lastCheckpointAt: now.Add(-2 * time.Second)}, "quiet-after-checkpoint"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, hintSkipReason(cfg, tc.in))
		})
	}
}

func TestBalloonGate(t *testing.T) {
	t.Parallel()
	assert.Empty(t, balloonGate(fc.BalloonCaps{Hinting: true}, nil))
	assert.Equal(t, "skipped-reporting-active", balloonGate(fc.BalloonCaps{Hinting: true, Reporting: true}, nil))
	assert.Equal(t, "skipped-not-configured", balloonGate(fc.BalloonCaps{}, nil), "no balloon, or one without hinting")
	assert.Equal(t, "skipped-not-configured", balloonGate(fc.BalloonCaps{Reporting: true}, nil))
	assert.Equal(t, "skipped-balloon-unknown", balloonGate(fc.BalloonCaps{Hinting: true}, errors.New("api busy")), "read errors fail closed")
}

func TestSealPending(t *testing.T) {
	t.Parallel()
	assert.False(t, sealPending(nil), "no seal registered")
	pending := utils.NewSetOnce[struct{}]()
	assert.True(t, sealPending(pending))
	require.NoError(t, pending.SetValue(struct{}{}))
	assert.False(t, sealPending(pending))
	failed := utils.NewSetOnce[struct{}]()
	require.NoError(t, failed.SetError(errors.New("seal failed")))
	assert.False(t, sealPending(failed), "a failed seal is settled, not pending")
}

// The seal gates read the pointers the in-place path swaps under their mutexes.
func TestHintDecisionInput_SealGatesUnderLock(t *testing.T) {
	t.Parallel()
	s, _ := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	pending := utils.NewSetOnce[struct{}]()
	s.memSealMu.Lock()
	s.memSealDone = pending
	s.memSealMu.Unlock()
	assert.Equal(t, "memory-seal-pending", hintSkipReason(fphTestConfig(), s.hintDecisionInput(time.Now())))
	require.NoError(t, pending.SetValue(struct{}{}))
	assert.Empty(t, hintSkipReason(fphTestConfig(), s.hintDecisionInput(time.Now())))
}

// Ending a checkpoint is observed as the quiet window, never as "nothing".
func TestHintDecisionInput_CheckpointEndIsNeverMissed(t *testing.T) {
	t.Parallel()
	s, _ := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	cfg := fphTestConfig()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if !assert.True(t, s.BeginInPlaceCheckpoint()) {
				return
			}
			s.EndInPlaceCheckpoint()
		}
	})
	require.Eventually(t, func() bool { return s.lastCheckpointEndedAt.Load() != 0 }, time.Second, time.Millisecond)
	for range 2000 {
		reason := hintSkipReason(cfg, s.hintDecisionInput(time.Now()))
		assert.NotEmpty(t, reason, "a tick raced a checkpoint end and would have hinted")
	}
	close(stop)
	wg.Wait()
}

// The loop drains on every tick in steady state and stops with its context.
func TestRunPeriodicHintingLoop_DrainsAndStops(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	r := &fakeRunner{}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runPeriodicHintingLoop(ctx, fixedCfg(fphTestConfig()), openGates(), r)
	}()
	require.Eventually(t, func() bool { return r.drains.Load() >= 3 }, 2*time.Second, 5*time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not stop on cancel")
	}
	for _, oc := range o.list() {
		assert.Equal(t, "ok", oc)
	}
	assert.Zero(t, r.stops.Load(), "completed cycles are never stopped")
	assert.Len(t, o.freedList(), o.count("ok"), "every completed run records what it freed")
	for _, b := range o.freedList() {
		assert.Equal(t, uint64(1<<20), b)
	}
}

// Freed bytes are recorded for completed runs only, and never when FC's
// counter has not moved (or moved backwards after a restart).
func TestHintOnce_FreedBytesOnlyOnCompletedRuns(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	r := &fakeRunner{drain: func(context.Context) error { return guestSilent() }}
	s.hintOnce(t.Context(), fphTestConfig(), r, testAdmission())
	assert.Empty(t, o.freedList(), "a timed-out run frees nothing on the record")

	s2, o2 := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	r2 := &flatFreedRunner{}
	assert.Equal(t, hintOK, s2.hintOnce(t.Context(), fphTestConfig(), r2, testAdmission()).res)
	assert.Empty(t, o2.freedList(), "no delta, no record")
}

// flatFreedRunner completes every run with FC's freed counter unchanged, flush
// or not.
type flatFreedRunner struct{ fakeRunner }

func (*flatFreedRunner) HintFreedBytes() uint64 { return 42 }

func (*flatFreedRunner) FlushHintFreedBytes(context.Context) (uint64, error) { return 42, nil }

// servedMemory is a memory backend whose serve counters the test controls.
type servedMemory struct {
	*uffd.NoopMemory

	pages, deferred atomic.Int64
}

func (m *servedMemory) ServeStats() userfaultfd.ServeSnapshot {
	return userfaultfd.ServeSnapshot{Pages: m.pages.Load(), Deferred: m.deferred.Load()}
}

// A completed run records what the guest faulted across it, and the next tick
// records what it faulted in the interval after it: the re-faults a run
// caused land on the run's record, not on the next run's.
func TestRunPeriodicHintingLoop_RunFaultsRecorded(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	mem := &servedMemory{NoopMemory: uffd.NewNoopMemory(1<<30, 2<<20)}
	s.Resources.memory = mem
	r := &fakeRunner{drain: func(context.Context) error {
		mem.pages.Add(3)
		mem.deferred.Add(1)

		return nil
	}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go s.runPeriodicHintingLoop(ctx, fixedCfg(fphTestConfig()), openGates(), r)

	require.Eventually(t, func() bool { return len(o.faultsFor(hintWindowInterval, "ok")) > 0 }, 2*time.Second, 5*time.Millisecond)
	run := o.faultsFor(hintWindowRun, "ok")
	assert.Equal(t, int64(3), run["served"][0], "the run window brackets the drain")
	assert.Equal(t, int64(1), run["deferred"][0])
	assert.Equal(t, int64(0), run["wp"][0])
	assert.Equal(t, int64(0), o.faultsFor(hintWindowInterval, "ok")["served"][0], "nothing faulted between the run and the next tick")

	// Faults between runs belong to the interval after the last run.
	mem.pages.Add(50)
	require.Eventually(t, func() bool {
		return slices.Contains(o.faultsFor(hintWindowInterval, "ok")["served"], int64(50))
	}, 2*time.Second, 5*time.Millisecond, "the interval record carries the re-faults, the run record never does")
	for _, n := range o.faultsFor(hintWindowRun, "ok")["served"] {
		assert.Equal(t, int64(3), n)
	}
}

// A tick that found no host slot labels the interval after it as host-busy,
// not idle: saturation skips stay out of the baseline.
func TestRunPeriodicHintingLoop_HostBusyLabelsInterval(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	mem := &servedMemory{NoopMemory: uffd.NewNoopMemory(1<<30, 2<<20)}
	s.Resources.memory = mem
	cfg := fphTestConfig()
	cfg.HostSlots = 1
	gates := openGates()
	require.True(t, gates.admission.tryAcquire(1), "the one slot is taken for the whole test")
	r := &fakeRunner{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go s.runPeriodicHintingLoop(ctx, fixedCfg(cfg), gates, r)

	require.Eventually(t, func() bool { return o.count("skipped-host-busy") >= 2 }, 2*time.Second, 5*time.Millisecond)
	mem.pages.Add(6)
	require.Eventually(t, func() bool {
		return slices.Contains(o.faultsFor(hintWindowInterval, "skipped-host-busy")["served"], int64(6))
	}, 2*time.Second, 5*time.Millisecond)
	assert.Empty(t, o.faultsFor(hintWindowInterval, hintLabelIdle))
	assert.Zero(t, r.drains.Load())
}

// A run the guest engaged and the host stopped at its budget is the longest
// REMOVE burst the loop produces: its faults land on the record under its
// outcome, not only the completed runs'.
func TestHintOnce_TimeoutRecordsRunWindow(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	mem := &servedMemory{NoopMemory: uffd.NewNoopMemory(1<<30, 2<<20)}
	s.Resources.memory = mem
	cfg := fphTestConfig()
	cfg.Timeout = 20 * time.Millisecond
	r := &fakeRunner{drain: func(ctx context.Context) error {
		mem.pages.Add(7)
		<-ctx.Done()

		return inFlight(ctx.Err())
	}}
	assert.Equal(t, hintFailed, s.hintOnce(t.Context(), cfg, r, testAdmission()).res)
	assert.Equal(t, []int64{7}, o.faultsFor(hintWindowRun, "timeout")["served"])
	assert.Empty(t, o.faultsFor(hintWindowRun, "ok"))
}

// observe_only ticks like a treated sandbox and records the interval baseline
// under its own label, but never hints: the control a ramp is read against.
func TestRunPeriodicHintingLoop_ObserveOnly(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	mem := &servedMemory{NoopMemory: uffd.NewNoopMemory(1<<30, 2<<20)}
	s.Resources.memory = mem
	cfg := fphTestConfig()
	cfg.ObserveOnly = true
	r := &fakeRunner{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go s.runPeriodicHintingLoop(ctx, fixedCfg(cfg), openGates(), r)

	require.Eventually(t, func() bool { return o.count("skipped-observe-only") >= 2 }, 2*time.Second, 5*time.Millisecond)
	mem.pages.Add(9)
	require.Eventually(t, func() bool {
		return slices.Contains(o.faultsFor(hintWindowInterval, hintLabelObserveOnly)["served"], int64(9))
	}, 2*time.Second, 5*time.Millisecond, "the baseline carries what the guest faulted between ticks")
	assert.Zero(t, r.drains.Load(), "an observe-only sandbox never hints")
	assert.Empty(t, o.faultsFor(hintWindowRun, "ok"))

	// The control is gated like a treated sandbox: a tick the gate would have
	// skipped is labelled by the reason and stays out of the baseline.
	require.True(t, s.BeginInPlaceCheckpoint())
	require.Eventually(t, func() bool { return o.has("skipped-checkpoint-in-flight") }, 2*time.Second, 5*time.Millisecond)
	mem.pages.Add(4)
	require.Eventually(t, func() bool {
		return slices.Contains(o.faultsFor(hintWindowInterval, "skipped-checkpoint-in-flight")["served"], int64(4))
	}, 2*time.Second, 5*time.Millisecond)
	assert.NotContains(t, o.faultsFor(hintWindowInterval, hintLabelObserveOnly)["served"], int64(4))
	s.EndInPlaceCheckpoint()
}

// A sandbox inside its quiet window is never drained.
func TestRunPeriodicHintingLoop_QuietAfterStart(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now())
	r := &fakeRunner{}
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	s.runPeriodicHintingLoop(ctx, fixedCfg(fphTestConfig()), openGates(), r)
	assert.Zero(t, r.drains.Load())
	for _, oc := range o.list() {
		assert.Equal(t, "skipped-quiet-after-start", oc)
	}
}

// An in-flight in-place checkpoint suppresses runs on the same sandbox; ending
// it stamps a quiet window, after which runs resume. The quiet window is long
// so a delayed tick cannot miss it; resumption is checked by ageing the stamp.
func TestRunPeriodicHintingLoop_CheckpointGate(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	require.True(t, s.BeginInPlaceCheckpoint())
	r := &fakeRunner{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go s.runPeriodicHintingLoop(ctx, fixedCfg(fphTestConfig()), openGates(), r)

	require.Eventually(t, func() bool { return len(o.list()) >= 2 }, 2*time.Second, 5*time.Millisecond)
	assert.Zero(t, r.drains.Load(), "no runs while the checkpoint is in flight")
	for _, oc := range o.list() {
		assert.Equal(t, "skipped-checkpoint-in-flight", oc)
	}

	s.EndInPlaceCheckpoint()
	require.Eventually(t, func() bool { return o.has("skipped-quiet-after-checkpoint") }, 2*time.Second, 5*time.Millisecond, "the end of a checkpoint opens a quiet window")
	assert.Zero(t, r.drains.Load(), "no runs inside the quiet window")

	s.lastCheckpointEndedAt.Store(time.Now().Add(-time.Minute).UnixNano())
	require.Eventually(t, func() bool { return r.drains.Load() >= 1 }, 2*time.Second, 5*time.Millisecond, "runs resume once the quiet window has passed")
}

// Disabling the flag parks a running loop within one interval; re-enabling
// resumes it.
func TestRunPeriodicHintingLoop_DisableParksEnableResumes(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	var off atomic.Bool
	r := &fakeRunner{}
	done := make(chan struct{})
	go func() { defer close(done); s.runPeriodicHintingLoop(t.Context(), toggleCfg(&off), openGates(), r) }()
	require.Eventually(t, func() bool { return r.drains.Load() >= 1 }, 2*time.Second, 5*time.Millisecond)
	off.Store(true)
	require.Eventually(t, func() bool { return o.has("skipped-disabled") }, time.Second, 5*time.Millisecond)
	drainsAtDisable := r.drains.Load()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, drainsAtDisable, r.drains.Load(), "no runs while disabled")
	select {
	case <-done:
		t.Fatal("a disable must park the loop, not end it")
	default:
	}

	off.Store(false)
	require.Eventually(t, func() bool { return r.drains.Load() > drainsAtDisable }, 2*time.Second, 5*time.Millisecond, "re-enable is live")
}

// A disabled interval served mid-flight is a disable, not a panic.
func TestRunPeriodicHintingLoop_ZeroIntervalIsDisable(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	var calls atomic.Int32
	cfgFn := func() featureflags.PeriodicHintingConfig {
		cfg := fphTestConfig()
		if calls.Add(1) > 1 {
			cfg.Interval = 0
		}

		return cfg
	}
	r := &fakeRunner{}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); s.runPeriodicHintingLoop(ctx, cfgFn, openGates(), r) }()
	require.Eventually(t, func() bool { return o.has("skipped-disabled") }, 2*time.Second, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	assert.Zero(t, r.drains.Load(), "no cycle ran once the flag turned off")
	select {
	case <-done:
		t.Fatal("a disable parks the loop rather than ending it")
	default:
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("parked loop did not stop on cancel")
	}
}

// A loop started while disabled records nothing and joins when the flag turns
// on, running the gates at that point.
func TestRunPeriodicHintingLoop_JoinsOnEnable(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	var off atomic.Bool
	off.Store(true)
	gates := openGates()
	var gateChecks atomic.Int32
	gates.balloon = func(context.Context) (fc.BalloonCaps, error) {
		gateChecks.Add(1)

		return fc.BalloonCaps{Hinting: true}, nil
	}
	r := &fakeRunner{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go s.runPeriodicHintingLoop(ctx, toggleCfg(&off), gates, r)
	time.Sleep(50 * time.Millisecond)
	assert.Zero(t, gateChecks.Load(), "gates run only once the loop is enabled")
	assert.Empty(t, o.list(), "a loop that starts disabled records nothing")
	off.Store(false)
	require.Eventually(t, func() bool { return r.drains.Load() >= 1 }, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, int32(1), gateChecks.Load())
}

// Permanent gate outcomes end the loop and record why.
func TestRunPeriodicHintingLoop_GatesRecordAndExit(t *testing.T) {
	t.Parallel()
	hinting := func(context.Context) (fc.BalloonCaps, error) { return fc.BalloonCaps{Hinting: true}, nil }
	for _, tc := range []struct {
		name  string
		gates hintGates
		want  string
	}{
		{"fc unsupported", hintGates{fcSupported: func() bool { return false }, balloon: hinting, poll: time.Millisecond, admission: &hintAdmission{}}, "skipped-fc-unsupported"},
		{"reporting active", hintGates{fcSupported: func() bool { return true }, balloon: func(context.Context) (fc.BalloonCaps, error) {
			return fc.BalloonCaps{Hinting: true, Reporting: true}, nil
		}, poll: time.Millisecond, admission: &hintAdmission{}}, "skipped-reporting-active"},
		{"no hinting on the balloon", hintGates{fcSupported: func() bool { return true }, balloon: func(context.Context) (fc.BalloonCaps, error) { return fc.BalloonCaps{}, nil }, poll: time.Millisecond, admission: &hintAdmission{}}, "skipped-not-configured"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
			r := &fakeRunner{}
			s.runPeriodicHintingLoop(t.Context(), fixedCfg(fphTestConfig()), tc.gates, r)
			assert.Equal(t, []string{tc.want}, o.list())
			assert.Zero(t, r.drains.Load())
		})
	}
}

// A transient balloon-config read error skips this attempt and is retried;
// the loop runs once the read answers.
func TestRunPeriodicHintingLoop_BalloonReadErrorIsRetried(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	var reads atomic.Int32
	gates := openGates()
	gates.balloon = func(context.Context) (fc.BalloonCaps, error) {
		if reads.Add(1) < 3 {
			return fc.BalloonCaps{}, errors.New("api busy")
		}

		return fc.BalloonCaps{Hinting: true}, nil
	}
	r := &fakeRunner{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go s.runPeriodicHintingLoop(ctx, fixedCfg(fphTestConfig()), gates, r)
	require.Eventually(t, func() bool { return r.drains.Load() >= 1 }, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, 2, o.count("skipped-balloon-unknown"))
	assert.Equal(t, int32(3), reads.Load())
}

// A balloon that never answers is read hintMaxGateRetries times at the poll
// cadence, then at the slow cadence for as long as the loop lives; it runs as
// soon as a read answers, and a cancel releases it.
func TestRunPeriodicHintingLoop_BalloonReadErrorBacksOff(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	gates := openGates()
	gates.poll = time.Millisecond
	gates.slow = 40 * time.Millisecond
	var answer atomic.Bool
	gates.balloon = func(context.Context) (fc.BalloonCaps, error) {
		if answer.Load() {
			return fc.BalloonCaps{Hinting: true}, nil
		}

		return fc.BalloonCaps{}, errors.New("api busy")
	}
	r := &fakeRunner{}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); s.runPeriodicHintingLoop(ctx, fixedCfg(fphTestConfig()), gates, r) }()
	require.Eventually(t, func() bool { return o.count("skipped-balloon-unknown") >= hintMaxGateRetries }, 2*time.Second, time.Millisecond)
	at := time.Now()
	require.Eventually(t, func() bool { return o.count("skipped-balloon-unknown") >= hintMaxGateRetries+2 }, 2*time.Second, time.Millisecond)
	assert.GreaterOrEqual(t, time.Since(at), gates.slow, "after the retries the reads slow down")
	select {
	case <-done:
		t.Fatal("the loop gave up on the balloon")
	default:
	}
	answer.Store(true)
	require.Eventually(t, func() bool { return r.drains.Load() >= 1 }, 2*time.Second, 5*time.Millisecond, "a read that answers lets the loop run")
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not stop on cancel")
	}
}

// A Firecracker that cannot hint ends the loop at once, flag on or off: no
// goroutine parks for the sandbox's lifetime waiting for a flag it cannot act
// on, and with the flag off nothing is recorded.
func TestRunPeriodicHintingLoop_FcUnsupportedExitsWhileDisabled(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	var off atomic.Bool
	off.Store(true)
	gates := openGates()
	gates.fcSupported = func() bool { return false }
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runPeriodicHintingLoop(t.Context(), toggleCfg(&off), gates, &fakeRunner{})
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop parked on an unsupported Firecracker")
	}
	assert.Empty(t, o.list(), "a fleet with the feature off does not count sandbox starts")
}

// The loop ends once the process is gone rather than knocking on a dead
// socket every interval.
func TestRunPeriodicHintingLoop_ExitsWithProcess(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	r := &fakeRunner{}
	r.drain = func(context.Context) error {
		r.exited.Store(true)

		return errors.New("start balloon hinting: connection refused")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runPeriodicHintingLoop(t.Context(), fixedCfg(fphTestConfig()), openGates(), r)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("loop outlived the process")
	}
	assert.Equal(t, int32(1), r.drains.Load())
	assert.Empty(t, o.list())
}

// A loop whose context is already gone when it reaches the gates records
// nothing: neither the fail-closed balloon outcome nor a disable.
func TestRunPeriodicHintingLoop_CancelledBeforeGatesIsSilent(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	gates := openGates()
	gates.balloon = func(ctx context.Context) (fc.BalloonCaps, error) { return fc.BalloonCaps{}, ctx.Err() }
	s.runPeriodicHintingLoop(ctx, fixedCfg(fphTestConfig()), gates, &fakeRunner{})
	assert.Empty(t, o.list(), "a cancelled balloon read is not an unknown balloon")

	s2, o2 := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	var off atomic.Bool
	off.Store(true)
	s2.runPeriodicHintingLoop(ctx, toggleCfg(&off), openGates(), &fakeRunner{})
	assert.Empty(t, o2.list(), "a sandbox going away is not a disable")
}

// A guest that sits out SilentRuns runs in a row (silent timeouts or refused
// starts) is latched unresponsive once, backed off to UnresponsiveRetry, and
// released by the next completed run. A completed run in between resets the
// count.
func TestRunPeriodicHintingLoop_UnresponsiveGuestBacksOffAndRecovers(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	refused := fmt.Errorf("%w: 400", fc.ErrHintingStartRefused)
	script := []error{guestSilent(), guestSilent(), nil, guestSilent(), refused, guestSilent()}
	var calls atomic.Int32
	var recovered atomic.Bool
	r := &fakeRunner{drain: func(context.Context) error {
		if recovered.Load() {
			return nil
		}

		return script[min(int(calls.Add(1))-1, len(script)-1)]
	}}
	cfg := fphTestConfig()
	cfg.UnresponsiveRetry = 200 * time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); s.runPeriodicHintingLoop(ctx, fixedCfg(cfg), openGates(), r) }()

	require.Eventually(t, func() bool { return o.has("latched") }, 5*time.Second, 5*time.Millisecond)
	assert.True(t, s.hintUnresponsive.Load())
	assert.Equal(t, []string{"timeout-guest-silent", "timeout-guest-silent", "ok", "timeout-guest-silent", "refused", "timeout-guest-silent", "latched"}, o.list()[:7])
	assert.Equal(t, int32(4), r.stops.Load(), "every abandoned cycle was stopped, refusals have nothing to stop")

	// Latched: the loop keeps ticking at the interval but attempts only at the
	// backed-off cadence, recorded once.
	require.Eventually(t, func() bool { return r.drains.Load() >= 8 }, 5*time.Second, 5*time.Millisecond)
	assert.Equal(t, 1, o.count("latched"))
	assert.True(t, s.hintUnresponsive.Load())

	recovered.Store(true)
	require.Eventually(t, func() bool { return !s.hintUnresponsive.Load() }, 5*time.Second, 5*time.Millisecond, "a completed run releases the latch")
	assert.Equal(t, 1, o.count("released"))
	cancel()
	<-done
}

// SilentRuns of zero never latches.
func TestRunPeriodicHintingLoop_SilentRunsZeroNeverLatches(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	cfg := fphTestConfig()
	cfg.SilentRuns = 0
	r := &fakeRunner{drain: func(context.Context) error { return guestSilent() }}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); s.runPeriodicHintingLoop(ctx, fixedCfg(cfg), openGates(), r) }()
	require.Eventually(t, func() bool { return r.drains.Load() >= 6 }, 5*time.Second, 5*time.Millisecond)
	cancel()
	<-done
	assert.False(t, o.has("latched"))
	assert.False(t, s.hintUnresponsive.Load())
}

// The latch is the loop's state: a cancelled run does not count as a completed
// one (no release is recorded), and once the loop ends, or parks on a disable,
// nothing is retrying, so the latch is dropped rather than left to describe a
// guest nobody is checking.
func TestRunPeriodicHintingLoop_LatchDoesNotOutliveLoop(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	ctx, cancel := context.WithCancel(t.Context())
	inDrain := make(chan struct{}, 1)
	r := &fakeRunner{drain: func(ctx context.Context) error {
		if !s.hintUnresponsive.Load() {
			return guestSilent()
		}
		select {
		case inDrain <- struct{}{}:
		default:
		}
		<-ctx.Done()

		return inFlight(ctx.Err())
	}}
	done := make(chan struct{})
	go func() { defer close(done); s.runPeriodicHintingLoop(ctx, fixedCfg(fphTestConfig()), openGates(), r) }()
	require.Eventually(t, func() bool { return o.has("latched") }, 5*time.Second, 5*time.Millisecond)
	<-inDrain
	cancel()
	<-done
	assert.False(t, s.hintUnresponsive.Load(), "no loop, no latch")
	assert.False(t, o.has("released"), "a cancelled run is not a completed one")
	assert.Equal(t, 1, o.count("dropped"), "the latch that went with the loop is recorded as dropped")

	// Parking on a disable drops it too.
	s2, o2 := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	var off atomic.Bool
	r2 := &fakeRunner{drain: func(context.Context) error { return guestSilent() }}
	ctx2, cancel2 := context.WithCancel(t.Context())
	defer cancel2()
	go s2.runPeriodicHintingLoop(ctx2, toggleCfg(&off), openGates(), r2)
	require.Eventually(t, func() bool { return o2.has("latched") }, 5*time.Second, 5*time.Millisecond)
	assert.True(t, s2.hintWarned.Load(), "the first failure warned")
	off.Store(true)
	require.Eventually(t, func() bool { return o2.has("skipped-disabled") }, 5*time.Second, 5*time.Millisecond)
	assert.False(t, s2.hintUnresponsive.Load(), "a parked loop retries nothing, so it holds no latch")
	assert.False(t, s2.hintWarned.Load(), "the warn dedupe restarts with the loop")
	assert.False(t, o2.has("released"))
	assert.Equal(t, 1, o2.count("dropped"))
}

// A tick that is skipped (checkpoint in flight, quiet window) or
// finds no host slot spends no retry budget: a latched guest is attempted on
// the first tick that can run after the retry cadence has passed, not a
// further retry period later.
func TestRunPeriodicHintingLoop_LatchedRetryNotSpentBySkips(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	cfg := fphTestConfig()
	cfg.UnresponsiveRetry = 500 * time.Millisecond
	cfg.QuietAfterStart = 30 * time.Millisecond
	var engaged atomic.Bool
	r := &fakeRunner{drain: func(context.Context) error {
		if engaged.Load() {
			return nil
		}

		return guestSilent()
	}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go s.runPeriodicHintingLoop(ctx, fixedCfg(cfg), openGates(), r)
	require.Eventually(t, func() bool { return o.has("latched") }, 5*time.Second, 5*time.Millisecond)

	// The retry cadence passes while a checkpoint is in flight: every tick in
	// that window is skipped and must not re-arm the retry.
	require.True(t, s.BeginInPlaceCheckpoint())
	time.Sleep(650 * time.Millisecond)
	assert.True(t, o.has("skipped-checkpoint-in-flight"))
	drains := r.drains.Load()
	engaged.Store(true)
	s.EndInPlaceCheckpoint()
	start := time.Now()
	require.Eventually(t, func() bool { return r.drains.Load() > drains }, 2*time.Second, 5*time.Millisecond)
	assert.Less(t, time.Since(start), 250*time.Millisecond, "the attempt follows the quiet window, not a fresh retry period")
	require.Eventually(t, func() bool { return o.has("released") }, 2*time.Second, 5*time.Millisecond)
}

// A latched loop still re-reads the flag every interval: a disable lands
// within one interval, not one retry.
func TestRunPeriodicHintingLoop_LatchedLoopParksWithinOneInterval(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	var off atomic.Bool
	cfgFn := func() featureflags.PeriodicHintingConfig {
		cfg := fphTestConfig()
		cfg.UnresponsiveRetry = 3 * time.Second
		if off.Load() {
			cfg.Interval = 0
		}

		return cfg
	}
	r := &fakeRunner{drain: func(context.Context) error { return guestSilent() }}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go s.runPeriodicHintingLoop(ctx, cfgFn, openGates(), r)
	require.Eventually(t, func() bool { return o.has("latched") }, 5*time.Second, 5*time.Millisecond)
	drains := r.drains.Load()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, drains, r.drains.Load(), "no attempt before the retry cadence")
	off.Store(true)
	start := time.Now()
	require.Eventually(t, func() bool { return o.has("skipped-disabled") }, 2*time.Second, 5*time.Millisecond)
	assert.Less(t, time.Since(start), time.Second, "the disable landed well inside the 3 s retry")
}

// Transport errors and the timeouts of a guest that did engage never latch
// the guest unresponsive: the loop keeps trying at the normal cadence.
func TestRunPeriodicHintingLoop_EngagedFailuresDoNotLatch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"transport errors", errors.New("start balloon hinting: EOF")},
		{"slow guest", inFlight(context.DeadlineExceeded)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
			r := &fakeRunner{drain: func(context.Context) error { return tc.err }}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			go func() { defer close(done); s.runPeriodicHintingLoop(ctx, fixedCfg(fphTestConfig()), openGates(), r) }()
			require.Eventually(t, func() bool { return r.drains.Load() >= 6 }, 5*time.Second, 5*time.Millisecond)
			cancel()
			<-done
			assert.False(t, o.has("latched"))
			assert.False(t, s.hintUnresponsive.Load())
		})
	}
}

// The host-wide bound: a tick that finds no slot skips this run rather than
// queueing behind other sandboxes, holds a slot only while its run is in
// flight, and reads the capacity from the flag at each tick.
func TestHintOnce_HostSlots(t *testing.T) {
	t.Parallel()
	adm := &hintAdmission{}
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	cfg := fphTestConfig()
	cfg.HostSlots = 1
	require.True(t, adm.tryAcquire(1))
	r := &fakeRunner{}
	assert.Equal(t, hintAborted, s.hintOnce(t.Context(), cfg, r, adm).res)
	assert.Zero(t, r.drains.Load())
	assert.Equal(t, []string{"skipped-host-busy"}, o.list())

	cfg.HostSlots = 0
	assert.Equal(t, hintOK, s.hintOnce(t.Context(), cfg, r, adm).res, "zero is unbounded")

	adm.release()
	cfg.HostSlots = 1
	assert.Equal(t, hintOK, s.hintOnce(t.Context(), cfg, r, adm).res)
	assert.True(t, adm.tryAcquire(1), "the slot is released with the run")
	adm.release()

	// The slot is released on the failure paths too: a run that timed out and
	// was stopped, and a run that never started.
	cfg.Timeout = 20 * time.Millisecond
	rt := &fakeRunner{drain: func(ctx context.Context) error {
		<-ctx.Done()

		return inFlight(ctx.Err())
	}}
	assert.Equal(t, hintFailed, s.hintOnce(t.Context(), cfg, rt, adm).res)
	assert.True(t, adm.tryAcquire(1), "the slot is released after a timeout and stop")
	adm.release()
	rr := &fakeRunner{drain: func(context.Context) error { return fmt.Errorf("%w: 400", fc.ErrHintingStartRefused) }}
	assert.Equal(t, hintGuestSilent, s.hintOnce(t.Context(), cfg, rr, adm).res)
	assert.True(t, adm.tryAcquire(1), "the slot is released after a refusal")
	adm.release()
}

// A balloon that cannot hint ends the loop instead of counting empty runs as ok.
func TestRunPeriodicHintingLoop_NotConfiguredExits(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	r := &fakeRunner{drain: func(context.Context) error { return fc.ErrHintingNotConfigured }}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runPeriodicHintingLoop(t.Context(), fixedCfg(fphTestConfig()), openGates(), r)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not exit on not-configured")
	}
	assert.Equal(t, []string{"skipped-not-configured"}, o.list())
	assert.Equal(t, int32(1), r.drains.Load())
}

// A timeout on a guest that never engaged is stopped, recorded apart from a
// slow guest's timeout, and reported to the scheduler as silence.
func TestHintOnce_SilentGuestTimeout(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	r := &fakeRunner{drain: func(context.Context) error { return guestSilent() }}
	assert.Equal(t, hintGuestSilent, s.hintOnce(t.Context(), fphTestConfig(), r, testAdmission()).res)
	assert.Equal(t, int32(1), r.stops.Load())
	assert.Equal(t, []string{"timeout-guest-silent"}, o.list())
	assert.Equal(t, []string{"ok"}, o.stops())
}

// A run that outlives its timeout is stopped, with a sane stop context and the
// configured grace, and recorded as timeout.
func TestHintOnce_TimeoutStopsTheCycle(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	cfg := fphTestConfig()
	cfg.Timeout = 30 * time.Millisecond
	cfg.Stop.Grace = 9 * time.Millisecond
	r := &fakeRunner{drain: func(ctx context.Context) error {
		<-ctx.Done()

		return inFlight(ctx.Err())
	}}
	start := time.Now()
	assert.Equal(t, hintFailed, s.hintOnce(t.Context(), cfg, r, testAdmission()).res)
	assert.Less(t, time.Since(start), 500*time.Millisecond)
	assert.Equal(t, int32(1), r.stops.Load(), "the abandoned cycle is stopped")
	assert.Zero(t, r.badStopCx.Load(), "stop gets an uncancelled, bounded context")
	assert.Equal(t, int64(9*time.Millisecond), r.grace.Load(), "the flag's grace reaches the stop")
	assert.Equal(t, []string{"timeout"}, o.list())
}

// A start whose round-trip hit the deadline may have reached FC: stop it and
// record the timeout, exactly like a cycle that started.
func TestHintOnce_StartDeadlineStillStops(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	r := &fakeRunner{drain: func(context.Context) error {
		return inFlight(fmt.Errorf("start balloon hinting: %w", context.DeadlineExceeded))
	}}
	assert.Equal(t, hintFailed, s.hintOnce(t.Context(), fphTestConfig(), r, testAdmission()).res)
	assert.Equal(t, int32(1), r.stops.Load())
	assert.Zero(t, r.badStopCx.Load())
	assert.Equal(t, []string{"timeout"}, o.list())
}

// A cancelled in-flight cycle is stopped but not counted as a timeout.
func TestHintOnce_CancelStopsSilently(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	ctx, cancel := context.WithCancel(t.Context())
	r := &fakeRunner{drain: func(ctx context.Context) error {
		cancel()
		<-ctx.Done()

		return inFlight(ctx.Err())
	}}
	assert.Equal(t, hintAborted, s.hintOnce(ctx, fphTestConfig(), r, testAdmission()).res)
	assert.Equal(t, int32(1), r.stops.Load())
	assert.Zero(t, r.badStopCx.Load())
	assert.Empty(t, o.list())
}

// A start cancelled mid-round-trip may have reached FC: it is stopped anyway.
func TestHintOnce_CancelDuringStartStillStops(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	r := &fakeRunner{drain: func(context.Context) error {
		return inFlight(fmt.Errorf("start balloon hinting: %w", context.Canceled))
	}}
	assert.Equal(t, hintAborted, s.hintOnce(t.Context(), fphTestConfig(), r, testAdmission()).res)
	assert.Equal(t, int32(1), r.stops.Load())
	assert.Empty(t, o.list())
}

// An in-flight failure that is neither timeout nor cancel is stopped AND
// recorded as a failure.
func TestHintOnce_InFlightDescribeFailureIsRecorded(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	r := &fakeRunner{drain: func(context.Context) error { return inFlight(errors.New("balloon hinting status: EOF")) }}
	assert.Equal(t, hintFailed, s.hintOnce(t.Context(), fphTestConfig(), r, testAdmission()).res)
	assert.Equal(t, int32(1), r.stops.Load())
	assert.Equal(t, []string{"failed"}, o.list())
}

// The same describe failure on a guest that went away is not a failure.
func TestHintOnce_InFlightFailureAfterGuestExitIsSilent(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	r := &fakeRunner{}
	r.drain = func(context.Context) error {
		r.exited.Store(true)

		return inFlight(errors.New("balloon hinting status: EOF"))
	}
	assert.Equal(t, hintAborted, s.hintOnce(t.Context(), fphTestConfig(), r, testAdmission()).res)
	assert.Zero(t, r.stops.Load(), "nothing to stop on an exited process")
	assert.Empty(t, o.list())
}

// A start that fails because the guest went away is not a hinting failure.
func TestHintOnce_StartFailureAfterGuestExitIsSilent(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	r := &fakeRunner{}
	r.drain = func(context.Context) error {
		r.exited.Store(true)

		return errors.New("start balloon hinting: connection refused")
	}
	assert.Equal(t, hintAborted, s.hintOnce(t.Context(), fphTestConfig(), r, testAdmission()).res)
	assert.Zero(t, r.stops.Load())
	assert.Empty(t, o.list())
}

// Teardown: once the sandbox is stopping, or the process is gone, an abandoned
// cycle is left to die with the process: no stop call, no run outcome.
func TestHintOnce_StoppingOrExitedSkipsStop(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	ctx, cancel := context.WithCancel(t.Context())
	r := &fakeRunner{stopErr: errors.New("connection refused")}
	r.drain = func(ctx context.Context) error {
		s.stopping.Store(true)
		cancel()
		<-ctx.Done()

		return inFlight(ctx.Err())
	}
	assert.Equal(t, hintAborted, s.hintOnce(ctx, fphTestConfig(), r, testAdmission()).res)
	assert.Zero(t, r.stops.Load(), "a stopping sandbox does not stop its cycle")
	assert.Empty(t, o.list())
	assert.Equal(t, []string{"skipped-stopping"}, o.stops())

	// The kill races the socket, not the context: the API dies before the
	// exit is reaped, and neither a failed start nor a failed status read on
	// a stopping sandbox is a hinting failure.
	for name, drain := range map[string]func(context.Context) error{
		"start on a dead socket":  func(context.Context) error { return errors.New("start balloon hinting: EOF") },
		"status on a dead socket": func(context.Context) error { return inFlight(errors.New("balloon hinting status: EOF")) },
	} {
		sk, ok := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
		sk.stopping.Store(true)
		rk := &fakeRunner{drain: drain}
		assert.Equal(t, hintAborted, sk.hintOnce(t.Context(), fphTestConfig(), rk, testAdmission()).res, name)
		assert.Zero(t, rk.stops.Load(), name)
		assert.Empty(t, ok.list(), name)
	}

	s2, o2 := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	ctx2, cancel2 := context.WithCancel(t.Context())
	r2 := &fakeRunner{drain: func(ctx context.Context) error {
		cancel2()
		<-ctx.Done()

		return inFlight(ctx.Err())
	}}
	r2.exited.Store(true)
	assert.Equal(t, hintAborted, s2.hintOnce(ctx2, fphTestConfig(), r2, testAdmission()).res)
	assert.Zero(t, r2.stops.Load(), "an exited process has no cycle to stop")
	assert.Empty(t, o2.list())

	// The race the other way round: the stop was issued, then the process
	// exited before it could answer.
	s3, o3 := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	ctx3, cancel3 := context.WithCancel(t.Context())
	r3 := &fakeRunner{stopErr: errors.New("connection refused")}
	r3.stopHook = func() { r3.exited.Store(true) }
	r3.drain = func(ctx context.Context) error {
		cancel3()
		<-ctx.Done()

		return inFlight(ctx.Err())
	}
	assert.Equal(t, hintAborted, s3.hintOnce(ctx3, fphTestConfig(), r3, testAdmission()).res)
	assert.Equal(t, int32(1), r3.stops.Load())
	assert.Empty(t, o3.list(), "a stop that lost the race with exit is routine")
	assert.Equal(t, []string{"skipped-exited"}, o3.stops())
}

// A stop that fails on a live, running sandbox is visible once, on the stop
// series, and the run itself still records exactly one outcome.
func TestHintOnce_StopFailureIsRecordedOnce(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	cfg := fphTestConfig()
	cfg.Timeout = 20 * time.Millisecond
	r := &fakeRunner{drain: func(ctx context.Context) error {
		<-ctx.Done()

		return inFlight(ctx.Err())
	}, stopErr: errors.New("fc api down")}
	assert.Equal(t, hintFailed, s.hintOnce(t.Context(), cfg, r, testAdmission()).res)
	assert.Equal(t, []string{"timeout"}, o.list())
	assert.Equal(t, []string{"failed"}, o.stops())
}

// A stop that landed but was not acknowledged by the guest in time is a
// warning, not a failed stop.
func TestHintOnce_StopUnackedIsNotAFailedStop(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	cfg := fphTestConfig()
	cfg.Timeout = 20 * time.Millisecond
	r := &fakeRunner{
		drain: func(ctx context.Context) error {
			<-ctx.Done()

			return inFlight(ctx.Err())
		},
		stopErr: fmt.Errorf("%w: guest_cmd=7: %w", fc.ErrHintingStopUnacked, context.DeadlineExceeded),
	}
	assert.Equal(t, hintFailed, s.hintOnce(t.Context(), cfg, r, testAdmission()).res)
	assert.Equal(t, []string{"timeout"}, o.list())
	assert.Equal(t, []string{"unacked"}, o.stops())
}

// Drain errors before the cycle started are failures that leave the loop
// running and stop nothing; a refusal counts as the guest sitting out.
func TestHintOnce_FailureClassified(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		want hintResult
	}{
		{"start refused", fmt.Errorf("%w: 400", fc.ErrHintingStartRefused), hintGuestSilent},
		{"start refused, config unreadable", fmt.Errorf("%w: %w: 400", fc.ErrHintingStartRefused, fc.ErrHintingConfigUnknown), hintFailed},
		{"start failed", errors.New("start balloon hinting: EOF"), hintFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
			r := &fakeRunner{drain: func(context.Context) error { return tc.err }}
			assert.Equal(t, tc.want, s.hintOnce(t.Context(), fphTestConfig(), r, testAdmission()).res)
			want := "failed"
			if tc.want == hintGuestSilent {
				want = "refused"
			}
			assert.Equal(t, []string{want}, o.list())
			assert.Zero(t, r.stops.Load())
		})
	}
}

// A crash mid-cycle kills the API before the exit is reaped: a stop that fails
// on a process that exits moments later is not a failed stop.
func TestHintOnce_StopFailureRightBeforeExitIsSilent(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	r := &fakeRunner{drain: func(context.Context) error { return inFlight(errors.New("balloon hinting status: EOF")) }, stopErr: errors.New("connection refused")}
	r.stopHook = func() { time.AfterFunc(30*time.Millisecond, func() { r.exited.Store(true) }) }
	assert.Equal(t, hintAborted, s.hintOnce(t.Context(), fphTestConfig(), r, testAdmission()).res)
	assert.Equal(t, int32(1), r.stops.Load())
	assert.Empty(t, o.list(), "a failed run is not recorded for a crash")
	assert.Equal(t, []string{"skipped-exited"}, o.stops())
}

// Pause's barrier: a run holding fphMu finishes and stops its cycle before the
// barrier passes, and a run that was still waiting sees the cancelled context
// and never starts.
func TestHintOnce_PauseBarrier(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	ctx, cancel := context.WithCancel(t.Context())
	inDrain := make(chan struct{})
	r := &fakeRunner{drain: func(ctx context.Context) error {
		close(inDrain)
		<-ctx.Done()

		return inFlight(ctx.Err())
	}}
	go s.hintOnce(ctx, fphTestConfig(), r, testAdmission())
	<-inDrain

	// Pause path: stop the checks (cancel), then take the barrier.
	cancel()
	s.fphMu.Lock()
	assert.Equal(t, int32(1), r.stops.Load(), "the in-flight cycle was stopped before the barrier passed")
	s.fphMu.Unlock()

	// A second run arriving after cancellation must not start a cycle.
	assert.Equal(t, hintExit, s.hintOnce(ctx, fphTestConfig(), r, testAdmission()).res)
	assert.Equal(t, int32(1), r.drains.Load())
	assert.Empty(t, o.list())
}

// The pause drain and a periodic run share fphMu: the drain waits for a run in
// flight instead of overlapping it.
func TestPrePauseHintDrain_WaitsForRunInFlight(t *testing.T) {
	t.Parallel()
	s, _ := newFPHTestSandboxAt(time.Now().Add(-time.Minute))
	inDrain := make(chan struct{})
	release := make(chan struct{})
	var order []string
	var mu sync.Mutex
	note := func(what string) { mu.Lock(); order = append(order, what); mu.Unlock() }
	r := &fakeRunner{drain: func(context.Context) error {
		close(inDrain)
		<-release
		note("run done")

		return nil
	}}
	cfg := fphTestConfig()
	cfg.Timeout = time.Second
	go s.hintOnce(t.Context(), cfg, r, testAdmission())
	<-inDrain

	pauseDone := make(chan struct{})
	go func() {
		defer close(pauseDone)
		s.prePauseHintDrain(t.Context(), prePauseCfg(time.Second), &fakeRunner{drain: func(context.Context) error {
			note("pause drain")

			return nil
		}})
	}()
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	assert.Empty(t, order, "the pause drain did not overlap the run")
	mu.Unlock()
	close(release)
	<-pauseDone
	mu.Lock()
	assert.Equal(t, []string{"run done", "pause drain"}, order)
	mu.Unlock()
}
