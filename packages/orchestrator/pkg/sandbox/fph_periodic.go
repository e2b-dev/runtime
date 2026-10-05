//go:build linux

package sandbox

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var fphLatchCounter = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorFPHLatchCounterName))

// hintAdmission bounds the runs in flight on one host: every sandbox ticks on
// its own clock and knows nothing of its neighbours. The capacity is the
// flag's at the time of each tick, so it can change at runtime.
type hintAdmission struct {
	mu       sync.Mutex
	inFlight int
}

var hostHintAdmission = &hintAdmission{}

func (a *hintAdmission) tryAcquire(capacity int) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if capacity > 0 && a.inFlight >= capacity {
		return false
	}
	a.inFlight++

	return true
}

func (a *hintAdmission) release() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.inFlight--
}

// Periodic free-page hinting: a host-driven replacement for continuous
// free-page reporting. Reporting discards freed guest memory whenever the guest
// reports it, which under sync-WP lands REMOVE bursts on the serve loop at the
// worst moments (post-checkpoint re-arm, child boot, CoW windows). A hinting
// run does the same work as one batch, at a time the host picks, and nothing
// is ever queued in the device across a snapshot.
//
// The loop lives for as long as the sandbox's health checks do (see
// Checks.Start), so it stops at pause/kill and restarts after an in-place
// resume with everything else. Pause additionally waits on fphMu after
// stopping the checks: a run holds fphMu until the guest cycle is over or
// stopped, so no cycle survives into the snapshot.

const (
	hintPhasePeriodic = "periodic"
	// hintDisabledPoll is the cadence at which a parked loop re-reads the flag.
	hintDisabledPoll = 30 * time.Second
	// hintMaxGateRetries is how many balloon-config reads the gate makes at
	// the poll cadence before slowing to hintGateSlowPoll; hintGateReadTimeout
	// bounds each one.
	hintMaxGateRetries  = 10
	hintGateReadTimeout = 500 * time.Millisecond
	hintGateSlowPoll    = 5 * time.Minute
)

// hintResult is what one hintOnce call means for the scheduler.
type hintResult int

const (
	// hintOK: the cycle completed.
	hintOK hintResult = iota
	// hintAborted: ended with the sandbox (cancel, process gone); says
	// nothing about the guest.
	hintAborted
	// hintFailed: recorded as timeout or failed; the loop carries on.
	hintFailed
	// hintGuestSilent: the guest sat this one out (never echoed the command,
	// or FC refused the start); counts toward the unresponsive backoff.
	hintGuestSilent
	// hintExit: the loop must end.
	hintExit
)

// hintDecisionInput is the per-tick state the skip decision reads; gathering it
// is the only part of the tick that touches sandbox state.
type hintDecisionInput struct {
	now              time.Time
	startedAt        time.Time
	lastCheckpointAt time.Time
	checkpointActive bool
	memSealPending   bool
	rootfsSealPend   bool
}

// hintSkipReason is the pure per-tick decision. Empty means run.
func hintSkipReason(cfg featureflags.PeriodicHintingConfig, in hintDecisionInput) string {
	switch {
	case in.checkpointActive:
		return "checkpoint-in-flight"
	case in.memSealPending:
		return "memory-seal-pending"
	case in.rootfsSealPend:
		return "rootfs-seal-pending"
	case !in.startedAt.IsZero() && in.now.Sub(in.startedAt) < cfg.QuietAfterStart:
		return "quiet-after-start"
	case !in.lastCheckpointAt.IsZero() && in.now.Sub(in.lastCheckpointAt) < cfg.QuietAfterStart:
		return "quiet-after-checkpoint"
	}

	return ""
}

// balloonGate maps the balloon config to an outcome; empty means the loop may
// run. An error fails closed (hinting on top of reporting is the double
// traffic the loop exists to remove); the caller retries it.
func balloonGate(caps fc.BalloonCaps, err error) string {
	switch {
	case err != nil:
		return "skipped-balloon-unknown"
	case !caps.Hinting:
		return "skipped-not-configured"
	case caps.Reporting:
		return "skipped-reporting-active"
	}

	return ""
}

func sealPending(done *utils.SetOnce[struct{}]) bool {
	if done == nil {
		return false
	}
	_, err := done.Result()

	return errors.As(err, &utils.NotSetError{})
}

func (s *Sandbox) memSealPending() bool {
	s.memSealMu.Lock()
	defer s.memSealMu.Unlock()

	return sealPending(s.memSealDone)
}

func (s *Sandbox) rootfsSealPending() bool {
	s.rootfsSealMu.Lock()
	defer s.rootfsSealMu.Unlock()

	return sealPending(s.rootfsSealDone)
}

func (s *Sandbox) hintDecisionInput(now time.Time) hintDecisionInput {
	// Flag first, then timestamp: EndInPlaceCheckpoint stores the timestamp
	// before clearing the flag, so a reader that sees the flag clear also sees
	// the stamp and cannot skip the post-checkpoint quiet window.
	checkpointActive := s.inPlaceCheckpointInFlight.Load()
	var lastCkpt time.Time
	if ns := s.lastCheckpointEndedAt.Load(); ns != 0 {
		lastCkpt = time.Unix(0, ns)
	}

	return hintDecisionInput{
		now:              now,
		startedAt:        s.GetStartedAt(),
		lastCheckpointAt: lastCkpt,
		checkpointActive: checkpointActive,
		memSealPending:   s.memSealPending(),
		rootfsSealPend:   s.rootfsSealPending(),
	}
}

// hintGates are the per-sandbox checks that decide whether the loop may run at
// all, plus the cadences for the parked and retry states; injected so the
// scheduler is testable without a Firecracker process.
type hintGates struct {
	fcSupported func() bool
	balloon     func(ctx context.Context) (fc.BalloonCaps, error)
	poll        time.Duration
	slow        time.Duration
	// admission is the host-wide bound on runs in flight; hintOnce takes it
	// from here unconditionally.
	admission *hintAdmission
}

// runPeriodicHinting is launched from Checks.Start and returns when ctx is
// cancelled or when hinting cannot apply to this sandbox.
func (s *Sandbox) runPeriodicHinting(ctx context.Context) {
	if s.featureFlags == nil {
		return
	}
	cfgFn := func() featureflags.PeriodicHintingConfig {
		return featureflags.GetPeriodicHintingConfig(ctx, s.featureFlags, sandboxLDContext(s.Runtime, s.Config))
	}
	// Without a LaunchDarkly backend the fallback is final: nothing to poll.
	if !s.featureFlags.Live() && !cfgFn().Enabled() {
		return
	}
	s.runPeriodicHintingLoop(ctx, cfgFn, s.periodicHintGates(), s.process)
}

// periodicHintGates are the production gates: the running Firecracker's
// release, its balloon, and the host-wide admission.
func (s *Sandbox) periodicHintGates() hintGates {
	return hintGates{
		fcSupported: func() bool { return fc.FCSupportsFreePageHinting(s.Config.FirecrackerConfig.FirecrackerVersion) },
		balloon:     s.process.BalloonCaps,
		poll:        hintDisabledPoll,
		slow:        hintGateSlowPoll,
		admission:   hostHintAdmission,
	}
}

func (s *Sandbox) recordPeriodicRun(ctx context.Context, outcome string, took time.Duration) {
	s.recordHintRun(ctx, hintPhasePeriodic, outcome, took)
}

// waitUntilEnabled blocks until the flag serves an interval > 0 or ctx ends.
// A disable that parks a running loop is recorded once (record); a loop that
// starts disabled records nothing, so the run series is not dominated by
// sandbox starts while the flag is off.
func (s *Sandbox) waitUntilEnabled(ctx context.Context, cfgFn func() featureflags.PeriodicHintingConfig, poll time.Duration, record bool) (featureflags.PeriodicHintingConfig, bool) {
	cfg := cfgFn()
	if cfg.Enabled() {
		return cfg, true
	}
	if ctx.Err() != nil {
		return cfg, false
	}
	if record {
		s.recordPeriodicRun(ctx, "skipped-disabled", 0)
	}
	for sleepOrDone(ctx, poll) {
		if cfg = cfgFn(); cfg.Enabled() {
			return cfg, true
		}
	}

	return cfg, false
}

// passBalloonGate reads the balloon config until it answers or ctx ends; true
// means hinting may run. What the balloon was built with is a template
// property and ends the loop; an unanswered read only skips this attempt, at
// the poll cadence for the first hintMaxGateRetries and slowly after that.
func (s *Sandbox) passBalloonGate(ctx context.Context, gates hintGates) bool {
	for attempt := 1; ; attempt++ {
		readCtx, cancel := context.WithTimeout(ctx, hintGateReadTimeout)
		outcome := balloonGate(gates.balloon(readCtx))
		cancel()
		if ctx.Err() != nil {
			// A read that failed because the sandbox is going away is not
			// an unknown balloon.
			return false
		}
		if outcome == "" {
			return true
		}
		s.recordPeriodicRun(ctx, outcome, 0)
		if outcome != "skipped-balloon-unknown" {
			return false
		}
		wait := gates.poll
		if attempt >= hintMaxGateRetries {
			wait = max(gates.slow, gates.poll)
		}
		if !sleepOrDone(ctx, wait) {
			return false
		}
	}
}

// runPeriodicHintingLoop is the scheduler. The config is re-read every tick so
// a flag change lands within one interval, in both directions: a disable parks
// the loop in waitUntilEnabled instead of ending it. Gates and runner are
// injected for tests.
func (s *Sandbox) runPeriodicHintingLoop(ctx context.Context, cfgFn func() featureflags.PeriodicHintingConfig, gates hintGates, runner hintRunner) {
	// The latch is this loop's state: it must not outlive it, or it would
	// describe a guest nothing is retrying. The warn dedupe restarts with it.
	defer s.dropHintLatch(ctx)

	// The static gate first: no goroutine parks for the sandbox's lifetime on
	// a Firecracker that cannot hint. Recorded only when the flag asked for a
	// loop, so a fleet with the feature off does not count sandbox starts.
	if !gates.fcSupported() {
		if cfgFn().Enabled() {
			s.recordPeriodicRun(ctx, "skipped-fc-unsupported", 0)
		}

		return
	}
	cfg, ok := s.waitUntilEnabled(ctx, cfgFn, gates.poll, false)
	if !ok {
		return
	}
	if !s.passBalloonGate(ctx, gates) {
		return
	}

	for {
		sbxlogger.I(s).Info(ctx, "periodic free-page hinting on",
			zap.Duration("interval", cfg.Interval), zap.Duration("timeout", cfg.Timeout),
			zap.Duration("quiet_after_start", cfg.QuietAfterStart))

		if stop := s.hintTicks(ctx, cfg, cfgFn, runner, gates.admission); stop {
			return
		}
		// Disabled mid-flight: park until re-enabled (gates already passed).
		// A parked loop retries nothing, so it holds no latch.
		s.dropHintLatch(ctx)
		if cfg, ok = s.waitUntilEnabled(ctx, cfgFn, gates.poll, true); !ok {
			return
		}
	}
}

// hintTicks runs ticks until the flag disables the loop (returns false) or the
// loop must end (returns true: context gone, process gone, or the balloon
// cannot hint). The disable itself is recorded by waitUntilEnabled on
// re-entry, once. A guest that sits out cfg.SilentRuns runs since its last
// completed one is latched unresponsive and attempted only every
// cfg.UnresponsiveRetry until a run completes; the loop keeps ticking at the
// interval in between so a flag change still lands within one interval. Slow
// guests and API errors neither count nor reset.
func (s *Sandbox) hintTicks(ctx context.Context, cfg featureflags.PeriodicHintingConfig, cfgFn func() featureflags.PeriodicHintingConfig, runner hintRunner, adm *hintAdmission) (stopLoop bool) {
	// Jitter the first tick so a burst of resumes does not hint in lockstep.
	timer := time.NewTimer(cfg.Interval + rand.N(max(cfg.Interval/2, 1))) //nolint:gosec // scheduling jitter, not security
	defer timer.Stop()

	// Every tick records the interval since the previous one on the run
	// record: under the last attempt's outcome when there was one, otherwise
	// under what the previous tick did, which is the baseline an attempt is
	// read against (an observe_only sandbox only ever records that baseline).
	var pending *hintOutcome
	var prevTick *userfaultfd.ServeSnapshot
	prevLabel := ""
	silent := 0
	var retryAt time.Time
	for {
		select {
		case <-ctx.Done():
			return true
		case <-timer.C:
		}
		timer.Reset(cfg.Interval)

		if cfg = cfgFn(); !cfg.Enabled() {
			return false
		}

		now := time.Now()
		in := s.hintDecisionInput(now)
		cur := s.Resources.memory.ServeStats()
		switch {
		case pending != nil:
			s.recordRunFaults(ctx, hintWindowInterval, pending.outcome, pending.after, cur)
			pending = nil
		case prevTick != nil:
			s.recordRunFaults(ctx, hintWindowInterval, prevLabel, *prevTick, cur)
		}
		prevTick, prevLabel = &cur, hintLabelIdle
		if s.hintUnresponsive.Load() && now.Before(retryAt) {
			prevLabel = hintLabelBackoff

			continue
		}
		if reason := hintSkipReason(cfg, in); reason != "" {
			// A skipped tick spends no retry budget: the next tick may attempt.
			s.recordPeriodicRun(ctx, "skipped-"+reason, 0)
			prevLabel = "skipped-" + reason

			continue
		}
		// Gated exactly like a treated sandbox, so the control's baseline holds
		// only the ticks that would have run.
		if cfg.ObserveOnly {
			s.recordPeriodicRun(ctx, "skipped-"+hintLabelObserveOnly, 0)
			prevLabel = hintLabelObserveOnly

			continue
		}
		out := s.hintOnce(ctx, cfg, runner, adm)
		switch {
		case out.outcome != "":
			pending, prevTick = &out, nil
		case out.label != "":
			prevLabel = out.label
		}
		res := out.res
		switch res {
		case hintExit:
			return true
		case hintAborted:
			if runner.Exited() {
				return true
			}
		case hintOK:
			silent = 0
			s.hintWarned.Store(false)
			if s.hintUnresponsive.CompareAndSwap(true, false) {
				s.recordHintLatch(ctx, "released")
				sbxlogger.I(s).Info(ctx, "periodic free-page hinting: guest engaged again")
			}
		case hintGuestSilent:
			if silent++; cfg.SilentRuns > 0 && silent >= cfg.SilentRuns && s.hintUnresponsive.CompareAndSwap(false, true) {
				s.recordHintLatch(ctx, "latched")
				sbxlogger.I(s).Warn(ctx, "periodic free-page hinting: guest never engages; backing off", zap.Int("runs", silent), zap.Duration("retry", cfg.UnresponsiveRetry))
			}
		case hintFailed:
		}
		// Only an attempt on a latched guest (the latching one included) arms
		// the next retry; an aborted tick (host busy, cancel) attempted nothing.
		if res != hintAborted && s.hintUnresponsive.Load() {
			retryAt = time.Now().Add(max(cfg.UnresponsiveRetry, cfg.Interval))
		}
	}
}

// dropHintLatch forgets the latch when the loop that owned it parks or ends;
// recorded so latched minus released does not count guests nobody is
// retrying.
func (s *Sandbox) dropHintLatch(ctx context.Context) {
	s.hintWarned.Store(false)
	if s.hintUnresponsive.CompareAndSwap(true, false) {
		s.recordHintLatch(ctx, "dropped")
	}
}

const (
	hintLabelIdle        = "idle"
	hintLabelBackoff     = "skipped-backoff"
	hintLabelObserveOnly = "observe-only"
)

// hintOutcome is one tick's attempt: the loop's verdict, the run outcome as
// recorded (empty when no cycle was attempted) and the serve snapshot at the
// end of the attempt, the start of the interval window that follows.
type hintOutcome struct {
	res     hintResult
	outcome string
	after   userfaultfd.ServeSnapshot
	// label names a tick that attempted nothing for the interval baseline
	// (skipped-host-busy); empty for an attempt or a loop exit.
	label string
}

// hintOnce runs one cycle under fphMu. hintExit means the loop must end (the
// balloon cannot hint, or the context was cancelled before the cycle began).
// A cycle that may still be running when the drain returns is stopped before
// the mutex is released, so Pause's barrier on fphMu is enough to guarantee
// no cycle is in flight. Exactly one outcome is recorded per run.
func (s *Sandbox) hintOnce(ctx context.Context, cfg featureflags.PeriodicHintingConfig, runner hintRunner, adm *hintAdmission) hintOutcome {
	s.fphMu.Lock()
	defer s.fphMu.Unlock()

	if ctx.Err() != nil {
		// Cancelled while waiting for the mutex (Pause won the race): do not
		// start a cycle the pause path would then have to stop.
		return hintOutcome{res: hintExit}
	}
	if !adm.tryAcquire(cfg.HostSlots) {
		s.recordPeriodicRun(ctx, "skipped-host-busy", 0)

		return hintOutcome{res: hintAborted, label: "skipped-host-busy"}
	}
	defer adm.release()

	before := s.Resources.memory.ServeStats()
	// attempt closes a cycle that ran, or started and was stopped: the run
	// outcome and the run window's faults go on the record together.
	attempt := func(outcome string, took time.Duration, res hintResult) hintOutcome {
		after := s.Resources.memory.ServeStats()
		s.recordPeriodicRun(ctx, outcome, took)
		s.recordRunFaults(ctx, hintWindowRun, outcome, before, after)

		return hintOutcome{res: res, outcome: outcome, after: after}
	}

	runCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	start := time.Now()
	freedBefore := runner.HintFreedBytes()
	err := runner.DrainBalloon(runCtx)
	took := time.Since(start)
	cancel()

	switch {
	case err == nil:
		out := attempt("ok", took, hintOK)
		s.recordHintFreed(ctx, hintPhasePeriodic, freedBefore, s.hintFreedAfter(ctx, runner))

		return out
	case errors.Is(err, fc.ErrHintingNotConfigured):
		s.recordPeriodicRun(ctx, "skipped-not-configured", 0)

		return hintOutcome{res: hintExit}
	case errors.Is(err, fc.ErrHintingInFlight):
		s.stopAbandonedHint(ctx, cfg.Stop, runner)
		switch {
		case s.stopping.Load() || runner.Exited() || errors.Is(err, context.Canceled):
			// Stopped with the sandbox, or the guest went away: nothing to record.
			return hintOutcome{res: hintAborted}
		case errors.Is(err, fc.ErrHintingGuestSilent):
			out := attempt("timeout-guest-silent", took, hintGuestSilent)
			s.hintLog(ctx, "periodic free-page hinting run timed out without the guest engaging; cycle stopped", zap.Duration("timeout", cfg.Timeout))

			return out
		case errors.Is(err, context.DeadlineExceeded):
			out := attempt("timeout", took, hintFailed)
			s.hintLog(ctx, "periodic free-page hinting run timed out; cycle stopped", zap.Duration("timeout", cfg.Timeout))

			return out
		default:
			out := attempt("failed", took, hintFailed)
			s.hintLog(ctx, "periodic free-page hinting run failed after start; cycle stopped", zap.Error(err))

			return out
		}
	case s.stopping.Load() || runner.Exited():
		// The start failed because the sandbox is going away: not a hinting
		// failure. The socket dies before the exit is reaped.
		return hintOutcome{res: hintAborted}
	case errors.Is(err, fc.ErrHintingStartRefused) && !errors.Is(err, fc.ErrHintingConfigUnknown):
		// FC would not start it: the guest has no balloon driver, or has not
		// activated it yet. Nothing to stop.
		out := attempt("refused", took, hintGuestSilent)
		s.hintLog(ctx, "periodic free-page hinting start refused", zap.Error(err))

		return out
	default:
		// Failed before the request landed.
		out := attempt("failed", took, hintFailed)
		s.hintLog(ctx, "periodic free-page hinting run failed", zap.Error(err))

		return out
	}
}

// hintLog is Warn for the first failure after a completed run and Debug for
// the repeats: the state change is the alert, the attempts behind it are not.
func (s *Sandbox) hintLog(ctx context.Context, msg string, fields ...zap.Field) {
	if s.hintUnresponsive.Load() || !s.hintWarned.CompareAndSwap(false, true) {
		sbxlogger.I(s).Debug(ctx, msg, fields...)

		return
	}
	sbxlogger.I(s).Warn(ctx, msg, fields...)
}

func (s *Sandbox) recordHintLatch(ctx context.Context, state string) {
	fphLatchCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("state", state)))
	if s.fphObserve != nil {
		s.fphObserve("latch:"+state, 0)
	}
}
