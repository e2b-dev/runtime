//go:build linux

package sandbox

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	fphRunCounter      = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorFPHRunCounterName))
	fphStopCounter     = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorFPHStopCounterName))
	fphFreedCounter    = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorFPHFreedCounterName))
	fphRunDurationHis  = utils.Must(telemetry.GetHistogram(meter, telemetry.FPHRunDurationName))
	fphStopDurationHis = utils.Must(telemetry.GetHistogram(meter, telemetry.FPHStopDurationName))
	fphRunFaultsHis    = utils.Must(telemetry.GetHistogram(meter, telemetry.FPHRunFaultsName))
)

const hintPhasePrePause = "pre-pause"

// hintRunner is the FC surface a hinting drain needs; *fc.Process satisfies it.
type hintRunner interface {
	DrainBalloon(ctx context.Context) error
	StopBalloonHinting(ctx context.Context, ackGrace time.Duration) error
	Exited() bool
	HintFreedBytes() uint64
	FlushHintFreedBytes(ctx context.Context) (uint64, error)
}

// hintFreedReadTimeout bounds the metrics flush that attributes what a
// completed drain freed; a flush that does not land in time costs the record,
// not the drain.
const hintFreedReadTimeout = 500 * time.Millisecond

// prePauseHintDrain is Pause's synchronous hinting run before the snapshot,
// bounded by cfg.Timeout. It never fails the pause: a balloon without hinting
// is a no-op, a transient refusal a warning, anything else an error report. A
// cycle that outlives the budget is stopped before the snapshot, because FC
// restarts a cycle on a second start rather than refusing it and a guest still
// hinting would discard into the snapshot. A request that is already gone
// starts nothing. It shares fphMu with the periodic loop, so it waits for a
// run in flight instead of overlapping it.
func (s *Sandbox) prePauseHintDrain(ctx context.Context, cfg featureflags.PrePauseHintConfig, runner hintRunner) {
	if ctx.Err() != nil {
		return
	}
	s.fphMu.Lock()
	defer s.fphMu.Unlock()

	// The budget starts once the drain owns the device: a wait behind a run in
	// flight is not the drain's time.
	drainCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	start := time.Now()
	freedBefore := runner.HintFreedBytes()
	err := runner.DrainBalloon(drainCtx)
	took := time.Since(start)
	switch {
	case err == nil:
		s.recordHintRun(ctx, hintPhasePrePause, "ok", took)
		s.recordHintFreed(ctx, hintPhasePrePause, freedBefore, s.hintFreedAfter(ctx, runner))
	case errors.Is(err, fc.ErrHintingNotConfigured):
		s.recordHintRun(ctx, hintPhasePrePause, "not-configured", 0)
	case errors.Is(err, fc.ErrHintingInFlight) && errors.Is(err, context.Canceled):
		// The caller went away mid-drain: stop the cycle, do not alert.
		s.stopAbandonedHint(ctx, cfg.Stop, runner)
		s.recordHintRun(ctx, hintPhasePrePause, "cancelled", took)
		sbxlogger.I(s).Warn(ctx, "balloon hinting drain cancelled before pause; cycle stopped", zap.Error(err))
	case errors.Is(err, fc.ErrHintingInFlight):
		s.stopAbandonedHint(ctx, cfg.Stop, runner)
		s.recordHintRun(ctx, hintPhasePrePause, inFlightOutcome(err), took)
		telemetry.ReportError(ctx, "balloon hinting drain did not finish before pause; cycle stopped", err)
	case errors.Is(err, fc.ErrHintingStartRefused):
		s.recordHintRun(ctx, hintPhasePrePause, "refused", took)
		sbxlogger.I(s).Warn(ctx, "balloon hinting drain refused by FC (continuing pause)", zap.Error(err))
	default:
		s.recordHintRun(ctx, hintPhasePrePause, "failed", took)
		telemetry.ReportError(ctx, "balloon hinting drain failed (continuing pause)", err)
	}
}

// inFlightOutcome names an ErrHintingInFlight: a guest that never engaged, a
// cycle that outlived its budget, or a status read that failed.
func inFlightOutcome(err error) string {
	switch {
	case errors.Is(err, fc.ErrHintingGuestSilent):
		return "timeout-guest-silent"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	}

	return "failed"
}

// stopAbandonedHint ends a hinting cycle the host stopped waiting for; the
// guest keeps hinting otherwise. A sandbox that is being stopped, or a process
// that has exited, takes its cycle with it; a crash mid-cycle kills the API
// before the exit is reaped; a stop the guest did not acknowledge in time
// still landed in FC. A zero stop timeout leaves the cycle running, as before
// the stop existed.
func (s *Sandbox) stopAbandonedHint(ctx context.Context, stop featureflags.HintStopConfig, runner hintRunner) {
	ctx, span := tracer.Start(ctx, "stop abandoned hinting cycle")
	defer span.End()

	if s.stopping.Load() {
		s.recordHintStop(ctx, "skipped-stopping", 0)

		return
	}
	if runner.Exited() {
		s.recordHintStop(ctx, "skipped-exited", 0)

		return
	}
	if stop.Timeout <= 0 {
		s.recordHintStop(ctx, "skipped-disabled", 0)
		sbxlogger.I(s).Warn(ctx, "balloon hinting: cycle left running, stop disabled by flag")

		return
	}
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stop.Timeout)
	defer cancel()
	start := time.Now()
	err := runner.StopBalloonHinting(stopCtx, stop.Grace)
	took := time.Since(start)
	if err != nil {
		// Give the reaper a moment before blaming the stop.
		for i := 0; i < 5 && !runner.Exited(); i++ {
			if !sleepOrDone(ctx, 20*time.Millisecond) {
				break
			}
		}
	}
	switch {
	case err == nil:
		s.recordHintStop(ctx, "ok", took)
	case s.stopping.Load():
		s.recordHintStop(ctx, "skipped-stopping", took)
	case runner.Exited():
		s.recordHintStop(ctx, "skipped-exited", took)
	case errors.Is(err, fc.ErrHintingStopUnacked):
		s.recordHintStop(ctx, "unacked", took)
		sbxlogger.I(s).Warn(ctx, "balloon hinting: guest did not acknowledge the stop in time", zap.Error(err))
	default:
		s.recordHintStop(ctx, "failed", took)
		telemetry.ReportError(ctx, "balloon hinting: stopping an abandoned cycle failed", err)
	}
}

// sleepOrDone waits d or until ctx ends; false means ctx ended.
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(max(d, time.Millisecond))
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// recordHintRun is the one place a hinting drain or run lands on the run
// counter; the phase tells the pre-pause drain and the periodic loop apart.
func (s *Sandbox) recordHintRun(ctx context.Context, phase, outcome string, took time.Duration) {
	fphRunCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("phase", phase), attribute.String("outcome", outcome)))
	if outcome == "ok" {
		fphRunDurationHis.Record(ctx, took.Milliseconds(), metric.WithAttributes(attribute.String("phase", phase)))
	}
	if s.fphObserve != nil {
		s.fphObserve(phase+":"+outcome, took)
	}
}

func (s *Sandbox) recordHintStop(ctx context.Context, outcome string, took time.Duration) {
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("outcome", outcome))
	fphStopCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
	if outcome == "ok" || outcome == "unacked" {
		fphStopDurationHis.Record(ctx, took.Milliseconds())
	}
	if s.fphObserve != nil {
		s.fphObserve("stop:"+outcome, took)
	}
}

// hintFreedAfter reads FC's freed counter after a completed drain, flushing
// first so the drain's own discards are in it; a flush that fails or does not
// land in time falls back to the last observed value.
func (s *Sandbox) hintFreedAfter(ctx context.Context, runner hintRunner) uint64 {
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), hintFreedReadTimeout)
	defer cancel()
	after, err := runner.FlushHintFreedBytes(flushCtx)
	if err != nil {
		sbxlogger.I(s).Debug(ctx, "balloon hinting: freed-bytes flush did not land; recording the last observed value", zap.Error(err))

		return runner.HintFreedBytes()
	}

	return after
}

// recordHintFreed adds what FC discarded across a completed drain, as the
// difference of the cumulative counter before and after it.
const (
	hintWindowRun      = "run"
	hintWindowInterval = "interval"
)

// recordRunFaults puts what the guest faulted through the serve loop on the
// run record: across an attempt itself (window=run, under its outcome), and
// over the interval that followed a tick (window=interval, under the attempt's
// outcome or, for a tick that attempted nothing, the reason it did not: the
// baseline). The re-faults a run caused are the interval after it.
func (s *Sandbox) recordRunFaults(ctx context.Context, window, outcome string, before, after userfaultfd.ServeSnapshot) {
	for _, d := range [...]struct {
		kind string
		n    int64
	}{
		{"served", after.Pages - before.Pages},
		{"deferred", after.Deferred - before.Deferred},
		{"wp", after.WPFaults - before.WPFaults},
		{"wp_deferred", after.WPDeferred - before.WPDeferred},
	} {
		fphRunFaultsHis.Record(ctx, d.n, metric.WithAttributes(attribute.String("kind", d.kind), attribute.String("window", window), attribute.String("outcome", outcome)))
		if s.fphObserveFaults != nil {
			s.fphObserveFaults(d.kind, window, outcome, d.n)
		}
	}
}

func (s *Sandbox) recordHintFreed(ctx context.Context, phase string, before, after uint64) {
	if after <= before {
		return
	}
	fphFreedCounter.Add(ctx, int64(after-before), metric.WithAttributes(attribute.String("phase", phase))) //nolint:gosec // bounded by guest memory
	if s.fphObserveFreed != nil {
		s.fphObserveFreed(phase, after-before)
	}
}
