//go:build linux

package sandbox

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	memoryResizeCounter          = utils.Must(telemetry.GetCounter(meter, telemetry.SandboxMemoryResize))
	memoryResizeWaitHistogram    = utils.Must(telemetry.GetHistogram(meter, telemetry.SandboxMemoryResizeWaitHistogramName))
	memoryResizePlugHistogram    = utils.Must(telemetry.GetHistogram(meter, telemetry.SandboxMemoryResizePlugHistogramName))
	memoryResizeConfirmHistogram = utils.Must(telemetry.GetHistogram(meter, telemetry.SandboxMemoryResizeConfirmHistogramName))
)

// memoryHotplugOps is the Firecracker side of the hotplug region a sandbox reads and resizes.
type memoryHotplugOps interface {
	MemoryHotplugStatus(ctx context.Context) (fc.MemoryHotplugStatus, error)
	RequestMemoryHotplug(ctx context.Context, requestedMB int64) error
}

// memoryHotplug is the hotplug side of this sandbox's Firecracker process.
func (s *Sandbox) memoryHotplug() memoryHotplugOps {
	if s.hotplugOps != nil {
		return s.hotplugOps
	}

	return s.process
}

// How far confirming a start's change to the plugged memory got. The first four settle it: nothing
// later changes them, and no confirmation goes on after them.
const (
	// memoryResizeConfirmed: Firecracker reports the request plugged, and envd reports the memory in
	// use, unpending, with the plugged memory onlined as movable.
	memoryResizeConfirmed = "confirmed"
	// memoryResizeEnvdUnsupported: envd predates the memory limit report or its layout, or the
	// start skipped envd; only Firecracker confirmed.
	memoryResizeEnvdUnsupported = "envd_unsupported"
	// memoryResizeFirecrackerError: Firecracker refused a request or a status read with an error
	// retrying cannot change.
	memoryResizeFirecrackerError = "firecracker_error"
	// memoryResizeAborted: the sandbox stopped (killed, paused away or exited) or the start ended
	// before confirmation settled. Not a failure of the resize.
	memoryResizeAborted = "aborted"

	// memoryResizePlugTimeout: the guest had not plugged the requested size yet.
	memoryResizePlugTimeout = "plug_timeout"
	// memoryResizeDeviceInactive: Firecracker refused a cold boot's request because the guest driver
	// had not activated the device yet.
	memoryResizeDeviceInactive = "device_inactive"
	// memoryResizeFirecrackerUnavailable: a request or status read failed in a way that may pass.
	memoryResizeFirecrackerUnavailable = "firecracker_unavailable"
	// memoryResizeEnvdUnconfirmed: no /init of the confirmation answered with a reading yet.
	memoryResizeEnvdUnconfirmed = "envd_unconfirmed"
	// memoryResizeEnvdMismatch: envd answered, but its limit is pending or its memory is short.
	memoryResizeEnvdMismatch = "envd_mismatch"
)

// Retry pacing. Within the start's bounded wait Firecracker's status, a local API read, is polled
// at the memory-resize-poll-milliseconds interval, kept within memoryResizePollFloor and
// memoryResizePollCeiling so a bad value can neither busy-loop nor stall the wait. Once the request
// is plugged only envd is left to ask, and the next step waits for envd's own retry rather than the
// status poll. An /init is sent again only after the previous one answered without confirming, and
// the first answer after a resume's plug usually does, its limit still pending while envd catches
// up with the memory just onlined; so the start re-asks after 2 ms and doubles the pause to a 20 ms
// cap, which confirms a typical resume on its second /init while keeping a guest that never
// confirms to a few dozen /inits over the wait. In the background both back off exponentially to
// longer caps.
const (
	memoryResizePollFloor    = time.Millisecond
	memoryResizePollCeiling  = 100 * time.Millisecond
	memoryResizeEnvdRetry    = 2 * time.Millisecond
	memoryResizeEnvdRetryCap = 20 * time.Millisecond

	memoryResizeBackgroundPoll    = 50 * time.Millisecond
	memoryResizeBackgroundPollCap = 5 * time.Second
	memoryResizeBackgroundEnvd    = 200 * time.Millisecond
	memoryResizeBackgroundEnvdCap = 10 * time.Second
)

// memoryResizeBackgroundBudget bounds the confirmation that goes on after a start returned.
const memoryResizeBackgroundBudget = time.Minute

// memoryResizeDeadlineMargin is the part of the start's deadline the bounded wait leaves to what
// follows envd, making the sandbox live, so a slow resize cannot fail the start.
const memoryResizeDeadlineMargin = time.Second

// memoryResize is a start's change to the memory plugged into the hotplug region, and how far
// confirming it got. The start claims it once (completeMemoryResize), and its bounded wait and then
// its background continuation own it alone, one after the other.
type memoryResize struct {
	requestedMB int64
	// coldBoot starts with nothing plugged and can send its request only once the guest driver
	// is up; a resume sent its target before the vCPUs ran.
	coldBoot bool
	// background is set once the start returned and the confirmation goes on without it.
	background bool
	// withoutEnvd is set for a start that skipped envd: Firecracker's report of the plug settles
	// the confirmation.
	withoutEnvd bool
	// statusPoll is how often the start's bounded wait reads Firecracker's status; 0 is the flag's
	// default.
	statusPoll time.Duration
	// handoff is the start's end while its bounded wait runs: the confirmation then goes on in
	// the background, as when the wait runs out. nil in the background.
	handoff <-chan struct{}

	sent    bool
	plugged bool
	// reinit is an envd /init the confirmation sent and has not read yet, and reinitAfter the
	// reading number before it was sent.
	reinit      *envdReinit
	reinitAfter uint64
	nextReinit  time.Time
	// checkedReading is set once the reading the start's own /init left was looked at.
	checkedReading bool

	pollDelay time.Duration
	envdDelay time.Duration

	// What the confirmation saw, for its logs, spans and histograms: the last error, Firecracker's
	// last status, the last envd reading it judged, whether the start's own reading settled it,
	// when it began and when Firecracker reported the request plugged.
	lastErr     error
	lastStatus  fc.MemoryHotplugStatus
	lastReading *envdMemoryReading
	initSkipped bool
	began       time.Time
	pluggedAt   time.Time
	// span is the current stage's span; lastRetry is the last retry recorded on it, so a poll that
	// keeps finding the same answer records it once.
	span      trace.Span
	lastRetry string
}

// event records name on the current stage's span.
func (r *memoryResize) event(name string, attrs ...attribute.KeyValue) {
	if r.span != nil {
		r.span.AddEvent(name, trace.WithAttributes(attrs...))
	}
}

// retry records an unsettled step on the span when its outcome or error changed since the last one.
func (r *memoryResize) retry(outcome string, err error) {
	key := outcome
	attrs := []attribute.KeyValue{attribute.String("outcome", outcome)}
	if err != nil {
		key += ": " + err.Error()
		attrs = append(attrs, attribute.String("error", err.Error()))
		if fault := fc.MemoryHotplugFaultMessage(err); fault != "" {
			attrs = append(attrs, attribute.String("firecracker_fault", fault))
		}
	}
	if key == r.lastRetry {
		return
	}
	r.lastRetry = key
	r.event("retry", attrs...)
}

// pendingOutcome is where a confirmation stands before its next step: the request not plugged
// yet, or plugged and envd's report still due.
func (r *memoryResize) pendingOutcome() string {
	if r.plugged {
		return memoryResizeEnvdUnconfirmed
	}

	return memoryResizePlugTimeout
}

// pollInterval is how long to wait before the next step, and advances the background backoff.
func (r *memoryResize) pollInterval() time.Duration {
	if !r.background {
		if r.statusPoll > 0 {
			return r.statusPoll
		}

		return memoryResizePoll(featureflags.MemoryResizePollMilliseconds.Fallback())
	}
	r.pollDelay = backoff(r.pollDelay, memoryResizeBackgroundPoll, memoryResizeBackgroundPollCap)

	return r.pollDelay
}

// envdInterval is how long to wait before the next /init, and advances the background backoff.
func (r *memoryResize) envdInterval() time.Duration {
	if !r.background {
		r.envdDelay = backoff(r.envdDelay, memoryResizeEnvdRetry, memoryResizeEnvdRetryCap)

		return r.envdDelay
	}
	r.envdDelay = backoff(r.envdDelay, memoryResizeBackgroundEnvd, memoryResizeBackgroundEnvdCap)

	return r.envdDelay
}

// nextStepIn is how long to wait before the next step: until the next /init is due once the request
// is plugged and one is scheduled, or the status poll otherwise.
func (r *memoryResize) nextStepIn() time.Duration {
	if r.plugged {
		if due := time.Until(r.nextReinit); due > 0 {
			return due
		}
	}

	return r.pollInterval()
}

// backoff doubles prev from base up to limit.
func backoff(prev, base, limit time.Duration) time.Duration {
	if prev == 0 {
		return base
	}

	return min(2*prev, limit)
}

// completeMemoryResize finishes a start that changed its plugged memory, once envd answered. A cold
// boot sends its request now. Then, within one bounded wait, it waits for Firecracker to report the
// requested memory plugged and for envd to report it in use: its limit following the new memory
// and the plugged memory onlined as movable. A shortfall never fails or holds the start: it is
// logged and counted, and confirmation goes on in the background until it settles, the sandbox
// stops, or memoryResizeBackgroundBudget runs out. A start that kept its size waits for nothing.
//
// Completing claims the resize, so it runs once per lifecycle: a ResumeVM of a sandbox already
// running, as a retained capture's recovery runs it, finds nothing to complete and cannot start a
// second confirmation beside a background one.
//
// The start's context bounds the wait and never settles it: the wait ends
// memoryResizeDeadlineMargin before the start's deadline, so making the sandbox live keeps its
// time, and a start that ends sooner hands the confirmation to the background as an expired wait
// does. Only the sandbox stopping aborts it.
func (s *Sandbox) completeMemoryResize(ctx context.Context, startType StartType) {
	r := s.memoryResize.Swap(nil)
	if r == nil {
		return
	}

	ctx, span := tracer.Start(ctx, "memory_resize", trace.WithAttributes(s.memoryResizeAttrs(r, startType)...))
	defer span.End()
	r.span = span
	r.began = time.Now()
	r.statusPoll = s.memoryResizePollInterval(ctx)
	until := r.began.Add(s.memoryResizeWait(ctx))
	if deadline, ok := ctx.Deadline(); ok && deadline.Add(-memoryResizeDeadlineMargin).Before(until) {
		until = deadline.Add(-memoryResizeDeadlineMargin)
	}
	outcome, settled := r.pendingOutcome(), false
	if r.began.Before(until) {
		r.handoff = ctx.Done()
		outcome, settled = s.driveMemoryResize(context.WithoutCancel(ctx), r, startType, until)
		r.handoff = nil
	}
	span.SetAttributes(attribute.String("outcome", outcome), attribute.Bool("settled", settled))
	s.recordMemoryResizeWait(ctx, r, startType, outcome)
	s.countMemoryResize(ctx, outcome, startType, "start")
	if settled {
		s.logSettledMemoryResize(ctx, r, startType, outcome)

		return
	}

	s.log().Warn(ctx, "start returned before its plugged memory was confirmed, confirming in the background",
		append(s.memoryResizeFields(r, startType, outcome), zap.Duration("waited", time.Since(r.began)))...)
	r.background = true
	r.envdDelay = 0
	go s.continueMemoryResize(context.WithoutCancel(ctx), r, startType, span.SpanContext()) //nolint:contextcheck // outlives the start; bounded by the sandbox and memoryResizeBackgroundBudget
}

// completeMemoryResizeWithoutEnvd finishes a start whose envd wait was skipped (a bare boot, or one
// held by a debugger). No envd answer says the guest driver is up, so the whole confirmation runs
// in the background, where a cold boot's request is retried while the device is not active, and
// with no envd to ask, Firecracker's report of the plug settles it.
func (s *Sandbox) completeMemoryResizeWithoutEnvd(ctx context.Context, startType StartType) {
	r := s.memoryResize.Swap(nil)
	if r == nil {
		return
	}
	r.background = true
	r.withoutEnvd = true
	r.began = time.Now()
	go s.continueMemoryResize(context.WithoutCancel(ctx), r, startType, trace.SpanContextFromContext(ctx)) //nolint:contextcheck // outlives the start; bounded by the sandbox and memoryResizeBackgroundBudget
}

// continueMemoryResize carries a confirmation past its start until it settles, the sandbox's
// Firecracker exits, or memoryResizeBackgroundBudget runs out, and counts its final outcome. Its span
// starts a trace of its own, linked to the start's.
func (s *Sandbox) continueMemoryResize(ctx context.Context, r *memoryResize, startType StartType, start trace.SpanContext) {
	ctx, span := tracer.Start(ctx, "memory_resize_background", trace.WithNewRoot(),
		trace.WithLinks(trace.Link{SpanContext: start}), trace.WithAttributes(s.memoryResizeAttrs(r, startType)...))
	defer span.End()
	r.span = span
	r.lastRetry = ""
	outcome, settled := s.driveMemoryResize(ctx, r, startType, time.Now().Add(memoryResizeBackgroundBudget))
	span.SetAttributes(attribute.String("outcome", outcome), attribute.Bool("settled", settled))
	s.countMemoryResize(ctx, outcome, startType, "background")
	if !settled {
		s.log().Warn(ctx, "plugged memory was never confirmed", s.memoryResizeFields(r, startType, outcome)...)

		return
	}
	s.logSettledMemoryResize(ctx, r, startType, outcome)
}

// memoryResizeAttrs describe a resize on its spans.
func (s *Sandbox) memoryResizeAttrs(r *memoryResize, startType StartType) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int64("requested_mb", r.requestedMB),
		attribute.Int64("region_mb", s.memoryHotplugMB),
		attribute.Int64("boot_ram_mb", s.Config.RamMB),
		attribute.String("start_type", string(startType)),
	}
}

// recordMemoryResizeWait records the start's wait, whole and split at the plug: until Firecracker
// reported the request plugged (or the start gave up), and from then until the start ended.
func (s *Sandbox) recordMemoryResizeWait(ctx context.Context, r *memoryResize, startType StartType, outcome string) {
	attrs := metric.WithAttributes(attribute.String("outcome", outcome), attribute.String("start_type", string(startType)))
	now := time.Now()
	memoryResizeWaitHistogram.Record(ctx, now.Sub(r.began).Milliseconds(), attrs)
	if r.pluggedAt.IsZero() {
		memoryResizePlugHistogram.Record(ctx, now.Sub(r.began).Milliseconds(), attrs)

		return
	}
	memoryResizePlugHistogram.Record(ctx, r.pluggedAt.Sub(r.began).Milliseconds(), attrs)
	memoryResizeConfirmHistogram.Record(ctx, now.Sub(r.pluggedAt).Milliseconds(), attrs)
}

// logSettledMemoryResize logs a resize that settled: one Info line when it was confirmed, with or
// without envd's report, and a warning when Firecracker refused it for good.
func (s *Sandbox) logSettledMemoryResize(ctx context.Context, r *memoryResize, startType StartType, outcome string) {
	switch outcome {
	case memoryResizeConfirmed, memoryResizeEnvdUnsupported:
		now := time.Now()
		plugMS, confirmMS := now.Sub(r.began).Milliseconds(), int64(0)
		if !r.pluggedAt.IsZero() {
			plugMS, confirmMS = r.pluggedAt.Sub(r.began).Milliseconds(), now.Sub(r.pluggedAt).Milliseconds()
		}
		s.log().Info(ctx, "sandbox memory resize confirmed",
			zap.String("outcome", outcome),
			zap.String("start_type", string(startType)),
			zap.Int64("requested_mb", r.requestedMB),
			zap.Int64("plugged_mb", r.lastStatus.PluggedMB),
			zap.Int64("plug_ms", plugMS),
			zap.Int64("confirm_ms", confirmMS),
			zap.Int64("total_ms", now.Sub(r.began).Milliseconds()),
			zap.Bool("init_skipped", r.initSkipped))
	case memoryResizeFirecrackerError:
		s.log().Warn(ctx, "Firecracker refused the start's memory resize for good", s.memoryResizeFields(r, startType, outcome)...)
	}
}

// memoryResizeFields are what a resize's warnings carry: the request and the region, Firecracker's
// last status, the last error with Firecracker's fault message, and envd's last report against what
// confirmation expected of it.
func (s *Sandbox) memoryResizeFields(r *memoryResize, startType StartType, outcome string) []zap.Field {
	fields := []zap.Field{
		zap.String("outcome", outcome),
		zap.String("start_type", string(startType)),
		zap.Int64("requested_mb", r.requestedMB),
		zap.Int64("region_mb", s.memoryHotplugMB),
		zap.Int64("boot_ram_mb", s.Config.RamMB),
		zap.Int64("firecracker_plugged_mb", r.lastStatus.PluggedMB),
		zap.Int64("firecracker_requested_mb", r.lastStatus.RequestedMB),
	}
	if r.lastErr != nil {
		fields = append(fields, zap.Error(r.lastErr))
		if fault := fc.MemoryHotplugFaultMessage(r.lastErr); fault != "" {
			fields = append(fields, zap.String("firecracker_fault", fault))
		}
	}
	expect := expectPlug(s.Config.RamMB, r.requestedMB)
	fields = append(fields,
		zap.Int64("expected_mem_total_min", expect.memTotalMin),
		zap.Int64("mem_total_tolerance", expect.allowance),
		zap.Int64("expected_movable_min", expect.movableMin))
	if rd := r.lastReading; rd != nil && rd.report != nil && rd.report.Limit != nil {
		fields = append(fields,
			zap.Uint64("envd_mem_total", rd.report.Limit.MemTotal),
			zap.Uint64("envd_applied", rd.report.Limit.Applied),
			zap.Bool("envd_pending", rd.report.Limit.Pending))
		if rd.report.Limit.Error != "" {
			fields = append(fields, zap.String("envd_error", rd.report.Limit.Error))
		}
		if layout := rd.report.Layout; layout != nil {
			fields = append(fields, zap.Uint64("envd_movable", layout.Movable), zap.Uint64("envd_kernel", layout.Kernel))
		}
	}

	return fields
}

// readingAttrs are envd's report on a span event.
func readingAttrs(rd *envdMemoryReading) []attribute.KeyValue {
	if rd == nil || rd.report == nil || rd.report.Limit == nil {
		return nil
	}
	attrs := []attribute.KeyValue{
		attribute.Int64("mem_total", int64(rd.report.Limit.MemTotal)),
		attribute.Int64("applied", int64(rd.report.Limit.Applied)),
		attribute.Bool("pending", rd.report.Limit.Pending),
	}
	if layout := rd.report.Layout; layout != nil {
		attrs = append(attrs, attribute.Int64("movable", int64(layout.Movable)), attribute.Int64("kernel", int64(layout.Kernel)))
	}

	return attrs
}

func (s *Sandbox) countMemoryResize(ctx context.Context, outcome string, startType StartType, stage string) {
	memoryResizeCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", outcome),
		attribute.String("start_type", string(startType)),
		attribute.String("stage", stage),
	))
}

// memoryResizeWait is the bounded wait of a start that changed its plugged memory.
func (s *Sandbox) memoryResizeWait(ctx context.Context) time.Duration {
	ms := featureflags.MemoryResizeWaitMilliseconds.Fallback()
	if s.featureFlags != nil {
		ms = s.featureFlags.IntFlag(ctx, featureflags.MemoryResizeWaitMilliseconds, sandboxLDContext(s.Runtime, s.Config))
	}

	return time.Duration(max(ms, 0)) * time.Millisecond
}

// memoryResizePollInterval is how often the start's bounded wait reads Firecracker's status.
func (s *Sandbox) memoryResizePollInterval(ctx context.Context) time.Duration {
	ms := featureflags.MemoryResizePollMilliseconds.Fallback()
	if s.featureFlags != nil {
		ms = s.featureFlags.IntFlag(ctx, featureflags.MemoryResizePollMilliseconds, sandboxLDContext(s.Runtime, s.Config))
	}

	return memoryResizePoll(ms)
}

// memoryResizePoll is a poll of ms milliseconds kept within memoryResizePollFloor and
// memoryResizePollCeiling; 0 or less is the flag's default.
func memoryResizePoll(ms int) time.Duration {
	if ms <= 0 {
		ms = featureflags.MemoryResizePollMilliseconds.Fallback()
	}

	return min(max(time.Duration(ms)*time.Millisecond, memoryResizePollFloor), memoryResizePollCeiling)
}

// processExited is the sandbox's Firecracker exit, nil without a process (in tests).
func (s *Sandbox) processExited() <-chan struct{} {
	if s.process == nil {
		return nil
	}

	return s.process.Exit.Done()
}

// driveMemoryResize advances the confirmation until it settles, the sandbox stops, ctx ends,
// until passes or the start hands it off (r.handoff), and returns how far it got. settled is true
// for an outcome no later step changes.
func (s *Sandbox) driveMemoryResize(ctx context.Context, r *memoryResize, startType StartType, until time.Time) (outcome string, settled bool) {
	exited := s.processExited()
	outcome = r.pendingOutcome()
	for {
		select {
		case <-exited:
			return memoryResizeAborted, true
		case <-r.handoff:
			return outcome, false
		default:
		}
		if ctx.Err() != nil {
			return memoryResizeAborted, true
		}
		outcome, settled = s.memoryResizeStep(ctx, r, startType, until)
		if settled {
			return outcome, true
		}
		wait := time.Until(until)
		if wait <= 0 {
			return outcome, false
		}
		select {
		case <-ctx.Done():
			return memoryResizeAborted, true
		case <-exited:
			return memoryResizeAborted, true
		case <-r.handoff:
			return outcome, false
		case <-time.After(min(wait, r.nextStepIn())):
		}
	}
}

// failed keeps a failed Firecracker call's error for the resize's logs and span and classes it.
func (r *memoryResize) failed(err error) (string, bool) {
	r.lastErr = err
	outcome, settled := firecrackerOutcome(err)
	if !settled {
		r.retry(outcome, err)
	}

	return outcome, settled
}

// firecrackerOutcome classes a failed Firecracker call: an inactive device and anything that may
// pass go on, a permanent refusal settles the confirmation.
func firecrackerOutcome(err error) (string, bool) {
	switch {
	case fc.MemoryHotplugDeviceNotActive(err):
		return memoryResizeDeviceInactive, false
	case fc.MemoryHotplugErrorPermanent(err):
		return memoryResizeFirecrackerError, true
	default:
		return memoryResizeFirecrackerUnavailable, false
	}
}

// memoryResizeStep takes the confirmation one step further: send a cold boot's request, read
// Firecracker's status until the request is plugged, then read envd's report.
//
// Every Firecracker call is bounded by until, the start's wait or the background budget, so a slow
// one cannot hold the stage past it.
func (s *Sandbox) memoryResizeStep(ctx context.Context, r *memoryResize, startType StartType, until time.Time) (string, bool) {
	ops := s.memoryHotplug()
	fcCtx, cancel := context.WithDeadline(ctx, until)
	defer cancel()
	if r.coldBoot && !r.sent {
		err := ops.RequestMemoryHotplug(fcCtx, r.requestedMB)
		if err != nil {
			return r.failed(err)
		}
		r.sent = true
		r.event("size_sent", attribute.Int64("requested_mb", r.requestedMB))
	}

	if !r.plugged {
		status, err := ops.MemoryHotplugStatus(fcCtx)
		if err != nil {
			return r.failed(err)
		}
		r.lastStatus = status
		s.lastPluggedMB.Store(status.PluggedMB)
		if status.PluggedMB != r.requestedMB {
			r.retry(memoryResizePlugTimeout, nil)

			return memoryResizePlugTimeout, false
		}
		r.plugged = true
		r.pluggedAt = time.Now()
		r.event("plugged", attribute.Int64("plugged_mb", status.PluggedMB),
			attribute.Int64("elapsed_ms", r.pluggedAt.Sub(r.began).Milliseconds()))
	}
	if r.withoutEnvd {
		r.event("envd_skipped")

		return memoryResizeEnvdUnsupported, true
	}

	outcome, settled := s.confirmEnvdMemory(ctx, r, startType, until)
	if !settled {
		r.retry(outcome, nil)
	}

	return outcome, settled
}

// confirmEnvdMemory reads envd's report of the plugged memory. The reading the start's own /init
// left is looked at first: if it already shows the plug, or shows an envd that cannot report it,
// that settles it with no /init. Otherwise it sends an /init after the plug and reads its answer:
// confirmed once the limit follows the plugged memory, is not pending, and the plugged memory came
// online movable.
//
// The background stage sends no /init until the sandbox went live: a resume's envd live upgrade runs
// between the start's return and the sandbox going live, and an /init sent across it would reach
// the envd being replaced. The start's own wait ends before the upgrade begins, so it may send one,
// and it cuts the one still unanswered when the wait ends.
func (s *Sandbox) confirmEnvdMemory(ctx context.Context, r *memoryResize, startType StartType, until time.Time) (string, bool) {
	if !r.checkedReading {
		r.checkedReading = true
		if outcome, settled := s.judgeEnvdMemoryReading(s.latestEnvdMemoryReading(), r, false); settled {
			r.initSkipped = true

			return outcome, true
		}
	}

	if r.reinit == nil {
		if r.background && !s.wentLive.Load() {
			return memoryResizeEnvdUnconfirmed, false
		}
		if time.Now().Before(r.nextReinit) {
			return memoryResizeEnvdMismatch, false
		}
		r.reinitAfter = s.envdMemoryReadings.Load()
		start := func(ctx context.Context) *envdReinit { return s.startMemoryConfirmReinit(ctx, startType) }
		if s.memoryReinit != nil {
			start = s.memoryReinit
		}
		r.reinit = start(ctx)
		if r.reinit == nil {
			// A Pause holds re-inits until its freeze landed.
			return memoryResizeEnvdUnconfirmed, false
		}
	}

	// An answer already in counts even when the wait is over.
	select {
	case <-r.reinit.done:
	default:
		timer := time.NewTimer(max(time.Until(until), 0))
		defer timer.Stop()
		select {
		case <-r.reinit.done:
		case <-ctx.Done():
			return memoryResizeAborted, true
		case <-r.handoff:
			r.reinit.replace()
			r.reinit = nil

			return memoryResizeEnvdUnconfirmed, false
		case <-timer.C:
			if !r.background {
				r.reinit.replace()
				r.reinit = nil
			}

			return memoryResizeEnvdUnconfirmed, false
		}
	}
	r.reinit = nil
	r.nextReinit = time.Now().Add(r.envdInterval())

	reading := s.latestEnvdMemoryReading()
	if reading == nil || reading.seq <= r.reinitAfter {
		return memoryResizeEnvdUnconfirmed, false
	}
	if outcome, settled := s.judgeEnvdMemoryReading(reading, r, true); settled {
		return outcome, true
	}

	return memoryResizeEnvdMismatch, false
}

// judgeEnvdMemoryReading settles the confirmation on a reading that shows the plug, or that comes
// from an envd without the memory limit report or its layout. Anything else settles nothing. envd
// leaves the layout out until its limit worker first read it, which a cold boot's first /init can
// precede, so a missing layout settles only on an /init sent after the plug (ownInit): the plug's
// memory events have made the worker read it by then.
func (s *Sandbox) judgeEnvdMemoryReading(reading *envdMemoryReading, r *memoryResize, ownInit bool) (string, bool) {
	if reading == nil {
		return "", false
	}
	r.lastReading = reading
	if reading.report == nil || (ownInit && reading.report.Layout == nil) {
		r.event("envd_unsupported", readingAttrs(reading)...)

		return memoryResizeEnvdUnsupported, true
	}
	if reading.report.Layout == nil {
		return "", false
	}
	if memoryReportShowsPlug(reading.report, s.Config.RamMB, r.requestedMB) {
		r.event("envd_confirmed", append(readingAttrs(reading), attribute.Bool("init_skipped", !ownInit))...)

		return memoryResizeConfirmed, true
	}
	if ownInit {
		expect := expectPlug(s.Config.RamMB, r.requestedMB)
		r.event("envd_mismatch", append(readingAttrs(reading),
			attribute.Int64("expected_mem_total_min", expect.memTotalMin),
			attribute.Int64("expected_movable_min", expect.movableMin))...)
	}

	return "", false
}

// memoryConfirmReinit is the /init a start sends to read envd's report of its plugged memory.
func memoryConfirmReinit(startType StartType) envdReinitPurpose {
	return envdReinitPurpose{
		startType: startType,
		reason:    envdInitMemoryConfirm,
		failure:   "envd /init confirming the plugged memory failed",
	}
}

// memoryReportShowsPlug reports whether envd's report shows a guest of ramMB boot RAM with
// pluggedMB plugged: the limit not pending, the memory it was computed from (MemTotal) at least
// boot RAM plus the plugged memory less a reservation allowance, and the plugged memory in movable
// zones. MemTotal falls short of a guest's memory by what the kernel and firmware reserve at boot,
// about 5% of a 1 GiB guest on dev, and grows by the whole plugged size, since the page metadata
// of plugged memory comes out of memory already counted; the allowance is 64 MiB plus 1/16 of boot
// RAM. Under the auto-movable online policy, plugged memory up to three times boot RAM is all
// movable and the rest mostly so, so at least 90% of min(plugged, 3 x boot RAM) must be.
func memoryReportShowsPlug(report *EnvdMemoryReport, ramMB, pluggedMB int64) bool {
	if report.Limit == nil || report.Limit.Pending || report.Limit.Applied == 0 {
		return false
	}
	expect := expectPlug(ramMB, pluggedMB)
	if int64(report.Limit.MemTotal) < expect.memTotalMin {
		return false
	}
	if pluggedMB == 0 {
		return true
	}
	if report.Layout == nil {
		return false
	}

	return int64(report.Layout.Movable) >= expect.movableMin
}

// plugExpectation is what memoryReportShowsPlug requires of envd's report, in bytes: the least
// MemTotal, the reservation allowance it subtracted, and the least movable memory.
type plugExpectation struct {
	memTotalMin int64
	allowance   int64
	movableMin  int64
}

func expectPlug(ramMB, pluggedMB int64) plugExpectation {
	const mib = int64(1) << 20
	allowance := 64*mib + ramMB*mib/16

	return plugExpectation{
		memTotalMin: (ramMB+pluggedMB)*mib - allowance,
		allowance:   allowance,
		movableMin:  min(pluggedMB, 3*ramMB) * mib * 9 / 10,
	}
}
