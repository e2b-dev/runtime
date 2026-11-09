//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
)

// GuestRebootOutcome is what a reboot in place did about a guest reset. It
// is a metric label, so the set stays closed.
type GuestRebootOutcome string

const (
	GuestRebootRebooted       GuestRebootOutcome = "rebooted"
	GuestRebootRefusedLoop    GuestRebootOutcome = "refused_loop"
	GuestRebootRefusedTimeout GuestRebootOutcome = "refused_timeout"
	GuestRebootFailed         GuestRebootOutcome = "reboot_failed"
)

const (
	// maxConsecutiveGuestResets bounds the resets a guest may make without
	// envd answering in between. An update or a feature install can restart
	// a guest more than once before its services start.
	maxConsecutiveGuestResets = 3

	// defaultTypicalBoot is how long a guest takes from its reset to envd
	// answering, unless its profile says otherwise. A reboot is not started
	// with less time than this left before the sandbox's end time.
	defaultTypicalBoot = 10 * time.Second

	// rebootSealWait bounds the wait for an in-place checkpoint's background
	// seals that were running when the guest reset.
	rebootSealWait = 30 * time.Second

	// rebootStartBudget is what one boot attempt gets before its envd wait:
	// the cgroup, the process, the network assignment and the rootfs.
	rebootStartBudget = time.Minute
)

// rebootAttemptTimeoutFor bounds one boot attempt of a reboot in place. The
// network-assignment hook refuses a start without a deadline.
func rebootAttemptTimeoutFor(config *Config) time.Duration {
	return rebootStartBudget + rebootEnvdTimeoutFor(config)
}

// errCheckpointInFlight refuses a reboot while an in-place checkpoint holds
// the sandbox: it was using the process that just exited, and it fails on
// its own.
var errCheckpointInFlight = errors.New("an in-place checkpoint was running at the guest reset")

// admitGuestReboot decides whether a guest reset may boot the sandbox again:
// not when the guest keeps resetting without envd answering, and not when
// the sandbox's end time comes before a typical boot would finish. Empty
// means admitted.
func admitGuestReboot(now, endAt time.Time, typicalBoot time.Duration, consecutiveResets int) GuestRebootOutcome {
	if consecutiveResets > maxConsecutiveGuestResets {
		return GuestRebootRefusedLoop
	}
	if endAt.Sub(now) <= typicalBoot {
		return GuestRebootRefusedTimeout
	}

	return ""
}

// typicalBootFor is how long a reboot of this guest takes to envd answering.
func typicalBootFor(config *Config) time.Duration {
	if typical := config.GuestProfile().Reset().TypicalBoot; typical > 0 {
		return typical
	}

	return defaultTypicalBoot
}

// GuestRebootResult is the end of a reboot in place.
type GuestRebootResult struct {
	// Sandbox is the next lifecycle, live and serving; nil unless rebooted.
	Sandbox *Sandbox
	Outcome GuestRebootOutcome
	// ConsecutiveResets counts the resets this reboot handled, the first
	// included.
	ConsecutiveResets int
	Err               error
	// PendingKill is the reason of a Delete that cancelled the boot and
	// stopped waiting for its end. The Delete already answered, so with no
	// Sandbox the caller publishes the kill. With one, registered before
	// the cancel landed, the sandbox is live, and a retry or the API's
	// orphan sync ends it.
	PendingKill string
}

// RebootInPlace boots the sandbox whose guest reset itself again: a new
// Firecracker process, with fresh memory, on the same rootfs overlay and
// network slot, under the same sandbox ID, execution ID, envd access token
// and end time, with a new LifecycleID. The old lifecycle leaves the live map
// at the start and the new one enters it once envd answers; in between
// AwaitBoot and CancelBoot find the boot.
//
// The old process must have been reaped. Every failure stops the sandbox
// without booting: the resources go to the failed lifecycle's cleanup chain,
// or stay with old's when nothing took them. A guest that resets again
// before envd answers is booted again, up to maxConsecutiveGuestResets.
//
// The caller still closes old, which then releases only what it kept: its
// cgroup, its sandbox files and its stopped memory handler. It also starts
// the rebooted sandbox's health checks. A rebooted old ends with
// StopReasonRebooted, one whose boot a kill cancelled with StopReasonKilled;
// any other outcome leaves its stop reason unset.
func (f *Factory) RebootInPlace(ctx context.Context, old *Sandbox) (res GuestRebootResult) {
	ctx, end := traceGuestReboot(ctx, old)
	defer func() { end(res) }()

	if err := old.claimForReboot(ctx); err != nil {
		return GuestRebootResult{Outcome: GuestRebootFailed, ConsecutiveResets: 1, Err: err}
	}

	bootCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	defer func() {
		// The steps that fail on a cancelled boot report the context's error,
		// not why it was cancelled.
		if cause := context.Cause(bootCtx); res.Err != nil && errors.Is(cause, ErrBootCancelled) && !errors.Is(res.Err, ErrBootCancelled) {
			res.Err = errors.Join(cause, res.Err)
		}
	}()

	reservation, b, err := f.Sandboxes.beginBoot(ctx, old, cancel)
	if err != nil {
		return GuestRebootResult{Outcome: GuestRebootFailed, ConsecutiveResets: 1, Err: fmt.Errorf("take the sandbox out of the live map: %w", err)}
	}
	defer reservation.Release()
	defer func() {
		// After old's stop reason is set, so the lifecycle reads the kill.
		if killReason, ok := f.Sandboxes.endBoot(b); ok {
			res.PendingKill = killReason
		}
	}()

	// Read once the sandbox has left the live map: an Update moving the end
	// time now waits for the boot, so it cannot change under the admission.
	res = f.bootAgain(bootCtx, old, old.GetEndAt())
	if res.Sandbox == nil {
		// Set before endBoot lets CancelBoot return, so the lifecycle reads
		// the kill, not a crash to list as ended.
		if errors.Is(context.Cause(bootCtx), ErrBootCancelled) {
			old.SetStopReason(StopReasonKilled)
		}

		return res
	}

	next := res.Sandbox
	// Under bootCtx: a kill that cancelled the boot after envd answered is
	// still a kill, and a Delete that stopped waiting must not be followed
	// by a sandbox it will never see.
	if err := reservation.MarkRunningUnlessEnded(bootCtx, next); err != nil {
		if errors.Is(context.Cause(bootCtx), ErrBootCancelled) {
			next.SetStopReason(StopReasonKilled)
			old.SetStopReason(StopReasonKilled)
		} else {
			next.SetStopReason(StopReasonRegistrationFailed)
		}

		return GuestRebootResult{
			Outcome:           GuestRebootFailed,
			ConsecutiveResets: res.ConsecutiveResets,
			Err:               errors.Join(fmt.Errorf("register the rebooted sandbox: %w", err), stopAndClose(ctx, next)),
		}
	}

	// Only now: every outcome short of a live next lifecycle leaves old
	// reading as crashed.
	old.SetStopReason(StopReasonRebooted)

	return res
}

// traceGuestReboot starts the span of a reboot; the returned func ends it
// with the reboot's result.
func traceGuestReboot(ctx context.Context, old *Sandbox) (context.Context, func(GuestRebootResult)) {
	ctx, span := tracer.Start(ctx, "guest-reboot", //nolint:spancheck // the returned func ends it
		trace.WithLinks(trace.Link{SpanContext: old.executionSpan}),
		trace.WithAttributes(attribute.String("guest.event", string(fc.GuestEventRebootOrCrash))),
	)

	return ctx, func(res GuestRebootResult) { //nolint:spancheck // the returned func ends it
		span.SetAttributes(
			attribute.String("guest.reboot_outcome", string(res.Outcome)),
			attribute.Int("guest.consecutive_resets", res.ConsecutiveResets),
		)
		if res.Err != nil {
			span.RecordError(res.Err)
			span.SetStatus(codes.Error, res.Err.Error())
		}
		span.End()
	}
}

// bootAgain boots old's sandbox again on its slot and overlay until envd
// answers on a boot, as long as the admission allows. The sandbox it
// returns is not live yet.
func (f *Factory) bootAgain(ctx context.Context, old *Sandbox, endAt time.Time) GuestRebootResult {
	resets := 1
	typicalBoot := typicalBootFor(old.Config)
	failed := func(err error) GuestRebootResult {
		return GuestRebootResult{Outcome: GuestRebootFailed, ConsecutiveResets: resets, Err: err}
	}

	if outcome := admitGuestReboot(time.Now(), endAt, typicalBoot, resets); outcome != "" {
		return GuestRebootResult{Outcome: outcome, ConsecutiveResets: resets}
	}

	// Lifecycles whose guest reset again during their boot. Each is closed
	// once the next one took its slot and overlay.
	var handedOn []*Sandbox
	defer func() {
		for _, sbx := range handedOn {
			if err := sbx.Close(context.WithoutCancel(ctx)); err != nil {
				sbx.log().Warn(ctx, "closing a lifecycle that reset during its boot", zap.Error(err))
			}
		}
	}()

	from := old
	for {
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, rebootAttemptTimeoutFor(old.Config))
		next, reset, err := f.bootNextFn()(attemptCtx, from, endAt)
		cancelAttempt()
		if err == nil {
			return GuestRebootResult{Sandbox: next, Outcome: GuestRebootRebooted, ConsecutiveResets: resets}
		}

		if next == nil {
			return failed(err)
		}

		if !reset {
			next.SetStopReason(StopReasonKilled)

			return failed(errors.Join(err, stopAndClose(ctx, next)))
		}

		resets++
		handedOn = append(handedOn, next)
		if outcome := admitGuestReboot(time.Now(), endAt, typicalBoot, resets); outcome != "" {
			// next keeps its slot and overlay, and releases them when closed.
			return GuestRebootResult{Outcome: outcome, ConsecutiveResets: resets, Err: err}
		}

		from = next
	}
}

// ErrGuestResetOnStart reports a resume whose guest reset before envd first
// answered, and that was not booted again.
var ErrGuestResetOnStart = errors.New("the guest reset before envd answered")

// StartRebootReport receives what a resume did about a guest reset before
// envd first answered. resetAt is when the reset was seen.
type StartRebootReport func(ctx context.Context, res GuestRebootResult, resetAt time.Time)

// rebootOnStart handles a guest reset during a resume's envd wait. With
// report nil the resume fails as it always has. Otherwise the sandbox is
// booted again on its disk, and the next lifecycle is returned not live:
// old was never live, and the create's reservation still holds the ID.
func (f *Factory) rebootOnStart(ctx context.Context, old *Sandbox, report StartRebootReport) (*Sandbox, error) {
	if report == nil {
		return nil, ErrGuestResetOnStart
	}

	resetAt := time.Now()
	ctx, end := traceGuestReboot(ctx, old)
	res := f.bootAgain(ctx, old, old.GetEndAt())
	end(res)
	report(ctx, res, resetAt)

	if res.Sandbox == nil {
		return nil, errors.Join(fmt.Errorf("%w, and booting it again ended %s", ErrGuestResetOnStart, res.Outcome), res.Err)
	}

	return res.Sandbox, nil
}

// claimForReboot keeps in-place checkpoints out of a sandbox whose process
// exited, refuses if one was already running, and waits for the background
// seals of an earlier one. A failed rootfs seal leaves the overlay without a
// whole writable cache, which a boot must not use.
func (s *Sandbox) claimForReboot(ctx context.Context) error {
	if !s.BeginCheckpoint() {
		return errCheckpointInFlight
	}

	ctx, cancel := context.WithTimeout(ctx, rebootSealWait)
	defer cancel()

	// Memory belongs to the process that exited; only the wait matters.
	_ = s.waitForMemorySeal(ctx)

	if err := s.waitForRootfsSeal(ctx); err != nil {
		return fmt.Errorf("rootfs seal of an earlier checkpoint: %w", err)
	}

	return nil
}

// bootNextFn is the boot step of a reboot in place.
func (f *Factory) bootNextFn() bootNextFunc {
	if f.bootNext != nil {
		return f.bootNext
	}

	return f.bootInPlace
}

// bootNextFunc starts the next lifecycle from the one whose guest reset. It
// returns the next sandbox whenever its process started, envd answering or
// not; reset says its guest reset again before envd answered.
type bootNextFunc func(ctx context.Context, from *Sandbox, endAt time.Time) (next *Sandbox, reset bool, err error)

// bootInPlace starts the next Firecracker process on from's slot and overlay
// and waits for envd.
func (f *Factory) bootInPlace(ctx context.Context, from *Sandbox, endAt time.Time) (*Sandbox, bool, error) {
	next, err := f.startInPlace(ctx, from, endAt)
	if err != nil {
		return nil, false, err
	}

	if err := next.WaitForEnvd(ctx, StartTypeReboot, rebootEnvdTimeoutFor(from.Config)); err != nil {
		return next, resetDuringBoot(ctx, next, err), fmt.Errorf("wait for envd after the guest reset: %w", err)
	}

	return next, false, nil
}

// startInPlace creates the next Firecracker process on from's slot and
// overlay, once from's process has been reaped.
func (f *Factory) startInPlace(ctx context.Context, from *Sandbox, endAt time.Time) (*Sandbox, error) {
	select {
	case <-from.process.Exit.Done():
	default:
		return nil, errors.New("the previous Firecracker process has not exited")
	}

	if _, err := from.rootfs.Path(); err != nil {
		return nil, fmt.Errorf("rootfs overlay: %w", err)
	}

	meta, err := from.Template.Metadata()
	if err != nil {
		return nil, fmt.Errorf("get template metadata: %w", err)
	}

	boot, err := f.prepareColdBoot(ctx, from.Template, meta, from.Config, from.Runtime)
	if err != nil {
		return nil, err
	}

	timeout := time.Until(endAt)
	if timeout <= 0 {
		return nil, fmt.Errorf("sandbox end time %s is in the past", endAt)
	}

	// prepareColdBoot has resolved the boot's vCPU capacity and hotplug region into from.Config,
	// from the request or the snapshot, so the refusals in takeForBoot see what this boot will use.
	h, err := from.takeForBoot(ctx)
	if err != nil {
		return nil, err
	}

	// The disk the guest left behind is not known to be clean: a reset may
	// be a kernel crash. Recover it as any cold boot of an unfrozen rootfs,
	// for a guest whose profile takes the host-side journal replay.
	var preBoot PreBootFn
	if !from.Config.GuestProfile().Boot().NoFilesystemRecovery {
		preBoot = f.fsRecoverPreBoot(ctx, from.Runtime, false, fsRecoveryTriggerGuestReset, func(rootfs.RecoverOutcome) {})
	}

	next, err := f.CreateSandbox(
		ctx,
		from.Config,
		from.Runtime,
		boot.template,
		timeout,
		"",
		boot.processOptions,
		from.APIStoredConfig,
		WithPreBoot(preBoot),
		WithDeferredMarkRunning(),
		withNetworkAssignReason(NetworkAssignReasonReboot),
		withHandedOver(h),
		withEndAt(endAt),
	)
	if err != nil {
		return nil, fmt.Errorf("create sandbox on the same disk: %w", err)
	}

	return next, nil
}

// fsRecoveryTriggerGuestReset labels pre-boot recoveries of a disk whose
// guest reset itself.
const fsRecoveryTriggerGuestReset = "guest_reset"

// takeForBoot refuses a boot the guest cannot take before anything moves,
// then takes the slot and the overlay: a refusal inside CreateSandbox would
// come after the take and before the new cleanup chain holds them. It reads
// s.Config, so it must run after prepareColdBoot has resolved the boot into it.
func (s *Sandbox) takeForBoot(ctx context.Context) (*handedOver, error) {
	if err := refuseColdBootVcpus(s.Config); err != nil {
		return nil, err
	}
	if _, err := coldBootMemoryHotplug(s.Config, fc.ProcessOptions{}); err != nil {
		return nil, err
	}

	return s.takeForReboot(ctx)
}

// takeForReboot takes the network slot and the rootfs overlay from the
// cleanup chain. Taking one without the other returns it to its owner.
func (s *Sandbox) takeForReboot(ctx context.Context) (*handedOver, error) {
	releaseSlot, ok := s.slotRelease.take()
	if !ok {
		return nil, errors.New("the network slot was already released")
	}

	// Giving them back outlives the boot attempt, which is cancelled once this
	// fails; the pool's delayed return retries the release under this context.
	cleanupCtx := context.WithoutCancel(ctx)

	releaseRootfs, ok := s.rootfsRelease.take()
	if !ok {
		return nil, errors.Join(errors.New("the rootfs overlay was already closed"), releaseSlot(cleanupCtx))
	}

	// The slot moves on, this lifecycle's network entry and subscribers do
	// not: release them now, as the pool's return would have, so the proxy and
	// the NFS server drop this lifecycle before the next one takes the address.
	// One that fails keeps the address: the slot and the overlay go back to
	// their owner, and the pool's return retries the same release.
	release := s.networkRelease
	if release == nil {
		release = s.sandboxes.NetworkRelease(s.Runtime.SandboxID, s.LifecycleID)
	}
	if err := release(ctx, s.Slot.HostIPString()); err != nil {
		return nil, errors.Join(fmt.Errorf("release the network entry of the lifecycle: %w", err), releaseSlot(cleanupCtx), releaseRootfs(cleanupCtx))
	}

	h := &handedOver{
		slot:               s.Slot,
		rootfs:             s.rootfs,
		releaseSlot:        releaseSlot,
		releaseRootfs:      releaseRootfs,
		executionStartedAt: s.GetExecutionStartedAt(),
	}
	if v := s.offlineEnvdVersion.Load(); v != nil {
		h.envdVersion = *v
	}

	return h, nil
}

// resetDuringBoot reports whether a boot whose envd wait failed ended in a
// guest reset. It waits for the boot's process to be reaped.
func resetDuringBoot(ctx context.Context, sbx *Sandbox, waitErr error) bool {
	if !errors.Is(waitErr, ErrFcProcessExited) {
		return false
	}

	// The process, not the sandbox: a resume starts waiting for its sandbox
	// to end only once envd has answered.
	if err := sbx.process.Exit.WaitWithContext(ctx); err != nil && ctx.Err() != nil {
		return false
	}

	return fc.ClassifyExit(sbx.FirecrackerExit()).GuestEvent().Restarts()
}

// stopAndClose tears down a lifecycle that never became live.
func stopAndClose(ctx context.Context, sbx *Sandbox) error {
	ctx = context.WithoutCancel(ctx)

	return errors.Join(sbx.Stop(ctx), sbx.Close(ctx))
}
