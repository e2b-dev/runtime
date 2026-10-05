//go:build linux

package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envd"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// Slack covers shell start + envd round-trip overhead.
const reclaimOuterSlack = 500 * time.Millisecond

// freezeRoundTripMargin is held back from the freeze budget when telling envd how long
// to spend waiting, so envd stops waiting slightly before we stop listening. Without it
// a freeze that lands only just in time would still surface as a client timeout, which
// reports nothing about what the freeze achieved.
const freezeRoundTripMargin = 500 * time.Millisecond

// freezeTimeout bounds the native POST /freeze call when the flag is unavailable. The
// write is a single sysfs write, but the call also WAITS for the workload to stop, and
// that wait is the guest's cost: a cgroup whose tasks are idle stops in single-digit
// milliseconds, one in continuous I/O has been measured taking seconds. Kept at the
// historical value for now; the freeze metrics show how often it binds, and
// FreezeUserCgroupTimeoutMsFlag raises it without a redeploy once they do.
const freezeTimeout = 2 * time.Second

const (
	// syncMinTimeout floors the guest-sync deadline; it covers small-RAM
	// sandboxes and the shell round-trip.
	syncMinTimeout = 5 * time.Second

	// syncMaxTimeout caps the guest-sync deadline so a stuck sync still fails the
	// pause in bounded time rather than hanging it.
	syncMaxTimeout = 2 * time.Minute

	// syncFlushFloorBytesPerSec is a pessimistic floor for guest page-cache
	// flush throughput to the virtio disk under IO contention. The data a sync
	// must flush is bounded by the dirty page cache (≈ guest RAM; pages already
	// written back are not re-flushed), so the deadline scales with RAM against
	// this floor. Conservative on purpose: too low only over-waits, while too
	// high would falsely fail the (mandatory) pre-pause sync.
	syncFlushFloorBytesPerSec = 50 * 1024 * 1024
)

// buildReclaimScript builds the fstrim/sync/drop_caches/compact_memory chain.
// Returns ("", 0) when every step is disabled.
func (s *Sandbox) buildReclaimScript(cfg featureflags.ReclaimConfig) (string, time.Duration) {
	var (
		parts []string
		sum   time.Duration
	)

	steps := []struct {
		cap time.Duration
		cmd string
	}{
		{cfg.Fstrim, "fstrim -av"},
		{cfg.Sync, "sync"},
		{cfg.DropCaches, "echo 3 > /proc/sys/vm/drop_caches"},
		{cfg.CompactMemory, "echo 1 > /proc/sys/vm/compact_memory"},
	}

	for _, st := range steps {
		// %.3f at <1ms renders as 0.000 → GNU timeout reads as "no timeout".
		if st.cap < time.Millisecond {
			continue
		}
		parts = append(parts, fmt.Sprintf("timeout -s KILL %.3f sh -c %q >/dev/null 2>&1 || rc=$?", st.cap.Seconds(), st.cmd))
		sum += st.cap
	}
	if len(parts) == 0 {
		return "", 0
	}

	return "rc=0; " + strings.Join(parts, "; ") + "; exit $rc", sum + reclaimOuterSlack
}

// bestEffortReclaim optionally freezes user cgroups, then runs the
// fstrim/sync/drop_caches/compact_memory chain via envd before pause.
// skipEnvdHeapCollapse leaves envd's heap alone when no memfile is exported.
func (s *Sandbox) bestEffortReclaim(ctx context.Context, skipEnvdHeapCollapse bool) {
	ctx, span := tracer.Start(ctx, "envd-reclaim")
	defer span.End()

	ctx = featureflags.AddToContext(
		ctx,
		sandboxLDContext(s.Runtime, s.Config),
		featureflags.TeamContext(s.Runtime.TeamID),
		featureflags.TemplateContext(s.Runtime.TemplateID),
	)

	if s.featureFlags.BoolFlag(ctx, featureflags.FreezeUserCgroupFlag) {
		s.bestEffortFreeze(ctx)
	}

	if !skipEnvdHeapCollapse && s.featureFlags.BoolFlag(ctx, featureflags.CollapseEnvdHeapFlag) {
		s.bestEffortCollapse(ctx)
	}

	cfg := featureflags.GetReclaimConfig(ctx, s.featureFlags)
	script, timeout := s.buildReclaimScript(cfg)
	if script == "" {
		return
	}

	rcCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	stream, err := s.StartEnvdSystemShell(rcCtx, "/bin/sh", []string{"-c", script}, "root", timeout)
	if err != nil {
		s.log().Warn(ctx, "envd reclaim failed", zap.Error(err))

		return
	}
	defer stream.Close()

	var exitCode int32
	for stream.Receive() {
		if end := stream.Msg().GetEvent().GetEnd(); end != nil {
			exitCode = end.GetExitCode()
		}
	}
	if err := stream.Err(); err != nil {
		s.log().Warn(ctx, "envd reclaim stream error", zap.Error(err))

		return
	}
	if exitCode != 0 {
		s.log().Warn(ctx, "envd reclaim non-zero exit", zap.Int32("exit_code", exitCode))
	}
}

// ramScaledSyncTimeout derives the guest-sync deadline from guest RAM. The
// dirty page cache that sync must flush is bounded by RAM, divided by a
// pessimistic flush-throughput floor, then clamped to
// [syncMinTimeout, syncMaxTimeout].
func ramScaledSyncTimeout(ramMB int64) time.Duration {
	ramBytes := ramMB * 1024 * 1024
	d := time.Duration(ramBytes/syncFlushFloorBytesPerSec) * time.Second

	if d < syncMinTimeout {
		return syncMinTimeout
	}
	if d > syncMaxTimeout {
		return syncMaxTimeout
	}

	return d
}

// guestSyncTimeout returns the deadline for the pre-pause guest sync. The
// GuestSyncTimeoutMs feature flag pins it (milliseconds) when set to a positive
// value; otherwise it scales with guest RAM via ramScaledSyncTimeout.
func (s *Sandbox) guestSyncTimeout(ctx context.Context) time.Duration {
	if ms := s.featureFlags.IntFlag(ctx, featureflags.GuestSyncTimeoutMs,
		featureflags.SandboxContext(s.Runtime.SandboxID),
		featureflags.TeamContext(s.Runtime.TeamID),
		featureflags.TemplateContext(s.Runtime.TemplateID),
	); ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}

	return ramScaledSyncTimeout(s.Config.RamMB)
}

// guestPrepareFsForPause quiesces the guest rootfs before a filesystem-only
// pause. It returns frozen=true only when a real FIFREEZE ran (native for
// >= 0.6.6, or via the exec API for < 0.6.6) — i.e. the captured rootfs is
// crash-consistent; frozen=false means it fell back to a plain sync. The caller
// persists this into the snapshot metadata (fs_quiesced).
// thawOrLose, while true, makes the registered thaw treat a rootfs that stays
// frozen as a lost sandbox: the VM is resumed in place on the error path and
// a frozen rootfs would hang it. Pause clears it once the success path owns
// the thaw, because the cleanup chain is handed back with the snapshot and
// can run again later; nil (a destroy-path pause discards the VM with its
// frozen state) keeps the thaw best-effort.
func (s *Sandbox) guestPrepareFsForPause(ctx context.Context, cleanup *Cleanup, thawOrLose *bool) (frozen bool, e error) {
	supportsFsFreeze := s.envdSupportsFsFreeze(ctx)
	// Use guestSyncTimeout here, as fsfreeze also syncs the disk
	timeout := s.guestSyncTimeout(ctx)
	start := time.Now()

	// method records how the rootfs was quiesced: native "fsfreeze", "fsfreeze-exec"
	// (old envd, via the exec API), or "sync" (fallback). Updated as we proceed.
	method := "sync"

	ctx, span := tracer.Start(ctx, "envd-guest-fs-pause")
	defer span.End()

	// Record on every exit so slow and timed-out syncs are captured too.
	defer func() {
		didFreeze := method != "sync"
		span.SetAttributes(attribute.String("method", method), attribute.Bool("fsfreeze", didFreeze))
		guestSyncDurationHistogram.Record(ctx, time.Since(start).Milliseconds(),
			metric.WithAttributes(
				attribute.Bool("success", e == nil),
				attribute.Bool("fsfreeze", didFreeze),
				attribute.String("method", method),
				attribute.Int64("timeout_ms", timeout.Milliseconds()),
			),
		)
	}()

	if supportsFsFreeze {
		method = "fsfreeze"
		// fsfreeze flushes the rootfs AND blocks further writes until thaw,
		// closing the sync->pause race. FIFREEZE already syncs as part of
		// freezing, so a separate guest sync would be redundant.
		// If freezing aborted, thaw so we don't leave the live VM's
		// filesystem frozen; on success the VM is stopped during rootfs
		// export, so the frozen state is discarded with it and the thaw is a
		// harmless no-op.
		froze := false
		cleanup.Add(ctx, func(ctx context.Context) error {
			if !froze || thawOrLose == nil || !*thawOrLose || s.GetStopReason() == StopReasonKilled {
				// Aborted freeze (state unknown), a VM about to be discarded, one
				// already torn down by the resume cleanup, or a chain re-run after
				// the success path thawed: best effort.
				s.bestEffortFsthaw(ctx)

				return nil
			}
			// Same order as the success path: the workload's cgroups first, or a
			// thaw that runs inside the guest would wait behind them.
			s.bestEffortUnfreeze(ctx)

			return s.thawRootfsOrLose(ctx)
		})
		if err := s.callEnvdFsfreeze(ctx, timeout); err != nil {
			return false, fmt.Errorf("fsfreeze before filesystem-only pause: %w", err)
		}
		froze = true

		return true, nil
	}

	// Old envd, no native /fsfreeze. When enabled, freeze the rootfs via the exec
	// API so the snapshot is captured on a quiesced, consistent filesystem instead
	// of a merely sync'd one (which leaves the sync->pause write race open).
	if s.featureFlags.BoolFlag(ctx, featureflags.FsFreezeViaExecFlag, sandboxLDContext(s.Runtime, s.Config)) {
		// Probe for the fsfreeze binary first. `command -v` only inspects PATH and
		// never touches the rootfs, so if it's missing (or the probe itself errors)
		// the filesystem is definitely not frozen and it's safe to fall back to a
		// plain sync — the "never fail a pause just because fsfreeze is missing"
		// case. Only once we know the binary exists do we risk a freeze.
		hasFsfreeze, err := s.guestHasFsfreeze(ctx, timeout)
		if err != nil {
			s.log().Warn(ctx, "probing guest for fsfreeze failed; falling back to guest sync",
				zap.Error(err))
		} else if hasFsfreeze {
			// Register the rollback thaw before freezing so an aborted freeze can't
			// leave the live VM frozen; thawing a non-frozen fs is a harmless no-op.
			froze := false
			cleanup.Add(ctx, func(ctx context.Context) error {
				if !froze || thawOrLose == nil || !*thawOrLose || s.GetStopReason() == StopReasonKilled {
					s.bestEffortFsthawViaExec(ctx)

					return nil
				}
				s.bestEffortUnfreeze(ctx)

				return s.thawRootfsOrLose(ctx)
			})
			// Set method before the freeze — as the native path sets "fsfreeze"
			// before its call — so the deferred metric attributes an attempted but
			// failed/aborted freeze to fsfreeze-exec, not to the sync fallback.
			method = "fsfreeze-exec"
			// A freeze error here may leave the rootfs frozen: FIFREEZE persists
			// after the command exits, so a timeout or stream error that races a
			// freeze which already engaged still leaves it frozen. A fallback sync
			// would then block on the frozen fs, so abort the pause like the native
			// path does and let the registered cleanup thaw it — do not sync.
			if err := s.guestFsfreezeViaExec(ctx, timeout); err != nil {
				return false, fmt.Errorf("fsfreeze via exec before filesystem-only pause: %w", err)
			}

			froze = true
			s.log().Info(ctx, "froze guest rootfs via envd exec API before filesystem-only pause")

			return true, nil
		}
	}

	if err := s.guestSync(ctx, timeout); err != nil {
		return false, fmt.Errorf("guest sync before filesystem-only pause: %w", err)
	}

	return false, nil
}

// guestSync runs sync in the guest via envd so ext4 flushes dirty pages to the
// virtio disk. Mandatory before a filesystem-only pause: without a memory
// snapshot the guest page cache is lost, so callers must fail the pause on
// error instead of persisting a rootfs missing acknowledged writes. Unlike
// bestEffortReclaim's sync step (LD-flag gated, best-effort), this always runs
// and always reports failure.
func (s *Sandbox) guestSync(ctx context.Context, syncTimeout time.Duration) error {
	exitCode, err := s.runGuestShellCommand(ctx, syncTimeout, "sync")
	if err != nil {
		return err
	}
	if exitCode != 0 {
		return fmt.Errorf("guest sync exited with code %d", exitCode)
	}

	return nil
}

// runGuestShellCommand runs `sh -c <script>` in the guest as root via the envd
// process API and returns the command's exit code. Shared by the sync and
// fsfreeze/fsthaw steps of a filesystem-only pause.
func (s *Sandbox) runGuestShellCommand(ctx context.Context, timeout time.Duration, script string) (int32, error) {
	rcCtx, cancel := context.WithTimeout(ctx, timeout+reclaimOuterSlack)
	defer cancel()

	stream, err := s.StartEnvdSystemShell(rcCtx, "/bin/sh", []string{"-c", script}, "root", timeout)
	if err != nil {
		return -1, fmt.Errorf("start guest command: %w", err)
	}
	defer stream.Close()

	exitCode := int32(-1)
	for stream.Receive() {
		if end := stream.Msg().GetEvent().GetEnd(); end != nil {
			exitCode = end.GetExitCode()
		}
	}
	if err := stream.Err(); err != nil {
		return -1, fmt.Errorf("guest command stream: %w", err)
	}

	return exitCode, nil
}

// fsthawViaExecTimeout bounds the rollback thaw run through the exec API. Kept
// short: the thaw runs only on the pause-failure path, and one that blocks (e.g.
// the exec path touches the frozen rootfs) must not hang the cleanup — a bounded
// failure lets the caller tear the sandbox down instead of leaving it frozen.
const fsthawViaExecTimeout = 10 * time.Second

// guestHasFsfreeze reports whether the guest has an fsfreeze binary. It probes
// with `command -v`, which only inspects PATH and never touches the rootfs, so
// it is safe to run before deciding whether to freeze or fall back to sync. A
// non-zero exit means the binary is absent (a clean "fall back to sync" signal);
// only a stream/RPC failure is returned as an error.
func (s *Sandbox) guestHasFsfreeze(ctx context.Context, timeout time.Duration) (bool, error) {
	exitCode, err := s.runGuestShellCommand(ctx, timeout, "command -v fsfreeze >/dev/null 2>&1")
	if err != nil {
		return false, err
	}

	return exitCode == 0, nil
}

// guestFsfreezeViaExec freezes the guest rootfs with `fsfreeze -f /` through the
// envd exec API, for guests whose envd predates the native /fsfreeze endpoint.
// Callers must confirm the fsfreeze binary exists first (see guestHasFsfreeze).
// FIFREEZE flushes the rootfs and blocks further writes until thaw, closing the
// sync->pause race a plain sync leaves open; the freeze is a superblock property,
// so it persists after the command exits. A non-zero exit or stream error is
// returned so the caller aborts the pause rather than syncing a possibly-frozen
// rootfs (which would block).
func (s *Sandbox) guestFsfreezeViaExec(ctx context.Context, timeout time.Duration) error {
	exitCode, err := s.runGuestShellCommand(ctx, timeout, "fsfreeze -f /")
	if err != nil {
		return err
	}
	if exitCode != 0 {
		return fmt.Errorf("guest fsfreeze -f / exited with code %d", exitCode)
	}

	return nil
}

// bestEffortFsthawViaExec thaws the guest rootfs with `fsfreeze -u /` through the
// exec API on the pause-failure rollback, so a frozen filesystem can't leave the
// live VM deadlocked. Bounded and detached; FITHAW on a non-frozen filesystem is
// a harmless no-op, so it is safe even when no freeze actually happened.
//
// Caveat: the thaw spawns a process against a possibly-frozen rootfs. If the exec
// path writes to that rootfs it would block; the bounded timeout ensures we
// return rather than hang, but a timed-out thaw leaves the VM frozen and the
// sandbox should then be torn down. The pause success path never thaws.
func (s *Sandbox) bestEffortFsthawViaExec(ctx context.Context) {
	if err := s.fsthawViaExec(ctx); err != nil {
		s.log().Warn(ctx, "fsthaw via exec failed", zap.Error(err))
	}
}

func (s *Sandbox) fsthawViaExec(ctx context.Context) error {
	exitCode, err := s.runGuestShellCommand(context.WithoutCancel(ctx), fsthawViaExecTimeout,
		"command -v fsfreeze >/dev/null 2>&1 && fsfreeze -u /")
	if err != nil {
		return err
	}
	if exitCode != 0 {
		return fmt.Errorf("fsfreeze -u exited %d", exitCode)
	}

	return nil
}

const (
	fsthawAttempts     = 3
	fsthawRetryBackoff = 250 * time.Millisecond
)

// thawRootfs undoes a successful pre-pause freeze on a VM that keeps running:
// one call and two retries with doubling back-off, native /fsthaw or the exec
// fallback matching how the rootfs was frozen.
func (s *Sandbox) thawRootfs(ctx context.Context) error {
	ctx = context.WithoutCancel(ctx)
	native := s.envdSupportsFsFreeze(ctx)
	delay := fsthawRetryBackoff
	var err error
	for attempt := 1; attempt <= fsthawAttempts; attempt++ {
		start := time.Now()
		if native {
			err = s.callEnvdFsthaw(ctx, s.guestSyncTimeout(ctx))
		} else {
			err = s.fsthawViaExec(ctx)
		}
		envdFsthawDurationHistogram.Record(ctx, time.Since(start).Milliseconds(),
			metric.WithAttributes(attribute.Bool("success", err == nil), attribute.Int("attempt", attempt)))
		if err == nil {
			return nil
		}
		s.log().Warn(ctx, "rootfs thaw failed", zap.Int("attempt", attempt), zap.Error(err))
		if attempt < fsthawAttempts {
			time.Sleep(delay)
			delay *= 2
		}
	}

	return err
}

// ErrRootfsThawFailed tags an ErrSandboxLost whose lethal step was the
// rootfs thaw, not the resume: the VM came back, its rootfs stayed frozen. The
// RPC layer reads it to name the kill correctly.
var ErrRootfsThawFailed = errors.New("rootfs thaw failed")

// thawRootfsOrLose treats a rootfs that stays frozen as a lost sandbox: the
// guest runs but cannot write, and its health check, a read, keeps passing.
// Same exit as a failed in-place resume, so the API reaps the record.
func (s *Sandbox) thawRootfsOrLose(ctx context.Context) error {
	err := s.thawRootfs(ctx)
	if err == nil {
		return nil
	}
	s.SetStopReason(StopReasonKilled)

	return fmt.Errorf("rootfs thaw failed after in-place resume, sandbox torn down: %w",
		errors.Join(ErrSandboxLost, ErrRootfsThawFailed, err, s.Close(context.WithoutCancel(ctx))))
}

// envdSupportsCgroupFreeze reports whether the sandbox's envd exposes the
// native /freeze and /unfreeze endpoints. Bad version strings log and return
// false so we never accidentally call an unsupported endpoint.
func (s *Sandbox) envdSupportsCgroupFreeze(ctx context.Context) bool {
	ok, err := utils.IsGTEVersion(s.Config.Envd.Version, utils.MinEnvdVersionForCgroupFreeze)
	if err != nil {
		s.log().Warn(ctx, "cgroup freeze version gate: bad envd version", zap.String("envd_version", s.Config.Envd.Version), zap.Error(err))

		return false
	}

	return ok
}

// envdSupportsFsFreeze reports whether the sandbox's envd exposes the native
// /fsfreeze and /fsthaw endpoints. Bad version strings log and return false so
// the filesystem-only pause falls back to a plain guest sync.
func (s *Sandbox) envdSupportsFsFreeze(ctx context.Context) bool {
	ok, err := utils.IsGTEVersion(s.Config.Envd.Version, utils.MinEnvdVersionForFsFreeze)
	if err != nil {
		s.log().Warn(ctx, "fsfreeze version gate: bad envd version", zap.String("envd_version", s.Config.Envd.Version), zap.Error(err))

		return false
	}

	return ok
}

// envdSupportsHeapCollapse reports whether the sandbox's envd exposes the native
// /collapse endpoint. Bad version strings log and return false so we never call
// an unsupported endpoint.
func (s *Sandbox) envdSupportsHeapCollapse(ctx context.Context) bool {
	ok, err := utils.IsGTEVersion(s.Config.Envd.Version, utils.MinEnvdVersionForHeapCollapse)
	if err != nil {
		s.log().Warn(ctx, "heap collapse version gate: bad envd version", zap.String("envd_version", s.Config.Envd.Version), zap.Error(err))

		return false
	}

	return ok
}

// bestEffortCollapse asks envd to collapse its own heap into hugepages before
// pause, so on resume envd touches fewer distinct frames. Gated on envd version;
// failures are logged but never block pause.
func (s *Sandbox) bestEffortCollapse(ctx context.Context) {
	if !s.envdSupportsHeapCollapse(ctx) {
		return
	}

	ctx, span := tracer.Start(ctx, "envd-collapse")
	defer span.End()

	// Timeout comes straight from the flag, whose fallback (10s) is returned
	// whenever LD is unavailable or the flag is unset — so there is no separate
	// local default to keep in sync.
	timeout := time.Duration(s.featureFlags.IntFlag(ctx, featureflags.CollapseEnvdHeapTimeoutMsFlag)) * time.Millisecond

	start := time.Now()
	stats, err := s.callEnvdCollapse(ctx, timeout)
	elapsedMs := time.Since(start).Milliseconds()
	success := err == nil

	// Record the round-trip duration whether or not it succeeded: a timed-out or
	// failed collapse still spends time on the pause path and must be visible.
	envdCollapseDurationHistogram.Record(ctx, elapsedMs, metric.WithAttributes(attribute.Bool("success", success)))
	span.SetAttributes(
		attribute.Bool("collapse.success", success),
		attribute.Int64("collapse.duration_ms", elapsedMs),
	)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		s.log().Warn(ctx, "envd heap collapse failed", zap.Error(err))

		return
	}

	// Chunk-level efficacy split three ways so the dashboard can separate real
	// work from no-ops: attempts = collapsed + already_huge + skipped, where
	// collapsed = pages actually migrated this pause, already_huge = windows that
	// were already hugepages (MADV_COLLAPSE succeeded but did nothing).
	envdCollapseChunks.Add(ctx, int64(stats.Collapsed), metric.WithAttributes(attribute.String("result", "collapsed")))
	envdCollapseChunks.Add(ctx, int64(stats.AlreadyHuge), metric.WithAttributes(attribute.String("result", "already_huge")))
	envdCollapseChunks.Add(ctx, int64(stats.Skipped), metric.WithAttributes(attribute.String("result", "skipped")))
	span.SetAttributes(
		attribute.Int("collapse.regions", stats.Regions),
		attribute.Int("collapse.chunks", stats.Chunks),
		attribute.Int("collapse.collapsed", stats.Collapsed),
		attribute.Int("collapse.already_huge", stats.AlreadyHuge),
		attribute.Int("collapse.skipped", stats.Skipped),
	)

	s.log().Info(ctx, "envd heap collapsed",
		zap.Int("regions", stats.Regions),
		zap.Int("chunks", stats.Chunks),
		zap.Int("collapsed", stats.Collapsed),
		zap.Int("already_huge", stats.AlreadyHuge),
		zap.Int("skipped", stats.Skipped),
		zap.Int64("duration_ms", elapsedMs),
	)
}

// bestEffortFreeze calls envd's native /freeze endpoint with a tight, freeze-
// only deadline so it doesn't share a timeout budget with the rest of reclaim.
// Gated on envd version; failures are logged but never block pause.
func (s *Sandbox) bestEffortFreeze(ctx context.Context) {
	if !s.envdSupportsCgroupFreeze(ctx) {
		return
	}

	ctx, span := tracer.Start(ctx, "envd-freeze")
	defer span.End()

	timeout := freezeTimeout
	if ms := s.featureFlags.IntFlag(ctx, featureflags.FreezeUserCgroupTimeoutMsFlag); ms > 0 {
		timeout = time.Duration(ms) * time.Millisecond
	}

	// The flag is the only gate. There is deliberately no envd version check: an envd that
	// predates the mode parameter ignores it, freezes the legacy set and says so in the
	// response, which is both safe and visible. A version constant would add a number
	// nobody can set correctly before the release that carries this is cut, to prevent a
	// request that costs nothing -- the same reasoning that kept one out of the
	// confirmation work.
	hierarchy := s.featureFlags.BoolFlag(ctx, featureflags.FreezeGuestHierarchyFlag)
	maxCgroups := s.featureFlags.IntFlag(ctx, featureflags.FreezeGuestHierarchyMaxCgroupsFlag)

	start := time.Now()
	res, reported, err := s.callEnvdFreeze(ctx, timeout, hierarchy, maxCgroups)
	elapsedMs := time.Since(start).Milliseconds()
	success := err == nil
	// Distinguish "we gave up waiting" from "envd refused": both are failures, but only
	// the first says the budget is the binding constraint, which is what decides whether
	// to raise the flag.
	timedOut := errors.Is(err, context.DeadlineExceeded)

	// Record the round trip whether or not it succeeded: a freeze that burned the whole
	// budget still spent that time on the pause path and must be visible. Before this,
	// neither outcome was recorded anywhere -- a freeze that timed out and one that was
	// a silent no-op looked identical from outside.
	envdFreezeDurationHistogram.Record(ctx, elapsedMs, metric.WithAttributes(
		attribute.Bool("success", success),
		attribute.Bool("reported", reported),
		attribute.Bool("timed_out", timedOut),
		attribute.Int64("timeout_ms", timeout.Milliseconds()),
	))
	span.SetAttributes(
		attribute.Bool("freeze.success", success),
		attribute.Int64("freeze.duration_ms", elapsedMs),
		attribute.Bool("freeze.reported", reported),
		attribute.Bool("freeze.timed_out", timedOut),
		attribute.Int64("freeze.timeout_ms", timeout.Milliseconds()),
	)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		s.log().Warn(ctx, "envd freeze failed", zap.Error(err))

		return
	}

	if !reported {
		// An envd too old to answer with a body. The freeze was issued; whether it took
		// effect is unknowable, so record nothing rather than a misleading zero.
		return
	}

	// Split the two durations: sweep is our own cost and scales with cgroup count, wait
	// is the guest's and scales with how deep in I/O its tasks were. A single combined
	// number would send us tuning whichever is not the problem.
	// An envd predating the mode parameter omits it, which reads as legacy because that is
	// what it performed. Normalised once here so no metric ever carries an empty label.
	mode := string(res.Mode)
	if mode == "" {
		mode = string(envd.FreezeResultModeLegacy)
	}

	// Every freeze metric is cut by the mode that actually ran. Without it the walk's
	// numbers would be pooled with the legacy ones during any partial rollout, and the
	// legacy arm is precisely the baseline the walk's added cost has to be measured
	// against.
	modeAttr := metric.WithAttributes(attribute.String("mode", mode))

	envdFreezeSweepHistogram.Record(ctx, res.SweepMs, modeAttr)
	envdFreezeWaitHistogram.Record(ctx, res.WaitMs, modeAttr)
	envdFreezeVisitedHistogram.Record(ctx, int64(res.Visited), modeAttr)

	// Outcome split so a freeze that is issued but never lands is distinguishable from
	// one that works: not_frozen > 0 means the snapshot may capture a running workload.
	for outcome, count := range map[string]int{
		"frozen":     res.Frozen,
		"not_frozen": res.NotFrozen,
		"failed":     res.Failed,
		// Held apart from not_frozen: a guest that cannot report freeze state is not a
		// workload refusing to stop, and folding the two together would make guests with
		// no cgroup manager look like the failure this metric exists to catch.
		"unobservable": res.Unobservable,
		// Cgroups the guest had frozen itself, which we neither wrote to nor will thaw.
		// Counted so "the guest suspends its own containers" is visible as a population
		// rather than inferred from a thaw that quietly does less.
		"pre_frozen": res.PreFrozen,
		// Cgroups the guest removed while the sweep was working on them. Held apart from
		// failed because it is the guest's own churn rather than a property of the tree
		// we are about to snapshot -- systemd retires a transient unit on every timer
		// tick, and folding that into failed made routine churn look like a workload
		// that would not stop. An envd too old to report it sends nothing, which decodes
		// to zero rather than to a wrong number.
		"vanished": res.Vanished,
	} {
		envdFreezeCgroupsHistogram.Record(ctx, int64(count),
			metric.WithAttributes(
				attribute.String("outcome", outcome),
				attribute.String("mode", mode),
			))
	}

	// The mode envd reports, not the mode we asked for -- conflating the two would hide
	// exactly the case worth seeing during a rollout.
	span.SetAttributes(
		attribute.String("freeze.mode", mode),
		attribute.Bool("freeze.mode_requested_hierarchy", hierarchy),
		attribute.Int("freeze.visited", res.Visited),
		attribute.Int("freeze.allowlisted", res.Allowlisted),
		attribute.Bool("freeze.truncated", res.Truncated),
		attribute.Int("freeze.pre_frozen", res.PreFrozen),
		attribute.Int("freeze.requested", res.Requested),
		attribute.Int("freeze.frozen", res.Frozen),
		attribute.Int("freeze.not_frozen", res.NotFrozen),
		attribute.Int("freeze.failed", res.Failed),
		attribute.Int("freeze.vanished", res.Vanished),
		attribute.Int("freeze.unobservable", res.Unobservable),
		attribute.Int64("freeze.sweep_ms", res.SweepMs),
		attribute.Int64("freeze.wait_ms", res.WaitMs),
	)

	if res.Truncated {
		s.log().Warn(ctx, "pre-pause freeze walk hit its bound; coverage is incomplete",
			zap.Int("visited", res.Visited),
			zap.Int("max_cgroups", maxCgroups),
			zap.Int("requested", res.Requested),
		)
	}
	if res.NotFrozen > 0 || res.Failed > 0 {
		s.log().Warn(ctx, "pre-pause freeze did not stop the whole workload",
			zap.Int("requested", res.Requested),
			zap.Int("frozen", res.Frozen),
			zap.Int("not_frozen", res.NotFrozen),
			zap.Int("failed", res.Failed),
			// Not a trigger for this warning, but logged with it: without it a reader
			// checking these counts against requested finds them short and suspects the
			// wrong thing. It closes that gap only for a cgroup that vanished during the
			// settle poll -- one that vanished at the write was never requested.
			zap.Int("vanished", res.Vanished),
			zap.Int64("wait_ms", res.WaitMs),
		)
	}
}

// bestEffortUnfreeze calls envd's native /unfreeze endpoint with a tight
// deadline. Reserved for the Pause error-cleanup chain so a failed pause
// doesn't leave a live sandbox permanently frozen; the resume thaw is handled
// by /init's defer and must not be moved here. Gated on envd version;
// failures are logged. Uses context.WithoutCancel because callers run it from
// cleanup paths whose parent ctx may already be done.
func (s *Sandbox) bestEffortUnfreeze(ctx context.Context) {
	if !s.envdSupportsCgroupFreeze(ctx) {
		return
	}

	start := time.Now()
	err := s.callEnvdUnfreeze(context.WithoutCancel(ctx), freezeTimeout)
	envdUnfreezeDurationHistogram.Record(ctx, time.Since(start).Milliseconds(),
		metric.WithAttributes(attribute.Bool("success", err == nil)))

	if err != nil {
		s.log().Warn(ctx, "envd unfreeze failed", zap.Error(err))
	}
}

// bestEffortFsthaw thaws the guest rootfs via envd's native /fsthaw endpoint.
// Reserved for the filesystem-only pause error path so an aborted pause can't
// leave the live VM's filesystem frozen. Gated on envd version; failures are
// logged. Uses context.WithoutCancel because it runs from cleanup paths whose
// parent ctx may already be done.
func (s *Sandbox) bestEffortFsthaw(ctx context.Context) {
	if !s.envdSupportsFsFreeze(ctx) {
		return
	}

	if err := s.callEnvdFsthaw(context.WithoutCancel(ctx), s.guestSyncTimeout(ctx)); err != nil {
		s.log().Warn(ctx, "envd fsthaw failed", zap.Error(err))
	}
}

// freezeAudit is envd's resume-time audit of the frozen cgroup set, as carried by
// X-Envd-Freeze-Audit. JSON, and decoded field by field: a field a newer envd adds is
// ignored here rather than failing the whole parse, and one an older envd omits reads as its
// zero value -- which is why truncated is the field whose zero has to be the safe answer
// ("these counts are complete").
type freezeAudit struct {
	Visited    int64 `json:"visited"`
	Frozen     int64 `json:"frozen"`
	Escaped    int64 `json:"escaped"`
	Violations int64 `json:"violations"`
	Truncated  bool  `json:"truncated"`
}

// recordFreezeAudit turns envd's resume-time audit header into metrics and span attributes.
//
// Two counts, and they mean very different things:
//
//   - escaped is the residual the walk cannot close. A cgroup created after the pre-pause
//     sweep and before the snapshot was never a candidate, and cgroup v2 has no
//     freeze-on-create semantics to prevent it. A non-zero rate here sizes a race we
//     currently only reason about.
//   - violations is a BUG, not a race: a cgroup the resume depends on was frozen, meaning
//     either the allowlist is missing a name or the sweep froze a parent of one. Zero is
//     the only acceptable value, and two separate defects in this area were found by other
//     means before this counter existed.
//
// Advisory in that it never fails the resume, but a header that will not decode is LOGGED
// rather than dropped in silence. The alternative reads identically to a fleet where the
// audit is clean: violations is the signal this exists for, and its absence must not be
// indistinguishable from its being zero.
func (s *Sandbox) recordFreezeAudit(ctx context.Context, header string) {
	var a freezeAudit
	if err := json.Unmarshal([]byte(header), &a); err != nil {
		s.log().Warn(ctx, "could not decode the resume freeze audit header",
			zap.Error(err),
		)

		return
	}

	envdFreezeAuditHistogram.Record(ctx, a.Escaped,
		metric.WithAttributes(attribute.String("kind", "escaped")))
	envdFreezeAuditHistogram.Record(ctx, a.Violations,
		metric.WithAttributes(attribute.String("kind", "violations")))

	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.Int64("freeze.audit.visited", a.Visited),
			attribute.Int64("freeze.audit.frozen", a.Frozen),
			attribute.Int64("freeze.audit.escaped", a.Escaped),
			attribute.Int64("freeze.audit.violations", a.Violations),
			// Absent from an envd that predates the field, which decodes as false -- the
			// same as "not truncated", and the only safe default for a signal that means
			// "these counts are a floor".
			attribute.Bool("freeze.audit.truncated", a.Truncated),
		)
	}

	if a.Truncated {
		s.log().Warn(ctx, "the resume freeze audit stopped at its bound; its counts are a floor",
			zap.Int64("visited", a.Visited),
		)
	}

	if a.Violations > 0 {
		s.log().Error(ctx, "cgroups the resume depends on were frozen; the freeze allowlist did not hold",
			zap.Int64("violations", a.Violations),
			zap.Int64("frozen", a.Frozen),
			zap.Int64("visited", a.Visited),
		)
	}
}
