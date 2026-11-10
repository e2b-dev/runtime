package evictor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/pause"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	pollInterval               = 50 * time.Millisecond
	concurrencyRefreshInterval = 30 * time.Second
	// unsupportedEntryTTL bounds an entry no pause, kill or gone sandbox
	// removed.
	unsupportedEntryTTL = time.Hour
)

// unsupportedRefusal is an entry of unsupportedFilesystemOnly: when the node
// refused, and the execution it refused.
type unsupportedRefusal struct {
	at          time.Time
	executionID string
}

type Evictor struct {
	store         *sandbox.Store
	removeSandbox func(ctx context.Context, teamID uuid.UUID, sandboxID string, opts sandbox.RemoveOpts) error
	featureFlags  *featureflags.Client

	fsOnlyAutoPauseCounter metric.Int64Counter
	degradedCounter        metric.Int64Counter

	concurrencyLimiter *utils.AdjustableSemaphore

	// activeEvictions tracks concurrent eviction attempts for the same sandbox
	// so that overlapping ticks don't kick off multiple removeSandbox calls.
	activeEvictions sync.Map

	// unsupportedFilesystemOnly holds the sandboxes whose node refused a
	// filesystem-only pause as unsupported for their guest: later sweeps
	// never degrade them again, and one whose own policy asks for that
	// capture is killed instead, so no sweep spends a snapshot build on a
	// pause that cannot land. An entry (an unsupportedRefusal, read for its
	// execution) goes when the sandbox is paused, killed or gone, or
	// unsupportedEntryTTL after the refusal, after which the node is asked
	// once more.
	unsupportedFilesystemOnly sync.Map
}

// refusedFilesystemOnly reports whether the node refused the filesystem-only
// capture of this very execution of the sandbox as unsupported. A kill and
// the degrade skip are decided on it, so a later execution under the same
// ID, which may run on a node that takes the capture, is asked afresh.
func (e *Evictor) refusedFilesystemOnly(sbx sandbox.Sandbox) bool {
	value, ok := e.unsupportedFilesystemOnly.Load(sbx.SandboxID)
	if !ok {
		return false
	}
	refusal, ok := value.(unsupportedRefusal)

	return ok && refusal.executionID == sbx.ExecutionID
}

// rememberUnsupported records the node's unsupported refusal of the sandbox's
// filesystem-only capture.
func (e *Evictor) rememberUnsupported(sbx sandbox.Sandbox, now time.Time) {
	e.unsupportedFilesystemOnly.Store(sbx.SandboxID, unsupportedRefusal{at: now, executionID: sbx.ExecutionID})
}

func New(
	ctx context.Context,
	store *sandbox.Store,
	removeSandbox func(ctx context.Context, teamID uuid.UUID, sandboxID string, opts sandbox.RemoveOpts) error,
	featureFlags *featureflags.Client,
	meter metric.Meter,
) (*Evictor, error) {
	initialLimit := featureFlags.IntFlag(ctx, featureflags.MaxConcurrentEvictions)
	if initialLimit <= 0 {
		initialLimit = featureflags.MaxConcurrentEvictions.Fallback()
	}

	concurrencyLimiter, err := utils.NewAdjustableSemaphore(int64(initialLimit))
	if err != nil {
		return nil, fmt.Errorf("failed to create eviction concurrency semaphore: %w", err)
	}

	fsOnlyAutoPauseCounter, err := telemetry.GetCounter(meter, telemetry.ApiEvictorFsOnlyAutoPause)
	if err != nil {
		return nil, fmt.Errorf("failed to create fs-only auto-pause counter: %w", err)
	}

	degradedCounter, err := telemetry.GetCounter(meter, telemetry.ApiEvictorAutoPauseDegraded)
	if err != nil {
		return nil, fmt.Errorf("failed to create auto-pause degraded counter: %w", err)
	}

	e := &Evictor{
		store:                  store,
		removeSandbox:          removeSandbox,
		featureFlags:           featureFlags,
		concurrencyLimiter:     concurrencyLimiter,
		fsOnlyAutoPauseCounter: fsOnlyAutoPauseCounter,
		degradedCounter:        degradedCounter,
	}

	if _, err := telemetry.GetObservableUpDownCounter(meter, telemetry.EvictionsRunningCounterName,
		func(_ context.Context, observer metric.Int64Observer) error {
			var count int64
			e.activeEvictions.Range(func(_, _ any) bool {
				count++

				return true
			})

			observer.Observe(count)

			return nil
		}); err != nil {
		return nil, fmt.Errorf("failed to create evictor in-flight gauge: %w", err)
	}

	return e, nil
}

func (e *Evictor) Start(ctx context.Context) {
	var wg sync.WaitGroup
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	refreshTicker := time.NewTicker(concurrencyRefreshInterval)
	defer refreshTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Wait for in-flight evictions to finish for graceful shutdown.
			wg.Wait()

			return
		case <-refreshTicker.C:
			e.refreshConcurrencyLimit(ctx)
			e.forgetStaleUnsupported(time.Now())
		case <-ticker.C:
			sbxs, err := e.store.ExpiredItems(ctx)
			if err != nil {
				logger.L().Error(ctx, "Failed to get expired sandboxes", zap.Error(err))

				continue
			}

			now := time.Now()
			for _, item := range sbxs {
				if refusalHeld(item, now) {
					continue
				}

				// Skip if an eviction for this sandbox is already in flight.
				if _, loaded := e.activeEvictions.LoadOrStore(item.SandboxID, struct{}{}); loaded {
					continue
				}

				// Non-blocking acquire: if we're at capacity, skip and let the
				// next tick retry. Mirrors the previous errgroup.TryGo behavior.
				if !e.concurrencyLimiter.TryAcquire(1) {
					e.activeEvictions.Delete(item.SandboxID)

					logger.L().Debug(ctx, "Max concurrent evictions reached, skipping eviction this tick",
						logger.WithSandboxID(item.SandboxID),
						logger.WithTeamID(item.TeamID.String()),
					)

					continue
				}

				wg.Add(1)
				go func(item sandbox.Sandbox) {
					defer wg.Done()
					defer e.concurrencyLimiter.Release(1)
					defer e.activeEvictions.Delete(item.SandboxID)

					e.evictSandbox(ctx, item, causeTimeout)
				}(item)
			}
		}
	}
}

// forgetStaleUnsupported drops entries older than unsupportedEntryTTL: a
// removal by a user or another replica never reaches this evictor, and the
// expired set cannot tell, since a refused sandbox leaves it for its retry
// window. A dropped entry costs at most one refused filesystem-only pause.
func (e *Evictor) forgetStaleUnsupported(now time.Time) {
	e.unsupportedFilesystemOnly.Range(func(key, value any) bool {
		if refusal, ok := value.(unsupportedRefusal); !ok || now.Sub(refusal.at) > unsupportedEntryTTL {
			e.unsupportedFilesystemOnly.Delete(key)
		}

		return true
	})
}

func (e *Evictor) refreshConcurrencyLimit(ctx context.Context) {
	limit := e.featureFlags.IntFlag(ctx, featureflags.MaxConcurrentEvictions)
	if limit <= 0 {
		return
	}

	if err := e.concurrencyLimiter.SetLimit(int64(limit)); err != nil {
		logger.L().Error(ctx, "failed to adjust eviction concurrency semaphore",
			zap.Int("limit", limit), zap.Error(err))
	}
}

// evictionCause is why the evictor ends a sandbox, as its pause and kill
// reasons name it.
type evictionCause struct {
	pause pause.Reason
	kill  sandbox.KillReason
	// filesystemOnly makes a pause filesystem-only whatever the sandbox
	// asked for.
	filesystemOnly bool
	// beforeEndTime acts on a sandbox that has not reached its end time.
	beforeEndTime bool
}

var (
	causeTimeout = evictionCause{pause: pause.ReasonTimeout, kill: sandbox.KillReasonTimeout}
	// causeGuestPowerOff has no memory to keep.
	causeGuestPowerOff = evictionCause{pause: pause.ReasonGuestPowerOff, kill: sandbox.KillReasonGuestPowerOff, filesystemOnly: true, beforeEndTime: true}
)

// EvictPoweredOff runs the timeout action of a sandbox whose node reports
// that its guest powered off, without waiting for its end time: a kill, or a
// filesystem-only pause when the sandbox pauses on timeout. A store record of
// another execution, or one already being evicted, is left alone.
func (e *Evictor) EvictPoweredOff(ctx context.Context, node sandbox.NodeSandbox) {
	sbx, err := e.store.Get(ctx, node.TeamID, node.SandboxID)
	if err != nil {
		logger.L().Debug(ctx, "Powered-off sandbox not in the store", zap.Error(err), logger.WithSandboxID(node.SandboxID))

		return
	}
	// Only the node the record places the sandbox on may end it.
	if sbx.ExecutionID != node.ExecutionID || sbx.NodeID != node.NodeID || sbx.ClusterID != node.ClusterID {
		return
	}

	// A refused pause's retry window holds across API replicas, as in the
	// timeout sweep.
	if refusalHeld(sbx, time.Now()) {
		return
	}

	if _, loaded := e.activeEvictions.LoadOrStore(sbx.SandboxID, struct{}{}); loaded {
		return
	}
	defer e.activeEvictions.Delete(sbx.SandboxID)

	// At capacity, the next sync retries.
	if !e.concurrencyLimiter.TryAcquire(1) {
		return
	}
	defer e.concurrencyLimiter.Release(1)

	e.evictSandbox(ctx, sbx, causeGuestPowerOff)
}

func (e *Evictor) evictSandbox(ctx context.Context, sbx sandbox.Sandbox, cause evictionCause) {
	action := sandbox.StateActionKill
	// The filesystem-only capture a policy asks for that the node refused as
	// one the guest cannot take is not asked for again: the answer cannot
	// change, and every ask costs a snapshot build. Past its retry window
	// the sandbox takes the kill of its cause, as one whose pause cannot be
	// taken does. The disk-only capture of a powered-off guest is never
	// refused so, and keeps its pause.
	refused := !cause.filesystemOnly && sbx.AutoPauseFilesystemOnly && e.refusedFilesystemOnly(sbx)
	if refused {
		logger.L().Info(ctx, "Auto-pause refused by the node as a capture the guest cannot take; killing the sandbox with its timeout reason",
			logger.WithSandboxID(sbx.SandboxID),
			logger.WithTeamID(sbx.TeamID.String()),
			zap.String("kill_reason", cause.kill.String()),
		)
	}
	if sbx.AutoPause && canTake(sbx.State, sandbox.StateActionPause) && !refused {
		action = sandbox.StateActionPause
		pause.LogInitiated(ctx, sbx.SandboxID, sbx.TeamID.String(), cause.pause, sbx.AutoPauseFilesystemOnly || cause.filesystemOnly)
	}

	// The action was chosen from a scanned record. Pin the removal to that
	// execution so a resume landing in between is refused, not acted on.
	opts := sandbox.RemoveOpts{Action: action, Eviction: true, ExpectExecutionID: sbx.ExecutionID, BeforeEndTime: cause.beforeEndTime}
	degradeCause := ""
	switch action {
	case sandbox.StateActionKill:
		opts.Reason = cause.kill
	case sandbox.StateActionPause:
		// Honor the sandbox's auto-pause snapshot kind: filesystem-only drops
		// memory (cold-boots on resume); otherwise a full memory snapshot.
		// No version gate: producing a filesystem-only snapshot needs no
		// version-gated FC capability, so the release-contract check is
		// dropped fleet-wide.
		// A powered-off guest has no memory left to pause, so its cause takes
		// the disk-only capture.
		opts.FilesystemOnly = cause.filesystemOnly || sbx.AutoPauseFilesystemOnly
		if opts.FilesystemOnly {
			e.fsOnlyAutoPauseCounter.Add(ctx, 1, metric.WithAttributes(telemetry.WithTeamID(sbx.TeamID.String())))
		}
		// Strict, so a hung guest is refused and its episode clock runs; the
		// grace below decides when a memory pause goes on without it.
		opts.RequireEnvdHealthy = true
	}

	err := e.removeSandbox(context.WithoutCancel(ctx), sbx.TeamID, sbx.SandboxID, opts)
	// A filesystem-only auto-pause the node refuses as unsupported is never
	// retried as a memory pause: the policy drops the memory, so a memory
	// capture in its place would keep what the customer opted out of. The
	// node refuses such a sandbox at create, so a refusal here is a skew
	// between the two and is counted; the restored sandbox runs out its
	// retry window, and the next sweep kills it.
	if action == sandbox.StateActionPause && opts.FilesystemOnly && !cause.filesystemOnly && errors.Is(err, sandbox.PauseUnsupportedError{}) {
		e.rememberUnsupported(sbx, time.Now())
		e.degradedCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("cause", degradeCauseFsOnlyUnsupported),
			telemetry.WithTeamID(sbx.TeamID.String()),
		))
		logger.L().Warn(ctx, "Auto-pause withheld: the node refused the filesystem-only capture the sandbox opted into, and no memory capture is taken in its place; the sandbox is killed on the next sweep",
			logger.WithSandboxID(sbx.SandboxID),
			logger.WithTeamID(sbx.TeamID.String()),
			zap.String("cause", degradeCauseFsOnlyUnsupported),
			zap.Duration("past_expiry", time.Since(sbx.EndTime)),
		)
	}
	// A checkpoint in flight says nothing about the node's memory and ends on
	// its own, so the next sweep retries it as is. A guest agent that does
	// not answer is never degraded to filesystem-only, which needs the agent
	// to quiesce the rootfs; past the grace the memory pause is retried once
	// without requiring a healthy guest, so the node takes it and records
	// the miss. A filesystem-only sweep has no such retry: the sandbox stays
	// running and the withheld bypass is counted. A guest whose node refused
	// the degraded filesystem-only pause as unsupported is never degraded
	// again.
	bypassCause := ""
	if action == sandbox.StateActionPause && errors.Is(err, sandbox.PauseQueueExhaustedError{}) &&
		!errors.Is(err, sandbox.PauseRefusedCheckpointInProgressError{}) {
		switch {
		case errors.Is(err, sandbox.PauseRefusedEnvdUnresponsiveError{}):
			bypassCause = e.envdBypassCause(ctx, sbx, opts.FilesystemOnly, time.Now())
		case !opts.FilesystemOnly && !e.refusedFilesystemOnly(sbx):
			degradeCause = e.degradeCause(ctx, sbx, time.Now())
		}
	}
	switch bypassCause {
	case degradeCauseEnvdGraceFsOnly:
		// A filesystem-only capture of a hung guest would persist
		// unacknowledged writes, so the sandbox stays running.
		e.degradedCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("cause", bypassCause),
			telemetry.WithTeamID(sbx.TeamID.String()),
		))
		logger.L().Warn(ctx, "Auto-pause envd bypass withheld: a filesystem-only capture needs the guest agent to quiesce the rootfs, the sandbox stays running",
			logger.WithSandboxID(sbx.SandboxID),
			logger.WithTeamID(sbx.TeamID.String()),
			zap.String("cause", bypassCause),
			zap.Duration("past_expiry", time.Since(sbx.EndTime)),
		)
	case degradeCauseEnvdGraceWithheld:
		// The retry is pinned to an execution by ExpectExecutionID; without
		// one it could land on a resumed incarnation, so the sandbox stays.
		e.degradedCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("cause", bypassCause),
			telemetry.WithTeamID(sbx.TeamID.String()),
		))
		logger.L().Warn(ctx, "Auto-pause envd bypass withheld: the record names no execution to pin it to, the sandbox stays running",
			logger.WithSandboxID(sbx.SandboxID),
			logger.WithTeamID(sbx.TeamID.String()),
			zap.String("cause", bypassCause),
			zap.Duration("past_expiry", time.Since(sbx.EndTime)),
		)
	case degradeCauseEnvdGrace:
		// The refusal is an intermediate event; the memory pause that
		// follows, taken over the unresponsive agent and recorded as such,
		// is this sweep's one result.
		pause.LogRefused(ctx, sbx.SandboxID, sbx.TeamID.String(), cause.pause, opts.FilesystemOnly)
		opts.RequireEnvdHealthy = false
		e.degradedCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("cause", bypassCause),
			telemetry.WithTeamID(sbx.TeamID.String()),
		))
		logger.L().Warn(ctx, "Auto-pause retried without requiring a healthy guest agent: it refused for the whole grace period, the snapshot is recorded as taken over an unresponsive agent",
			logger.WithSandboxID(sbx.SandboxID),
			logger.WithTeamID(sbx.TeamID.String()),
			zap.String("cause", bypassCause),
			zap.Duration("past_expiry", time.Since(sbx.EndTime)),
		)
		err = e.removeSandbox(context.WithoutCancel(ctx), sbx.TeamID, sbx.SandboxID, opts)
	}
	if degradeCause != "" {
		// The refusal is an intermediate event; the filesystem-only pause
		// that follows is this sweep's one result.
		pause.LogRefused(ctx, sbx.SandboxID, sbx.TeamID.String(), cause.pause, opts.FilesystemOnly)
		opts.FilesystemOnly = true
		e.fsOnlyAutoPauseCounter.Add(ctx, 1, metric.WithAttributes(telemetry.WithTeamID(sbx.TeamID.String())))
		err = e.removeSandbox(context.WithoutCancel(ctx), sbx.TeamID, sbx.SandboxID, opts)
		if errors.Is(err, sandbox.PauseUnsupportedError{}) {
			// The degraded pause cannot land on this guest: the sweep's
			// outcome is the admission refusal of the memory pause, and the
			// next sweeps skip the degrade.
			e.rememberUnsupported(sbx, time.Now())
			pause.LogSkipped(ctx, sbx.SandboxID, sbx.TeamID.String(), pause.ReasonTimeout, pause.SkipReasonAdmissionRefused, false)

			return
		}
	}
	if err == nil || isGone(err) {
		e.unsupportedFilesystemOnly.Delete(sbx.SandboxID)
	}
	if err == nil && degradeCause != "" {
		// Counted once, when the degraded pause lands — a pause that keeps
		// failing past the budget must not re-count every tick.
		e.degradedCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("cause", degradeCause),
			telemetry.WithTeamID(sbx.TeamID.String()),
		))
		logger.L().Warn(ctx, "Auto-pause degraded to filesystem-only",
			logger.WithSandboxID(sbx.SandboxID),
			logger.WithTeamID(sbx.TeamID.String()),
			zap.String("cause", degradeCause),
			zap.Duration("past_expiry", time.Since(sbx.EndTime)),
		)
	}
	if err != nil {
		if action == sandbox.StateActionPause {
			if skip, ok := pauseSkipReason(err, sbx.State); ok {
				pause.LogSkipped(ctx, sbx.SandboxID, sbx.TeamID.String(), cause.pause, skip, opts.FilesystemOnly)
			} else {
				pause.LogFailure(ctx, sbx.SandboxID, sbx.TeamID.String(), cause.pause, opts.FilesystemOnly, err)
			}
		} else if !isKnownEvictionError(err, sbx.State) {
			logger.L().Debug(ctx, "Evicting sandbox failed",
				zap.Error(err),
				logger.WithSandboxID(sbx.SandboxID),
				logger.WithTeamID(sbx.TeamID.String()),
				zap.String("kill_reason", cause.kill.String()),
			)
		}

		return
	} else if action == sandbox.StateActionPause {
		pause.LogSuccess(ctx, sbx.SandboxID, sbx.TeamID.String(), cause.pause, opts.FilesystemOnly)
	}

	if action != sandbox.StateActionPause {
		logger.L().Debug(ctx, "Sandbox evicted",
			logger.WithSandboxID(sbx.SandboxID),
			zap.String("kill_reason", cause.kill.String()),
		)
	}
}

const (
	degradeCauseRefused           = "admission_refused"
	degradeCauseOverstay          = "overstay_budget"
	degradeCauseEnvdGrace         = "envd_grace"
	degradeCauseEnvdGraceWithheld = "envd_grace_withheld"
	degradeCauseEnvdGraceFsOnly   = "envd_grace_fs_only"
	degradeCauseFsOnlyUnsupported = "fs_only_unsupported"
)

// pauseSkipReason classifies a pause error that left the sandbox running for
// a later sweep. Anything else is a failure.
func pauseSkipReason(err error, state sandbox.State) (pause.SkipReason, bool) {
	switch {
	case errors.Is(err, sandbox.PauseRefusedCheckpointInProgressError{}):
		return pause.SkipReasonCheckpointInProgress, true
	case errors.Is(err, sandbox.PauseRefusedEnvdUnresponsiveError{}):
		return pause.SkipReasonEnvdUnresponsive, true
	case errors.Is(err, sandbox.PauseQueueExhaustedError{}):
		return pause.SkipReasonAdmissionRefused, true
	case errors.Is(err, sandbox.PauseUnsupportedError{}):
		return pause.SkipReasonUnsupported, true
	case errors.Is(err, sandbox.ErrDraining):
		return pause.SkipReasonDraining, true
	case errors.Is(err, sandbox.ErrLifecycleHeld):
		return pause.SkipReasonLifecycleHeld, true
	case isNotEvictableError(err):
		return pause.SkipReasonNotEvictable, true
	case isGone(err):
		return pause.SkipReasonNotFound, true
	case isStaleDecision(err, state):
		return pause.SkipReasonStateChanged, true
	default:
		return "", false
	}
}

func canTake(state sandbox.State, action sandbox.StateAction) bool {
	return state == action.TargetState || sandbox.AllowedTransitions[state][action.TargetState]
}

// degradeCause decides, for a memory auto-pause the node has just refused,
// whether this sweep degrades it to filesystem-only and why: a zero budget
// degrades at the first refusal, a positive one once the refusal episode has
// outlasted it, a negative one never. Only ever consulted on a refusal, so
// eviction lag alone never degrades anything.
func (e *Evictor) degradeCause(ctx context.Context, sbx sandbox.Sandbox, now time.Time) string {
	budgetMs := e.featureFlags.IntFlag(ctx, featureflags.AutoPauseOverstayBudgetMs,
		featureflags.TeamContext(sbx.TeamID.String()), featureflags.ClusterContext(sbx.ClusterID))
	switch {
	case budgetMs < 0:
		return ""
	case budgetMs == 0:
		return degradeCauseRefused
	case now.After(sbx.RefusalEpisodeStart(now).Add(time.Duration(budgetMs) * time.Millisecond)):
		return degradeCauseOverstay
	default:
		return ""
	}
}

// envdBypassCause names the bypass this sweep takes for an envd-refused
// auto-pause, or "" to hold: a negative grace never bypasses, zero at the first
// refusal, positive once the guest agent's own episode has lasted that long.
// A filesystem-only sweep and a record that names no execution are withheld.
func (e *Evictor) envdBypassCause(ctx context.Context, sbx sandbox.Sandbox, fsOnly bool, now time.Time) string {
	graceMs := e.featureFlags.IntFlag(ctx, featureflags.AutoPauseEnvdGraceMs,
		featureflags.TeamContext(sbx.TeamID.String()), featureflags.ClusterContext(sbx.ClusterID))
	switch {
	case graceMs < 0:
		return ""
	case now.Sub(sbx.EnvdRefusalEpisodeStart(now)) < time.Duration(graceMs)*time.Millisecond:
		return ""
	case fsOnly:
		return degradeCauseEnvdGraceFsOnly
	case sbx.ExecutionID == "":
		return degradeCauseEnvdGraceWithheld
	default:
		return degradeCauseEnvdGrace
	}
}

// refusalHeld reports whether a node-refused, restored auto-pause is still
// inside the retry window the restore stamped on its record — shared by every
// replica's sweep, since it travels with the record.
func refusalHeld(sbx sandbox.Sandbox, now time.Time) bool {
	return now.Before(sbx.RefusedUntil)
}

func isNotEvictableError(err error) bool {
	// A held sandbox expires once its hold is released.
	return errors.Is(err, sandbox.ErrEvictionInProgress) || errors.Is(err, sandbox.ErrEvictionNotNeeded) ||
		errors.Is(err, sandbox.ErrLifecycleHeld)
}

// isGone reports the scanned sandbox is no longer there: removed, or replaced
// by a new execution under the same ID.
func isGone(err error) bool {
	return errors.Is(err, sandbox.ErrNotFound) || errors.Is(err, sandbox.ErrExecutionMismatch)
}

// isStaleDecision reports a refusal explained by the sandbox moving between
// the expired-set read and StartRemoving, or by a lock that expired under
// this sweep, which wrote nothing. A refusal from the very state the action
// was chosen for is a real failure and stays one.
func isStaleDecision(err error, observed sandbox.State) bool {
	var transErr *sandbox.InvalidStateTransitionError

	return errors.Is(err, sandbox.ErrLockLost) || errors.As(err, &transErr) && transErr.CurrentState != observed
}

func isKnownEvictionError(err error, observed sandbox.State) bool {
	return isNotEvictableError(err) || isGone(err) || isStaleDecision(err, observed)
}
