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
)

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

					e.evictSandbox(ctx, item)
				}(item)
			}
		}
	}
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

func (e *Evictor) evictSandbox(ctx context.Context, sbx sandbox.Sandbox) {
	action := sandbox.StateActionKill
	if sbx.AutoPause && canTake(sbx.State, sandbox.StateActionPause) {
		action = sandbox.StateActionPause
		pause.LogInitiated(ctx, sbx.SandboxID, sbx.TeamID.String(), pause.ReasonTimeout, sbx.AutoPauseFilesystemOnly)
	}

	// The action was chosen from a scanned record. Pin the removal to that
	// execution so a resume landing in between is refused, not acted on.
	opts := sandbox.RemoveOpts{Action: action, Eviction: true, ExpectExecutionID: sbx.ExecutionID}
	degradeCause := ""
	switch action {
	case sandbox.StateActionKill:
		opts.Reason = sandbox.KillReasonTimeout
	case sandbox.StateActionPause:
		// Honor the sandbox's auto-pause snapshot kind: filesystem-only drops
		// memory (cold-boots on resume); otherwise a full memory snapshot.
		// No version gate: producing a filesystem-only snapshot needs no
		// version-gated FC capability, so the release-contract check is
		// dropped fleet-wide.
		opts.FilesystemOnly = sbx.AutoPauseFilesystemOnly
		if opts.FilesystemOnly {
			e.fsOnlyAutoPauseCounter.Add(ctx, 1)
		}
	}

	err := e.removeSandbox(context.WithoutCancel(ctx), sbx.TeamID, sbx.SandboxID, opts)
	if action == sandbox.StateActionPause && !opts.FilesystemOnly && errors.Is(err, sandbox.PauseQueueExhaustedError{}) {
		degradeCause = e.degradeCause(ctx, sbx, time.Now())
	}
	if degradeCause != "" {
		// The refusal is an intermediate event; the filesystem-only pause
		// that follows is this sweep's one result.
		pause.LogRefused(ctx, sbx.SandboxID, sbx.TeamID.String(), pause.ReasonTimeout, opts.FilesystemOnly)
		opts.FilesystemOnly = true
		e.fsOnlyAutoPauseCounter.Add(ctx, 1)
		err = e.removeSandbox(context.WithoutCancel(ctx), sbx.TeamID, sbx.SandboxID, opts)
	}
	if err == nil && degradeCause != "" {
		// Counted once, when the degraded pause lands — a pause that keeps
		// failing past the budget must not re-count every tick.
		e.degradedCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("cause", degradeCause)))
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
				pause.LogSkipped(ctx, sbx.SandboxID, sbx.TeamID.String(), pause.ReasonTimeout, skip, opts.FilesystemOnly)
			} else {
				pause.LogFailure(ctx, sbx.SandboxID, sbx.TeamID.String(), pause.ReasonTimeout, opts.FilesystemOnly, err)
			}
		} else if !isKnownEvictionError(err, sbx.State) {
			logger.L().Debug(ctx, "Evicting sandbox failed",
				zap.Error(err),
				logger.WithSandboxID(sbx.SandboxID),
				logger.WithTeamID(sbx.TeamID.String()),
				zap.String("kill_reason", sandbox.KillReasonTimeout.String()),
			)
		}

		return
	} else if action == sandbox.StateActionPause {
		pause.LogSuccess(ctx, sbx.SandboxID, sbx.TeamID.String(), pause.ReasonTimeout, opts.FilesystemOnly)
	}

	if action != sandbox.StateActionPause {
		logger.L().Debug(ctx, "Sandbox evicted",
			logger.WithSandboxID(sbx.SandboxID),
			zap.String("kill_reason", sandbox.KillReasonTimeout.String()),
		)
	}
}

const (
	degradeCauseRefused  = "admission_refused"
	degradeCauseOverstay = "overstay_budget"
)

// pauseSkipReason classifies a pause error that left the sandbox running for
// a later sweep. Anything else is a failure.
func pauseSkipReason(err error, state sandbox.State) (pause.SkipReason, bool) {
	switch {
	case errors.Is(err, sandbox.PauseQueueExhaustedError{}):
		return pause.SkipReasonAdmissionRefused, true
	case errors.Is(err, sandbox.ErrDraining):
		return pause.SkipReasonDraining, true
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

// refusalHeld reports whether a node-refused, restored auto-pause is still
// inside the retry window the restore stamped on its record — shared by every
// replica's sweep, since it travels with the record.
func refusalHeld(sbx sandbox.Sandbox, now time.Time) bool {
	return now.Before(sbx.RefusedUntil)
}

func isNotEvictableError(err error) bool {
	return errors.Is(err, sandbox.ErrEvictionInProgress) || errors.Is(err, sandbox.ErrEvictionNotNeeded)
}

// isGone reports the scanned sandbox is no longer there: removed, or replaced
// by a new execution under the same ID.
func isGone(err error) bool {
	return errors.Is(err, sandbox.ErrNotFound) || errors.Is(err, sandbox.ErrExecutionMismatch)
}

// isStaleDecision reports a refusal explained by the sandbox moving between
// the expired-set read and StartRemoving. A refusal from the very state the
// action was chosen for is a real failure and stays one.
func isStaleDecision(err error, observed sandbox.State) bool {
	var transErr *sandbox.InvalidStateTransitionError

	return errors.As(err, &transErr) && transErr.CurrentState != observed
}

func isKnownEvictionError(err error, observed sandbox.State) bool {
	return isNotEvictableError(err) || isGone(err) || isStaleDecision(err, observed)
}
