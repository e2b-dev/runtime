package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/middleware/otel/joined"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// StartRemoving initiates a state transition for a sandbox using atomic Lua scripts.
//
// The function handles concurrent requests safely:
//  1. Acquires a distributed lock on the sandbox
//  2. Checks if there's an ongoing transition via the transition key:
//     - If the same target state is in progress, waits for completion and returns the result
//     - If a different state is in progress, waits for it to complete, then retries
//     - If no transition is in progress, start the transition
//  3. Validates the state transition is allowed
//  4. Atomically updates the sandbox state and sets the transition key with a unique ID
//  5. Returns a callback that the caller must invoke to signal completion
//
// The callback is critical: it deletes the transition key
// and sets the result value with short TTL to notify waiters of the outcome.
func (s *Storage) StartRemoving(ctx context.Context, teamID uuid.UUID, sandboxID string, opts sandboxtypes.RemoveOpts) (sandboxtypes.StateTransition, bool, func(context.Context, error), error) {
	key := getSandboxKey(teamID.String(), sandboxID)
	transitionKey := getTransitionKey(teamID.String(), sandboxID)
	lockKey := redis_utils.GetLockKey(key)

	// Acquire distributed lock
	lock, err := s.locker.Obtain(ctx, lockKey, lockTimeout)
	if err != nil {
		return sandboxtypes.StateTransition{}, false, nil, fmt.Errorf("failed to obtain lock: %w", err)
	}

	// Ensure lock is released once
	releaseFunc := sync.OnceValue(func() error {
		return lock.Release(context.WithoutCancel(ctx))
	})

	defer func() {
		releaseErr := releaseFunc()
		if releaseErr != nil {
			logger.L().Error(ctx, "Failed to release lock", zap.Error(releaseErr))
		}
	}()

	// Get current sandbox state first
	data, err := s.redisClient.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return sandboxtypes.StateTransition{}, false, nil, fmt.Errorf("sandbox %q: %w", sandboxID, sandboxtypes.ErrNotFound)
	}
	if err != nil {
		return sandboxtypes.StateTransition{}, false, nil, fmt.Errorf("failed to get sandbox from Redis: %w", err)
	}

	var sbx sandboxtypes.Sandbox
	if err = json.Unmarshal(data, &sbx); err != nil {
		return sandboxtypes.StateTransition{}, false, nil, fmt.Errorf("failed to unmarshal sandbox: %w", err)
	}
	transition := sandboxtypes.StateTransition{Sandbox: sbx}

	// Fast path for a caller-pinned removal: refuse a stale one before doing any
	// further work, and before possibly waiting out an unrelated transition
	// below only to be refused at the end of it.
	//
	// This is not where the pin is enforced. The lock this holds excludes the
	// other lock-taking operations but not Add, which is lockless, so a resume
	// can still install a new incarnation between this comparison and the write.
	// The authority is the equivalent check inside startTransitionScript, which
	// is atomic with that write.
	if opts.ExpectExecutionID != "" && sbx.ExecutionID != opts.ExpectExecutionID {
		return transition, false, nil, fmt.Errorf(
			"sandbox %q is execution %q, expected %q: %w",
			sandboxID, sbx.ExecutionID, opts.ExpectExecutionID, sandboxtypes.ErrExecutionMismatch,
		)
	}

	// Check if there's an existing transition
	transactionID, err := s.redisClient.Get(ctx, transitionKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return transition, false, nil, fmt.Errorf("failed to check transition key: %w", err)
	}

	// Resolve eviction under the lock + re-check expiry
	if opts.Eviction {
		// if there's a transition already in place, don't do anything
		if transactionID != "" {
			return transition, false, nil, sandboxtypes.ErrEvictionInProgress
		}

		// if sandbox isn't expired (e.g. race condition with SetTimeout)
		if !sbx.IsExpired(time.Now()) {
			return transition, false, nil, sandboxtypes.ErrEvictionNotNeeded
		}
	}

	newState := opts.Action.TargetState

	if transactionID != "" {
		releaseErr := releaseFunc()
		if releaseErr != nil {
			logger.L().Warn(ctx, "Failed to release lock before waiting", zap.Error(releaseErr))
		}

		return s.handleExistingTransition(ctx, teamID, sbx, opts, transactionID)
	}

	// Check if already in target state
	if sbx.State == newState {
		logger.L().Debug(ctx, "Already in the same state", logger.WithSandboxID(sandboxID), zap.String("state", string(newState)))

		return transition, true, func(context.Context, error) {}, nil
	}

	// Validate state transition is allowed
	if !sandboxtypes.AllowedTransitions[sbx.State][newState] {
		return transition, false, nil, &sandboxtypes.InvalidStateTransitionError{CurrentState: sbx.State, TargetState: newState}
	}

	// Build the updated sandbox for Redis without mutating the original.
	// This ensures that on failure the caller sees the pre-mutation state,
	updated := sbx
	updated.State = newState
	if opts.Action.Effect == sandboxtypes.TransitionExpires {
		now := time.Now()
		if !updated.IsExpired(now) {
			updated.EndTime = now
		}
	}

	newData, err := json.Marshal(updated)
	if err != nil {
		return transition, false, nil, fmt.Errorf("failed to marshal sandbox: %w", err)
	}

	// Generate transition ID
	transitionID := uuid.New().String()
	resultKey := getTransitionResultKey(teamID.String(), sandboxID, transitionID)

	// Use atomic Lua script to update sandbox and set transition key with UUID
	ttlSeconds := int(transitionKeyTTL.Seconds())
	resultTtlSeconds := int(transitionResultKeyTTL.Seconds())

	eidKey := getSandboxExecutionIDKey(teamID.String(), sandboxID)
	written, err := startTransitionScript.Run(ctx, s.redisClient,
		[]string{key, transitionKey, resultKey, eidKey},
		newData, transitionID, ttlSeconds, resultTtlSeconds, opts.ExpectExecutionID, updated.ExecutionID,
	).Int64()
	if err != nil {
		return transition, false, nil, fmt.Errorf("failed to update sandbox state: %w", err)
	}
	if written == 0 {
		// The pin no longer holds. Add is lockless, so the incarnation can have
		// been replaced since the check above; the script refused rather than
		// overwrite whatever is there now, and nothing has been written.
		return transition, false, nil, fmt.Errorf(
			"sandbox %q is no longer execution %q: %w",
			sandboxID, opts.ExpectExecutionID, sandboxtypes.ErrExecutionMismatch,
		)
	}

	logger.L().Debug(ctx, "Started state transition", logger.WithSandboxID(sandboxID), zap.String("state", string(newState)), zap.String("transitionID", transitionID))

	transition.Sandbox = updated
	transition.OriginalEndTime = &sbx.EndTime

	return transition, false, s.createCallback(teamID, sandboxID, transitionKey, resultKey, transitionID, opts.Action), nil
}

// createCallback returns a callback function for completing a transition.
// For transient actions, it first restores the sandbox state to Running.
// On success, the callback deletes the transition key and sets empty result.
// On error, the callback deletes the transition key and sets error message in result.
func (s *Storage) createCallback(teamID uuid.UUID, sandboxID, transitionKey, resultKey, transitionID string, stateAction sandboxtypes.StateAction) func(context.Context, error) {
	return func(cbCtx context.Context, cbErr error) {
		logger.L().Debug(cbCtx, "Transition complete", logger.WithSandboxID(sandboxID), zap.String("state", string(stateAction.TargetState)), zap.String("transitionID", transitionID), zap.Error(cbErr))

		var restoreErr error
		if stateAction.Effect == sandboxtypes.TransitionTransient && cbErr == nil {
			restoreErr = s.restoreToRunning(cbCtx, teamID, sandboxID, stateAction.TargetState)
		}

		// Determine result value for waiters:
		// - Restore failure: propagate so callers know state is inconsistent
		// - Transient original failure: signal success so concurrent ops (e.g. kill) can proceed
		// - Non-transient failure: propagate the error
		resultValue := ""
		if restoreErr != nil {
			resultValue = fmt.Errorf("failed to restore sandbox to running: %w", restoreErr).Error()
		} else if cbErr != nil && stateAction.Effect != sandboxtypes.TransitionTransient {
			resultValue = cbErr.Error()
		}

		// Publish the result before waiters can observe completion; expired owners cannot delete a successor's key.
		err := finishTransitionScript.Run(cbCtx, s.redisClient, []string{transitionKey, resultKey},
			transitionID, resultValue, int(transitionResultKeyTTL.Seconds())).Err()
		if err != nil {
			logger.L().Warn(cbCtx, "Failed to finish state transition", logger.WithSandboxID(sandboxID), zap.String("transitionID", transitionID), zap.Error(err))
		}

		// Notify subscribers that the transition is complete so waitForTransition
		// goroutines wake up immediately rather than waiting for the next poll tick.
		// The routing key is published as the payload so the single global channel
		// can serve all sandboxes across all teams. The publish is handed off to
		// the shared publisher worker; on overflow the 1s fallback ticker in
		// waitForTransition covers any dropped notification.
		s.publisher.Publish(cbCtx, getTransitionRoutingKey(teamID.String(), sandboxID, transitionID))
	}
}

// restoreToRunning restores the sandbox to Running if it is still in the given transient state.
func (s *Storage) restoreToRunning(ctx context.Context, teamID uuid.UUID, sandboxID string, fromState sandboxtypes.State) error {
	_, err := s.Update(ctx, teamID, sandboxID, func(sbx sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
		if sbx.State != fromState {
			return sbx, nil
		}

		sbx.State = sandboxtypes.StateRunning

		return sbx, nil
	})

	return err
}

func (s *Storage) RestoreRunning(ctx context.Context, transition sandboxtypes.StateTransition, retryAfter time.Duration) (sandboxtypes.Sandbox, error) {
	if transition.OriginalEndTime == nil {
		return sandboxtypes.Sandbox{}, errors.New("transition has no rollback expiry")
	}
	sbx := transition.Sandbox
	teamID, sandboxID := sbx.TeamID, sbx.SandboxID
	key := getSandboxKey(teamID.String(), sandboxID)
	lock, err := s.locker.Obtain(ctx, redis_utils.GetLockKey(key), lockTimeout)
	if err != nil {
		return sandboxtypes.Sandbox{}, fmt.Errorf("failed to obtain restore lock: %w", err)
	}
	defer func() {
		if err := lock.Release(context.WithoutCancel(ctx)); err != nil {
			logger.L().Error(ctx, "Failed to release restore lock", zap.Error(err))
		}
	}()

	data, err := s.redisClient.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return sandboxtypes.Sandbox{}, sandboxtypes.ErrNotFound
	}
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	var restored sandboxtypes.Sandbox
	if err := json.Unmarshal(data, &restored); err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	if restored.ExecutionID != sbx.ExecutionID {
		return sandboxtypes.Sandbox{}, sandboxtypes.ErrExecutionMismatch
	}
	if restored.State != sbx.State {
		return sandboxtypes.Sandbox{}, fmt.Errorf("sandbox is in state %q, not %q", restored.State, sbx.State)
	}

	restored.State = sandboxtypes.StateRunning
	restored.EndTime = *transition.OriginalEndTime
	if retryAfter > 0 {
		now := time.Now()
		restored.RefusedSince = restored.RefusalEpisodeStart(now)
		restored.RefusedUntil = now.Add(retryAfter)
	}
	updated, err := json.Marshal(restored)
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	written, err := restoreSandboxScript.Run(ctx, s.redisClient, []string{key}, data, updated).Int64()
	if err != nil {
		return sandboxtypes.Sandbox{}, fmt.Errorf("failed to restore sandbox: %w", err)
	}
	if written == 0 {
		return sandboxtypes.Sandbox{}, sandboxtypes.ErrRestoreConflict
	}

	// The retry window only ever moves a member forward: an expired sandbox
	// leaves the sweep's window until the retry, a live one keeps its expiry.
	if !restored.RefusedUntil.After(restored.EndTime) {
		return restored, nil
	}

	if err := s.redisClient.ZAdd(ctx, globalExpirationSet, redis.Z{
		Score:  float64(restored.RefusedUntil.UnixMilli()),
		Member: sandboxExpirationMember(restored),
	}).Err(); err != nil {
		// RefusedUntil on the committed record still holds eviction until the retry.
		logger.L().Warn(ctx, "Failed to index restored sandbox retry window", zap.Error(err), logger.WithSandboxID(sandboxID))
	}

	return restored, nil
}

// WaitForStateChange waits for a sandbox state transition to complete.
func (s *Storage) WaitForStateChange(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	transitionKey := getTransitionKey(teamID.String(), sandboxID)
	transactionID, err := s.redisClient.Get(ctx, transitionKey).Result()
	if errors.Is(err, redis.Nil) {
		logger.L().Debug(ctx, "No ongoing transition", logger.WithSandboxID(sandboxID))

		return nil
	} else if err != nil {
		return fmt.Errorf("failed to check transition key: %w", err)
	}

	return s.waitForTransition(ctx, teamID, sandboxID, transactionID)
}

// waitForTransition waits for a specific transition to complete.
// It should receive a signal via the fan-out PubSub channel or fallback to a 1-second ticker
func (s *Storage) waitForTransition(
	ctx context.Context,
	teamID uuid.UUID,
	sandboxID,
	transitionID string,
) error {
	routingKey := getTransitionRoutingKey(teamID.String(), sandboxID, transitionID)
	transitionKey := getTransitionKey(teamID.String(), sandboxID)
	resultKey := getTransitionResultKey(teamID.String(), sandboxID, transitionID)

	// Subscribe to this specific transition's routing key so notifications
	// from other transitions for the same sandbox cannot wake us.
	ch, cleanup := s.subManager.subscribe(routingKey)
	defer cleanup()

	// Initial check: the transition may have completed before we subscribed.
	currentID, err := s.redisClient.Get(ctx, transitionKey).Result()
	if errors.Is(err, redis.Nil) || currentID != transitionID {
		return s.checkTransitionResult(ctx, resultKey)
	}
	if err != nil {
		return fmt.Errorf("failed to check transition key: %w", err)
	}

	// 1-second fallback ticker in case a PubSub message is missed.
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			return s.checkTransitionResult(ctx, resultKey)
		case <-ticker.C:
			// Fallback poll: check whether the transition key is still present.
			currentID, err := s.redisClient.Get(ctx, transitionKey).Result()
			if errors.Is(err, redis.Nil) || currentID != transitionID {
				return s.checkTransitionResult(ctx, resultKey)
			}
			if err != nil {
				return fmt.Errorf("failed to check transition key: %w", err)
			}
		}
	}
}

// checkTransitionResult checks the result key for the outcome of a completed transition.
func (s *Storage) checkTransitionResult(ctx context.Context, resultKey string) error {
	result, err := s.redisClient.Get(ctx, resultKey).Result()
	if errors.Is(err, redis.Nil) {
		// Result expired or never set - assume success (transition key was deleted)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to check transition result: %w", err)
	}

	if result == sandboxtypes.ErrTransitionRestored.Error() {
		return sandboxtypes.ErrTransitionRestored
	}

	if result != "" {
		return fmt.Errorf("transition failed: %s", result)
	}

	return nil
}

// handleExistingTransition waits out a transition that is already in flight and
// then either joins its result or retries the caller's own removal.
//
// It takes the whole RemoveOpts rather than the action alone because the retry
// is the caller's removal, attempted again: anything it drops is a caller
// instruction silently discarded across the wait. That has bitten
// FilesystemOnly (a filesystem-only pause retrying as a full memory snapshot)
// and Reason (a kill reason arriving as "unknown"), and matters most for
// ExpectExecutionID, since the wait is exactly the window in which the sandbox
// can be resumed under a new incarnation.
func (s *Storage) handleExistingTransition(
	ctx context.Context,
	teamID uuid.UUID,
	sbx sandboxtypes.Sandbox,
	opts sandboxtypes.RemoveOpts,
	transactionID string,
) (sandboxtypes.StateTransition, bool, func(context.Context, error), error) {
	newState := opts.Action.TargetState
	transition := sandboxtypes.StateTransition{Sandbox: sbx}

	if sbx.State == newState {
		// Same target state - wait for completion and return alreadyDone=true.
		// The caller inherits the in-flight transition's result without
		// doing the work itself: this is a joiner.
		joined.Mark(ctx)

		logger.L().Debug(ctx, "State transition already in progress to the same state, waiting",
			logger.WithSandboxID(sbx.SandboxID),
			zap.String("state", string(newState)))

		err := s.waitForTransition(ctx, teamID, sbx.SandboxID, transactionID)
		if errors.Is(err, sandboxtypes.ErrTransitionRestored) {
			// The pause the joiner rode was refused and the sandbox is running
			// again: the joiner gets the same retryable refusal, not a success.
			return transition, false, nil, sandboxtypes.PauseQueueExhaustedError{}
		}
		if err != nil {
			return transition, false, nil, fmt.Errorf("failed waiting for transition: %w", err)
		}

		return transition, true, func(context.Context, error) {}, nil
	}

	// Different state - validate transition and wait
	if !sandboxtypes.AllowedTransitions[sbx.State][newState] {
		return transition, false, nil, &sandboxtypes.InvalidStateTransitionError{CurrentState: sbx.State, TargetState: newState}
	}

	err := s.waitForTransition(ctx, teamID, sbx.SandboxID, transactionID)
	if err != nil && !errors.Is(err, sandboxtypes.ErrTransitionRestored) {
		return transition, false, nil, fmt.Errorf("failed waiting for transition: %w", err)
	}

	// Retry the caller's removal, in full, now the way is clear. Re-reading the
	// record under the lock is what re-checks the pin against whatever the
	// sandbox became while we waited.
	return s.StartRemoving(ctx, teamID, sbx.SandboxID, opts)
}
