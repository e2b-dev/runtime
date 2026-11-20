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

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

// BeginRemoving starts a state transition for a sandbox using atomic Lua scripts.
//
// The function handles concurrent requests safely:
//  1. Acquires the sandbox's lock
//  2. Checks if there's an ongoing transition via the transition key:
//     - If one is in progress, releases the lock and returns its ID as
//     InFlightID once the request is checked against it, without waiting
//     - If no transition is in progress, start the transition
//  3. Validates the state transition is allowed
//  4. Atomically updates the sandbox state and sets the transition key with a unique ID
//  5. Returns the transition with its ID, which the caller must end with
//     FinishTransition
//
// A record of another team is not found, as in Get, before the lock is
// taken.
func (s *Storage) BeginRemoving(ctx context.Context, teamID uuid.UUID, sandboxID string, opts sandboxtypes.RemoveOpts) (sandboxtypes.StateTransition, bool, error) {
	if err := s.refuseOtherTeam(ctx, teamID.String(), sandboxID); err != nil {
		return sandboxtypes.StateTransition{}, false, err
	}
	key := recordKey(sandboxID)
	transitionKey := transitionKeyFor(sandboxID)
	lockKey := redis_utils.GetLockKey(key)

	// Acquire distributed lock
	lock, err := s.locker.Obtain(ctx, lockKey, lockTimeout)
	if err != nil {
		return sandboxtypes.StateTransition{}, false, fmt.Errorf("failed to obtain lock: %w", err)
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

	_, sbx, err := s.readRecord(ctx, key, teamID, sandboxID)
	if err != nil {
		return sandboxtypes.StateTransition{}, false, err
	}
	transition := sandboxtypes.StateTransition{Sandbox: sbx}

	// Fast path for a caller-pinned removal: refuse a stale one before doing any
	// further work, and before the caller possibly waits out an unrelated
	// transition only to be refused at the end of it.
	//
	// This is not where the pin is enforced. Add takes this lock too, so only
	// a lock expired under a stalled call lets a resume install a new
	// incarnation between this comparison and the write. The authority is the
	// equivalent check inside startTransitionScript, which is atomic with that
	// write.
	if opts.ExpectExecutionID != "" && sbx.ExecutionID != opts.ExpectExecutionID {
		return transition, false, fmt.Errorf(
			"sandbox %q is execution %q, expected %q: %w",
			sandboxID, sbx.ExecutionID, opts.ExpectExecutionID, sandboxtypes.ErrExecutionMismatch,
		)
	}

	// A held record is refused before any wait: the hold lasts as long as its
	// operation, not a transition. Hold refuses a record in transition, so
	// one never starts under the other.
	if sbx.Held(time.Now()) {
		return transition, false, fmt.Errorf("sandbox %q: %w", sandboxID, sandboxtypes.ErrLifecycleHeld)
	}

	// Check if there's an existing transition
	transactionID, err := s.redisClient.Get(ctx, transitionKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return transition, false, fmt.Errorf("failed to check transition key: %w", err)
	}

	// Resolve eviction under the lock + re-check expiry
	if opts.Eviction {
		// if there's a transition already in place, don't do anything
		if transactionID != "" {
			return transition, false, sandboxtypes.ErrEvictionInProgress
		}

		// if sandbox isn't expired (e.g. race condition with SetTimeout)
		if !opts.BeforeEndTime && !sbx.IsExpired(time.Now()) {
			return transition, false, sandboxtypes.ErrEvictionNotNeeded
		}
	}

	newState := opts.Action.TargetState

	if transactionID != "" {
		releaseErr := releaseFunc()
		if releaseErr != nil {
			logger.L().Warn(ctx, "Failed to release lock before waiting", zap.Error(releaseErr))
		}

		if sbx.State == newState {
			// A pause that requires a healthy guest cannot ride a pause in flight,
			// whose snapshot may be taken over an unresponsive one: it is answered
			// as a conflicting transition and retried once the sandbox is paused.
			// (The sweep never gets here: a transition in flight ends its attempt
			// above.)
			if opts.Action == sandboxtypes.StateActionPause && opts.RequireEnvdHealthy {
				return transition, false, &sandboxtypes.InvalidStateTransitionError{CurrentState: sbx.State, TargetState: newState}
			}
		} else if !sandboxtypes.AllowedTransitions[sbx.State][newState] {
			return transition, false, &sandboxtypes.InvalidStateTransitionError{CurrentState: sbx.State, TargetState: newState}
		}
		transition.InFlightID = transactionID

		return transition, false, nil
	}

	// Check if already in target state
	if sbx.State == newState {
		logger.L().Debug(ctx, "Already in the same state", logger.WithSandboxID(sandboxID), zap.String("state", string(newState)))

		return transition, true, nil
	}

	// Validate state transition is allowed
	if !sandboxtypes.AllowedTransitions[sbx.State][newState] {
		return transition, false, &sandboxtypes.InvalidStateTransitionError{CurrentState: sbx.State, TargetState: newState}
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
		return transition, false, fmt.Errorf("failed to marshal sandbox: %w", err)
	}

	// Generate transition ID
	transitionID := uuid.New().String()
	resultKey := transitionResultKeyFor(sandboxID, transitionID)

	// Use atomic Lua script to update sandbox and set transition key with UUID
	ttlSeconds := int(transitionKeyTTL.Seconds())
	resultTtlSeconds := int(transitionResultKeyTTL.Seconds())

	written, err := RunScript(ctx, s.redisClient, "start_transition", startTransitionScript,
		[]string{key, transitionKey, resultKey, lock.Key()},
		newData, transitionID, ttlSeconds, resultTtlSeconds, opts.ExpectExecutionID, lockValueOf(lock),
	).Int64()
	if err != nil {
		return transition, false, fmt.Errorf("failed to update sandbox state: %w", err)
	}
	if written == 3 {
		return transition, false, fmt.Errorf("sandbox %q: %w", sandboxID, sandboxtypes.ErrLockLost)
	}
	if written == 2 {
		return transition, false, fmt.Errorf("sandbox %q: %w", sandboxID, sandboxtypes.ErrLifecycleHeld)
	}
	if written == 0 {
		// The pin no longer holds: the incarnation was replaced since the check
		// above, past a lock that expired; the script refused rather than
		// overwrite whatever is there now, and nothing has been written.
		return transition, false, fmt.Errorf(
			"sandbox %q is no longer execution %q: %w",
			sandboxID, opts.ExpectExecutionID, sandboxtypes.ErrExecutionMismatch,
		)
	}

	logger.L().Debug(ctx, "Started state transition", logger.WithSandboxID(sandboxID), zap.String("state", string(newState)), zap.String("transition_id", transitionID))

	transition.Sandbox = updated
	transition.OriginalEndTime = &sbx.EndTime
	transition.ID = transitionID

	return transition, false, nil
}

// FinishTransition ends the transition BeginRemoving started.
// A successful transient action restores Running unless the outcome is
// SkipRestore. Completing a transient action with an error also skips
// restoration and lets waiters proceed. Waiters must read the resulting state.
// Automatic restore failures and non-transient errors are published to waiters.
func (s *Storage) FinishTransition(ctx context.Context, teamID uuid.UUID, sandboxID, transitionID string, stateAction sandboxtypes.StateAction, outcome sandboxtypes.TransitionOutcome) error {
	transitionKey := transitionKeyFor(sandboxID)
	resultKey := transitionResultKeyFor(sandboxID, transitionID)

	var outcomeErr error
	if outcome.Failed {
		outcomeErr = errors.New(outcome.Error)
	}
	logger.L().Debug(ctx, "Transition complete", logger.WithSandboxID(sandboxID), zap.String("state", string(stateAction.TargetState)), zap.String("transition_id", transitionID), zap.Error(outcomeErr))

	var restoreErr error
	if stateAction.Effect == sandboxtypes.TransitionTransient && !outcome.Failed && !outcome.SkipRestore {
		restoreErr = s.restoreToRunning(ctx, teamID, sandboxID, stateAction.TargetState)
		if restoreErr != nil {
			restoreErr = fmt.Errorf("failed to restore sandbox to running: %w", restoreErr)
		}
	}

	// Determine result value for waiters:
	// - Restore failure: propagate so callers know state is inconsistent
	// - Transient original failure: signal success so concurrent ops (e.g. kill) can proceed
	// - Non-transient failure: propagate the error
	resultValue := ""
	if restoreErr != nil {
		resultValue = restoreErr.Error()
	} else if outcome.Failed && stateAction.Effect != sandboxtypes.TransitionTransient {
		resultValue = outcome.Error
	}

	// A restored pause's refusal class rides beside the result; the result
	// itself stays the text a replica that predates the class reads.
	refusalClass := ""
	if outcome.Restored {
		resultValue = sandboxtypes.ErrTransitionRestored.Error()
		refusalClass = outcome.RefusalClass
	}

	// Publish the result before waiters can observe completion; expired owners cannot delete a successor's key.
	err := RunScript(ctx, s.redisClient, "finish_transition", finishTransitionScript, []string{transitionKey, resultKey, refusalClassKey(resultKey)},
		transitionID, resultValue, int(transitionResultKeyTTL.Seconds()), refusalClass).Err()
	if err != nil {
		err = fmt.Errorf("failed to run finish-transition script: %w", err)
	}

	// Notify subscribers that the transition is complete so waitForTransition
	// goroutines wake up immediately rather than waiting for the next poll tick.
	// The routing key is published as the payload so the single global channel
	// can serve all sandboxes across all teams. The publish is handed off to
	// the shared publisher worker; on overflow the 1s fallback ticker in
	// waitForTransition covers any dropped notification.
	s.publisher.Publish(ctx, transitionRoutingKey(resultKey))

	return errors.Join(restoreErr, err)
}

// restoreToRunning restores the sandbox to Running if it is still in the given transient state.
func (s *Storage) restoreToRunning(ctx context.Context, teamID uuid.UUID, sandboxID string, fromState sandboxtypes.State) error {
	_, err := sandboxtypes.UpdateRecord(ctx, s, teamID, sandboxID, func(sbx sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
		if sbx.State != fromState {
			return sbx, nil
		}

		sbx.State = sandboxtypes.StateRunning

		return sbx, nil
	})

	return err
}

func (s *Storage) RestoreRunning(ctx context.Context, transition sandboxtypes.StateTransition, opts sandboxtypes.RestoreOpts) (sandboxtypes.Sandbox, error) {
	restored, err := s.restoreRunning(ctx, transition, func(restored *sandboxtypes.Sandbox) {
		if opts.RetryAfter > 0 {
			now := time.Now()
			episodeStart := time.Time{}
			if !opts.HoldOnly {
				episodeStart = restored.RefusalEpisodeStart(now)
			}
			restored.RefusedSince = episodeStart
			if opts.EnvdRefusal {
				restored.EnvdRefusedSince = restored.EnvdRefusalEpisodeStart(now)
				restored.EnvdRefusedUntil = now.Add(opts.RetryAfter)
			}
			restored.RefusedUntil = now.Add(opts.RetryAfter)
		}
	})
	if err != nil {
		return sandboxtypes.Sandbox{}, err
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
		logger.L().Warn(ctx, "Failed to index restored sandbox retry window", zap.Error(err), logger.WithSandboxID(restored.SandboxID))
	}

	return restored, nil
}

func (s *Storage) RestoreCheckpointed(ctx context.Context, transition sandboxtypes.StateTransition, placement *orchestrator.SandboxNetworkPlacement) (sandboxtypes.Sandbox, error) {
	return s.restoreRunning(ctx, transition, func(restored *sandboxtypes.Sandbox) {
		restored.NetworkPlacement = placement
	})
}

// restoreRunning puts the transition's record back to Running with its
// pre-clamp expiry and change applied, if the record is still the
// transition's execution in the transition's state, under the sandbox's
// lock. A record of another team is not found, as in Get, before the lock
// is taken. The write compares the whole record read under the lock, so a
// record replaced past an expired lock is left alone. A record rewritten
// under the compare is read again once, so a successor that landed
// meanwhile is reported as such, and ErrRestoreConflict only when the
// record keeps changing.
func (s *Storage) restoreRunning(ctx context.Context, transition sandboxtypes.StateTransition, change func(*sandboxtypes.Sandbox)) (_ sandboxtypes.Sandbox, err error) {
	if transition.OriginalEndTime == nil {
		return sandboxtypes.Sandbox{}, errors.New("transition has no rollback expiry")
	}
	if err := s.refuseOtherTeam(ctx, transition.Sandbox.TeamID.String(), transition.Sandbox.SandboxID); err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	key := recordKey(transition.Sandbox.SandboxID)
	lock, err := s.locker.Obtain(ctx, redis_utils.GetLockKey(key), lockTimeout)
	if err != nil {
		return sandboxtypes.Sandbox{}, fmt.Errorf("failed to obtain restore lock: %w", err)
	}
	defer releaseLock(ctx, lock.Release, &err)

	restored, err := s.compareAndRestore(ctx, key, transition, change)
	if errors.Is(err, sandboxtypes.ErrRestoreConflict) {
		restored, err = s.compareAndRestore(ctx, key, transition, change)
	}

	return restored, err
}

// compareAndRestore is one read and compare-and-set of restoreRunning.
func (s *Storage) compareAndRestore(ctx context.Context, key string, transition sandboxtypes.StateTransition, change func(*sandboxtypes.Sandbox)) (sandboxtypes.Sandbox, error) {
	sbx := transition.Sandbox
	data, restored, err := s.readRecord(ctx, key, sbx.TeamID, sbx.SandboxID)
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	if restored.ExecutionID != sbx.ExecutionID {
		return sandboxtypes.Sandbox{}, sandboxtypes.ErrExecutionMismatch
	}
	if restored.State != sbx.State {
		return sandboxtypes.Sandbox{}, &sandboxtypes.InvalidStateTransitionError{
			CurrentState: restored.State,
			TargetState:  sandboxtypes.StateRunning,
		}
	}

	restored.State = sandboxtypes.StateRunning
	restored.EndTime = *transition.OriginalEndTime
	change(&restored)
	updated, err := json.Marshal(restored)
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	written, err := RunScript(ctx, s.redisClient, "restore_sandbox", restoreSandboxScript, []string{key}, data, updated).Int64()
	if err != nil {
		return sandboxtypes.Sandbox{}, fmt.Errorf("failed to restore sandbox: %w", err)
	}
	if written == 0 {
		return sandboxtypes.Sandbox{}, sandboxtypes.ErrRestoreConflict
	}

	return restored, nil
}

// WaitForTransition waits for the transition transitionID or, with an empty
// one, for the sandbox's pending transition, found by its marker, to
// complete. The key carries no team, so a record of another team is refused
// first, as in Get; without a record, as on the kill path, the transition
// alone is followed.
func (s *Storage) WaitForTransition(ctx context.Context, teamID uuid.UUID, sandboxID string, transitionID string) (string, error) {
	if err := s.refuseOtherTeam(ctx, teamID.String(), sandboxID); err != nil {
		return transitionID, err
	}
	if transitionID == "" {
		id, err := s.redisClient.Get(ctx, transitionKeyFor(sandboxID)).Result()
		if errors.Is(err, redis.Nil) {
			logger.L().Debug(ctx, "No ongoing transition", logger.WithSandboxID(sandboxID))

			return "", nil
		}
		if err != nil {
			return "", fmt.Errorf("failed to check transition key: %w", err)
		}
		transitionID = id
	}

	return transitionID, s.waitForTransition(ctx, sandboxID, transitionID)
}

// waitForTransition waits for a specific transition to complete. A wait
// holds the ID it found: once the transition finishes the marker is gone,
// or a successor's is there, so reading it again would follow the wrong
// result key. It should receive a signal via the fan-out PubSub channel or
// fallback to a 1-second ticker.
func (s *Storage) waitForTransition(ctx context.Context, sandboxID, transitionID string) error {
	transitionKey := transitionKeyFor(sandboxID)
	resultKey := transitionResultKeyFor(sandboxID, transitionID)
	routingKey := transitionRoutingKey(resultKey)

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

// refusalClassKey holds the refusal class of a restored pause beside its
// transition result, for the result's lifetime.
func refusalClassKey(resultKey string) string {
	return resultKey + ":refusal"
}

// checkTransitionResult checks the result key for the outcome of a completed
// transition, and the refusal class beside it, in one round trip.
func (s *Storage) checkTransitionResult(ctx context.Context, resultKey string) error {
	keys := []string{resultKey, refusalClassKey(resultKey)}
	assertSameSlot("transition_result", keys)
	values, err := s.redisClient.MGet(ctx, keys...).Result()
	if err != nil {
		recordRedisError(ctx, err)

		return fmt.Errorf("failed to check transition result: %w", err)
	}
	result, ok := values[0].(string)
	if !ok {
		// Result expired or never set - assume success (transition key was deleted)
		return nil
	}

	if result == sandboxtypes.ErrTransitionRestored.Error() {
		class, _ := values[1].(string)

		return &sandboxtypes.RestoredTransitionError{Refusal: sandboxtypes.RefusalFromClass(class)}
	}

	if result != "" {
		return &sandboxtypes.TransitionFailedError{Message: result}
	}

	return nil
}
