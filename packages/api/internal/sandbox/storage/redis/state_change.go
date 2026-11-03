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
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/middleware/otel/joined"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// StartRemoving initiates a state transition for a sandbox using atomic Lua scripts.
//
// The function handles concurrent requests safely:
//  1. Resolves the form the record is in and acquires that form's lock on the sandbox
//  2. Checks if there's an ongoing transition via the transition key:
//     - If the same target state is in progress, waits for completion and returns the result
//     - If a different state is in progress, waits for it to complete, then retries
//     - If no transition is in progress, start the transition
//  3. Validates the state transition is allowed
//  4. Atomically updates the sandbox state and sets the transition key with a unique ID
//  5. Returns a callback that the caller must invoke to signal completion
//
// Every per-sandbox key is built from the resolved form. A record of another
// team is not found, as in Get: the sandbox-form key carries no team.
//
// The callback is critical: it deletes the transition key
// and sets the result value with short TTL to notify waiters of the outcome.
// A form found empty under its lock is resolved once more: the record a resume
// moved to the other form meanwhile is the live one, so the transition
// starts on it there, under its own lock; a pinned call still refuses it.
func (s *Storage) StartRemoving(ctx context.Context, teamID uuid.UUID, sandboxID string, opts sandboxtypes.RemoveOpts) (sandboxtypes.StateTransition, bool, func(context.Context, error), error) {
	team := teamID.String()
	form, err := s.resolveForm(ctx, team, sandboxID)
	if err != nil {
		return sandboxtypes.StateTransition{}, false, nil, err
	}

	transition, done, finish, err := s.startRemovingIn(ctx, form, teamID, sandboxID, opts)
	if errors.Is(err, errAbsentUnderLock) {
		if form, err = s.formMovedTo(ctx, team, sandboxID, form); err != nil {
			return sandboxtypes.StateTransition{}, false, nil, err
		}
		if transition, done, finish, err = s.startRemovingIn(ctx, form, teamID, sandboxID, opts); errors.Is(err, errAbsentUnderLock) {
			return sandboxtypes.StateTransition{}, false, nil, fmt.Errorf("sandbox %q: %w", sandboxID, sandboxtypes.ErrNotFound)
		}
	}

	return transition, done, finish, err
}

// startRemovingIn starts the transition on the record in form, under that
// form's lock; errAbsentUnderLock when the form holds no record of the team.
func (s *Storage) startRemovingIn(ctx context.Context, form keyForm, teamID uuid.UUID, sandboxID string, opts sandboxtypes.RemoveOpts) (sandboxtypes.StateTransition, bool, func(context.Context, error), error) {
	team := teamID.String()
	key := recordKey(form, team, sandboxID)
	transitionKey := transitionKeyFor(form, team, sandboxID)
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

	_, sbx, err := s.readRecord(ctx, key, teamID, sandboxID)
	if errors.Is(err, sandboxtypes.ErrNotFound) {
		return sandboxtypes.StateTransition{}, false, nil, errAbsentUnderLock
	}
	if err != nil {
		return sandboxtypes.StateTransition{}, false, nil, err
	}
	transition := sandboxtypes.StateTransition{Sandbox: sbx}

	// Fast path for a caller-pinned removal: refuse a stale one before doing any
	// further work, and before possibly waiting out an unrelated transition
	// below only to be refused at the end of it.
	//
	// This is not where the pin is enforced. Add takes this lock too, so only
	// a lock expired under a stalled call lets a resume install a new
	// incarnation between this comparison and the write. The authority is the
	// equivalent check inside startTransitionScript, which is atomic with that
	// write.
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
		if !opts.BeforeEndTime && !sbx.IsExpired(time.Now()) {
			return transition, false, nil, sandboxtypes.ErrEvictionNotNeeded
		}
	}

	newState := opts.Action.TargetState

	if transactionID != "" {
		releaseErr := releaseFunc()
		if releaseErr != nil {
			logger.L().Warn(ctx, "Failed to release lock before waiting", zap.Error(releaseErr))
		}

		return s.handleExistingTransition(ctx, teamID, sbx, opts, transitionRef{form: form, id: transactionID})
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
	resultKey := transitionResultKeyFor(form, team, sandboxID, transitionID)

	// Use atomic Lua script to update sandbox and set transition key with UUID
	ttlSeconds := int(transitionKeyTTL.Seconds())
	resultTtlSeconds := int(transitionResultKeyTTL.Seconds())

	written, err := RunScript(ctx, s.redisClient, "start_transition", startTransitionScript,
		[]string{key, transitionKey, resultKey},
		newData, transitionID, ttlSeconds, resultTtlSeconds, opts.ExpectExecutionID,
	).Int64()
	if err != nil {
		return transition, false, nil, fmt.Errorf("failed to update sandbox state: %w", err)
	}
	if written == 0 {
		// The pin no longer holds: the incarnation was replaced since the check
		// above, past a lock that expired; the script refused rather than
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
// A successful transient action restores Running unless the caller passes
// ErrSkipRestore. Completing a transient action with an error also skips
// restoration and lets waiters proceed. Waiters must read the resulting state.
// Automatic restore failures and non-transient errors are published to waiters.
func (s *Storage) createCallback(teamID uuid.UUID, sandboxID, transitionKey, resultKey, transitionID string, stateAction sandboxtypes.StateAction) func(context.Context, error) {
	return func(cbCtx context.Context, cbErr error) {
		skipRestore := errors.Is(cbErr, sandboxtypes.ErrSkipRestore)
		if skipRestore {
			cbErr = nil
		}
		logger.L().Debug(cbCtx, "Transition complete", logger.WithSandboxID(sandboxID), zap.String("state", string(stateAction.TargetState)), zap.String("transitionID", transitionID), zap.Error(cbErr))

		var restoreErr error
		if stateAction.Effect == sandboxtypes.TransitionTransient && cbErr == nil && !skipRestore {
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

		// A restored pause's refusal class rides beside the result; the result
		// itself stays the text a replica that predates the class reads.
		refusalClass := ""
		if restored, ok := errors.AsType[*sandboxtypes.RestoredTransitionError](cbErr); ok {
			resultValue = sandboxtypes.ErrTransitionRestored.Error()
			refusalClass = sandboxtypes.RefusalClass(restored.Refusal)
		}

		// Publish the result before waiters can observe completion; expired owners cannot delete a successor's key.
		err := RunScript(cbCtx, s.redisClient, "finish_transition", finishTransitionScript, []string{transitionKey, resultKey, refusalClassKey(resultKey)},
			transitionID, resultValue, int(transitionResultKeyTTL.Seconds()), refusalClass).Err()
		if err != nil {
			logger.L().Warn(cbCtx, "Failed to finish state transition", logger.WithSandboxID(sandboxID), zap.String("transitionID", transitionID), zap.Error(err))
		}

		// Notify subscribers that the transition is complete so waitForTransition
		// goroutines wake up immediately rather than waiting for the next poll tick.
		// The routing key is published as the payload so the single global channel
		// can serve all sandboxes across all teams. The publish is handed off to
		// the shared publisher worker; on overflow the 1s fallback ticker in
		// waitForTransition covers any dropped notification.
		s.publisher.Publish(cbCtx, transitionRoutingKey(resultKey))
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
// transition's execution in the transition's state, under the lock of the
// form the record is in, resolved before the lock. The write compares the
// whole record read under the lock, so a record replaced past an expired
// lock is left alone. A record rewritten under the compare is read again
// once, so a successor that landed meanwhile is reported as such, and
// ErrRestoreConflict only when the record keeps changing. A form found
// empty under its lock is checked against the other form, as Remove does:
// a record there is a resume's successor, reported as a mismatch.
func (s *Storage) restoreRunning(ctx context.Context, transition sandboxtypes.StateTransition, change func(*sandboxtypes.Sandbox)) (_ sandboxtypes.Sandbox, err error) {
	if transition.OriginalEndTime == nil {
		return sandboxtypes.Sandbox{}, errors.New("transition has no rollback expiry")
	}
	team := transition.Sandbox.TeamID.String()
	form, err := s.resolveForm(ctx, team, transition.Sandbox.SandboxID)
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	key := recordKey(form, team, transition.Sandbox.SandboxID)
	lock, err := s.locker.Obtain(ctx, redis_utils.GetLockKey(key), lockTimeout)
	if err != nil {
		return sandboxtypes.Sandbox{}, fmt.Errorf("failed to obtain restore lock: %w", err)
	}
	defer releaseLock(ctx, lock.Release, &err)

	restored, err := s.compareAndRestore(ctx, key, transition, change)
	if errors.Is(err, sandboxtypes.ErrRestoreConflict) {
		restored, err = s.compareAndRestore(ctx, key, transition, change)
	}
	if errors.Is(err, sandboxtypes.ErrNotFound) {
		// An Add holds both form locks, so a record in the other form under
		// this lock is a completed resume's successor, not this execution.
		_, moveErr := s.formMovedTo(ctx, team, transition.Sandbox.SandboxID, form)
		switch {
		case moveErr == nil:
			return sandboxtypes.Sandbox{}, fmt.Errorf("sandbox %q: %w", transition.Sandbox.SandboxID, sandboxtypes.ErrExecutionMismatch)
		case !errors.Is(moveErr, sandboxtypes.ErrNotFound):
			return sandboxtypes.Sandbox{}, moveErr
		}
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

// WaitForStateChange waits for a sandbox state transition to complete. The
// transition marker names the form. A record that exists in either form is
// waited on in that form only: no other form's marker can be the live
// record's, and a stale one, left by a crashed transition until its TTL, is
// not joined. Without a record, as on the kill path, the sandbox form's
// marker is probed first, then the team form's. A sandbox-form record of
// another team is not found by the probe, as in Get, so its transition is
// never joined. Past the probe the wait takes no tenant guard: without a
// record the sandbox form's marker carries no team, and every caller has
// read the record as the team's before it waits.
func (s *Storage) WaitForStateChange(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	team := teamID.String()
	recordForm, exists, err := s.formOfExisting(ctx, team, sandboxID)
	if err != nil {
		return err
	}
	forms := []keyForm{formSandbox, formTeam}
	if exists {
		forms = []keyForm{recordForm}
	}

	for _, form := range forms {
		id, err := s.redisClient.Get(ctx, transitionKeyFor(form, team, sandboxID)).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to check transition key: %w", err)
		}

		return s.waitForTransition(ctx, teamID, sandboxID, transitionRef{form: form, id: id})
	}

	logger.L().Debug(ctx, "No ongoing transition", logger.WithSandboxID(sandboxID))

	return nil
}

// transitionRef names a transition by the form its keys are in and its ID.
// A wait holds the pair it found: once the transition finishes neither
// marker exists, or a successor's does under the other form, so resolving
// again would read the wrong result key.
type transitionRef struct {
	form keyForm
	id   string
}

// waitForTransition waits for a specific transition to complete.
// It should receive a signal via the fan-out PubSub channel or fallback to a 1-second ticker
func (s *Storage) waitForTransition(ctx context.Context, teamID uuid.UUID, sandboxID string, ref transitionRef) error {
	team := teamID.String()
	transitionKey := transitionKeyFor(ref.form, team, sandboxID)
	resultKey := transitionResultKeyFor(ref.form, team, sandboxID, ref.id)
	routingKey := transitionRoutingKey(resultKey)

	// Subscribe to this specific transition's routing key so notifications
	// from other transitions for the same sandbox cannot wake us.
	ch, cleanup := s.subManager.subscribe(routingKey)
	defer cleanup()

	// Initial check: the transition may have completed before we subscribed.
	currentID, err := s.redisClient.Get(ctx, transitionKey).Result()
	if errors.Is(err, redis.Nil) || currentID != ref.id {
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
			if errors.Is(err, redis.Nil) || currentID != ref.id {
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
	ref transitionRef,
) (sandboxtypes.StateTransition, bool, func(context.Context, error), error) {
	newState := opts.Action.TargetState
	transition := sandboxtypes.StateTransition{Sandbox: sbx}

	if sbx.State == newState {
		// A pause that requires a healthy guest cannot ride a pause in flight,
		// whose snapshot may be taken over an unresponsive one: it is answered
		// as a conflicting transition and retried once the sandbox is paused.
		// (The sweep never gets here: a transition in flight ends its attempt
		// above.)
		if opts.Action == sandboxtypes.StateActionPause && opts.RequireEnvdHealthy {
			return transition, false, nil, &sandboxtypes.InvalidStateTransitionError{CurrentState: sbx.State, TargetState: newState}
		}

		// Same target state - wait for completion and return alreadyDone=true.
		// The caller inherits the in-flight transition's result without
		// doing the work itself: this is a joiner.
		joined.Mark(ctx)

		logger.L().Debug(ctx, "State transition already in progress to the same state, waiting",
			logger.WithSandboxID(sbx.SandboxID),
			zap.String("state", string(newState)))

		err := s.waitForTransition(ctx, teamID, sbx.SandboxID, ref)
		if restored, ok := errors.AsType[*sandboxtypes.RestoredTransitionError](err); ok {
			// The pause the joiner rode was refused as one the guest cannot take
			// and the sandbox is running again: a joiner asking for the same
			// kind never succeeds; a memory pause that joined it may retry.
			if errors.Is(restored.Refusal, sandboxtypes.PauseUnsupportedError{}) && !opts.FilesystemOnly {
				return transition, false, nil, sandboxtypes.PauseQueueExhaustedError{}
			}

			// The pause the joiner rode was refused and the sandbox is running
			// again: the joiner gets the refusal the starter got, not a success.
			return transition, false, nil, restored.Refusal
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

	err := s.waitForTransition(ctx, teamID, sbx.SandboxID, ref)
	if err != nil && !errors.Is(err, sandboxtypes.ErrTransitionRestored) {
		return transition, false, nil, fmt.Errorf("failed waiting for transition: %w", err)
	}

	// Retry the caller's removal, in full, now the way is clear. Re-reading the
	// record under the lock is what re-checks the pin against whatever the
	// sandbox became while we waited.
	return s.StartRemoving(ctx, teamID, sbx.SandboxID, opts)
}
