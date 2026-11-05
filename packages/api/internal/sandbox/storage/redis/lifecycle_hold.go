package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// HoldExecution marks the running execution as held until `until`, which must
// be in the future. The same hold again is a no-op and keeps its deadline. It
// refuses with sandboxtypes.ErrHoldConflict a record of another execution, not
// running, in a transition, or under a live hold of another operation. A
// record in a transition is refused because the transition may still restore
// or remove it when it finishes.
func (s *Storage) HoldExecution(ctx context.Context, hold sandboxtypes.Hold, until time.Time) error {
	if hold.ID == "" {
		return errors.New("empty hold ID")
	}
	if !until.After(time.Now()) {
		return fmt.Errorf("hold deadline %s already passed", until.Format(time.RFC3339Nano))
	}

	return s.withRecordLock(ctx, hold.TeamID, hold.SandboxID, func(form keyForm, lock *storageLock, sbx sandboxtypes.Sandbox) error {
		// Read again under the lock: the wait for it may have outlasted the
		// deadline, and a hold written past it would read as released.
		now := time.Now()
		if !until.After(now) {
			return fmt.Errorf("hold deadline %s passed while waiting for the record's lock", until.Format(time.RFC3339Nano))
		}
		switch {
		case sbx.ExecutionID != hold.ExecutionID:
			return sandboxtypes.ErrHoldConflict
		case sbx.LifecycleHold == hold.ID && sbx.Held(now):
			return nil
		case sbx.Held(now) || sbx.State != sandboxtypes.StateRunning:
			return sandboxtypes.ErrHoldConflict
		}
		// Under the record's lock, which every transition's start takes.
		inTransition, err := s.redisClient.Exists(ctx, transitionKeyFor(form, hold.TeamID.String(), hold.SandboxID)).Result()
		if err != nil {
			return fmt.Errorf("read the sandbox's transition: %w", err)
		}
		if inTransition > 0 {
			return sandboxtypes.ErrHoldConflict
		}
		sbx.LifecycleHold, sbx.LifecycleHoldUntil = hold.ID, until.UnixMilli()

		return s.writeHeld(ctx, form, lock, sbx)
	})
}

// MoveHeldExecution changes the held execution's node to nodeID and releases
// the hold in one write, and reports whether it wrote. A repeat after that
// write finds the execution on nodeID and not under this hold, and succeeds
// without writing: the API never changes the node of a running execution
// otherwise, so that record is this hold's own earlier move. It refuses with
// sandboxtypes.ErrNotHeld a record of this execution not under this hold,
// sandboxtypes.ErrExecutionMismatch a record of another execution and
// sandboxtypes.ErrNotFound a record gone.
func (s *Storage) MoveHeldExecution(ctx context.Context, hold sandboxtypes.Hold, nodeID string) (bool, error) {
	moved := false
	err := s.withHeldRecord(ctx, hold, func(form keyForm, lock *storageLock, sbx sandboxtypes.Sandbox, held bool) error {
		switch {
		case !held && sbx.NodeID == nodeID:
			return nil
		case !held:
			return sandboxtypes.ErrNotHeld
		}
		sbx.NodeID = nodeID
		sbx.LifecycleHold, sbx.LifecycleHoldUntil = "", 0
		if err := s.writeHeld(ctx, form, lock, sbx); err != nil {
			return err
		}
		moved = true

		return nil
	})

	return moved, err
}

// ReleaseHeldExecution clears the hold without moving the record;
// sandboxtypes.ErrNotHeld when the record is not under this hold.
func (s *Storage) ReleaseHeldExecution(ctx context.Context, hold sandboxtypes.Hold) error {
	return s.withHeldRecord(ctx, hold, func(form keyForm, lock *storageLock, sbx sandboxtypes.Sandbox, held bool) error {
		if !held {
			return sandboxtypes.ErrNotHeld
		}
		sbx.LifecycleHold, sbx.LifecycleHoldUntil = "", 0

		return s.writeHeld(ctx, form, lock, sbx)
	})
}

// RemoveHeldExecution removes the held record and returns it as it was: no
// guest of this execution may run anywhere, and a node still running one
// finds it an orphan. Under the record's lock it removes the record, still
// held, through the form's remove path, so the gone set, the index entry and
// the expiration member go as Remove takes them. A removal that fails keeps
// the hold for a retry, and a retry after the record went finds
// sandboxtypes.ErrNotFound.
func (s *Storage) RemoveHeldExecution(ctx context.Context, hold sandboxtypes.Hold) (sandboxtypes.Sandbox, error) {
	var removed sandboxtypes.Sandbox
	err := s.withHeldRecord(ctx, hold, func(form keyForm, lock *storageLock, sbx sandboxtypes.Sandbox, held bool) error {
		if !held {
			return sandboxtypes.ErrNotHeld
		}
		if err := s.removeUnderLock(ctx, form, lock, hold.TeamID, hold.SandboxID, hold.ExecutionID, hold.ID); err != nil {
			return err
		}
		removed = sbx

		return nil
	})
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}

	return removed, nil
}

// withHeldRecord runs change on the hold's execution record under its lock,
// held reporting whether the hold is on it now. A record of another execution
// is sandboxtypes.ErrExecutionMismatch.
func (s *Storage) withHeldRecord(
	ctx context.Context,
	hold sandboxtypes.Hold,
	change func(form keyForm, lock *storageLock, sbx sandboxtypes.Sandbox, held bool) error,
) error {
	if hold.ID == "" {
		return errors.New("empty hold ID")
	}

	return s.withRecordLock(ctx, hold.TeamID, hold.SandboxID, func(form keyForm, lock *storageLock, sbx sandboxtypes.Sandbox) error {
		if sbx.ExecutionID != hold.ExecutionID {
			return fmt.Errorf("sandbox %q: %w", hold.SandboxID, sandboxtypes.ErrExecutionMismatch)
		}

		return change(form, lock, sbx, sbx.LifecycleHold == hold.ID && sbx.Held(time.Now()))
	})
}

// withRecordLock runs change on the sandbox's record under the lock of the
// form it is in, resolved and retried once as Update does when a resume
// moved the record to the other form meanwhile.
func (s *Storage) withRecordLock(ctx context.Context, teamID uuid.UUID, sandboxID string, change func(keyForm, *storageLock, sandboxtypes.Sandbox) error) error {
	team := teamID.String()
	form, err := s.resolveForm(ctx, team, sandboxID)
	if err != nil {
		return err
	}

	err = s.withRecordLockIn(ctx, form, teamID, sandboxID, change)
	if errors.Is(err, errAbsentUnderLock) {
		if form, err = s.formMovedTo(ctx, team, sandboxID, form); err != nil {
			return err
		}
		if err = s.withRecordLockIn(ctx, form, teamID, sandboxID, change); errors.Is(err, errAbsentUnderLock) {
			return fmt.Errorf("sandbox %q: %w", sandboxID, sandboxtypes.ErrNotFound)
		}
	}

	return err
}

// withRecordLockIn runs change on the record in form under that form's lock;
// errAbsentUnderLock when the form holds no record of the team.
func (s *Storage) withRecordLockIn(ctx context.Context, form keyForm, teamID uuid.UUID, sandboxID string, change func(keyForm, *storageLock, sandboxtypes.Sandbox) error) (err error) {
	key := recordKey(form, teamID.String(), sandboxID)
	lock, err := s.locker.Obtain(ctx, redis_utils.GetLockKey(key), lockTimeout)
	if err != nil {
		return fmt.Errorf("failed to obtain lock: %w", err)
	}
	defer releaseLock(ctx, lock.Release, &err)

	_, sbx, err := s.readRecord(ctx, key, teamID, sandboxID)
	if errors.Is(err, sandboxtypes.ErrNotFound) {
		return errAbsentUnderLock
	}
	if err != nil {
		return err
	}

	return change(form, lock, sbx)
}

// writeHeld writes sbx back to its record while lock still holds it.
func (s *Storage) writeHeld(ctx context.Context, form keyForm, lock *storageLock, sbx sandboxtypes.Sandbox) error {
	data, err := json.Marshal(sbx)
	if err != nil {
		return fmt.Errorf("failed to marshal sandbox: %w", err)
	}
	if err := s.setUnderLock(ctx, lock, recordKey(form, sbx.TeamID.String(), sbx.SandboxID), data); err != nil {
		return fmt.Errorf("failed to store sandbox in Redis: %w", err)
	}

	return nil
}
