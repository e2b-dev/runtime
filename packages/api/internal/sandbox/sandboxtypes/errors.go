package sandboxtypes

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

type LimitExceededError struct {
	TeamID uuid.UUID
}

func (e *LimitExceededError) Error() string {
	return fmt.Sprintf("team %s has exceeded the limit", e.TeamID.String())
}

var ErrNotFound = errors.New("sandbox not found")

type InvalidStateTransitionError struct {
	CurrentState State
	TargetState  State
}

func (e *InvalidStateTransitionError) Error() string {
	return fmt.Sprintf("invalid state transition from %s to %s", e.CurrentState, e.TargetState)
}

type NotRunningError struct {
	SandboxID string
	State     State
}

func (e *NotRunningError) Error() string {
	return fmt.Sprintf("sandbox %s is not running (state: %s)", e.SandboxID, e.State)
}

var ErrAlreadyExists = errors.New("sandbox already exists")

var ErrEvictionInProgress = errors.New("sandbox eviction already in progress")

var ErrEvictionNotNeeded = errors.New("sandbox eviction not needed")

// ErrTransitionRestored is the result waiters on a pause transition see when
// the node refused the pause retryably and the sandbox went back to Running:
// the state changed, but not to the one they waited for.
var ErrTransitionRestored = errors.New("pause refused and sandbox restored to running")

// ErrDraining is this replica's refusal to start work that would outlive its
// shutdown; the sandbox keeps running and another replica can take it.
var ErrDraining = errors.New("api is draining")

// PauseQueueExhaustedError is a node's retryable refusal to snapshot right
// now; the sandbox keeps running and the same request can be retried.
type PauseQueueExhaustedError struct{}

func (PauseQueueExhaustedError) Error() string {
	return "The pause queue is exhausted"
}

// ErrExecutionMismatch reports that the stored sandbox is a different
// incarnation than the caller intended to act on — the one it saw was already
// removed and the ID reused by a resume or recreate. Raised by StartRemoving
// when the caller opted in via RemoveOpts.ExpectExecutionID, and always by
// RestoreRunning.
var ErrExecutionMismatch = errors.New("sandbox execution no longer matches")

// ErrSandboxKilled reports that a reservation was refused because a concurrent
// DELETE claimed the sandbox ID for removal (see ReservationStorage.ClaimKill).
// A resume that hits this must not publish the sandbox: the kill was accepted
// first and is irreversible.
var ErrSandboxKilled = errors.New("sandbox was concurrently killed")

// ErrRestoreConflict reports that the record was rewritten between the
// restore's read and its compare-and-set; nothing was written.
var ErrRestoreConflict = errors.New("sandbox changed during restoration")
