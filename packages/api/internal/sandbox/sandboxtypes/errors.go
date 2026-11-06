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

// ErrSkipRestore completes a transition without automatically restoring Running.
// The caller owns the restore attempt and reports any failure separately.
// Waiters are released to retry their operation, even if the record is still
// Snapshotting. Completion does not assert that the sandbox is Running.
var ErrSkipRestore = errors.New("complete transition without restoring the sandbox")

// ErrDraining is this replica's refusal to start work that would outlive its
// shutdown; the sandbox keeps running and another replica can take it.
var ErrDraining = errors.New("api is draining")

// PauseQueueExhaustedError is a node's retryable refusal to snapshot right
// now; the sandbox keeps running and the same request can be retried.
type PauseQueueExhaustedError struct{}

func (PauseQueueExhaustedError) Error() string {
	return "The pause queue is exhausted"
}

// PauseRefusedCheckpointInProgressError is a node's retryable refusal to
// snapshot while another checkpoint of the same sandbox is in flight; the
// sandbox keeps running and the request succeeds once that checkpoint ends.
// It matches PauseQueueExhaustedError for every caller that does not tell
// the two apart, so a refusal flows through the same restore and retry paths.
type PauseRefusedCheckpointInProgressError struct{}

func (PauseRefusedCheckpointInProgressError) Error() string {
	return "A checkpoint of the sandbox is in progress"
}

func (PauseRefusedCheckpointInProgressError) Is(target error) bool {
	_, ok := target.(PauseQueueExhaustedError)

	return ok
}

// RestoredTransitionError is what the starter of a refused pause finishes
// its transition with and what the pause's waiters receive: it is
// ErrTransitionRestored to every existing check and carries the refusal, so
// a pause that joined the refused one answers as the starter did instead of
// reading a busy node. Between replicas the refusal travels as its class
// token beside the result; a replica that predates the token reads the
// restore alone.
type RestoredTransitionError struct {
	Refusal error
}

func (e *RestoredTransitionError) Error() string {
	if e.Refusal == nil {
		return ErrTransitionRestored.Error()
	}

	return ErrTransitionRestored.Error() + ": " + e.Refusal.Error()
}

func (e *RestoredTransitionError) Unwrap() []error {
	if e.Refusal == nil {
		return []error{ErrTransitionRestored}
	}

	return []error{ErrTransitionRestored, e.Refusal}
}

// refusalClasses are the refusals a restored pause's waiters are told
// about, by the token that names each between API replicas.
var refusalClasses = []struct {
	token   string
	refusal error
}{
	{"checkpoint_in_progress", PauseRefusedCheckpointInProgressError{}},
	{"envd_unresponsive", PauseRefusedEnvdUnresponsiveError{}},
}

// RefusalClass is the token naming a refusal for a restored pause's waiters;
// empty for the busy-node refusal, which needs none.
func RefusalClass(refusal error) string {
	for _, class := range refusalClasses {
		if errors.Is(refusal, class.refusal) {
			return class.token
		}
	}

	return ""
}

// RefusalFromClass is the refusal a token names. An empty token, or one this
// build does not know, is the busy-node refusal.
func RefusalFromClass(token string) error {
	for _, class := range refusalClasses {
		if class.token == token {
			return class.refusal
		}
	}

	return PauseQueueExhaustedError{}
}

// PauseRefusedEnvdUnresponsiveError is a node's retryable refusal of a memory
// pause because the sandbox's guest agent did not answer its health probe; a
// snapshot taken now would never resume. The sandbox keeps running and its VM
// is untouched. It matches PauseQueueExhaustedError for every caller that does
// not tell the two apart, so the refusal flows through the same restore and
// retry paths.
type PauseRefusedEnvdUnresponsiveError struct{}

func (PauseRefusedEnvdUnresponsiveError) Error() string {
	return "The sandbox guest agent is not responding"
}

func (PauseRefusedEnvdUnresponsiveError) Is(target error) bool {
	_, ok := target.(PauseQueueExhaustedError)

	return ok
}

// ErrExecutionMismatch reports that the stored sandbox is a different
// incarnation than the caller intended to act on — the one it saw was already
// removed and the ID reused by a resume or recreate. Raised by StartRemoving
// when the caller opted in via RemoveOpts.ExpectExecutionID, and always by
// RestoreRunning.
var ErrExecutionMismatch = errors.New("sandbox execution no longer matches")

// ErrSandboxKilled reports that a delete claimed the sandbox ID first.
var ErrSandboxKilled = errors.New("sandbox was concurrently killed")

// ErrRestoreConflict reports that the record was rewritten between the
// restore's read and its compare-and-set; nothing was written.
var ErrRestoreConflict = errors.New("sandbox changed during restoration")
