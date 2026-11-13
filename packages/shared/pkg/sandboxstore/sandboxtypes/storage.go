package sandboxtypes

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/middleware/otel/joined"
)

const (
	StorageNameRedis = "redis"
)

// Storage is the persistence interface implemented by the redis backend.
//
//nolint:interfacebloat // one backend implements the whole record store; the groups are embedded interfaces
type Storage interface {
	// Add stores a sandbox. A record of the same execution already stored is
	// kept: the write is then a late duplicate of the one that landed.
	Add(ctx context.Context, sandbox Sandbox) error
	Get(ctx context.Context, teamID uuid.UUID, sandboxID string) (Sandbox, error)
	// Remove deletes the record of the expected execution, or of any when
	// none is expected; another execution's is left whole and
	// ErrExecutionMismatch returned. A sandbox already gone is a success.
	Remove(ctx context.Context, teamID uuid.UUID, sandboxID, expectedExecutionID string) error

	TeamItems(ctx context.Context, teamID uuid.UUID, states []State) ([]Sandbox, error)
	ExpiredItems(ctx context.Context) ([]Sandbox, error)
	TeamsWithSandboxCount(ctx context.Context) (map[uuid.UUID]int64, error)

	RecordUpdates
	StateTransitions
	LifecycleHolds
	Reconcile(ctx context.Context, sandboxes []NodeSandbox, nodeID string) []NodeSandbox
}

// RestoreOpts says how a refused pause's record goes back to Running.
type RestoreOpts struct {
	// RetryAfter holds the sandbox out of every replica's eviction sweep for
	// this long; zero leaves the retry window as it was.
	RetryAfter time.Duration
	// HoldOnly keeps the sweep off for RetryAfter without charging the
	// refusal episode the evictor's overstay budget is measured against: for
	// a refusal that says nothing about the node, such as a checkpoint
	// holding the sandbox. A stamped episode ends, so the next node refusal
	// counts from itself: a busy-node episode that was live before the
	// checkpoint refusal is forgotten on purpose, since the checkpoint's own
	// duration says nothing about the node.
	HoldOnly bool
	// EnvdRefusal stamps the guest agent's own episode (EnvdRefusedSince,
	// EnvdRefusedUntil); any other restore leaves both as they are.
	EnvdRefusal bool
}

// UpdateToken names one BeginUpdate's hold on the record's lock. It is
// opaque: a caller only passes it back to CommitUpdate or AbortUpdate, on
// any instance of the same backend.
type UpdateToken string

// RecordUpdates change a record under its lock in two calls, so the caller's
// work runs between them while other writers wait. UpdateRecord runs the
// protocol; use it rather than the calls.
type RecordUpdates interface {
	// BeginUpdate takes the record's lock and returns the record as stored.
	// ErrNotFound for a record absent or of another team, ErrLifecycleHeld
	// for a held one. On an error no lock is held. Otherwise the caller must
	// end with CommitUpdate or AbortUpdate; the lock expires if it does not.
	BeginUpdate(ctx context.Context, teamID uuid.UUID, sandboxID string) (Sandbox, UpdateToken, error)
	// CommitUpdate writes updated only while the lock still holds token,
	// ErrLockLost otherwise with nothing written, and releases the lock in
	// every case.
	CommitUpdate(ctx context.Context, teamID uuid.UUID, sandboxID string, token UpdateToken, updated Sandbox) (Sandbox, error)
	// AbortUpdate releases the lock and writes nothing.
	AbortUpdate(ctx context.Context, teamID uuid.UUID, sandboxID string, token UpdateToken) error
}

// UpdateRecord applies updateFunc to the record while its lock is held and
// stores the result. A failed or panicking updateFunc stores nothing and
// releases the lock.
func UpdateRecord(ctx context.Context, s RecordUpdates, teamID uuid.UUID, sandboxID string, updateFunc func(Sandbox) (Sandbox, error)) (Sandbox, error) {
	sbx, token, err := s.BeginUpdate(ctx, teamID, sandboxID)
	if err != nil {
		return Sandbox{}, err
	}

	committing := false
	defer func() {
		if committing {
			return
		}
		if abortErr := s.AbortUpdate(context.WithoutCancel(ctx), teamID, sandboxID, token); abortErr != nil {
			logger.L().Error(ctx, "Failed to release lock", zap.Error(abortErr))
		}
	}()

	updated, err := updateFunc(sbx)
	if err != nil {
		return Sandbox{}, fmt.Errorf("failed to update sandbox: %w", err)
	}
	committing = true

	return s.CommitUpdate(ctx, teamID, sandboxID, token, updated)
}

// StateTransitions is the removal state machine: start a transition, undo one
// the node refused, end a checkpoint with the network it left running, or
// wait one out. StartRemoving runs a removal; use it rather than
// BeginRemoving.
type StateTransitions interface {
	// BeginRemoving starts the transition opts asks for and returns it with
	// ID set, or alreadyDone when the record is in the target state. It
	// never waits: with another transition in flight it starts none and
	// returns InFlightID, unless the request cannot follow that transition.
	BeginRemoving(ctx context.Context, teamID uuid.UUID, sandboxID string, opts RemoveOpts) (StateTransition, bool, error)
	// FinishTransition ends the transition BeginRemoving started, the one
	// with transitionID, with the outcome of the caller's work, and wakes
	// its waiters. A transitionID that is no longer the sandbox's current
	// one leaves the current transition open. It returns the failure of an
	// automatic restore to Running as well as its own.
	FinishTransition(ctx context.Context, teamID uuid.UUID, sandboxID string, transitionID string, action StateAction, outcome TransitionOutcome) error
	RestoreRunning(ctx context.Context, transition StateTransition, opts RestoreOpts) (Sandbox, error)
	// RestoreCheckpointed puts the record of a checkpoint's transition back
	// to Running with placement, nil for the worker's own network, as
	// RestoreRunning does without a retry window. It refuses a record that
	// is another execution, left the transition's state or changed under it.
	RestoreCheckpointed(ctx context.Context, transition StateTransition, placement *orchestrator.SandboxNetworkPlacement) (Sandbox, error)
	// WaitForTransition waits out the transition transitionID, or with an
	// empty one the sandbox's open transition, if any, and returns the ID it
	// waited for with the outcome. A wait the context ended can be resumed
	// with that ID while the result is kept; a transition without a kept
	// result is a success. A record already gone is no refusal.
	WaitForTransition(ctx context.Context, teamID uuid.UUID, sandboxID string, transitionID string) (string, error)
}

// StartRemoving begins the removal opts asks for and waits out a transition
// in flight: a request for the same target state joins it and gets its
// outcome, one for another state begins again once it ends. The returned
// transition has ID set only when this call started it; the caller must end
// it with FinishTransition.
func StartRemoving(ctx context.Context, s StateTransitions, teamID uuid.UUID, sandboxID string, opts RemoveOpts) (StateTransition, bool, error) {
	for {
		transition, alreadyDone, err := s.BeginRemoving(ctx, teamID, sandboxID, opts)
		if err != nil || transition.InFlightID == "" {
			return transition, alreadyDone, err
		}
		inFlightID := transition.InFlightID
		transition.InFlightID = ""

		if transition.Sandbox.State == opts.Action.TargetState {
			// Same target state - wait for completion and return alreadyDone=true.
			// The caller inherits the in-flight transition's result without
			// doing the work itself: this is a joiner.
			joined.Mark(ctx)

			logger.L().Debug(ctx, "State transition already in progress to the same state, waiting",
				logger.WithSandboxID(transition.Sandbox.SandboxID),
				zap.String("state", string(opts.Action.TargetState)))

			_, err := s.WaitForTransition(ctx, teamID, sandboxID, inFlightID)
			if restored, ok := errors.AsType[*RestoredTransitionError](err); ok {
				// The pause the joiner rode was refused as one the guest cannot take
				// and the sandbox is running again: a joiner asking for the same
				// kind never succeeds; a memory pause that joined it may retry.
				if errors.Is(restored.Refusal, PauseUnsupportedError{}) && !opts.FilesystemOnly {
					return transition, false, PauseQueueExhaustedError{}
				}

				// The pause the joiner rode was refused and the sandbox is running
				// again: the joiner gets the refusal the starter got, not a success.
				return transition, false, restored.Refusal
			}
			if err != nil {
				return transition, false, fmt.Errorf("failed waiting for transition: %w", err)
			}

			return transition, true, nil
		}

		_, err = s.WaitForTransition(ctx, teamID, sandboxID, inFlightID)
		if err != nil && !errors.Is(err, ErrTransitionRestored) {
			return transition, false, fmt.Errorf("failed waiting for transition: %w", err)
		}
		// Begin again with opts whole: a field dropped across the wait is a
		// caller instruction discarded, and the begin re-checks
		// ExpectExecutionID against whatever the sandbox became meanwhile.
	}
}

// Hold names the hold one operation has on a running execution's record.
type Hold struct {
	TeamID      uuid.UUID
	SandboxID   string
	ExecutionID string
	// SourceNodeID is the node the execution is on when held: a hold
	// taken from a placement the execution has left since is refused.
	SourceNodeID string
	// ID is the holding operation's own ID, stored as lifecycleHold.
	ID string
}

// LifecycleHolds let one operation hold a running execution's record. While
// the hold lasts, every ordinary transition, update, replacement and
// removal of the record is refused with ErrLifecycleHeld.
type LifecycleHolds interface {
	// HoldExecution marks the running execution as held until `until`, which
	// must be in the future. The same hold again is a no-op and keeps its
	// deadline. It refuses with ErrHoldConflict a record of another
	// execution, on another node than SourceNodeID, not running, in a
	// transition, under a live hold of another operation, or a deadline
	// already passed.
	HoldExecution(ctx context.Context, hold Hold, until time.Time) error
	// MoveHeldExecution changes the execution's node to nodeID and releases
	// the hold in one write, and reports whether it wrote: a repeat that
	// finds the execution already on nodeID and not under this hold succeeds
	// without writing. ErrNotHeld for a record of this execution not under
	// this hold, ErrExecutionMismatch for another execution, ErrNotFound for
	// a record gone.
	MoveHeldExecution(ctx context.Context, hold Hold, nodeID string) (bool, error)
	// ReleaseHeldExecution clears the hold and leaves the record where it is.
	ReleaseHeldExecution(ctx context.Context, hold Hold) error
	// RemoveHeldExecution removes the held record as Remove does, without
	// clearing the hold first, and returns it as it was: no guest of this
	// execution may run anywhere, and the holder reports its end from the
	// record. A failed removal keeps the hold, so the holder can retry it.
	RemoveHeldExecution(ctx context.Context, hold Hold) (Sandbox, error)
}

type StateTransition struct {
	Sandbox Sandbox
	// Only the caller that starts the transition owns its pre-clamp expiry.
	OriginalEndTime *time.Time
	// ID is set only for the caller that started the transition, which must
	// end it with FinishTransition.
	ID string
	// InFlightID is set when another transition of the sandbox is in flight
	// and none was started: the caller waits it out with WaitForTransition.
	InFlightID string
}

// TransitionOutcome is the result of the work done under a transition, as
// FinishTransition records it.
type TransitionOutcome struct {
	// Failed means the work returned an error, whose text is Error.
	Failed bool
	Error  string
	// SkipRestore ends a transient transition without restoring Running.
	SkipRestore bool
	// Restored means the node refused and the record went back to Running;
	// RefusalClass is RefusalClass of that refusal.
	Restored     bool
	RefusalClass string
}

// TransitionOutcomeOf is the outcome of work that returned err. ErrSkipRestore
// is not a failure.
func TransitionOutcomeOf(err error) TransitionOutcome {
	if err == nil {
		return TransitionOutcome{}
	}
	if errors.Is(err, ErrSkipRestore) {
		return TransitionOutcome{SkipRestore: true}
	}

	outcome := TransitionOutcome{Failed: true, Error: err.Error()}
	if restored, ok := errors.AsType[*RestoredTransitionError](err); ok {
		outcome.Restored = true
		outcome.RefusalClass = RefusalClass(restored.Refusal)
	}

	return outcome
}

// ReservationStorage tracks per-team sandbox-start reservations to enforce
// concurrency limits.
type ReservationStorage interface {
	// Reserve returns reserved=true when the caller owns the start and must
	// end it with FinishStart. reserved=false with a nil error means a start
	// of the sandbox is already pending; the caller waits with WaitForStart.
	// Errors: ErrAlreadyExists, *LimitExceededError, ErrSandboxKilled, or a
	// backend failure.
	Reserve(ctx context.Context, teamID uuid.UUID, sandboxID string, limit int) (reserved bool, err error)
	// FinishStart stores result for the waiters, removes the pending start
	// and wakes the waiters.
	FinishStart(ctx context.Context, teamID uuid.UUID, sandboxID string, result []byte) error
	// WaitForStart waits until no start of the sandbox is pending and returns
	// the result its FinishStart stored. A start that ended without a result
	// is an error.
	WaitForStart(ctx context.Context, teamID uuid.UUID, sandboxID string) (result []byte, err error)
	Release(ctx context.Context, teamID uuid.UUID, sandboxID string) error

	// ClaimKill returns false when a resume is in flight or the sandbox is
	// running again; the caller must then keep the snapshot.
	ClaimKill(ctx context.Context, teamID uuid.UUID, sandboxID string) (claimID string, claimed bool, err error)
	ReleaseKillClaim(ctx context.Context, teamID uuid.UUID, sandboxID, claimID string) error
}
