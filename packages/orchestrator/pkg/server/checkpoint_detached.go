package server

import (
	"context"
	"errors"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
)

var (
	errCheckpointSnapshotTimeout    = errors.New("checkpoint snapshot exceeded its budget")
	errResumeAfterCheckpointTimeout = errors.New("resume after checkpoint timed out")
	errCheckpointServerShutdown     = errors.New("server shutting down during checkpoint")
	errCheckpointDeleted            = errors.New("sandbox deleted during checkpoint")
)

// deletedDuringCheckpointError is the cause a Delete ends a checkpoint with; it
// matches errCheckpointDeleted and carries the kill reason the checkpoint
// publishes with.
type deletedDuringCheckpointError struct {
	killReason string
}

func (d *deletedDuringCheckpointError) Error() string { return errCheckpointDeleted.Error() }

func (d *deletedDuringCheckpointError) Is(target error) bool { return target == errCheckpointDeleted }

// detachedCheckpoint is a resume-fresh checkpoint from before its
// reservation until the resumed sandbox is live or, on failure, until its
// rollback has run and the reservation is released.
type detachedCheckpoint struct {
	cancel context.CancelCauseFunc
	// cause is what ended the checkpoint's context, once it has.
	cause func() error
	done  chan struct{}
	// release ends the claim; safe to call more than once.
	release func()

	mu sync.Mutex
	// registered is the lifecycle the checkpoint is publishing live, set
	// before the registration so a Delete that finds it live during the
	// registration's notifications owns it; nothing is live under it before.
	registered string
	// original is the lifecycle the checkpoint started from. The map puts it
	// back live when a subscriber panics in the reservation, and it is then
	// the Delete's to kill like the published one.
	original string
	// reserved is set once the checkpoint holds the sandbox's reservation,
	// refused once the reservation was refused: a checkpoint that never
	// held the sandbox owns nothing a Delete's end could kill, while one
	// still taking it may.
	reserved bool
	refused  bool
	// acknowledged is the kill reason of a Delete that answered success with
	// no sandbox of the checkpoint's live at its final read, taken under mu
	// with that read. A lifecycle the checkpoint makes live after it, by a
	// publication the cancellation did not reach or by the map's restore, is
	// the checkpoint's to kill with it: the Delete is gone.
	acknowledged string
}

// setRegistered records the lifecycle the checkpoint published live.
func (op *detachedCheckpoint) setRegistered(lifecycleID string) {
	op.mu.Lock()
	defer op.mu.Unlock()
	op.registered = lifecycleID
}

// reserve takes the sandbox out of the live map under the checkpoint's
// reservation and records that the checkpoint holds it.
func (op *detachedCheckpoint) reserve(ctx context.Context, sandboxes *sandbox.Map, sbx *sandbox.Sandbox) (*sandbox.Reservation, error) {
	reservation, err := sandboxes.MarkStoppingReserved(ctx, sbx.Runtime.SandboxID, sbx.LifecycleID)
	op.mu.Lock()
	defer op.mu.Unlock()
	if err != nil {
		op.refused = true

		return nil, err
	}
	op.reserved = true

	return reservation, nil
}

// reservedSandbox reports whether the checkpoint holds the sandbox's
// reservation.
func (op *detachedCheckpoint) reservedSandbox() bool {
	op.mu.Lock()
	defer op.mu.Unlock()

	return op.reserved
}

// endedWithTheDeletesKill reports whether the checkpoint, now ended, ended
// for a Delete's cause while it held the sandbox.
func (op *detachedCheckpoint) endedWithTheDeletesKill() bool {
	return op.reservedSandbox() && errors.Is(op.cause(), errCheckpointDeleted)
}

// owns reports whether live is a lifecycle of this checkpoint's: the one it
// published, or the one it started from, live again after the reservation
// was undone. A lifecycle another operation registered under the ID is not.
func (op *detachedCheckpoint) owns(live *sandbox.Sandbox) bool {
	op.mu.Lock()
	defer op.mu.Unlock()

	return op.ownsLocked(live)
}

func (op *detachedCheckpoint) ownsLocked(live *sandbox.Sandbox) bool {
	if live == nil {
		return false
	}

	return (op.registered != "" && op.registered == live.LifecycleID) || live.LifecycleID == op.original
}

// takeOrAcknowledge is a Delete's final decision at its deadline, in one
// critical section with the checkpoint's publication and reservation: the
// live sandbox the checkpoint owns, read under mu, or nothing. With nothing
// in hand the Delete's kill is recorded as acknowledged when kill is set
// (the checkpoint ended for this Delete) and the reservation was not
// refused; the first acknowledgement is kept. The publication inserts the
// sandbox before it reads the acknowledgement under the same mu, so one of
// the two sees the other: the Delete is handed the sandbox, or the
// checkpoint kills what it made live. ended reports whether this Delete is
// what ends the lifecycle.
func (op *detachedCheckpoint) takeOrAcknowledge(read func() (*sandbox.Sandbox, bool), killReason string, kill bool) (live *sandbox.Sandbox, ended bool) {
	op.mu.Lock()
	defer op.mu.Unlock()

	if live, _ := read(); op.ownsLocked(live) {
		return live, true
	}
	if !kill || op.refused {
		return nil, false
	}
	if op.acknowledged == "" {
		op.acknowledged = killReason
	}

	return nil, true
}

// acknowledgedDelete reports the kill a Delete answered success for with
// nothing of the checkpoint's live, once one has.
func (op *detachedCheckpoint) acknowledgedDelete() (string, bool) {
	op.mu.Lock()
	defer op.mu.Unlock()

	return op.acknowledged, op.acknowledged != ""
}

// registerDetachedCheckpoint claims sbx's ID for a resume-fresh checkpoint
// before the reservation takes it out of the live map, so Delete, Checkpoint
// and Pause find the operation on every interleaving: the claim is taken
// while the sandbox is still live and released once the resumed sandbox is
// live or, on failure, after the reservation is, so at no point is the ID
// in neither registry. The returned context keeps the caller's values but
// not its cancellation; it ends on server shutdown or on a Delete for the
// sandbox, and the phase budgets are derived from it. The caller cancels it
// when done. Refused while another checkpoint of the ID is in flight on
// either route: the sandbox's own checkpoint marker is taken with the claim
// and held as long as it, so an in-place checkpoint, which leaves the
// sandbox live, cannot start on the same VM while the claim stands, and
// the claim is refused while one runs.
func (s *Server) registerDetachedCheckpoint(ctx context.Context, sbx *sandbox.Sandbox) (context.Context, *detachedCheckpoint, bool) {
	detached, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	op := &detachedCheckpoint{cancel: cancel, cause: func() error { return context.Cause(detached) }, done: make(chan struct{}), original: sbx.LifecycleID}
	sandboxID := sbx.Runtime.SandboxID

	s.detachedCheckpointsMu.Lock()
	if _, busy := s.detachedCheckpoints[sandboxID]; busy {
		s.detachedCheckpointsMu.Unlock()
		cancel(nil)

		return nil, nil, false
	}
	if !sbx.BeginCheckpoint() {
		s.detachedCheckpointsMu.Unlock()
		cancel(nil)

		return nil, nil, false
	}
	s.detachedCheckpoints[sandboxID] = op
	s.detachedCheckpointsMu.Unlock()

	// A server with no shutdown context never shuts down.
	stopWatch := func() bool { return false }
	if s.shutdownCtx != nil {
		stopWatch = context.AfterFunc(s.shutdownCtx, func() { cancel(errCheckpointServerShutdown) })
	}

	op.release = sync.OnceFunc(func() {
		s.detachedCheckpointsMu.Lock()
		if s.detachedCheckpoints[sandboxID] == op {
			delete(s.detachedCheckpoints, sandboxID)
		}
		s.detachedCheckpointsMu.Unlock()

		sbx.EndCheckpoint()
		stopWatch()
		close(op.done)
	})

	return detached, op, true
}

func (s *Server) detachedCheckpointFor(sandboxID string) *detachedCheckpoint {
	s.detachedCheckpointsMu.Lock()
	defer s.detachedCheckpointsMu.Unlock()

	return s.detachedCheckpoints[sandboxID]
}

// heldByCheckpoint answers a read of sandboxID that missed the live map, or a
// marking of it that failed: true while a resume-fresh checkpoint holds the
// sandbox, and also when the sandbox is live again, which is how the caller
// sees a checkpoint whose claim ended between its read and this call (the
// claim is taken before the live entry is removed and released after the
// resumed sandbox is live). Both are answered retryably; false means the
// sandbox is gone.
func (s *Server) heldByCheckpoint(sandboxID string) bool {
	if s.detachedCheckpointFor(sandboxID) != nil {
		return true
	}
	_, live := s.sandboxFactory.Sandboxes.Get(sandboxID)

	return live
}

// endCheckpointForDelete ends the in-flight checkpoint of sandboxID on a
// Delete's behalf with killReason and waits for its claim to end, bounded by
// ctx. It hands back the live sandbox the checkpoint owns, if any; ended is
// true only when this Delete ends the lifecycle, which at the deadline
// includes a reservation still being taken.
func (s *Server) endCheckpointForDelete(ctx context.Context, sandboxID, killReason string) (live *sandbox.Sandbox, ended bool) {
	op := s.detachedCheckpointFor(sandboxID)
	if op == nil {
		live, ok := s.sandboxFactory.Sandboxes.Get(sandboxID)

		return live, ok
	}

	op.cancel(&deletedDuringCheckpointError{killReason: killReason})
	if live, _ := s.sandboxFactory.Sandboxes.Get(sandboxID); op.owns(live) {
		return live, true
	}

	select {
	case <-op.done:
	case <-ctx.Done():
		read := func() (*sandbox.Sandbox, bool) { return s.sandboxFactory.Sandboxes.Get(sandboxID) }
		live, ended := op.takeOrAcknowledge(read, killReason, errors.Is(op.cause(), errCheckpointDeleted))
		if live != nil || !ended {
			return live, ended
		}
		logger.L().Info(ctx, "delete answered before the checkpoint it ended finished its rollback",
			logger.WithSandboxID(sandboxID), zap.String("kill_reason", killReason), zap.Error(context.Cause(ctx)))

		return nil, true
	}
	if live, _ := s.sandboxFactory.Sandboxes.Get(sandboxID); op.owns(live) {
		return live, true
	}

	return nil, op.endedWithTheDeletesKill()
}

// killForAcknowledgedDelete kills a sandbox of the checkpoint's that is live
// after a Delete answered success with none in hand: the resumed sandbox a
// publication made live past the cancellation, or the original the map put
// back after a subscriber panic in the reservation. Run after the
// publication and as the checkpoint ends; a sandbox another operation has
// taken meanwhile is left to it.
func (s *Server) killForAcknowledgedDelete(ctx context.Context, op *detachedCheckpoint, steps resumeFreshSteps, sandboxID string) {
	killReason, ok := op.acknowledgedDelete()
	if !ok {
		return
	}
	live, _ := s.sandboxFactory.Sandboxes.Get(sandboxID)
	if !op.owns(live) {
		return
	}
	if !s.sandboxFactory.Sandboxes.MarkStopping(ctx, live.Runtime.SandboxID, live.LifecycleID) {
		logger.L().Warn(ctx, "sandbox a delete acknowledged during its checkpoint left the live map under another operation before the kill",
			logger.WithSandboxID(sandboxID), logger.WithLifecycleID(live.LifecycleID), zap.String("kill_reason", killReason))

		return
	}
	live.SetStopReason(sandbox.StopReasonKilled)
	sbxlogger.E(live).Info(ctx, "Killing sandbox a delete acknowledged during its checkpoint", zap.String("kill_reason", killReason))
	steps.stopAsync(context.WithoutCancel(ctx), live)
	s.emitSandboxKilled(ctx, live, killReason)
}

// deleteKillToPublish names the kill a checkpoint publishes as it ends: the
// one a Delete ended it with, while the checkpoint still owned the sandbox,
// that is reserved and with the resumed sandbox never live. Once it is live
// a Delete kills it on the normal path, and with no reservation the map put
// the sandbox back. cause is the detached context's, the value a Delete
// reads, not a phase context's.
func deleteKillToPublish(cause error, checkpointErr error, reserved, wentLive bool) (string, bool) {
	var deleted *deletedDuringCheckpointError
	if checkpointErr == nil || !reserved || wentLive || !errors.As(cause, &deleted) {
		return "", false
	}

	return deleted.killReason, true
}

// checkpointEnded reports a cancellation that a step did not observe, so a
// step that ignored it cannot carry the checkpoint on to registration.
func checkpointEnded(ctx context.Context, sandboxID string) error {
	err := ctx.Err()
	if err == nil {
		return nil
	}

	return status.Errorf(codes.Internal, "checkpoint of sandbox '%s' ended: %s", sandboxID, withContextCause(ctx, err))
}

// BeginShutdown ends the work that outlives its callers (detached
// checkpoints) so the gRPC server can stop gracefully. Close calls it too.
func (s *Server) BeginShutdown() {
	if s.shutdown != nil {
		s.shutdown()
	}
}

// errSandboxOperationInProgress is Checkpoint's answer while a checkpoint
// claim holds the sandbox, from the claim until its resumed sandbox is live.
func errSandboxOperationInProgress(sandboxID string) error {
	return checkpointInProgressStatus(codes.FailedPrecondition, "an operation is already in progress for sandbox '%s'", sandboxID).Err()
}

// withContextCause names what ended ctx when err is ctx's own error, or a
// gRPC status of the same kind as it (Canceled for a cancelled context,
// DeadlineExceeded for an expired one), as the envd init wait surfaces it;
// a step's own error, its own inner deadline under a cancelled context
// included, is returned as is.
func withContextCause(ctx context.Context, err error) error {
	if err == nil || ctx.Err() == nil {
		return err
	}
	if !errors.Is(err, ctx.Err()) && status.Code(err) != status.FromContextError(ctx.Err()).Code() {
		return err
	}
	//nolint:errorlint // A wrapped cancellation can itself carry the diagnosis; only ctx.Err() is generic.
	if cause := context.Cause(ctx); cause != nil && cause != ctx.Err() && !errors.Is(err, cause) {
		return errors.Join(err, cause)
	}

	return err
}
