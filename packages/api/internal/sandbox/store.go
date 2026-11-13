package sandbox

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

type CreationMetadata struct {
	IsResume       bool
	TeamName       string
	RequestHeader  http.Header
	MCPServerNames []string
}

type (
	OrphanCallback   func(ctx context.Context, sbx NodeSandbox)
	CreationCallback func(ctx context.Context, sbx Sandbox, meta CreationMetadata)
)

const sbxRemoveTimeout = 10 * time.Second

// Storage and ReservationStorage are re-exported from sandboxtypes so external
// callers can continue to use sandbox.Storage / sandbox.ReservationStorage.
// They live in sandboxtypes (a leaf package) so storage backends can implement
// them without creating an import cycle back into package sandbox.
type (
	Storage            = sandboxtypes.Storage
	ReservationStorage = sandboxtypes.ReservationStorage
)

type Callbacks struct {
	// AsyncNewlyCreatedSandbox is called asynchronously for newly created sandboxes (Add called with non-nil CreationMetadata).
	AsyncNewlyCreatedSandbox CreationCallback
	// KillOrphanSandbox kills an orphaned sandbox on the orchestrator node via gRPC.
	// Used during sync when the Redis backend detects sandboxes running on a node but not present in the store.
	KillOrphanSandbox OrphanCallback
	// EndPoweredOffSandbox runs the timeout action of a sandbox whose node
	// reports that its guest powered off. Called during sync for every such
	// sandbox the node lists. It must return without waiting for the action:
	// a pause can outlast the sync interval.
	EndPoweredOffSandbox OrphanCallback
	// EndEndedSandbox ends the record of a sandbox its node lists as ended
	// with nothing kept, as a kill would. Called during sync for every such
	// sandbox the node lists, which it does for a while after the end.
	EndEndedSandbox OrphanCallback
}

type Store struct {
	storage   Storage
	callbacks Callbacks

	reservations ReservationStorage
}

func NewStore(
	backend Storage,
	reservations ReservationStorage,
	callbacks Callbacks,
) *Store {
	return &Store{
		storage:      backend,
		reservations: reservations,
		callbacks:    callbacks,
	}
}

// Add inserts a sandbox into the store. It never overwrites a stored record
// of the same execution (a retried or delayed duplicate of it must not take
// the record back). A non-nil creation argument fires the
// AsyncNewlyCreatedSandbox callback.
func (s *Store) Add(ctx context.Context, sandbox Sandbox, creation *CreationMetadata) error {
	sbxlogger.I(sandbox).Debug(ctx, "Adding sandbox to cache",
		zap.Bool("newly_created", creation != nil),
		logger.Time("start_time", sandbox.StartTime),
		logger.Time("end_time", sandbox.EndTime),
	)

	endTime := sandbox.EndTime

	if endTime.Sub(sandbox.StartTime) > sandbox.MaxInstanceLength {
		sandbox.EndTime = sandbox.StartTime.Add(sandbox.MaxInstanceLength)
	}

	if err := s.storage.Add(ctx, sandbox); err != nil {
		return err
	}

	if creation != nil {
		meta := *creation
		go s.callbacks.AsyncNewlyCreatedSandbox(context.WithoutCancel(ctx), sandbox, meta)
	}

	return nil
}

func (s *Store) Get(ctx context.Context, teamID uuid.UUID, sandboxID string) (Sandbox, error) {
	return s.storage.Get(ctx, teamID, sandboxID)
}

// Remove deletes the sandbox's record when it is the expected execution's,
// or any execution's when none is expected, and releases its reservation. A
// record another execution took over is left in place with its reservation:
// the successor's own lifecycle releases it, and a release here would drop
// a start still pending and the result its waiters read. A held record keeps
// its reservation too, since it stays in place, as does a record whose lock
// expired under this call, which may be a successor's by now.
func (s *Store) Remove(ctx context.Context, teamID uuid.UUID, sandboxID, expectedExecutionID string) {
	err := s.storage.Remove(ctx, teamID, sandboxID, expectedExecutionID)
	switch {
	case errors.Is(err, ErrExecutionMismatch):
		logger.L().Info(ctx, "Sandbox record superseded, left in place", logger.WithSandboxID(sandboxID))

		return
	case errors.Is(err, ErrLifecycleHeld):
		logger.L().Warn(ctx, "Sandbox record held, left in place", logger.WithSandboxID(sandboxID))

		return
	case errors.Is(err, ErrLockLost):
		logger.L().Warn(ctx, "Sandbox lock lost before removal, record left in place", logger.WithSandboxID(sandboxID))

		return
	case err != nil:
		logger.L().Error(ctx, "Failed to remove sandbox from storage", zap.Error(err), logger.WithSandboxID(sandboxID))
	}

	err = s.reservations.Release(ctx, teamID, sandboxID)
	if err != nil {
		logger.L().Error(ctx, "Failed to release reservation", zap.Error(err), logger.WithSandboxID(sandboxID))
	}
}

func (s *Store) TeamItems(ctx context.Context, teamID uuid.UUID, states []State) ([]Sandbox, error) {
	return s.storage.TeamItems(ctx, teamID, states)
}

func (s *Store) ExpiredItems(ctx context.Context) ([]Sandbox, error) {
	return s.storage.ExpiredItems(ctx)
}

func (s *Store) TeamsWithSandboxes(ctx context.Context) (map[uuid.UUID]int64, error) {
	return s.storage.TeamsWithSandboxCount(ctx)
}

func (s *Store) Update(ctx context.Context, teamID uuid.UUID, sandboxID string, updateFunc func(sandbox Sandbox) (Sandbox, error)) (Sandbox, error) {
	return sandboxtypes.UpdateRecord(ctx, s.storage, teamID, sandboxID, updateFunc)
}

// HoldExecution marks the running execution as held until `until`; see
// sandboxtypes.LifecycleHolds.
func (s *Store) HoldExecution(ctx context.Context, hold Hold, until time.Time) error {
	return s.storage.HoldExecution(ctx, hold, until)
}

// MoveHeldExecution changes the held execution's node to nodeID and releases
// the hold, and reports whether it wrote.
func (s *Store) MoveHeldExecution(ctx context.Context, hold Hold, nodeID string) (bool, error) {
	return s.storage.MoveHeldExecution(ctx, hold, nodeID)
}

// ReleaseHeldExecution clears the hold without moving the record.
func (s *Store) ReleaseHeldExecution(ctx context.Context, hold Hold) error {
	return s.storage.ReleaseHeldExecution(ctx, hold)
}

// RemoveHeldExecution removes the held record, returned as it was, and, as
// Remove does, releases the sandbox's reservation.
func (s *Store) RemoveHeldExecution(ctx context.Context, hold Hold) (Sandbox, error) {
	removed, err := s.storage.RemoveHeldExecution(ctx, hold)
	if err != nil {
		return Sandbox{}, err
	}
	if err := s.reservations.Release(ctx, hold.TeamID, hold.SandboxID); err != nil {
		logger.L().Error(ctx, "Failed to release reservation", zap.Error(err), logger.WithSandboxID(hold.SandboxID))
	}

	return removed, nil
}

// StartRemoving returns, for the caller that started the transition, the
// callback that ends it with the caller's error; a no-op when this call
// started no transition.
func (s *Store) StartRemoving(ctx context.Context, teamID uuid.UUID, sandboxID string, opts RemoveOpts) (StateTransition, bool, func(context.Context, error), error) {
	transition, alreadyDone, err := sandboxtypes.StartRemoving(ctx, s.storage, teamID, sandboxID, opts)
	if err != nil {
		return transition, false, nil, err
	}
	if transition.ID == "" {
		return transition, alreadyDone, func(context.Context, error) {}, nil
	}

	return transition, alreadyDone, func(ctx context.Context, workErr error) {
		err := s.storage.FinishTransition(ctx, teamID, sandboxID, transition.ID, opts.Action, sandboxtypes.TransitionOutcomeOf(workErr))
		if err != nil {
			logger.L().Warn(ctx, "Failed to finish state transition", logger.WithSandboxID(sandboxID), zap.String("transition_id", transition.ID), zap.Error(err))
		}
	}, nil
}

func (s *Store) RestoreRunning(ctx context.Context, transition StateTransition, opts RestoreOpts) (Sandbox, error) {
	return s.storage.RestoreRunning(ctx, transition, opts)
}

func (s *Store) RestoreCheckpointed(ctx context.Context, transition StateTransition, placement *orchestrator.SandboxNetworkPlacement) (Sandbox, error) {
	return s.storage.RestoreCheckpointed(ctx, transition, placement)
}

func (s *Store) WaitForStateChange(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	_, err := s.storage.WaitForTransition(ctx, teamID, sandboxID, "")

	return err
}

func (s *Store) Reconcile(ctx context.Context, sandboxes []NodeSandbox, nodeID string) {
	// Redis is the source of truth — divergent sandboxes are orphans running
	// on the node but not present in the store. Kill them.
	// An ended sandbox holds nothing on the node, so it is no orphan even
	// once its record is gone.
	var running, ended []NodeSandbox
	for _, sbx := range sandboxes {
		if sbx.EndedOnNode() {
			ended = append(ended, sbx)
		} else {
			running = append(running, sbx)
		}
	}
	orphans := s.storage.Reconcile(ctx, running, nodeID)

	// A powered-off sandbox gets its timeout action now rather than at its
	// end time. The callback does not wait for it, and the evictor drops a
	// repeat while one is in flight.
	if s.callbacks.EndPoweredOffSandbox != nil {
		for _, sbx := range sandboxes {
			if sbx.DiskKept {
				s.callbacks.EndPoweredOffSandbox(context.WithoutCancel(ctx), sbx)
			}
		}
	}

	wg := sync.WaitGroup{}
	for _, sbx := range orphans {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sbxRemoveTimeout)
			defer cancel()
			s.callbacks.KillOrphanSandbox(ctx, sbx)
		})
	}
	if s.callbacks.EndEndedSandbox != nil {
		for _, sbx := range ended {
			wg.Go(func() {
				ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sbxRemoveTimeout)
				defer cancel()
				s.callbacks.EndEndedSandbox(ctx, sbx)
			})
		}
	}

	wg.Wait()
}

// Reserve returns either finishStart, when the caller owns the start, or
// waitForStart, when the sandbox is already starting or running; callers
// compare them with nil.
func (s *Store) Reserve(ctx context.Context, teamID uuid.UUID, sandboxID string, limit int) (finishStart func(Sandbox, error), waitForStart func(ctx context.Context) (Sandbox, error), err error) {
	reserved, err := s.reservations.Reserve(ctx, teamID, sandboxID, limit)
	if err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			// Try to get the sandbox from the storage if already exists
			return nil, func(ctx context.Context) (Sandbox, error) {
				return s.storage.Get(ctx, teamID, sandboxID)
			}, nil
		}

		return nil, nil, err
	}

	if !reserved {
		return nil, func(ctx context.Context) (Sandbox, error) {
			result, err := s.reservations.WaitForStart(ctx, teamID, sandboxID)
			if err != nil {
				return Sandbox{}, err
			}

			return decodeResult(result)
		}, nil
	}

	return s.finishStart(ctx, teamID, sandboxID), nil, nil
}

func (s *Store) finishStart(ctx context.Context, teamID uuid.UUID, sandboxID string) func(Sandbox, error) {
	return func(sbx Sandbox, startErr error) {
		bgCtx := context.WithoutCancel(ctx)

		resultData, encodeErr := encodeResult(sbx, startErr)
		if encodeErr != nil {
			logger.L().Error(ctx, "failed to encode reservation result",
				zap.Error(encodeErr),
				logger.WithSandboxID(sandboxID),
			)

			// Still try to remove from pending even if encoding fails.
			_ = s.reservations.Release(bgCtx, teamID, sandboxID)

			return
		}

		if err := s.reservations.FinishStart(bgCtx, teamID, sandboxID, resultData); err != nil {
			logger.L().Error(ctx, "failed to run finishStart script",
				zap.Error(err),
				logger.WithSandboxID(sandboxID),
			)
		}
	}
}

func (s *Store) ClaimKill(ctx context.Context, teamID uuid.UUID, sandboxID string) (string, bool, error) {
	return s.reservations.ClaimKill(ctx, teamID, sandboxID)
}

func (s *Store) ReleaseKillClaim(ctx context.Context, teamID uuid.UUID, sandboxID, claimID string) error {
	return s.reservations.ReleaseKillClaim(ctx, teamID, sandboxID, claimID)
}

// WaitForStart returns once no start of the sandbox is pending. How that start
// ended is the starter's to report, so only a ctx error is returned.
func (s *Store) WaitForStart(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	_, _ = s.reservations.WaitForStart(ctx, teamID, sandboxID)

	return ctx.Err()
}
