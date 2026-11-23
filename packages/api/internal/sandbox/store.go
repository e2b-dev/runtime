package sandbox

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/api/internal/sandbox")

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

const (
	backendRedis         = "redis"
	backendSchedulingAPI = "scheduling-api"
)

type backends struct {
	slug         string
	storage      Storage
	reservations ReservationStorage
}

type Store struct {
	local     backends
	remote    *backends
	flags     *featureflags.Client
	callbacks Callbacks
}

type Option func(*Store)

func WithRemoteBackend(storage Storage, reservations ReservationStorage, flags *featureflags.Client) Option {
	return func(s *Store) {
		s.remote = &backends{slug: backendSchedulingAPI, storage: storage, reservations: reservations}
		s.flags = flags
	}
}

func NewStore(
	backend Storage,
	reservations ReservationStorage,
	callbacks Callbacks,
	opts ...Option,
) *Store {
	s := &Store{
		local:     backends{slug: backendRedis, storage: backend, reservations: reservations},
		callbacks: callbacks,
	}
	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Every Store method picks once and keeps that pair for the whole call, returned
// closures included: an update token of one backend is not valid on the other.
func (s *Store) pick(ctx context.Context, teamID uuid.UUID, sandboxID string) backends {
	if s.remote == nil || teamID == uuid.Nil {
		return s.local
	}

	contexts := []ldcontext.Context{featureflags.TeamContext(teamID.String())}
	if sandboxID != "" {
		contexts = append(contexts, featureflags.SandboxContext(sandboxID))
	}
	if s.flags.BoolFlag(ctx, featureflags.SandboxStateSchedulingAPIFlag, contexts...) {
		return *s.remote
	}

	return s.local
}

//nolint:spancheck // the caller ends the span
func (s *Store) begin(ctx context.Context, method string, teamID uuid.UUID, sandboxID string) (context.Context, backends, trace.Span) {
	b := s.pick(ctx, teamID, sandboxID)
	// The evictor polls ExpiredItems every 50 ms from a background context;
	// that poll must start no trace.
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ctx, b, noop.Span{}
	}
	ctx, span := tracer.Start(ctx, "sandbox-store "+method,
		trace.WithAttributes(attribute.String("sandbox.store.backend", b.slug)))

	return ctx, b, span
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

	bctx, b, span := s.begin(ctx, "add", sandbox.TeamID, sandbox.SandboxID)
	defer span.End()

	if err := b.storage.Add(bctx, sandbox); err != nil {
		return err
	}

	if creation != nil {
		meta := *creation
		go s.callbacks.AsyncNewlyCreatedSandbox(context.WithoutCancel(ctx), sandbox, meta)
	}

	return nil
}

func (s *Store) Get(ctx context.Context, teamID uuid.UUID, sandboxID string) (Sandbox, error) {
	bctx, b, span := s.begin(ctx, "get", teamID, sandboxID)
	defer span.End()

	return b.storage.Get(bctx, teamID, sandboxID)
}

// Remove deletes the sandbox's record when it is the expected execution's,
// or any execution's when none is expected, and releases its reservation. A
// record another execution took over is left in place with its reservation:
// the successor's own lifecycle releases it, and a release here would drop
// a start still pending and the result its waiters read. A held record keeps
// its reservation too, since it stays in place, as does a record whose lock
// expired under this call, which may be a successor's by now.
func (s *Store) Remove(ctx context.Context, teamID uuid.UUID, sandboxID, expectedExecutionID string) {
	bctx, b, span := s.begin(ctx, "remove", teamID, sandboxID)
	defer span.End()

	err := b.storage.Remove(bctx, teamID, sandboxID, expectedExecutionID)
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

	err = b.reservations.Release(bctx, teamID, sandboxID)
	if err != nil {
		logger.L().Error(ctx, "Failed to release reservation", zap.Error(err), logger.WithSandboxID(sandboxID))
	}
}

func (s *Store) TeamItems(ctx context.Context, teamID uuid.UUID, states []State) ([]Sandbox, error) {
	bctx, b, span := s.begin(ctx, "team-items", teamID, "")
	defer span.End()

	return b.storage.TeamItems(bctx, teamID, states)
}

func (s *Store) ExpiredItems(ctx context.Context) ([]Sandbox, error) {
	bctx, b, span := s.begin(ctx, "expired-items", uuid.Nil, "")
	defer span.End()

	return b.storage.ExpiredItems(bctx)
}

func (s *Store) TeamsWithSandboxes(ctx context.Context) (map[uuid.UUID]int64, error) {
	bctx, b, span := s.begin(ctx, "teams-with-sandboxes", uuid.Nil, "")
	defer span.End()

	return b.storage.TeamsWithSandboxCount(bctx)
}

// Update applies updateFunc to the record under its lock. It waits out a
// lifecycle hold, as waitOutHold describes, and then updates the record as
// the hold left it.
func (s *Store) Update(ctx context.Context, teamID uuid.UUID, sandboxID string, updateFunc func(sandbox Sandbox) (Sandbox, error)) (Sandbox, error) {
	bctx, b, span := s.begin(ctx, "update", teamID, sandboxID)
	defer span.End()

	var updated Sandbox
	err := waitOutHold(bctx, func() (err error) {
		updated, err = sandboxtypes.UpdateRecord(bctx, b.storage, teamID, sandboxID, updateFunc)

		return err
	})

	return updated, err
}

// holdWaitLimit is how long a write waits for a lifecycle hold to end before
// it is refused. A migration holds a sandbox for seconds, and the SDKs give
// up on a request after 60 seconds by default, so the refusal has to come
// well before that.
const holdWaitLimit = 30 * time.Second

// holdRetryInterval is how long a waiting write leaves between two tries, and
// so the longest it keeps waiting once its hold has ended.
const holdRetryInterval = 250 * time.Millisecond

// waitOutHold runs begin again for as long as a lifecycle hold refuses it.
// It starts no new try once holdWaitLimit has passed or ctx has ended, and
// returns the last refusal, ErrLifecycleHeld, which the caller answers as
// retryable. The limit bounds the wait for the hold only: a try in progress
// keeps the caller's own deadline, so one admitted late still applies. A
// refused begin has changed nothing, so repeating it replays no effect.
// Writes waiting on one hold are admitted in no particular order.
func waitOutHold(ctx context.Context, begin func() error) error {
	waitCtx, cancel := context.WithTimeout(ctx, holdWaitLimit)
	defer cancel()

	for {
		err := begin()
		if !errors.Is(err, ErrLifecycleHeld) {
			return err
		}
		select {
		case <-waitCtx.Done():
			return err
		case <-time.After(holdRetryInterval):
		}
	}
}

// HoldExecution marks the running execution as held until `until`; see
// sandboxtypes.LifecycleHolds.
func (s *Store) HoldExecution(ctx context.Context, hold Hold, until time.Time) error {
	bctx, b, span := s.begin(ctx, "hold-execution", hold.TeamID, hold.SandboxID)
	defer span.End()

	return b.storage.HoldExecution(bctx, hold, until)
}

// MoveHeldExecution changes the held execution's node to nodeID and releases
// the hold, and reports whether it wrote.
func (s *Store) MoveHeldExecution(ctx context.Context, hold Hold, nodeID string) (bool, error) {
	bctx, b, span := s.begin(ctx, "move-held-execution", hold.TeamID, hold.SandboxID)
	defer span.End()

	return b.storage.MoveHeldExecution(bctx, hold, nodeID)
}

// ReleaseHeldExecution clears the hold without moving the record.
func (s *Store) ReleaseHeldExecution(ctx context.Context, hold Hold) error {
	bctx, b, span := s.begin(ctx, "release-held-execution", hold.TeamID, hold.SandboxID)
	defer span.End()

	return b.storage.ReleaseHeldExecution(bctx, hold)
}

// RemoveHeldExecution removes the held record, returned as it was, and, as
// Remove does, releases the sandbox's reservation.
func (s *Store) RemoveHeldExecution(ctx context.Context, hold Hold) (Sandbox, error) {
	bctx, b, span := s.begin(ctx, "remove-held-execution", hold.TeamID, hold.SandboxID)
	defer span.End()

	removed, err := b.storage.RemoveHeldExecution(bctx, hold)
	if err != nil {
		return Sandbox{}, err
	}
	if err := b.reservations.Release(bctx, hold.TeamID, hold.SandboxID); err != nil {
		logger.L().Error(ctx, "Failed to release reservation", zap.Error(err), logger.WithSandboxID(hold.SandboxID))
	}

	return removed, nil
}

// StartRemoving returns, for the caller that started the transition, the
// callback that ends it with the caller's error; a no-op when this call
// started no transition.
func (s *Store) StartRemoving(ctx context.Context, teamID uuid.UUID, sandboxID string, opts RemoveOpts) (StateTransition, bool, func(context.Context, error), error) {
	bctx, b, span := s.begin(ctx, "start-removing", teamID, sandboxID)
	defer span.End()

	var transition StateTransition
	var alreadyDone bool
	begin := func() (err error) {
		transition, alreadyDone, err = sandboxtypes.StartRemoving(bctx, b.storage, teamID, sandboxID, opts)

		return err
	}
	var err error
	if opts.WaitForLifecycleHold {
		err = waitOutHold(bctx, begin)
	} else {
		err = begin()
	}
	if err != nil {
		return transition, false, nil, err
	}
	if transition.ID == "" {
		return transition, alreadyDone, func(context.Context, error) {}, nil
	}

	return transition, alreadyDone, func(ctx context.Context, workErr error) {
		err := b.storage.FinishTransition(ctx, teamID, sandboxID, transition.ID, opts.Action, sandboxtypes.TransitionOutcomeOf(workErr))
		if err != nil {
			logger.L().Warn(ctx, "Failed to finish state transition", logger.WithSandboxID(sandboxID), zap.String("transition_id", transition.ID), zap.Error(err))
		}
	}, nil
}

func (s *Store) RestoreRunning(ctx context.Context, transition StateTransition, opts RestoreOpts) (Sandbox, error) {
	bctx, b, span := s.begin(ctx, "restore-running", transition.Sandbox.TeamID, transition.Sandbox.SandboxID)
	defer span.End()

	return b.storage.RestoreRunning(bctx, transition, opts)
}

func (s *Store) RestoreCheckpointed(ctx context.Context, transition StateTransition, placement *orchestrator.SandboxNetworkPlacement) (Sandbox, error) {
	bctx, b, span := s.begin(ctx, "restore-checkpointed", transition.Sandbox.TeamID, transition.Sandbox.SandboxID)
	defer span.End()

	return b.storage.RestoreCheckpointed(bctx, transition, placement)
}

func (s *Store) WaitForStateChange(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	bctx, b, span := s.begin(ctx, "wait-for-state-change", teamID, sandboxID)
	defer span.End()

	_, err := b.storage.WaitForTransition(bctx, teamID, sandboxID, "")

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
	bctx, b, span := s.begin(ctx, "reconcile", uuid.Nil, "")
	defer span.End()

	orphans := b.storage.Reconcile(bctx, running, nodeID)

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
	bctx, b, span := s.begin(ctx, "reserve", teamID, sandboxID)
	defer span.End()

	reserved, err := b.reservations.Reserve(bctx, teamID, sandboxID, limit)
	if err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			// Try to get the sandbox from the storage if already exists
			return nil, func(ctx context.Context) (Sandbox, error) {
				return b.storage.Get(ctx, teamID, sandboxID)
			}, nil
		}

		return nil, nil, err
	}

	if !reserved {
		return nil, func(ctx context.Context) (Sandbox, error) {
			result, err := b.reservations.WaitForStart(ctx, teamID, sandboxID)
			if err != nil {
				return Sandbox{}, err
			}

			return decodeResult(result)
		}, nil
	}

	return b.finishStart(ctx, teamID, sandboxID), nil, nil
}

func (b backends) finishStart(ctx context.Context, teamID uuid.UUID, sandboxID string) func(Sandbox, error) {
	return func(sbx Sandbox, startErr error) {
		bgCtx := context.WithoutCancel(ctx)

		resultData, encodeErr := encodeResult(sbx, startErr)
		if encodeErr != nil {
			logger.L().Error(ctx, "failed to encode reservation result",
				zap.Error(encodeErr),
				logger.WithSandboxID(sandboxID),
			)

			// Still try to remove from pending even if encoding fails.
			_ = b.reservations.Release(bgCtx, teamID, sandboxID)

			return
		}

		if err := b.reservations.FinishStart(bgCtx, teamID, sandboxID, resultData); err != nil {
			logger.L().Error(ctx, "failed to run finishStart script",
				zap.Error(err),
				logger.WithSandboxID(sandboxID),
			)
		}
	}
}

func (s *Store) ClaimKill(ctx context.Context, teamID uuid.UUID, sandboxID string) (string, bool, error) {
	bctx, b, span := s.begin(ctx, "claim-kill", teamID, sandboxID)
	defer span.End()

	return b.reservations.ClaimKill(bctx, teamID, sandboxID)
}

func (s *Store) ReleaseKillClaim(ctx context.Context, teamID uuid.UUID, sandboxID, claimID string) error {
	bctx, b, span := s.begin(ctx, "release-kill-claim", teamID, sandboxID)
	defer span.End()

	return b.reservations.ReleaseKillClaim(bctx, teamID, sandboxID, claimID)
}

// WaitForStart returns once no start of the sandbox is pending. How that start
// ended is the starter's to report, so only a ctx error is returned.
func (s *Store) WaitForStart(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	bctx, b, span := s.begin(ctx, "wait-for-start", teamID, sandboxID)
	defer span.End()

	_, _ = b.reservations.WaitForStart(bctx, teamID, sandboxID)

	return ctx.Err()
}
