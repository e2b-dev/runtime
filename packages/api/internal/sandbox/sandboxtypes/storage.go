package sandboxtypes

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

const (
	StorageNameRedis = "redis"
)

// Storage is the persistence interface implemented by the redis backend.
type Storage interface {
	// Add stores a sandbox. A record of the same execution already stored is
	// kept: the write is then a late duplicate of the one that landed.
	Add(ctx context.Context, sandbox Sandbox) error
	// Replace stores a sandbox over whatever is stored under its ID.
	Replace(ctx context.Context, sandbox Sandbox) error
	Get(ctx context.Context, teamID uuid.UUID, sandboxID string) (Sandbox, error)
	// Remove deletes the record of the expected execution, or of any when
	// none is expected; another execution's is left whole and
	// ErrExecutionMismatch returned. A sandbox already gone is a success.
	Remove(ctx context.Context, teamID uuid.UUID, sandboxID, expectedExecutionID string) error

	TeamItems(ctx context.Context, teamID uuid.UUID, states []State) ([]Sandbox, error)
	ExpiredItems(ctx context.Context) ([]Sandbox, error)
	TeamsWithSandboxCount(ctx context.Context) (map[uuid.UUID]int64, error)

	Update(ctx context.Context, teamID uuid.UUID, sandboxID string, updateFunc func(sandbox Sandbox) (Sandbox, error)) (Sandbox, error)
	StateTransitions
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

// StateTransitions is the removal state machine: start a transition, undo one
// the node refused, end a checkpoint with the network it left running, or
// wait one out.
type StateTransitions interface {
	StartRemoving(ctx context.Context, teamID uuid.UUID, sandboxID string, opts RemoveOpts) (StateTransition, bool, func(context.Context, error), error)
	RestoreRunning(ctx context.Context, transition StateTransition, opts RestoreOpts) (Sandbox, error)
	// RestoreCheckpointed puts the record of a checkpoint's transition back
	// to Running with placement, nil for the worker's own network, as
	// RestoreRunning does without a retry window. It refuses a record that
	// is another execution, left the transition's state or changed under it.
	RestoreCheckpointed(ctx context.Context, transition StateTransition, placement *orchestrator.SandboxNetworkPlacement) (Sandbox, error)
	// WaitForStateChange waits out the sandbox's transition, if one is open.
	// It takes no tenant guard, since the record may already be gone: the
	// caller reads the record as the team's before it waits.
	WaitForStateChange(ctx context.Context, teamID uuid.UUID, sandboxID string) error
}

type StateTransition struct {
	Sandbox Sandbox
	// Only the caller that starts the transition owns its pre-clamp expiry.
	OriginalEndTime *time.Time
}

// ReservationStorage tracks per-team sandbox-start reservations to enforce
// concurrency limits.
type ReservationStorage interface {
	Reserve(ctx context.Context, teamID uuid.UUID, sandboxID string, limit int) (finishStart func(Sandbox, error), waitForStart func(ctx context.Context) (Sandbox, error), err error)
	Release(ctx context.Context, teamID uuid.UUID, sandboxID string) error

	// ClaimKill returns false when a resume is in flight or the sandbox is
	// running again; the caller must then keep the snapshot.
	ClaimKill(ctx context.Context, teamID uuid.UUID, sandboxID string) (claimID string, claimed bool, err error)
	ReleaseKillClaim(ctx context.Context, teamID uuid.UUID, sandboxID, claimID string) error
	WaitForStart(ctx context.Context, teamID uuid.UUID, sandboxID string) error
}
