package sandboxtypes

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const (
	StorageNameRedis = "redis"
)

// Storage is the persistence interface implemented by the redis backend.
type Storage interface {
	Add(ctx context.Context, sandbox Sandbox) error
	Get(ctx context.Context, teamID uuid.UUID, sandboxID string) (Sandbox, error)
	Remove(ctx context.Context, teamID uuid.UUID, sandboxID string, executionID string) error

	TeamItems(ctx context.Context, teamID uuid.UUID, states []State) ([]Sandbox, error)
	ExpiredItems(ctx context.Context) ([]Sandbox, error)
	TeamsWithSandboxCount(ctx context.Context) (map[uuid.UUID]int64, error)

	Update(ctx context.Context, teamID uuid.UUID, sandboxID string, updateFunc func(sandbox Sandbox) (Sandbox, error)) (Sandbox, error)
	StateTransitions
	Reconcile(ctx context.Context, sandboxes []NodeSandbox, nodeID string) []NodeSandbox
}

// StateTransitions is the removal state machine: start a transition, undo one
// the node refused, or wait one out.
type StateTransitions interface {
	StartRemoving(ctx context.Context, teamID uuid.UUID, sandboxID string, opts RemoveOpts) (StateTransition, bool, func(context.Context, error), error)
	RestoreRunning(ctx context.Context, transition StateTransition, retryAfter time.Duration) (Sandbox, error)
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
}
