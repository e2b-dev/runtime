package sandbox_catalog

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
)

type SandboxInfo struct {
	OrchestratorID string `json:"orchestrator_id"`
	OrchestratorIP string `json:"orchestrator_ip"` // used only for cases where orchestrator is not registered in edge pool

	ExecutionID      string    `json:"execution_id"`
	StartedAt        time.Time `json:"sandbox_started_at"`          // when sandbox was started
	MaxLengthInHours int64     `json:"sandbox_max_length_in_hours"` // how long can sandbox can possibly run (in hours)

	// LifecycleID is the Firecracker process serving this execution; it changes
	// on every VM start, including a move of the same execution to another
	// worker. OrchestratorID above is the worker process instance.
	LifecycleID string `json:"lifecycle_id,omitempty"`

	// NetworkPlacement is the routed-network placement. Absent means the
	// sandbox uses the legacy per-worker network.
	NetworkPlacement *NetworkPlacement `json:"network_placement,omitempty"`
}

type SandboxesCatalog interface {
	GetSandbox(ctx context.Context, sandboxID string) (*SandboxInfo, error)
	StoreSandbox(ctx context.Context, sandboxID string, sandboxInfo *SandboxInfo, expiration time.Duration) error
	DeleteSandbox(ctx context.Context, sandboxID string, executionID string) error
	Close(ctx context.Context) error
}

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog")

var ErrSandboxNotFound = errors.New("sandbox not found")
