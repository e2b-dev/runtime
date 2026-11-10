package proxy

import (
	"context"

	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
)

// SandboxResumer is the one call the resume flights share with the resumer
// they wrap.
type SandboxResumer interface {
	// Resume attempts to resume/start the sandbox and returns its route on
	// success: the orchestrator IP and, for a routed network, the placement
	// of the execution the API answered for. Callers share the result and
	// must not modify it.
	Resume(ctx context.Context, sandboxId string, sandboxPort uint64, trafficAccessToken string, envdAccessToken string) (*catalog.SandboxInfo, error)
}

// PausedSandboxResumer is the resumer main wires and initialises: Init
// observes the connection it holds. The proxy takes only SandboxResumer.
type PausedSandboxResumer interface {
	Init(ctx context.Context)
	SandboxResumer
}
