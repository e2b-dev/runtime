//go:build linux

package network

import (
	"context"
	"time"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// PoolInterface abstracts the network slot pool so v1 (*Pool) and v2 pools can
// be swapped. The existing *Pool already satisfies this interface.
type PoolInterface interface {
	Get(ctx context.Context, network *orchestrator.SandboxNetworkConfig, class sandboxtypes.EgressClass) (*Slot, error)
	ReturnAsync(ctx context.Context, slot *Slot, releasedFn ReleaseNotify, returnDelay time.Duration) error
	Close(ctx context.Context) error
}
