//go:build linux

package network

import (
	"context"
	"errors"
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

// ErrSlotsLeft reports that Close reached its teardown deadline with slots
// still pooled. Their namespaces stay in place until a start reclaims them.
var ErrSlotsLeft = errors.New("network slots left in place at the teardown deadline")

type teardownDeadlineKey struct{}

// WithTeardownDeadline makes Close stop tearing pooled slots down at deadline
// and leave the rest, so set it only when the next start reclaims leftover
// slots. Close still publishes closed and waits for in-flight returns, which
// tear their own slots down. A context deadline cannot carry this: Close
// returns at once when its context has ended.
func WithTeardownDeadline(ctx context.Context, deadline time.Time) context.Context {
	return context.WithValue(ctx, teardownDeadlineKey{}, deadline)
}

// TeardownDeadlinePassed reports whether Close has to leave the slots that are
// still pooled.
func TeardownDeadlinePassed(ctx context.Context) bool {
	deadline, ok := ctx.Value(teardownDeadlineKey{}).(time.Time)

	return ok && !time.Now().Before(deadline)
}
