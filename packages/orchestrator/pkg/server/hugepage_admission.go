//go:build linux

package server

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// hugePageAdmission holds the starts of hugepage guests to the pages the node
// can still allocate. A start faults its guest memory in over seconds, so the
// pool does not show it yet: the memory of every start it admitted counts as
// taken until that start ends.
type hugePageAdmission struct {
	// read serves the last good sample when a read fails, and errs only
	// before the first one.
	read func(context.Context) (*metrics.MemoryMetrics, error)

	mu         sync.Mutex
	pendingMiB int64
}

// hugePageShortage is a start the free pages cannot back.
type hugePageShortage struct {
	ramMB        int64
	neededPages  uint64
	freePages    uint64
	pendingPages uint64
}

// freeHugePages is how many hugepages the node can still allocate: the pool
// ceiling (base pool plus overcommit allowance) less the pages in use and the
// pages reserved by mappings not yet faulted. ok is false when the metrics
// carry no ceiling or no page size, as placement treats them.
func freeHugePages(m *metrics.MemoryMetrics) (uint64, bool) {
	if m == nil || m.HugePagesCapacity == 0 || m.HugePageSizeBytes == 0 {
		return 0, false
	}
	committed := m.HugePagesUsed + m.HugePagesReserved
	if committed >= m.HugePagesCapacity {
		return 0, true
	}

	return m.HugePagesCapacity - committed, true
}

func hugePagesFor(mib int64, pageSize uint64) uint64 {
	if mib <= 0 || pageSize == 0 {
		return 0
	}
	bytes := uint64(mib) << 20

	return (bytes + pageSize - 1) / pageSize
}

// admit takes ramMB of hugepages for one start until release is called, or
// returns the shortage. A read error or a node without a ceiling admits: the
// check cannot run there.
func (a *hugePageAdmission) admit(ctx context.Context, ramMB int64) (func(), *hugePageShortage) {
	a.mu.Lock()
	defer a.mu.Unlock()

	m, err := a.read(ctx)
	if err != nil {
		logger.L().Warn(ctx, "Cannot read the hugepage pool, admitting the start unchecked", zap.Error(err))
	}
	free, ok := freeHugePages(m)
	if err == nil && ok {
		needed := hugePagesFor(ramMB, m.HugePageSizeBytes)
		pending := hugePagesFor(a.pendingMiB, m.HugePageSizeBytes)
		if pending > free || needed > free-pending {
			return nil, &hugePageShortage{ramMB: ramMB, neededPages: needed, freePages: free, pendingPages: pending}
		}
	}

	return a.holdLocked(ramMB), nil
}

// hold takes ramMB of hugepages for a start that is never refused until
// release is called.
func (a *hugePageAdmission) hold(ramMB int64) func() {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.holdLocked(ramMB)
}

func (a *hugePageAdmission) holdLocked(ramMB int64) func() {
	a.pendingMiB += ramMB
	var once sync.Once

	return func() {
		once.Do(func() {
			a.mu.Lock()
			a.pendingMiB -= ramMB
			a.mu.Unlock()
		})
	}
}

func usesHugePages(sbx *sandbox.Sandbox) bool { return sbx.Config.HugePages }

// admitHugePages refuses a hugepage start the node cannot back before it
// takes anything, or returns the release of the pages it holds for the start.
// The refusal carries no create-refused detail, so placement tries another
// node. While a sandbox on this node is tearing down its pages are on their
// way back, so the refusal is ResourceExhausted, which placement retries
// here as well. Only hugepage lifecycles count, and a teardown counts when it
// was running at the pool read, as one that ended since may have returned
// pages the read did not see.
func (s *Server) admitHugePages(ctx context.Context, cfg *orchestrator.SandboxConfig) (func(), error) {
	if s.hugePages == nil || !cfg.GetHugePages() || !s.featureFlags.BoolFlag(ctx, featureflags.CreateHugePageAdmissionFlag) {
		return func() {}, nil
	}

	stoppingBefore := s.sandboxFactory.Sandboxes.StoppingMatching(usesHugePages)
	release, shortage := s.hugePages.admit(ctx, cfg.GetRamMb())
	if shortage == nil {
		return release, nil
	}

	stopping := max(stoppingBefore, s.sandboxFactory.Sandboxes.StoppingMatching(usesHugePages))
	telemetry.ReportEvent(ctx, "create refused: not enough free hugepages",
		attribute.Int64("ram_mb", shortage.ramMB),
		attribute.Int64("hugepages.needed", int64(shortage.neededPages)),
		attribute.Int64("hugepages.free", int64(shortage.freePages)),
		attribute.Int64("hugepages.pending", int64(shortage.pendingPages)),
		attribute.Int("stopping", stopping))
	logger.L().Warn(ctx, "Insufficient hugepages for sandbox start",
		logger.WithSandboxID(cfg.GetSandboxId()),
		zap.Int64("ram_mb", shortage.ramMB),
		zap.Uint64("needed_pages", shortage.neededPages),
		zap.Uint64("free_pages", shortage.freePages),
		zap.Uint64("pending_pages", shortage.pendingPages),
		zap.Int("stopping", stopping))

	if stopping > 0 {
		return nil, status.Errorf(codes.ResourceExhausted, "not enough free hugepages for %d MiB of guest memory while a sandbox is still stopping on this node, please retry", shortage.ramMB)
	}

	return nil, status.Errorf(codes.FailedPrecondition, "this node cannot allocate %d MiB of guest memory in hugepages", shortage.ramMB)
}

// holdHugePages counts the guest memory of a hugepage start the admission
// does not refuse, a checkpoint's resume or a prefetch harvest, until the
// returned release is called.
func (s *Server) holdHugePages(ctx context.Context, cfg *sandbox.Config) func() {
	if s.hugePages == nil || !cfg.HugePages || !s.featureFlags.BoolFlag(ctx, featureflags.CreateHugePageAdmissionFlag) {
		return func() {}
	}

	return s.hugePages.hold(cfg.RamMB)
}
