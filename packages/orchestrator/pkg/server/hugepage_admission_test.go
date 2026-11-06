//go:build linux

package server

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

const testHugePageSize = 2 << 20

// fakeMeminfo serves fixed hugepage counters and counts the reads.
type fakeMeminfo struct {
	memory metrics.MemoryMetrics
	err    error
	reads  atomic.Int64
}

func (f *fakeMeminfo) read(context.Context) (*metrics.MemoryMetrics, error) {
	f.reads.Add(1)
	if f.err != nil {
		return nil, f.err
	}
	m := f.memory

	return &m, nil
}

// pool is a node with capacity pages of which used are faulted and reserved
// are reserved but not yet faulted.
func pool(capacity, used, reserved uint64) *fakeMeminfo {
	return &fakeMeminfo{memory: metrics.MemoryMetrics{
		HugePagesTotal:    capacity,
		HugePagesUsed:     used,
		HugePagesReserved: reserved,
		HugePageSizeBytes: testHugePageSize,
		HugePagesCapacity: capacity,
	}}
}

func hugePageAdmissionServer(t *testing.T, flagOn bool, meminfo *fakeMeminfo) *Server {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.CreateHugePageAdmissionFlag.Key()).VariationForAll(flagOn))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	s := duplicateCreateTestServer()
	s.featureFlags = ff
	s.hugePages = &hugePageAdmission{read: meminfo.read}

	return s
}

func TestFreeHugePages(t *testing.T) {
	t.Parallel()

	free, ok := freeHugePages(&pool(5120, 2000, 72).memory)
	assert.True(t, ok)
	assert.Equal(t, uint64(3048), free, "the ceiling less used and reserved")

	free, ok = freeHugePages(&pool(5120, 5000, 200).memory)
	assert.True(t, ok)
	assert.Zero(t, free, "an overcommitted pool has none")

	_, ok = freeHugePages(&pool(0, 0, 0).memory)
	assert.False(t, ok, "no ceiling: the check cannot run")

	noPageSize := pool(5120, 0, 0).memory
	noPageSize.HugePageSizeBytes = 0
	_, ok = freeHugePages(&noPageSize)
	assert.False(t, ok)

	_, ok = freeHugePages(nil)
	assert.False(t, ok)
}

// A start is admitted while the free pages, less those held by the starts
// already admitted, cover its memory; each release gives its pages back once.
func TestHugePageAdmissionCountsAdmittedStarts(t *testing.T) {
	t.Parallel()

	admission := &hugePageAdmission{read: pool(5120, 2000, 72).read}

	first, shortage := admission.admit(t.Context(), 4096)
	require.Nil(t, shortage, "2048 of 3048 free pages")

	_, shortage = admission.admit(t.Context(), 4096)
	require.NotNil(t, shortage, "the first start still holds 2048 pages")
	assert.Equal(t, hugePageShortage{ramMB: 4096, neededPages: 2048, freePages: 3048, pendingPages: 2048}, *shortage)

	second, shortage := admission.admit(t.Context(), 2000)
	require.Nil(t, shortage, "exactly the 1000 pages left")

	first()
	first()
	third, shortage := admission.admit(t.Context(), 4096)
	require.Nil(t, shortage, "a double release does not hand back more than it took")

	_, shortage = admission.admit(t.Context(), 2)
	require.NotNil(t, shortage)

	second()
	third()
	assert.Zero(t, admission.pendingMiB)
}

// A held start is never refused but counts against the admitted ones.
func TestHugePageAdmissionCountsHeldStarts(t *testing.T) {
	t.Parallel()

	admission := &hugePageAdmission{read: pool(5120, 4000, 0).read}

	release := admission.hold(4096)
	_, shortage := admission.admit(t.Context(), 1)
	require.NotNil(t, shortage, "2048 of 1120 free pages are held")
	assert.Equal(t, uint64(2048), shortage.pendingPages)

	release()
	release()
	admitted, shortage := admission.admit(t.Context(), 2048)
	require.Nil(t, shortage)
	admitted()
	assert.Zero(t, admission.pendingMiB)
}

// holdHugePages holds the memory of a hugepage guest with the flag on and
// nothing otherwise.
func TestHoldHugePages(t *testing.T) {
	t.Parallel()

	on := hugePageAdmissionServer(t, true, pool(5120, 0, 0))
	release := on.holdHugePages(t.Context(), sandbox.NewConfig(sandbox.Config{RamMB: 4096, HugePages: true}))
	assert.Equal(t, int64(4096), on.hugePages.pendingMiB)
	release()
	assert.Zero(t, on.hugePages.pendingMiB)

	on.holdHugePages(t.Context(), sandbox.NewConfig(sandbox.Config{RamMB: 4096}))()
	assert.Zero(t, on.hugePages.pendingMiB)

	off := hugePageAdmissionServer(t, false, pool(5120, 0, 0))
	off.holdHugePages(t.Context(), sandbox.NewConfig(sandbox.Config{RamMB: 4096, HugePages: true}))
	assert.Zero(t, off.hugePages.pendingMiB, "flag off holds nothing")
}

// A node whose pool cannot be read or has no ceiling admits every start.
func TestHugePageAdmissionAdmitsWhenUnchecked(t *testing.T) {
	t.Parallel()

	failing := &fakeMeminfo{err: errors.New("meminfo unreadable")}
	release, shortage := (&hugePageAdmission{read: failing.read}).admit(t.Context(), 1<<20)
	require.Nil(t, shortage)
	release()

	release, shortage = (&hugePageAdmission{read: pool(0, 0, 0).read}).admit(t.Context(), 1<<20)
	require.Nil(t, shortage)
	release()
}

// The refusal carries no create-refused detail, so placement tries another
// node; while a sandbox on the node is stopping it is ResourceExhausted.
func TestAdmitHugePagesRefusal(t *testing.T) {
	t.Parallel()

	s := hugePageAdmissionServer(t, true, pool(5120, 4000, 100))
	sbx := &orchestrator.SandboxConfig{SandboxId: "sandbox-2", RamMb: 4096, HugePages: true}

	_, err := s.admitHugePages(t.Context(), sbx)
	st := status.Convert(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	assert.Equal(t, "this node cannot allocate 4096 MiB of guest memory in hugepages", st.Message(), "the API forwards this text: no pool counters")
	assert.False(t, hasCreateRefusalDetail(st), "another node may have the pages")

	stopping := drainTestSandbox(t, "lifecycle-stopping")
	stopping.Config.HugePages = true
	require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), stopping))
	require.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), stopping.Runtime.SandboxID, "lifecycle-stopping"))

	_, err = s.admitHugePages(t.Context(), sbx)
	st = status.Convert(err)
	assert.Equal(t, codes.ResourceExhausted, st.Code(), "the stopping sandbox's pages are on their way back")
	assert.False(t, hasCreateRefusalDetail(st))

	plain := drainTestSandbox(t, "lifecycle-plain")
	plain.Runtime.SandboxID = "sandbox-3"
	require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), plain))
	require.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), "sandbox-3", "lifecycle-plain"))
	s.sandboxFactory.Sandboxes.MarkStopped(t.Context(), stopping)

	_, err = s.admitHugePages(t.Context(), sbx)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "a stopping sandbox without hugepages returns none")
}

// A teardown that ends while the pool is read may have returned pages the
// read did not see, so the refusal stays retryable on this node.
func TestAdmitHugePagesCountsATeardownEndingDuringTheRead(t *testing.T) {
	t.Parallel()

	meminfo := pool(5120, 4000, 100)
	s := hugePageAdmissionServer(t, true, meminfo)

	stopping := drainTestSandbox(t, "lifecycle-stopping")
	stopping.Config.HugePages = true
	require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), stopping))
	require.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), stopping.Runtime.SandboxID, "lifecycle-stopping"))
	s.hugePages.read = func(ctx context.Context) (*metrics.MemoryMetrics, error) {
		m, err := meminfo.read(ctx)
		s.sandboxFactory.Sandboxes.MarkStopped(ctx, stopping)

		return m, err
	}

	_, err := s.admitHugePages(t.Context(), &orchestrator.SandboxConfig{SandboxId: "sandbox-2", RamMb: 4096, HugePages: true})
	require.Zero(t, s.sandboxFactory.Sandboxes.Stopping(), "the teardown ended during the read")
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
}

// A guest without hugepages and every guest with the flag off never read the
// pool and are never refused.
func TestAdmitHugePagesControl(t *testing.T) {
	t.Parallel()

	exhausted := pool(5120, 5120, 0)

	on := hugePageAdmissionServer(t, true, exhausted)
	release, err := on.admitHugePages(t.Context(), &orchestrator.SandboxConfig{SandboxId: "sandbox-2", RamMb: 4096})
	require.NoError(t, err)
	release()

	off := hugePageAdmissionServer(t, false, exhausted)
	release, err = off.admitHugePages(t.Context(), &orchestrator.SandboxConfig{SandboxId: "sandbox-2", RamMb: 4096, HugePages: true})
	require.NoError(t, err)
	release()

	unwired := hugePageAdmissionServer(t, true, exhausted)
	unwired.hugePages = nil
	release, err = unwired.admitHugePages(t.Context(), &orchestrator.SandboxConfig{SandboxId: "sandbox-2", RamMb: 4096, HugePages: true})
	require.NoError(t, err)
	release()

	assert.Zero(t, exhausted.reads.Load())
}

// Create refuses before it touches the template cache (nil here, so reaching
// it would panic) and gives back the sandbox ID and the start slot.
func TestCreate_RefusesAStartTheFreeHugePagesCannotBack(t *testing.T) {
	t.Parallel()

	s := hugePageAdmissionServer(t, true, pool(5120, 5000, 0))

	_, err := s.Create(t.Context(), &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{SandboxId: "sandbox-2", ExecutionId: "exec-1", Vcpu: 2, RamMb: 4096, HugePages: true},
	})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	assert.Zero(t, s.sandboxFactory.Sandboxes.Count())
	reservation, err := s.sandboxFactory.Sandboxes.Reserve("sandbox-2")
	require.NoError(t, err, "the refusal released the sandbox ID")
	reservation.Release()
	require.True(t, s.startingSandboxes.TryAcquire(1), "the refusal released the start slot")
	s.startingSandboxes.Release(1)
}
