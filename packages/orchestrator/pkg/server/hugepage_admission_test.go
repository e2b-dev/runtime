//go:build linux

package server

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
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
	release := on.holdHugePages(t.Context(), sandbox.NewConfig(sandbox.Config{RamMB: 4096, HugePages: true}), metadata.Template{})
	assert.Equal(t, int64(4096), on.hugePages.pendingMiB)
	release()
	assert.Zero(t, on.hugePages.pendingMiB)

	on.holdHugePages(t.Context(), sandbox.NewConfig(sandbox.Config{RamMB: 4096}), metadata.Template{})()
	assert.Zero(t, on.hugePages.pendingMiB)

	off := hugePageAdmissionServer(t, false, pool(5120, 0, 0))
	off.holdHugePages(t.Context(), sandbox.NewConfig(sandbox.Config{RamMB: 4096, HugePages: true}), metadata.Template{})
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
	sbx := &orchestrator.SandboxConfig{SandboxId: "sandbox-2", RamMb: 4096}

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

	_, err := s.admitHugePages(t.Context(), &orchestrator.SandboxConfig{SandboxId: "sandbox-2", RamMb: 4096})
	require.Zero(t, s.sandboxFactory.Sandboxes.Stopping(), "the teardown ended during the read")
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
}

// Every start uses hugepages, so a request that leaves huge_pages unset is
// still checked against the pool.
func TestAdmitHugePagesIgnoresTheRequestedPageSize(t *testing.T) {
	t.Parallel()

	s := hugePageAdmissionServer(t, true, pool(5120, 5120, 0))
	_, err := s.admitHugePages(t.Context(), &orchestrator.SandboxConfig{SandboxId: "sandbox-2", RamMb: 4096})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// With the flag off or the pool unwired, a start never reads the pool and is
// never refused.
func TestAdmitHugePagesControl(t *testing.T) {
	t.Parallel()

	exhausted := pool(5120, 5120, 0)

	off := hugePageAdmissionServer(t, false, exhausted)
	release, err := off.admitHugePages(t.Context(), &orchestrator.SandboxConfig{SandboxId: "sandbox-2", RamMb: 4096})
	require.NoError(t, err)
	release()

	unwired := hugePageAdmissionServer(t, true, exhausted)
	unwired.hugePages = nil
	release, err = unwired.admitHugePages(t.Context(), &orchestrator.SandboxConfig{SandboxId: "sandbox-2", RamMb: 4096})
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
		Sandbox: &orchestrator.SandboxConfig{SandboxId: "sandbox-2", ExecutionId: "exec-1", Vcpu: 2, RamMb: 4096},
	})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	assert.Zero(t, s.sandboxFactory.Sandboxes.Count())
	reservation, err := s.sandboxFactory.Sandboxes.Reserve("sandbox-2")
	require.NoError(t, err, "the refusal released the sandbox ID")
	reservation.Release()
	require.True(t, s.startingSandboxes.TryAcquire(1), "the refusal released the start slot")
	s.startingSandboxes.Release(1)
}

// Admission, before the template is read, charges boot RAM whatever size the start asks for; the
// requested size is charged once the template accepted it, and a hold counts the memory the start
// runs with.
func TestHugePageAdmissionChargesTheRequestedMemory(t *testing.T) {
	t.Parallel()

	s := hugePageAdmissionServer(t, true, pool(5120, 0, 0))
	release, err := s.admitHugePages(t.Context(), &orchestrator.SandboxConfig{SandboxId: "sandbox-1", RamMb: 1024, MemoryMb: 12288})
	require.NoError(t, err, "a size the template has not checked yet is not charged")
	assert.Equal(t, int64(1024), s.hugePages.pendingMiB)
	release()

	_, err = s.admitHugePagesMB(t.Context(), "sandbox-2", 12288)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "12 GiB does not fit a 10 GiB pool")

	hold := s.holdHugePages(t.Context(), sandbox.NewConfig(sandbox.Config{RamMB: 1024, MemoryMB: 4096, HugePages: true}), metadata.Template{})
	assert.Equal(t, int64(4096), s.hugePages.pendingMiB)
	hold()

	// A request off the slot grid is refused by the resume; until then the hold counts it as the
	// whole slots a handshake would reserve.
	hold = s.holdHugePages(t.Context(), sandbox.NewConfig(sandbox.Config{RamMB: 1024, MemoryMB: 1024 + 1026, HugePages: true}), metadata.Template{MemoryHotplugMB: 3072})
	assert.Equal(t, int64(1024+1152), s.hugePages.pendingMiB)
	hold()
}

// A memory resume that names no size runs at the size its snapshot had asked for, and is charged
// it. With 2 GiB of boot RAM, created at 6 GiB and paused with 4 GiB recorded as plugged, it does
// not fit a node with 5 GiB of free hugepages, as it would not with that size named.
func TestResumeWithoutASizeIsChargedTheRecordedMemory(t *testing.T) {
	t.Parallel()

	meta := metadata.Template{MemoryHotplugMB: 6144, MemoryHotplugRequestedMB: 4096}
	for name, memoryMB := range map[string]int64{"no size named": 0, "the same size named": 6144} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := sandbox.NewConfig(sandbox.Config{RamMB: 2048, MemoryMB: memoryMB})
			_, hotplugMB, err := startMemory(meta, cfg, false)
			require.NoError(t, err)
			assert.Equal(t, int64(4096), hotplugMB)

			s := hugePageAdmissionServer(t, true, pool(2560, 0, 0))
			release, err := s.admitHugePages(t.Context(), &orchestrator.SandboxConfig{SandboxId: "sandbox-1", RamMb: 2048, MemoryMb: memoryMB})
			require.NoError(t, err, "boot RAM fits")
			defer release()
			_, err = s.admitHugePagesMB(t.Context(), "sandbox-1", hotplugMB)
			assert.Equal(t, codes.FailedPrecondition, status.Code(err), "6 GiB does not fit 5 GiB of free hugepages")
		})
	}
}

// A checkpoint's resume or a prefetch harvest of a sandbox that kept its snapshot's size holds the
// memory the guest restores: boot RAM plus what the snapshot had asked to plug.
func TestHoldHugePagesCountsTheRecordedMemory(t *testing.T) {
	t.Parallel()

	s := hugePageAdmissionServer(t, true, pool(5120, 0, 0))
	meta := metadata.Template{MemoryHotplugMB: 6144, MemoryHotplugRequestedMB: 4096}
	hold := s.holdHugePages(t.Context(), sandbox.NewConfig(sandbox.Config{RamMB: 2048, HugePages: true}), meta)
	assert.Equal(t, int64(6144), s.hugePages.pendingMiB)
	hold()

	hold = s.holdHugePages(t.Context(), sandbox.NewConfig(sandbox.Config{RamMB: 2048, MemoryMB: 1024, HugePages: true}), meta)
	assert.Equal(t, int64(6144), s.hugePages.pendingMiB, "a request the resume refuses still holds the recorded size")
	hold()
	assert.Zero(t, s.hugePages.pendingMiB)
}

// A start whose guest memory cannot be mapped reports the memory it was charged, so placement sees
// its real need: a named size, or the size a memory resume's snapshot had asked for.
func TestMemoryRefusalReportsTheChargedSize(t *testing.T) {
	t.Parallel()

	meta := metadata.Template{MemoryHotplugMB: 6144, MemoryHotplugRequestedMB: 4096}
	s := memoryRefusalServer(t, true)
	for name, memoryMB := range map[string]int64{"no size named": 0, "a larger size named": 8192} {
		startMB, _, err := startMemory(meta, sandbox.NewConfig(sandbox.Config{RamMB: 2048, MemoryMB: memoryMB}), false)
		require.NoError(t, err)
		refusal := s.memoryRefusal(t.Context(), guestMemoryChain(), startMB, 0, sandboxtypes.RuntimeMetadata{})
		assert.Contains(t, status.Convert(refusal).Message(), fmt.Sprintf("%d MiB", max(memoryMB, 6144)), name)
	}
}

// A requested size is checked against the template before any of it is charged: one the template
// cannot take is the caller's error, refused as InvalidArgument whatever the node has free.
func TestRequestedHotplugCharge(t *testing.T) {
	t.Parallel()

	meta := metadata.Template{MemoryHotplugMB: 3072, MemoryHotplugRequestedMB: 1024}
	for name, tc := range map[string]struct {
		memoryMB int64
		coldBoot bool
		want     int64
		wantErr  error
	}{
		"no size named keeps the snapshot's":      {memoryMB: 0, want: 1024},
		"no size named on a cold boot plugs back": {memoryMB: 0, coldBoot: true, want: 1024},
		"boot RAM named on a cold boot":           {memoryMB: 1024, coldBoot: true, want: 0},
		"a size within the region":                {memoryMB: 3072, want: 2048},
		"beyond the region":                       {memoryMB: 8192, wantErr: fc.ErrMemoryRequestInvalid},
		"below the snapshot's request":            {memoryMB: 1024, wantErr: fc.ErrMemoryHotplugShrink},
		"a cold boot may plug less":               {memoryMB: 1536, coldBoot: true, want: 512},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			total, got, err := startMemory(meta, sandbox.NewConfig(sandbox.Config{RamMB: 1024, MemoryMB: tc.memoryMB}), tc.coldBoot)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, 1024+tc.want, total)
		})
	}

	s := hugePageAdmissionServer(t, true, pool(2048, 0, 0))
	_, _, err := startMemory(meta, sandbox.NewConfig(sandbox.Config{RamMB: 1024, MemoryMB: 8192}), false)
	st := status.Convert(s.memoryHotplugRefusal(t.Context(), err, sandboxtypes.RuntimeMetadata{}))
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

// A reboot or filesystem-only resume that names no size plugs back what its lineage had asked for,
// which admission, run before the template is read, did not see: it is charged on top, and a start
// that names a size is charged that size instead.
func TestColdBootPlugBackIsCharged(t *testing.T) {
	t.Parallel()

	meta := metadata.Template{MemoryHotplugMB: 3072, MemoryHotplugRequestedMB: 2048}
	for name, tc := range map[string]struct {
		meta              metadata.Template
		memoryMB          int64
		wantTotal, wantHP int64
	}{
		"no size plugs back the lineage's": {meta: meta, wantTotal: 3072, wantHP: 2048},
		"a named size wins":                {meta: meta, memoryMB: 2048, wantTotal: 2048, wantHP: 1024},
		"a lineage that plugged nothing":   {meta: metadata.Template{}, wantTotal: 1024, wantHP: 0},
	} {
		total, hotplugMB, err := startMemory(tc.meta, sandbox.NewConfig(sandbox.Config{RamMB: 1024, MemoryMB: tc.memoryMB}), true)
		require.NoError(t, err, name)
		assert.Equal(t, tc.wantTotal, total, name)
		assert.Equal(t, tc.wantHP, hotplugMB, name)
	}

	s := hugePageAdmissionServer(t, true, pool(1400, 0, 0))
	release, err := s.admitHugePages(t.Context(), &orchestrator.SandboxConfig{SandboxId: "sandbox-1", RamMb: 1024})
	require.NoError(t, err)
	defer release()
	_, err = s.admitHugePagesMB(t.Context(), "sandbox-1", 2048)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "1 GiB plus 2 GiB plugged back does not fit a 2.7 GiB pool")
}
