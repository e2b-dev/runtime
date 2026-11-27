//go:build linux

package sandbox

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/clickhouse/pkg/hoststats"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/cgroup"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type memoryDelivery struct {
	mu   sync.Mutex
	rows []hoststats.SandboxHostStat
	err  error
}

func (d *memoryDelivery) Push(s hoststats.SandboxHostStat) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rows = append(d.rows, s)

	return d.err
}
func (*memoryDelivery) Close(context.Context) error { return nil }

func TestHostStatsMemoryLifecycle(t *testing.T) {
	t.Parallel()
	delivery := &memoryDelivery{}
	var usage block.MemoryUsage
	var ready bool
	cpu := &cgroup.Stats{}
	h := NewHostStatsCollector(HostStatsMetadata{ExecutionID: "same-execution"}, delivery,
		func(context.Context) (*cgroup.Stats, error) { return cpu, nil },
		func() (block.MemoryUsage, bool) { return usage, ready })
	sample := func() hoststats.SandboxHostStat {
		t.Helper()
		require.NoError(t, h.CollectSample(t.Context()))

		return delivery.rows[len(delivery.rows)-1]
	}
	require.False(t, sample().GuestMemoryMeasured)
	ready = true
	usage = block.MemoryUsage{PageStateBytes: [block.NumStates]int64{block.Clean: 4096}, HeldMiBMillis: 100}
	got := sample()
	require.True(t, got.GuestMemoryMeasured)
	require.Equal(t, uint64(4096), got.GuestMemoryHeldBytes)
	require.Equal(t, uint64(100), got.DeltaGuestMemoryHeldMiBMillis)
	usage.HeldMiBMillis = 150
	cpu.CPUUsageUsec = 50
	delivery.err = errors.New("full")
	require.Error(t, h.CollectSample(t.Context()))
	require.Equal(t, uint64(100), h.prev.GuestMemoryHeldMiBMillis)
	usage.HeldMiBMillis = 175
	cpu.CPUUsageUsec = 75
	delivery.err = nil
	got = sample()
	require.Equal(t, uint64(75), got.DeltaGuestMemoryHeldMiBMillis)
	require.Equal(t, uint64(75), got.DeltaCgroupCPUUsageUsec)
	usage.HeldMiBMillis = 10
	require.Zero(t, sample().DeltaGuestMemoryHeldMiBMillis)
	ready = false
	got = sample()
	require.False(t, got.GuestMemoryMeasured)
	require.Zero(t, got.GuestMemoryHeldBytes)
	require.Zero(t, got.GuestMemoryHeldMiBMillis)
	require.Zero(t, got.DeltaGuestMemoryHeldMiBMillis)
	cold := &Sandbox{Resources: &Resources{memory: uffd.NewNoopMemory(4096, 4096)}}
	h = NewHostStatsCollector(h.metadata, delivery, h.cgroupStats, cold.MemoryUsage)
	require.False(t, sample().GuestMemoryMeasured)
}

func TestHostStatsFinalMemorySample(t *testing.T) {
	t.Parallel()
	tr := block.NewMemoryTracker(4096)
	tr.MarkInstalled(0, 256, block.Clean)
	tr.StopMemoryUsage()
	final := tr.MemoryUsage()
	delivery := &memoryDelivery{}
	h := NewHostStatsCollector(HostStatsMetadata{}, delivery,
		func(context.Context) (*cgroup.Stats, error) { return &cgroup.Stats{}, nil },
		func() (block.MemoryUsage, bool) { return tr.MemoryUsage(), true })
	go h.Start(t.Context())
	h.Stop(t.Context())
	require.Len(t, delivery.rows, 2)
	got := delivery.rows[1]
	require.True(t, got.GuestMemoryMeasured)
	require.Equal(t, final.HeldMiBMillis, got.DeltaGuestMemoryHeldMiBMillis)
	require.Equal(t, final.HeldBytes(), got.GuestMemoryHeldBytes)
}

func TestMemoryUsageUnavailable(t *testing.T) {
	t.Parallel()
	for _, s := range []*Sandbox{{}, {Resources: &Resources{memory: uffd.NewNoopMemory(4096, 4096)}}} {
		_, ok := s.MemoryUsage()
		require.False(t, ok)
	}
}

func TestHostStatsFinalSampleWaitsForMemory(t *testing.T) {
	t.Parallel()
	var total atomic.Uint64
	delivery := &memoryDelivery{}
	id := uuid.NewString()
	h := NewHostStatsCollector(HostStatsMetadata{SandboxID: id}, delivery,
		func(context.Context) (*cgroup.Stats, error) { return &cgroup.Stats{}, nil },
		func() (block.MemoryUsage, bool) { return block.MemoryUsage{HeldMiBMillis: total.Load()}, true })
	go h.Start(t.Context())
	stopHostStatsAfterMemory(t.Context(), h, func(context.Context) error {
		total.Store(12345)

		return errors.New("handler failed after its final transition")
	})
	got := delivery.rows[len(delivery.rows)-1]
	require.True(t, got.GuestMemoryMeasured)
	require.Equal(t, uint64(12345), got.GuestMemoryHeldMiBMillis)
	require.Zero(t, testLogObserver.FilterMessage("memory handler wait ended before final host stats sample").FilterField(zap.String("sandbox.id", id)).Len())
}

func TestHostStatsFinalSampleBoundsMemoryWait(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		delivery := &memoryDelivery{}
		id := uuid.NewString()
		h := NewHostStatsCollector(HostStatsMetadata{SandboxID: id}, delivery,
			func(context.Context) (*cgroup.Stats, error) { return &cgroup.Stats{}, nil },
			func() (block.MemoryUsage, bool) { return block.MemoryUsage{HeldMiBMillis: 12345}, true })
		go h.Start(t.Context())
		t.Cleanup(func() { h.Stop(t.Context()) })
		start := time.Now()
		stopHostStatsAfterMemory(t.Context(), h, func(ctx context.Context) error {
			_, bounded := ctx.Deadline()
			require.True(t, bounded)
			<-ctx.Done()

			return ctx.Err()
		})
		require.Equal(t, 3*time.Second, time.Since(start))
		fields := sandboxLog(t, "memory handler wait ended before final host stats sample", id)
		require.Equal(t, context.DeadlineExceeded.Error(), fields["error"])
		got := delivery.rows[len(delivery.rows)-1]
		require.True(t, got.GuestMemoryMeasured)
		require.Equal(t, uint64(12345), got.GuestMemoryHeldMiBMillis)
	})
}

func TestHostStatsLogsMemoryAnomaliesWithSandbox(t *testing.T) {
	t.Parallel()
	// The log observer is shared by every test and every -count run.
	sandboxID := "held-memory-anomaly-log-" + uuid.NewString()
	var usage block.MemoryUsage
	h := NewHostStatsCollector(HostStatsMetadata{SandboxID: sandboxID, ExecutionID: "execution"}, &memoryDelivery{},
		func(context.Context) (*cgroup.Stats, error) { return &cgroup.Stats{}, nil },
		func() (block.MemoryUsage, bool) { return usage, true })
	logged := func() int {
		return testLogObserver.FilterMessage("held-memory accounting anomaly").
			FilterField(logger.WithSandboxID(sandboxID)).
			FilterField(logger.WithExecutionID("execution")).Len()
	}
	require.NoError(t, h.CollectSample(t.Context()))
	require.Zero(t, logged())
	usage.Anomalies = 2
	require.NoError(t, h.CollectSample(t.Context()))
	require.NoError(t, h.CollectSample(t.Context()))
	require.Equal(t, 1, logged())
	usage.Anomalies = 3
	require.NoError(t, h.CollectSample(t.Context()))
	require.Equal(t, 2, logged())
}

func TestHostStatsMemoryWaitIsNotSampled(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var total atomic.Uint64
		delivery := &memoryDelivery{}
		h := NewHostStatsCollector(HostStatsMetadata{SandboxID: uuid.NewString()}, delivery,
			func(context.Context) (*cgroup.Stats, error) { return &cgroup.Stats{}, nil },
			func() (block.MemoryUsage, bool) { return block.MemoryUsage{HeldMiBMillis: total.Load()}, true })
		go h.Start(t.Context())
		time.Sleep(1500 * time.Millisecond)
		synctest.Wait()
		require.Len(t, delivery.rows, 2)
		// The wait crosses two sampling ticks.
		stopHostStatsAfterMemory(t.Context(), h, func(context.Context) error {
			time.Sleep(2500 * time.Millisecond)
			total.Store(777)

			return nil
		})
		require.Len(t, delivery.rows, 3)
		final := delivery.rows[2]
		require.Equal(t, uint64(500_000), final.IntervalUs)
		require.Equal(t, uint64(777), final.GuestMemoryHeldMiBMillis)
	})
}
