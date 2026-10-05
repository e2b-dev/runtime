//go:build linux

package sandbox

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/clickhouse/pkg/hoststats"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/cgroup"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// CgroupStatsFunc is a function that returns current cgroup resource usage statistics.
type CgroupStatsFunc func(ctx context.Context) (*cgroup.Stats, error)

const hostStatsSamplingInterval = 1 * time.Second

type HostStatsCollector struct {
	metadata    HostStatsMetadata
	delivery    hoststats.Delivery
	cgroupStats CgroupStatsFunc

	prev hoststats.SandboxHostStat // previous sample; seeded with zero counters at construction

	stopCh    chan struct{}
	stoppedCh chan struct{}
	stopOnce  sync.Once
}

type HostStatsMetadata struct {
	SandboxID   string
	ExecutionID string
	TemplateID  string
	BuildID     string
	TeamID      uuid.UUID
	VCPUCount   int64
	MemoryMB    int64
	SandboxType sandboxtypes.SandboxType
}

func NewHostStatsCollector(
	metadata HostStatsMetadata,
	delivery hoststats.Delivery,
	cgroupStats CgroupStatsFunc,
) *HostStatsCollector {
	return &HostStatsCollector{
		metadata:    metadata,
		delivery:    delivery,
		cgroupStats: cgroupStats,
		// Zero baseline so the first sample produces real deltas and interval.
		prev: hoststats.SandboxHostStat{
			Timestamp: time.Now(),
		},
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
}

// saturatingSub returns a - b, or 0 if b > a.
// This prevents uint64 underflow wrapping when cgroup counters reset
// (e.g. after sandbox resume from snapshot or cgroup migration).
func saturatingSub(a, b uint64) uint64 {
	if a < b {
		return 0
	}

	return a - b
}

// CollectSample collects a single cgroup statistics sample for the sandbox,
// computes deltas from the previous sample, and delivers the row.
func (h *HostStatsCollector) CollectSample(ctx context.Context) error {
	cgroupStats, err := h.cgroupStats(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cgroup stats: %w", err)
	}

	if cgroupStats == nil {
		return nil
	}

	now := time.Now()

	stat := hoststats.SandboxHostStat{
		Timestamp:                now,
		SandboxID:                h.metadata.SandboxID,
		SandboxExecutionID:       h.metadata.ExecutionID,
		SandboxTemplateID:        h.metadata.TemplateID,
		SandboxBuildID:           h.metadata.BuildID,
		SandboxTeamID:            h.metadata.TeamID,
		SandboxVCPUCount:         h.metadata.VCPUCount,
		SandboxMemoryMB:          h.metadata.MemoryMB,
		CgroupCPUUsageUsec:       cgroupStats.CPUUsageUsec,
		CgroupCPUUserUsec:        cgroupStats.CPUUserUsec,
		CgroupCPUSystemUsec:      cgroupStats.CPUSystemUsec,
		CgroupMemoryUsage:        cgroupStats.MemoryUsageBytes,
		CgroupMemoryPeak:         cgroupStats.MemoryPeakBytes,
		DeltaCgroupCPUUsageUsec:  saturatingSub(cgroupStats.CPUUsageUsec, h.prev.CgroupCPUUsageUsec),
		DeltaCgroupCPUUserUsec:   saturatingSub(cgroupStats.CPUUserUsec, h.prev.CgroupCPUUserUsec),
		DeltaCgroupCPUSystemUsec: saturatingSub(cgroupStats.CPUSystemUsec, h.prev.CgroupCPUSystemUsec),
		IntervalUs:               uint64(now.Sub(h.prev.Timestamp).Microseconds()),
		SandboxType:              h.metadata.SandboxType.String(),
	}

	if err := h.delivery.Push(stat); err != nil {
		return fmt.Errorf("failed to push stat to delivery: %w", err)
	}

	h.prev = stat

	return nil
}

// Start begins periodic collection of host statistics.
func (h *HostStatsCollector) Start(ctx context.Context) {
	defer close(h.stoppedCh)

	// Push the zero baseline as the first row. The first real CollectSample
	// on the ticker tick will diff against prev (zero counters at prev.Timestamp),
	// capturing all values accumulated since then without missing anything.
	initial := hoststats.SandboxHostStat{
		Timestamp:          h.prev.Timestamp,
		SandboxID:          h.metadata.SandboxID,
		SandboxExecutionID: h.metadata.ExecutionID,
		SandboxTemplateID:  h.metadata.TemplateID,
		SandboxBuildID:     h.metadata.BuildID,
		SandboxTeamID:      h.metadata.TeamID,
		SandboxVCPUCount:   h.metadata.VCPUCount,
		SandboxMemoryMB:    h.metadata.MemoryMB,
		SandboxType:        h.metadata.SandboxType.String(),
	}
	if err := h.delivery.Push(initial); err != nil {
		logger.L().Error(ctx, "failed to push initial host stats baseline",
			logger.WithSandboxID(h.metadata.SandboxID),
			zap.Error(err))
	}

	ticker := time.NewTicker(hostStatsSamplingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := h.CollectSample(ctx); err != nil {
				// Log error but continue sampling - don't kill the sandbox
				logger.L().Error(ctx, "failed to collect host stats sample",
					logger.WithSandboxID(h.metadata.SandboxID),
					zap.Error(err))
			}
		case <-h.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// Stop halts periodic collection and takes a final sample
func (h *HostStatsCollector) Stop(ctx context.Context) {
	h.stopOnce.Do(func() {
		close(h.stopCh)
		<-h.stoppedCh

		// Take final sample before process terminates
		if err := h.CollectSample(ctx); err != nil {
			// Log but don't fail the shutdown
			logger.L().Error(ctx, "failed to collect final host stats sample",
				logger.WithSandboxID(h.metadata.SandboxID),
				zap.Error(err))
		}
	})
}
