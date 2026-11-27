//go:build linux

package sandbox

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/clickhouse/pkg/hoststats"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// initializeHostStatsCollector initializes the host stats collector for a sandbox.
// This is a best-effort operation - errors are logged but do not fail the sandbox initialization.
func initializeHostStatsCollector(
	ctx context.Context,
	sbx *Sandbox,
	runtime sandboxtypes.RuntimeMetadata,
	config *Config,
	hostStatsDelivery hoststats.Delivery,
) {
	teamID, err := uuid.Parse(runtime.TeamID)
	if err != nil {
		logger.L().Warn(ctx, "invalid team ID for host stats, using zero UUID",
			logger.WithTeamID(runtime.TeamID), zap.Error(err))
	}

	collector := NewHostStatsCollector(
		HostStatsMetadata{
			SandboxID:   runtime.SandboxID,
			ExecutionID: runtime.ExecutionID,
			TemplateID:  runtime.TemplateID,
			BuildID:     runtime.BuildID,
			TeamID:      teamID,
			VCPUCount:   config.Vcpu,
			MemoryMB:    sbx.MemoryMB(),
			SandboxType: runtime.SandboxType,
		},
		hostStatsDelivery,
		sbx.cgroupHandle.GetStats,
		sbx.MemoryUsage,
	)

	sbx.hostStatsCollector = collector

	go collector.Start(ctx)
}

const hostStatsMemoryExitTimeout = 3 * time.Second

// Give the final sample a bounded chance to observe the handler's frozen total.
// Sampling stops first, so the wait is not recorded as sandbox run time.
func stopHostStatsAfterMemory(ctx context.Context, collector *HostStatsCollector, wait func(context.Context) error) {
	collector.StopAfter(ctx, func() {
		waitCtx, cancel := context.WithTimeout(ctx, hostStatsMemoryExitTimeout)
		defer cancel()
		_ = wait(waitCtx)
		if err := waitCtx.Err(); err != nil {
			logger.L().Warn(ctx, "memory handler wait ended before final host stats sample",
				logger.WithSandboxID(collector.metadata.SandboxID), zap.Error(err))
		}
	})
}
