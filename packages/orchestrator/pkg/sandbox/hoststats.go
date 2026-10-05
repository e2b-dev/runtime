//go:build linux

package sandbox

import (
	"context"

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
			MemoryMB:    config.RamMB,
			SandboxType: runtime.SandboxType,
		},
		hostStatsDelivery,
		sbx.cgroupHandle.GetStats,
	)

	sbx.hostStatsCollector = collector

	go collector.Start(ctx)
}
