package orchestrator

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// KillTeamSandboxes kills every running sandbox of the team, ten at a time.
// A failed kill is counted, not returned: the only error is failing to list
// the team's sandboxes.
func (o *Orchestrator) KillTeamSandboxes(ctx context.Context, teamID uuid.UUID, reason sandbox.KillReason) (killed, failed int, err error) {
	ctx, span := tracer.Start(ctx, "kill-team-sandboxes")
	defer span.End()

	sandboxes, err := o.GetSandboxes(ctx, teamID, []sandbox.State{sandbox.StateRunning})
	if err != nil {
		return 0, 0, fmt.Errorf("list team sandboxes: %w", err)
	}

	logger.L().Info(ctx, "Found sandboxes to kill",
		logger.WithTeamID(teamID.String()),
		zap.Int("count", len(sandboxes)),
	)

	killedCount := atomic.Int64{}
	failedCount := atomic.Int64{}

	wg := errgroup.Group{}
	wg.SetLimit(10)

	for _, sbx := range sandboxes {
		wg.Go(func() error {
			err := o.RemoveSandbox(ctx, sbx.TeamID, sbx.SandboxID, sandbox.RemoveOpts{
				Action: sandbox.StateActionKill,
				Reason: reason,
			})
			if err != nil {
				logger.L().Error(ctx, "Failed to kill sandbox",
					logger.WithSandboxID(sbx.SandboxID),
					logger.WithTeamID(teamID.String()),
					zap.String("kill_reason", reason.String()),
					zap.Error(err))
				failedCount.Add(1)
			} else {
				logger.L().Debug(ctx, "Successfully killed sandbox",
					logger.WithSandboxID(sbx.SandboxID),
					logger.WithTeamID(teamID.String()),
					zap.String("kill_reason", reason.String()))
				killedCount.Add(1)
			}

			return nil
		})
	}

	_ = wg.Wait()

	return int(killedCount.Load()), int(failedCount.Load()), nil
}
