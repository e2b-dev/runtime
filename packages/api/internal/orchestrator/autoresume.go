package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	apisandbox "github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	sharedproxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const MaxAutoResumeTransitionRetries = 3

var ErrSandboxStillTransitioning = errors.New(sharedproxygrpc.SandboxStillTransitioningMessage)

func (o *Orchestrator) HandleExistingSandboxAutoResume(
	ctx context.Context,
	teamID uuid.UUID,
	sandboxID string,
	sbx apisandbox.Sandbox,
	transitionWaitBudget time.Duration,
) (*sharedproxygrpc.SandboxResumeResponse, bool, error) {
	transitionCtx, cancel := context.WithTimeout(ctx, transitionWaitBudget)
	defer cancel()

	attempts := 0

	for {
		switch sbx.State {
		case apisandbox.StatePausing, apisandbox.StateSnapshotting:
			if attempts >= MaxAutoResumeTransitionRetries {
				logger.L().Warn(
					ctx,
					"Sandbox is still transitioning after auto-resume retries",
					logger.WithSandboxID(sandboxID),
					zap.String("state", string(sbx.State)),
					zap.Int("attempts", attempts),
				)

				return nil, false, ErrSandboxStillTransitioning
			}

			attempts++
			waitErrMsg := "error waiting for sandbox to pause"
			if sbx.State == apisandbox.StatePausing {
				logger.L().Debug(ctx, "Waiting for sandbox to pause before auto-resume", logger.WithSandboxID(sandboxID), zap.Int("attempt", attempts))
			} else {
				waitErrMsg = "error waiting for sandbox snapshot to finish"
				logger.L().Debug(ctx, "Waiting for sandbox snapshot to finish before auto-resume", logger.WithSandboxID(sandboxID), zap.Int("attempt", attempts))
			}

			err := o.WaitForStateChange(transitionCtx, teamID, sandboxID)
			if err != nil && !errors.Is(err, apisandbox.ErrTransitionRestored) {
				if errors.Is(transitionCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
					logger.L().Warn(
						ctx,
						"Sandbox transition wait timed out during auto-resume",
						logger.WithSandboxID(sandboxID),
						zap.String("state", string(sbx.State)),
						zap.Int("attempt", attempts),
						zap.Duration("budget", transitionWaitBudget),
					)

					return nil, false, ErrSandboxStillTransitioning
				}

				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil, false, err
				}

				return nil, false, errors.New(waitErrMsg)
			}

			updatedSandbox, getSandboxErr := o.GetSandbox(ctx, teamID, sandboxID)
			if getSandboxErr == nil {
				sbx = updatedSandbox

				continue
			}
			if errors.Is(getSandboxErr, apisandbox.ErrNotFound) {
				return nil, false, nil
			}

			return nil, false, fmt.Errorf("failed to refresh sandbox state: %w", getSandboxErr)
		case apisandbox.StateKilling:
			logger.L().Debug(ctx, "Sandbox is being killed, cannot auto-resume", logger.WithSandboxID(sandboxID))

			return nil, false, apisandbox.ErrNotFound
		case apisandbox.StateRunning:
			return o.runningSandboxResumeResponse(ctx, teamID, sandboxID, sbx)
		default:
			logger.L().Error(ctx, "Sandbox is in an unknown state during auto-resume", logger.WithSandboxID(sandboxID), zap.String("state", string(sbx.State)))

			return nil, false, errors.New("sandbox is in an unknown state")
		}
	}
}

// runningSandboxResumeResponse routes a running sandbox. An orchestrator
// missing from the node cache is discovered on demand, which only modes
// whose ingress ends on the orchestrator need.
func (o *Orchestrator) runningSandboxResumeResponse(ctx context.Context, teamID uuid.UUID, sandboxID string, sbx apisandbox.Sandbox) (*sharedproxygrpc.SandboxResumeResponse, bool, error) {
	resp, err := resumeResponse(sbx, func() (string, error) {
		node := o.getOrConnectNode(ctx, sbx.ClusterID, sbx.NodeID)
		nodeIP := ""
		if node != nil {
			// For github actions in cicd, we don't have node ips and need to handle that specially
			nodeIP = routeNodeIPAddress(node, env.IsLocal())
		}
		if nodeIP == "" {
			logger.L().Error(
				ctx,
				"Sandbox is running but routing info is not available during auto-resume",
				logger.WithSandboxID(sandboxID),
				logger.WithTeamID(teamID.String()),
				logger.WithNodeID(sbx.NodeID),
				logger.WithClusterID(sbx.ClusterID),
			)

			return "", errors.New("sandbox is running but routing info is not available yet")
		}

		return nodeIP, nil
	})
	if err != nil {
		return nil, false, err
	}

	return resp, true, nil
}
