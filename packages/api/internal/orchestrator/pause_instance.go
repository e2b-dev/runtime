package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/gogo/status"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// Defined in the sandbox package so the evictor can classify it without an
// import cycle.
type PauseQueueExhaustedError = sandbox.PauseQueueExhaustedError

func (o *Orchestrator) pauseSandbox(ctx context.Context, node *nodemanager.Node, sbx sandbox.Sandbox, filesystemOnly bool, restoreOnRefusal bool) error {
	_, err := o.pauseSandboxWithEvidence(ctx, node, sbx, filesystemOnly, restoreOnRefusal, 0, false)

	return err
}

func (o *Orchestrator) pauseSandboxWithEvidence(ctx context.Context, node *nodemanager.Node, sbx sandbox.Sandbox, filesystemOnly bool, restoreOnRefusal bool, remainingLifetime time.Duration, waitForStorage bool) (string, error) {
	ctx, span := tracer.Start(ctx, "pause-sandbox")
	defer span.End()

	result, err := o.throttledUpsertSnapshot(ctx, buildUpsertSnapshotParams(sbx, node, filesystemOnly, remainingLifetime))
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error inserting snapshot for env", err)

		return "", err
	}

	// The snapshot's CPU info is pinned to the source build (see
	// buildUpsertSnapshotParams), so the node the pause physically ran on is not
	// persisted. Log it for debugging cross-generation pools.
	originNodeCPU := node.MachineInfo()
	logger.L().Info(ctx, "Snapshotting sandbox",
		logger.WithSandboxID(sbx.SandboxID),
		zap.String("origin_node_id", node.ID),
		zap.String("origin_node_cpu_architecture", originNodeCPU.CPUArchitecture),
		zap.String("origin_node_cpu_family", originNodeCPU.CPUFamily),
		zap.String("origin_node_cpu_model", originNodeCPU.CPUModel),
		zap.String("origin_node_cpu_model_name", originNodeCPU.CPUModelName),
		zap.Strings("origin_node_cpu_flags", originNodeCPU.CPUFlags),
		zap.String("source_build_id", sbx.BuildID.String()),
	)

	err = snapshotInstance(ctx, node, sbx, result.TemplateID, result.BuildID.String(), filesystemOnly, restoreOnRefusal, waitForStorage)
	if err != nil {
		// The build is already committed, and nothing reaps one left non-terminal.
		o.failSnapshotBuild(ctx, result.BuildID, err)

		if errors.Is(err, PauseQueueExhaustedError{}) {
			telemetry.ReportEvent(ctx, "pause refused retryably", telemetry.WithSandboxID(sbx.SandboxID))

			return "", PauseQueueExhaustedError{}
		}

		telemetry.ReportCriticalError(ctx, "error pausing sandbox", err)

		return "", fmt.Errorf("error pausing sandbox: %w", err)
	}

	if err := o.finishSnapshotBuild(ctx, result.BuildID, types.BuildStatusSuccess); err != nil {
		telemetry.ReportCriticalError(ctx, "error pausing sandbox", err)

		return "", fmt.Errorf("error pausing sandbox: %w", err)
	}

	o.snapshotCache.Invalidate(context.WithoutCancel(ctx), sbx.SandboxID)

	return result.BuildID.String(), nil
}

func snapshotInstance(ctx context.Context, node *nodemanager.Node, sbx sandbox.Sandbox, templateID, buildID string, filesystemOnly bool, restoreOnRefusal bool, waitForStorage bool) error {
	childCtx, childSpan := tracer.Start(ctx, "snapshot-instance")
	defer childSpan.End()

	client, childCtx := node.GetSandboxDeleteCtx(childCtx, sbx.SandboxID, sbx.ExecutionID, restoreOnRefusal)
	response, err := client.Sandbox.Pause(
		childCtx, &orchestrator.SandboxPauseRequest{
			SandboxId:      sbx.SandboxID,
			TemplateId:     templateID,
			BuildId:        buildID,
			FilesystemOnly: filesystemOnly,
			WaitForStorage: waitForStorage,
			ExecutionId:    sbx.ExecutionID,
		},
	)

	if err == nil {
		if waitForStorage && (response == nil || !response.GetStorageDurable()) {
			return errors.New("pause completed without durable storage confirmation")
		}
		telemetry.ReportEvent(ctx, "Paused sandbox")

		return nil
	}

	st, ok := status.FromError(err)
	if !ok {
		return err
	}

	if st.Code() == codes.ResourceExhausted {
		logger.L().Warn(ctx, "Pause refused by the node", logger.WithSandboxID(sbx.SandboxID), zap.String("node_message", st.Message()))

		return PauseQueueExhaustedError{}
	}

	// Only the edge answers a pause with Aborted: the node refused and the
	// route could not be restored (a node never emits it).
	if st.Code() == codes.Aborted {
		logger.L().Warn(ctx, "Pause refused by the node but its route was lost", logger.WithSandboxID(sbx.SandboxID), zap.String("edge_message", st.Message()))

		return ErrRefusedRouteLost
	}

	return fmt.Errorf("failed to pause sandbox '%s': %w", sbx.SandboxID, err)
}

func (o *Orchestrator) WaitForStateChange(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	return o.sandboxStore.WaitForStateChange(ctx, teamID, sandboxID)
}

func buildUpsertSnapshotParams(sbx sandbox.Sandbox, node *nodemanager.Node, filesystemOnly bool, remaining ...time.Duration) queries.UpsertSnapshotParams {
	metadata := types.JSONBStringMap(sbx.Metadata)
	if metadata == nil {
		metadata = types.JSONBStringMap{}
	}

	var clusterID *uuid.UUID
	if sbx.ClusterID != consts.LocalClusterID {
		clusterID = &sbx.ClusterID
	}

	remainingLifetime := time.Duration(0)
	if len(remaining) > 0 {
		remainingLifetime = remaining[0]
	}
	remainingLifetimeSeconds := uint64(0)
	if remainingLifetime > 0 {
		// Round up so a valid sub-second remainder cannot serialize as the
		// legacy zero/unset value and accidentally regain the default lifetime.
		remainingLifetimeSeconds = uint64(math.Ceil(remainingLifetime.Seconds()))
	}

	return queries.UpsertSnapshotParams{
		// Used if there's no snapshot for this sandbox yet
		TemplateID:     id.Generate(),
		TeamID:         sbx.TeamID,
		ClusterID:      clusterID,
		BaseTemplateID: sbx.BaseTemplateID,
		SandboxID:      sbx.SandboxID,
		StartedAt:      pgtype.Timestamptz{Time: sbx.StartTime, Valid: true},
		Vcpu:           sbx.VCpu,
		RamMb:          sbx.RamMB,
		// We don't know this information
		FreeDiskSizeMb:      0,
		TotalDiskSizeMb:     &sbx.TotalDiskSizeMB,
		Metadata:            metadata,
		KernelVersion:       sbx.KernelVersion,
		FirecrackerVersion:  sbx.FirecrackerVersion,
		EnvdVersion:         &sbx.EnvdVersion,
		Secure:              sbx.EnvdAccessToken != nil,
		AllowInternetAccess: sbx.AllowInternetAccess,
		AutoPause:           sbx.AutoPause,
		Config: &types.PausedSandboxConfig{
			Version:                  types.PausedSandboxConfigVersion,
			Network:                  sbx.Network,
			AutoResume:               sbx.AutoResume,
			VolumeMounts:             sbx.VolumeMounts,
			FilesystemOnly:           filesystemOnly,
			AutoPauseFilesystemOnly:  sbx.AutoPauseFilesystemOnly,
			Iam:                      sbx.Iam,
			RemainingLifetimeSeconds: &remainingLifetimeSeconds,
		},
		OriginNodeID: node.ID,
		Status:       types.BuildStatusSnapshotting,
		// Pin the snapshot's CPU info to the source build instead of the executing
		// node, so a pause/resume across CPU generations stays compatible.
		SourceBuildID: sbx.BuildID,
	}
}

// throttledUpsertSnapshot runs UpsertSnapshot gated by the snapshot upsert semaphore.
func (o *Orchestrator) throttledUpsertSnapshot(ctx context.Context, params queries.UpsertSnapshotParams) (queries.UpsertSnapshotRow, error) {
	if err := o.snapshotUpsertSem.Acquire(ctx, 1); err != nil {
		return queries.UpsertSnapshotRow{}, err
	}
	defer o.snapshotUpsertSem.Release(1)

	return o.sqlcDB.UpsertSnapshot(ctx, params)
}
