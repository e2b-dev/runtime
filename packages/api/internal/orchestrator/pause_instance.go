package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/pause"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// Defined in the sandbox package so the evictor can classify them without an
// import cycle.
type (
	PauseQueueExhaustedError              = sandbox.PauseQueueExhaustedError
	PauseRefusedCheckpointInProgressError = sandbox.PauseRefusedCheckpointInProgressError
	PauseRefusedEnvdUnresponsiveError     = sandbox.PauseRefusedEnvdUnresponsiveError
	PauseUnsupportedError                 = sandbox.PauseUnsupportedError
)

// pauseRefusal classifies a node's answer to Pause as a refusal that left the
// sandbox running: ResourceExhausted is the retryable one (a checkpoint in
// flight or an unresponsive guest agent by its typed detail, a busy node
// otherwise), and a FailedPrecondition carrying the orchestrator's
// unsupported-pause detail is the permanent one. A FailedPrecondition without
// the detail, and every other answer, is nil: the failure is handled as before.
func pauseRefusal(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}

	switch st.Code() {
	case codes.ResourceExhausted:
		if isCheckpointInProgress(err) {
			return PauseRefusedCheckpointInProgressError{}
		}
		if isEnvdUnresponsive(err) {
			return PauseRefusedEnvdUnresponsiveError{}
		}

		return PauseQueueExhaustedError{}
	case codes.FailedPrecondition:
		if unsupportedPauseRefusal(st) {
			return PauseUnsupportedError{}
		}
	}

	return nil
}

// pauseRefusalOf returns the refusal an error wraps, unwrapped and narrowed
// to its own type, or nil.
func pauseRefusalOf(err error) error {
	switch {
	case errors.Is(err, PauseRefusedCheckpointInProgressError{}):
		return PauseRefusedCheckpointInProgressError{}
	case errors.Is(err, PauseRefusedEnvdUnresponsiveError{}):
		return PauseRefusedEnvdUnresponsiveError{}
	case errors.Is(err, PauseQueueExhaustedError{}):
		return PauseQueueExhaustedError{}
	case errors.Is(err, PauseUnsupportedError{}):
		return PauseUnsupportedError{}
	default:
		return nil
	}
}

// pauseSandbox takes the snapshot opts asks for: its kind, and whether the
// node must refuse it when the guest agent does not answer.
func (o *Orchestrator) pauseSandbox(ctx context.Context, node *nodemanager.Node, sbx sandbox.Sandbox, opts sandbox.RemoveOpts) error {
	ctx, span := tracer.Start(ctx, "pause-sandbox")
	defer span.End()

	filesystemOnly := opts.FilesystemOnly

	// The requested kind is recorded only once the pause has succeeded: a pause
	// the node refuses leaves the sandbox running on its previous build, whose
	// kind the row must keep describing.
	currentKind, result, err := o.upsertSnapshotKeepingKind(ctx, sbx, node, filesystemOnly)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error inserting snapshot for env", err)

		return err
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

	// The node is strict unless told it may proceed, so an API that cannot
	// persist its answer never gets an unmarked snapshot of a hung guest.
	captured, envdUnresponsive, err := snapshotInstance(ctx, node, sbx, result.TemplateID, result.BuildID.String(), filesystemOnly, !opts.RequireEnvdHealthy && !filesystemOnly)
	if err != nil {
		// The build is already committed, and nothing reaps one left non-terminal.
		o.failSnapshotBuild(ctx, result.BuildID, err)

		if refusal := pauseRefusalOf(err); refusal != nil {
			telemetry.ReportEvent(ctx, "pause refused by the node", telemetry.WithSandboxID(sbx.SandboxID))

			return refusal
		}

		telemetry.ReportCriticalError(ctx, "error pausing sandbox", err)

		return fmt.Errorf("error pausing sandbox: %w", err)
	}

	// A disk-only capture has no memory for the mark to describe, whatever
	// the node answered.
	marked := envdUnresponsive && !captured
	if marked {
		reason := pause.ReasonRequest
		if opts.Eviction {
			reason = pause.ReasonTimeout
		}
		pause.LogGuestAgentUnresponsive(ctx, sbx.SandboxID, sbx.TeamID.String(), reason, filesystemOnly)
	}

	// The node may capture a full pause as disk-only (a powered-off guest);
	// both records carry the kind it took.
	if err := o.finishSnapshotBuildWithKind(ctx, result.BuildID, sbx.SandboxID, currentKind, captured, marked, types.BuildStatusSuccess, captured != filesystemOnly); err != nil {
		telemetry.ReportCriticalError(ctx, "error pausing sandbox", err)

		return fmt.Errorf("error pausing sandbox: %w", err)
	}

	o.snapshotCache.Invalidate(context.WithoutCancel(ctx), sbx.SandboxID)

	return nil
}

// snapshotInstance pauses the sandbox on its node and reports whether the
// snapshot it took is disk-only, and whether the node took it while the
// guest agent did not answer its health probe, which allowEnvdUnresponsive
// lets it do.
func snapshotInstance(ctx context.Context, node *nodemanager.Node, sbx sandbox.Sandbox, templateID, buildID string, filesystemOnly, allowEnvdUnresponsive bool) (captured, envdUnresponsive bool, err error) {
	childCtx, childSpan := tracer.Start(ctx, "snapshot-instance")
	defer childSpan.End()

	client, childCtx := node.GetSandboxClient(childCtx, sbx.TeamID.String())
	resp, err := client.Sandbox.Pause(
		childCtx, &orchestrator.SandboxPauseRequest{
			SandboxId:             sbx.SandboxID,
			TemplateId:            templateID,
			BuildId:               buildID,
			FilesystemOnly:        filesystemOnly,
			AllowEnvdUnresponsive: allowEnvdUnresponsive,
		},
	)

	if err == nil {
		telemetry.ReportEvent(ctx, "Paused sandbox")

		// A node without the field answers false: it took what was asked.
		return filesystemOnly || resp.GetFilesystemOnly(), resp.GetEnvdUnresponsive(), nil
	}

	if refusal := pauseRefusal(err); refusal != nil {
		logger.L().Warn(ctx, "Pause refused by the node", logger.WithSandboxID(sbx.SandboxID), zap.String("refusal", refusal.Error()), zap.String("node_message", status.Convert(err).Message()))

		return false, false, refusal
	}

	return false, false, fmt.Errorf("failed to pause sandbox '%s': %w", sbx.SandboxID, err)
}

func (o *Orchestrator) WaitForStateChange(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	return o.sandboxStore.WaitForStateChange(ctx, teamID, sandboxID)
}

// buildUpsertSnapshotParams labels the row with currentKind, the kind of its
// latest ready build, and the new build with filesystemOnly, the kind it is
// being taken as.
func buildUpsertSnapshotParams(sbx sandbox.Sandbox, node *nodemanager.Node, currentKind, filesystemOnly bool) queries.UpsertSnapshotParams {
	metadata := types.JSONBStringMap(sbx.Metadata)
	if metadata == nil {
		metadata = types.JSONBStringMap{}
	}

	var clusterID *uuid.UUID
	if sbx.ClusterID != consts.LocalClusterID {
		clusterID = &sbx.ClusterID
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
			Version:                 types.PausedSandboxConfigVersion,
			Network:                 sbx.Network,
			AutoResume:              sbx.AutoResume,
			VolumeMounts:            sbx.VolumeMounts,
			FilesystemOnly:          currentKind,
			AutoPauseFilesystemOnly: sbx.AutoPauseFilesystemOnly,
			Iam:                     sbx.Iam,
		},
		OriginNodeID: node.ID,
		Status:       types.BuildStatusSnapshotting,
		// Pin the snapshot's CPU info to the source build instead of the executing
		// node, so a pause/resume across CPU generations stays compatible.
		SourceBuildID:       sbx.BuildID,
		BuildFilesystemOnly: filesystemOnly,
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
