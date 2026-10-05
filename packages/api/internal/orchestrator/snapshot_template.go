package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gogo/status"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/storageopts"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

type SnapshotTemplateResult struct {
	TemplateID string
	BuildID    uuid.UUID
}

type SnapshotTemplateOpts struct {
	// ExistingTemplateID is set when the alias resolved to an existing template owned by the team.
	ExistingTemplateID *string
	// Alias is the parsed alias name (without namespace). Set when a name was provided.
	Alias *string
	// Namespace is the team slug used for alias scoping. Set when a name was provided.
	Namespace *string
	// Tag is the build tag parsed from the name, defaults to "default".
	Tag string
	// FilesystemOnly persists the rootfs without a memory snapshot; sandboxes
	// created from the template cold-boot. The source sandbox keeps running.
	FilesystemOnly bool
}

// CreateSnapshotTemplate creates a persistent snapshot template from a running sandbox and immediately resumes it.
// The handler is responsible for parsing the name, resolving aliases via the cache, and populating opts.
func (o *Orchestrator) CreateSnapshotTemplate(ctx context.Context, teamID uuid.UUID, sandboxID string, opts SnapshotTemplateOpts) (result SnapshotTemplateResult, e error) {
	ctx, span := tracer.Start(ctx, "create-snapshot-template")
	defer span.End()

	// Tracked from the start, like a pause: a checkpoint's work can outlive
	// the request, and a drain that already stopped waiting must not admit one.
	releaseWork, ok := o.TrackWork()
	if !ok {
		return SnapshotTemplateResult{}, ErrDraining
	}
	defer releaseWork()

	transition, alreadyDone, finishSnapshotting, err := o.sandboxStore.StartRemoving(ctx, teamID, sandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionSnapshot})
	sbx := transition.Sandbox
	if err != nil {
		return SnapshotTemplateResult{}, fmt.Errorf("failed to start snapshotting: %w", err)
	}

	if alreadyDone {
		return SnapshotTemplateResult{}, &sandbox.InvalidStateTransitionError{
			CurrentState: sandbox.StateSnapshotting,
			TargetState:  sandbox.StateSnapshotting,
		}
	}

	// finish completes the snapshotting transition exactly once.
	// On success (nil) it restores the sandbox to Running.
	// On error it leaves the state as Snapshotting so that
	// RemoveSandbox can transition directly to Killing.
	var once sync.Once
	finish := func(err error) {
		once.Do(func() {
			finishSnapshotting(context.WithoutCancel(ctx), err)
		})
	}
	defer finish(nil)

	node := o.getOrConnectNode(ctx, sbx.ClusterID, sbx.NodeID)
	if node == nil {
		return SnapshotTemplateResult{}, fmt.Errorf("node '%s' not found", sbx.NodeID)
	}

	currentKind, upsertResult, err := o.upsertSnapshotKeepingKind(ctx, sbx, node)
	if err != nil {
		return SnapshotTemplateResult{}, err
	}

	snapshotTemplateEnvID, err := o.resolveOrCreateSnapshotTemplate(ctx, sandboxID, teamID, upsertResult.BuildID, sbx.NodeID, sbx.ClusterID, opts)
	if err != nil {
		o.failSnapshotBuild(ctx, upsertResult.BuildID, err)

		return SnapshotTemplateResult{}, err
	}

	// Checkpoint pauses the sandbox, snapshots it, and resumes it on the
	// orchestrator with the same ExecutionID. On error the orchestrator
	// kills the sandbox itself; RemoveSandbox is still needed to clean up
	// API-side state (store, routing, analytics).
	client, childCtx := node.GetClient(ctx)
	_, err = client.Sandbox.Checkpoint(childCtx, &orchestrator.SandboxCheckpointRequest{
		SandboxId:      sbx.SandboxID,
		BuildId:        upsertResult.BuildID.String(),
		Metadata:       map[string]string{storageopts.ObjectMetadataTemplateID: snapshotTemplateEnvID},
		FilesystemOnly: opts.FilesystemOnly,
	})
	if err != nil {
		// Cleanup must run even when the checkpoint failed because this
		// request's context was cancelled (e.g. client disconnect) — a
		// cancelled ctx must not leave the build non-failed while the
		// sandbox is restored to Running. Mirrors CheckpointSandbox.
		cleanupCtx := context.WithoutCancel(ctx)

		o.failSnapshotBuild(cleanupCtx, upsertResult.BuildID, err)

		// The orchestrator returns these when the sandbox is still running
		// healthy on its node (rejected before pausing the VM, or an
		// in-place checkpoint that only lost its artifact): restore it to
		// Running instead of killing it. Mirrors CheckpointSandbox.
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.FailedPrecondition:
				finish(nil)
				if isFilesystemOnlyDisabled(err) {
					return SnapshotTemplateResult{}, FilesystemOnlyDisabledError{}
				}

				return SnapshotTemplateResult{}, fmt.Errorf("checkpoint rejected: %w", err)
			case codes.ResourceExhausted:
				finish(nil)

				return SnapshotTemplateResult{}, PauseQueueExhaustedError{}
			case codes.Canceled, codes.DeadlineExceeded:
				// Only when OUR ctx died (client disconnect): the code is then
				// generated by the gRPC client locally and masks the
				// orchestrator's real verdict — the checkpoint may have
				// succeeded, been rejected with the sandbox healthy, or
				// failed either way. Killing on unknown destroys a healthy
				// in-place sandbox (the node keeps it running through a
				// checkpoint); restoring at worst leaves a stale row that
				// expiry eviction cleans up. A Canceled that arrives WITHOUT
				// our ctx being done is the orchestrator's own and falls
				// through to the kill below.
				if ctx.Err() != nil {
					finish(nil)

					return SnapshotTemplateResult{}, fmt.Errorf("checkpoint abandoned by caller: %w", err)
				}
			}
		}

		// Complete the snapshotting transition with error — leaves state as
		// Snapshotting (no restore to Running) and clears the transition key
		// so RemoveSandbox can proceed without deadlock.
		finish(err)

		if killErr := o.RemoveSandbox(cleanupCtx, teamID, sandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionKill}); killErr != nil {
			telemetry.ReportError(cleanupCtx, "error killing sandbox after failed checkpoint", killErr)
		}

		return SnapshotTemplateResult{}, fmt.Errorf("checkpoint failed: %w", err)
	}

	if err := o.finishSnapshotBuildWithKind(ctx, upsertResult.BuildID, sandboxID, currentKind, opts.FilesystemOnly, types.BuildStatusUploaded); err != nil {
		return SnapshotTemplateResult{}, fmt.Errorf("error updating build status: %w", err)
	}

	o.snapshotCache.Invalidate(context.WithoutCancel(ctx), sandboxID)

	telemetry.ReportEvent(ctx, "Snapshot template completed")

	return SnapshotTemplateResult{
		TemplateID: snapshotTemplateEnvID,
		BuildID:    upsertResult.BuildID,
	}, nil
}

// The pool sets no statement timeout, so a detached write blocked on a lock has
// nothing else to stop it.
const buildStatusWriteTimeout = 10 * time.Second

func detachedBuildStatusCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), buildStatusWriteTimeout)
}

// upsertSnapshotKeepingKind creates the snapshot row and its new build without
// touching the row's kind. The kind describes the latest ready build, which
// this build is not yet: it is recorded by finishSnapshotBuildWithKind once
// the build is ready, so a refused or failed pause, checkpoint or fork never
// changes what the previous build is resumed as. It returns the kind it kept.
func (o *Orchestrator) upsertSnapshotKeepingKind(ctx context.Context, sbx sandbox.Sandbox, node *nodemanager.Node) (bool, queries.UpsertSnapshotRow, error) {
	currentKind, err := o.sqlcDB.GetSnapshotFilesystemOnly(ctx, sbx.SandboxID)
	if err != nil && !dberrors.IsNotFoundError(err) {
		return false, queries.UpsertSnapshotRow{}, fmt.Errorf("error reading snapshot kind: %w", err)
	}

	upsertResult, err := o.throttledUpsertSnapshot(ctx, buildUpsertSnapshotParams(sbx, node, currentKind))
	if err != nil {
		return false, queries.UpsertSnapshotRow{}, fmt.Errorf("error upserting snapshot: %w", err)
	}

	return currentKind, upsertResult, nil
}

// finishSnapshotBuildWithKind records the build's terminal success status and,
// when the kind changed, the new kind on the sandbox's snapshot row in the
// same transaction: the row's kind and its latest ready build must never be
// observed apart, or a resume would memory-restore a memoryless build.
func (o *Orchestrator) finishSnapshotBuildWithKind(ctx context.Context, buildID uuid.UUID, sandboxID string, currentKind, wantKind bool, status types.BuildStatus) error {
	if currentKind == wantKind {
		return o.finishSnapshotBuild(ctx, buildID, status)
	}

	writeCtx, cancel := detachedBuildStatusCtx(ctx)
	defer cancel()

	txDB, tx, err := o.sqlcDB.WithTx(writeCtx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(writeCtx) }()

	if err := txDB.UpdateSnapshotFilesystemOnly(writeCtx, queries.UpdateSnapshotFilesystemOnlyParams{
		SandboxID:      sandboxID,
		FilesystemOnly: wantKind,
	}); err != nil {
		return fmt.Errorf("record snapshot kind: %w", err)
	}

	if err := txDB.UpdateEnvBuildStatus(writeCtx, queries.UpdateEnvBuildStatusParams{
		Status:     status,
		FinishedAt: new(time.Now()),
		Reason:     types.BuildReason{},
		BuildID:    buildID,
	}); err != nil {
		return fmt.Errorf("update build status: %w", err)
	}

	return tx.Commit(writeCtx)
}

// finishSnapshotBuild records a snapshot build's terminal status, detached from
// the caller so a cancelled request cannot abandon the write.
// ListTeamSnapshotTemplates returns only terminal-success builds, so a build
// left non-terminal hides a durable snapshot for good.
func (o *Orchestrator) finishSnapshotBuild(ctx context.Context, buildID uuid.UUID, status types.BuildStatus) error {
	writeCtx, cancel := detachedBuildStatusCtx(ctx)
	defer cancel()

	return o.sqlcDB.UpdateEnvBuildStatus(writeCtx, queries.UpdateEnvBuildStatusParams{
		Status:     status,
		FinishedAt: new(time.Now()),
		Reason:     types.BuildReason{},
		BuildID:    buildID,
	})
}

// failSnapshotBuild marks a snapshot build failed, detached for the same reason
// as finishSnapshotBuild.
func (o *Orchestrator) failSnapshotBuild(ctx context.Context, buildID uuid.UUID, cause error) {
	writeCtx, cancel := detachedBuildStatusCtx(ctx)
	defer cancel()

	err := o.sqlcDB.UpdateEnvBuildStatus(writeCtx, queries.UpdateEnvBuildStatusParams{
		Status:     types.BuildStatusFailed,
		FinishedAt: new(time.Now()),
		Reason:     types.BuildReason{Message: cause.Error()},
		BuildID:    buildID,
	})
	if err != nil {
		telemetry.ReportError(ctx, "error failing build", err)
	}
}

func (o *Orchestrator) resolveOrCreateSnapshotTemplate(
	ctx context.Context,
	sandboxID string,
	teamID uuid.UUID,
	buildID uuid.UUID,
	originNodeID string,
	clusterID uuid.UUID,
	opts SnapshotTemplateOpts,
) (string, error) {
	// Existing template — just assign the build
	if opts.ExistingTemplateID != nil {
		rows, err := o.sqlcDB.CreateTemplateBuildAssignment(ctx, queries.CreateTemplateBuildAssignmentParams{
			TemplateID: *opts.ExistingTemplateID,
			BuildID:    buildID,
			Tag:        opts.Tag,
		})
		if err != nil {
			return "", fmt.Errorf("error assigning build to existing template: %w", err)
		}
		if rows == 0 {
			return "", fmt.Errorf("template '%s' not found", *opts.ExistingTemplateID)
		}

		return *opts.ExistingTemplateID, nil
	}

	var clusterIDPtr *uuid.UUID
	if clusterID != consts.LocalClusterID {
		clusterIDPtr = &clusterID
	}

	// Create new snapshot template env
	envID, err := o.sqlcDB.CreateSnapshotTemplateEnv(ctx, queries.CreateSnapshotTemplateEnvParams{
		SnapshotID:   id.Generate(),
		TeamID:       teamID,
		ClusterID:    clusterIDPtr,
		SandboxID:    sandboxID,
		OriginNodeID: &originNodeID,
		BuildID:      &buildID,
		Tag:          opts.Tag,
	})
	if err != nil {
		return "", fmt.Errorf("error creating snapshot template env: %w", err)
	}

	// Create alias if a name was provided
	if opts.Alias != nil && opts.Namespace != nil {
		err = o.sqlcDB.CreateTemplateAlias(ctx, queries.CreateTemplateAliasParams{
			Alias:      *opts.Alias,
			TemplateID: envID,
			Namespace:  opts.Namespace,
		})
		if err != nil {
			return "", fmt.Errorf("error creating alias '%s': %w", *opts.Alias, err)
		}
	}

	return envID, nil
}

// NodeFeatureMissingError reports a snapshot option the sandbox's orchestrator
// predates. The sandbox cannot move to a newer one short of a pause and
// resume, so the caller must change the request.
type NodeFeatureMissingError struct {
	NodeID      string
	NodeVersion string
	Feature     string
	MinVersion  string
}

func (e NodeFeatureMissingError) Error() string {
	return fmt.Sprintf("node '%s' runs orchestrator %q, below %s required for %s", e.NodeID, e.NodeVersion, e.MinVersion, e.Feature)
}

// EnsureSnapshotSupport refuses a snapshot option the sandbox's orchestrator
// release does not implement. It needs no state transition, so the handler
// runs it before the sandbox leaves Running: a node that predates the request
// field would take a memory checkpoint and answer as if it had not.
func (o *Orchestrator) EnsureSnapshotSupport(ctx context.Context, sbx sandbox.Sandbox, opts SnapshotTemplateOpts) error {
	if !opts.FilesystemOnly {
		return nil
	}

	node := o.getOrConnectNode(ctx, sbx.ClusterID, sbx.NodeID)
	if node == nil {
		return fmt.Errorf("node '%s' not found", sbx.NodeID)
	}

	return checkSnapshotSupport(node, opts)
}

func checkSnapshotSupport(node *nodemanager.Node, opts SnapshotTemplateOpts) error {
	if !opts.FilesystemOnly {
		return nil
	}

	requirement := placement.Require(placement.FilesystemOnlyCheckpoint)
	if placement.NodeSatisfiesFeatures(node, requirement) {
		return nil
	}

	return NodeFeatureMissingError{
		NodeID:      node.ID,
		NodeVersion: node.Metadata().Version,
		Feature:     placement.FilesystemOnlyCheckpoint.Name(),
		MinVersion:  requirement.MinVersion(),
	}
}

// FilesystemOnlyDisabledError is the orchestrator refusing a filesystem-only
// checkpoint because its flag is off there, after the API's own pre-flight let
// the request through (the two evaluations can briefly disagree).
type FilesystemOnlyDisabledError struct{}

func (FilesystemOnlyDisabledError) Error() string {
	return "filesystem-only checkpoint disabled on the sandbox's node"
}

// Read through the gRPC status package: the gogo one used above for codes
// cannot decode this detail type.
func isFilesystemOnlyDisabled(err error) bool {
	st, ok := grpcstatus.FromError(err)
	if !ok {
		return false
	}
	for _, detail := range st.Details() {
		if userErr, ok := detail.(*orchestrator.UserError); ok && userErr.GetCode() == orchestrator.UserErrorCode_FILESYSTEM_ONLY_CHECKPOINT_DISABLED {
			return true
		}
	}

	return false
}
