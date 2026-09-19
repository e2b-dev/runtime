package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gogo/status"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	e2bcatalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
)

// refusalRetryAfter is how long every API replica's eviction sweep leaves a
// node-refused, restored auto-pause alone before asking the node again.
const refusalRetryAfter = 10 * time.Second

const pauseTimeout = 80 * time.Second

// interruptedCreateKillTimeout bounds the best-effort kill of an instance a
// node may have created for a request that was cancelled before registration.
const interruptedCreateKillTimeout = 30 * time.Second

func (o *Orchestrator) RemoveSandbox(ctx context.Context, teamID uuid.UUID, sandboxID string, opts sandbox.RemoveOpts) error {
	ctx, span := tracer.Start(ctx, "remove-sandbox")
	defer span.End()

	// A pause outlives its caller, so it is tracked from the start: a drain
	// that already stopped waiting must not admit one.
	if opts.Action == sandbox.StateActionPause {
		releaseWork, ok := o.TrackWork()
		if !ok {
			return ErrDraining
		}
		defer releaseWork()
	}

	transition, alreadyDone, finish, err := o.sandboxStore.StartRemoving(ctx, teamID, sandboxID, opts)
	sbx := transition.Sandbox
	if err != nil {
		// For eviction, propagate all errors to the evictor.
		if opts.Eviction {
			return err
		}

		switch opts.Action {
		case sandbox.StateActionKill:
			if errors.Is(err, sandbox.ErrNotFound) {
				logger.L().Info(ctx, "Sandbox not found, already removed",
					logger.WithSandboxID(sandboxID),
					zap.String("kill_reason", opts.Reason.String()),
				)

				return ErrSandboxNotFound
			}

			switch sbx.State {
			case sandbox.StateKilling:
				logger.L().Info(ctx, "Sandbox is already killed",
					logger.WithSandboxID(sandboxID),
					zap.String("kill_reason", opts.Reason.String()),
				)

				return nil
			default: // It shouldn't happen the sandbox ended in paused state
				logger.L().Error(ctx, "Error killing sandbox",
					zap.Error(err),
					logger.WithSandboxID(sandboxID),
					zap.String("kill_reason", opts.Reason.String()),
				)

				return ErrSandboxOperationFailed
			}
		case sandbox.StateActionPause:
			if errors.Is(err, sandbox.ErrNotFound) {
				logger.L().Info(ctx, "Sandbox not found for pause", logger.WithSandboxID(sandboxID))

				return ErrSandboxNotFound
			}

			if transErr, ok := errors.AsType[*sandbox.InvalidStateTransitionError](err); ok {
				if transErr.CurrentState == sandbox.StateKilling {
					logger.L().Info(ctx, "Sandbox is already killed", logger.WithSandboxID(sandboxID))

					return ErrSandboxNotFound
				}

				return fmt.Errorf("sandbox is in '%s' state: %w", transErr.CurrentState, err)
			}

			if errors.Is(err, PauseQueueExhaustedError{}) {
				return PauseQueueExhaustedError{}
			}

			logger.L().Error(ctx, "Error pausing sandbox", zap.Error(err), logger.WithSandboxID(sandboxID))

			return ErrSandboxOperationFailed
		default:
			logger.L().Error(ctx, "Invalid state action", logger.WithSandboxID(sandboxID), zap.String("state_action", opts.Action.Name))

			return ErrSandboxOperationFailed
		}
	}
	defer func() {
		finish(context.WithoutCancel(ctx), err)
	}()

	if alreadyDone {
		logger.L().Info(ctx, "Sandbox was already in the process of being removed", logger.WithSandboxID(sandboxID), zap.String("state", string(sbx.State)))

		if time.Since(sbx.EndTime) > sandbox.StaleCutoff && opts.Action.Effect == sandbox.TransitionExpires {
			o.sandboxStore.Remove(context.WithoutCancel(ctx), teamID, sandboxID)
			go o.analyticsRemove(context.WithoutCancel(ctx), sbx, opts.Action)
		}

		return nil
	}

	if opts.Action == sandbox.StateActionPause {
		// Once the transition commits, caller cancellation must not abandon its snapshot.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), pauseTimeout)
		defer cancel()
	}

	// Team and cluster contexts travel explicitly: the evictor's sweep has
	// no request context, and a per-cluster rule is how a cluster whose edge
	// is not yet rolled stays off.
	restoreOnRefusal := opts.Action == sandbox.StateActionPause &&
		o.featureFlagsClient.BoolFlag(ctx, featureflags.PauseRefusalRestoreFlag,
			featureflags.TeamContext(teamID.String()), featureflags.ClusterContext(sbx.ClusterID))

	preserveRecord := false
	defer func() {
		if preserveRecord {
			return
		}
		o.sandboxStore.Remove(context.WithoutCancel(ctx), teamID, sandboxID)
		go o.analyticsRemove(context.WithoutCancel(ctx), sbx, opts.Action)
	}()
	err = o.removeSandboxFromNode(ctx, sbx, opts.Action, opts.Reason, opts.FilesystemOnly, restoreOnRefusal)
	if err != nil {
		if errors.Is(err, PauseQueueExhaustedError{}) {
			if restoreOnRefusal {
				outcome := o.restoreRefusedPause(context.WithoutCancel(ctx), transition)
				o.recordRefusalRestore(ctx, outcome, opts.Eviction)
				switch outcome {
				case restoreOutcomeRestored:
					preserveRecord = true
					err = sandbox.ErrTransitionRestored
				case restoreOutcomeSuperseded:
					preserveRecord = true
					err = sandbox.ErrTransitionRestored

					return fmt.Errorf("%w: %w", ErrSandboxNotFound, sandbox.ErrExecutionMismatch)
				}
			}

			logger.L().Info(ctx, "Pause refused retryably by the node",
				logger.WithSandboxID(sbx.SandboxID),
				zap.Bool("restored", preserveRecord),
			)

			if !preserveRecord {
				if restoreOnRefusal {
					// The edge put the route back on the node's refusal; the
					// record is going away, so the VM and its route go now.
					o.killRefusedSandbox(ctx, sbx)
				}

				return ErrSandboxOperationFailed
			}

			return PauseQueueExhaustedError{}
		}

		if errors.Is(err, ErrRefusedRouteLost) {
			// The record is going and the route is already gone: kill the VM
			// now rather than leaving it to the orphan reconciler.
			o.recordRefusalRestore(ctx, restoreOutcomeRouteRestoreFailed, opts.Eviction)
			logger.L().Error(ctx, "Pause refused by the node but the edge lost its route; removing the sandbox", logger.WithSandboxID(sbx.SandboxID))
			o.killRefusedSandbox(ctx, sbx)

			return ErrSandboxOperationFailed
		}

		fields := []zap.Field{
			zap.String("state_action", opts.Action.Name),
			zap.Error(err),
			logger.WithSandboxID(sbx.SandboxID),
		}
		if opts.Action == sandbox.StateActionKill {
			fields = append(fields, zap.String("kill_reason", opts.Reason.String()))
		}

		logger.L().Error(ctx, "Error removing sandbox", fields...)

		return ErrSandboxOperationFailed
	}

	return nil
}

type restoreOutcome string

const (
	restoreOutcomeRestored           restoreOutcome = "restored"
	restoreOutcomeRestoreFailed      restoreOutcome = "restore_failed"
	restoreOutcomeRouteRestoreFailed restoreOutcome = "route_restore_failed"
	restoreOutcomeSuperseded         restoreOutcome = "superseded"
)

func (o *Orchestrator) recordRefusalRestore(ctx context.Context, outcome restoreOutcome, eviction bool) {
	caller := "request"
	if eviction {
		caller = "eviction"
	}

	o.pauseRefusalRestoreCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", string(outcome)),
		attribute.String("caller", caller),
	))
}

func (o *Orchestrator) killRefusedSandbox(ctx context.Context, sbx sandbox.Sandbox) {
	ctx = context.WithoutCancel(ctx)
	node := o.getOrConnectNode(ctx, sbx.ClusterID, sbx.NodeID)
	if node == nil {
		logger.L().Error(ctx, "failed to get node to kill a refused sandbox after a failed restore", logger.WithNodeID(sbx.NodeID), logger.WithSandboxID(sbx.SandboxID))

		return
	}

	if err := o.killSandboxOnNode(ctx, node, sbx.ToNodeSandbox(), sandbox.KillReasonOrphaned); err != nil {
		logger.L().Error(ctx, "failed to kill a refused sandbox after a failed restore", zap.Error(err), logger.WithSandboxID(sbx.SandboxID))
	}
}

// Record first, routing second: a routing entry must never outlive its record.
// On a cluster node the routing helper is a no-op by design: the edge owns
// that catalog and puts the entry back itself on a refusal, failing the RPC
// if it cannot, so the fail-closed check below covers local nodes only.
func (o *Orchestrator) restoreRefusedPause(ctx context.Context, transition sandbox.StateTransition) restoreOutcome {
	sbx := transition.Sandbox
	restoredSbx, err := o.sandboxStore.RestoreRunning(ctx, transition, refusalRetryAfter)
	if err != nil {
		if errors.Is(err, sandbox.ErrExecutionMismatch) || errors.Is(err, sandbox.ErrRestoreConflict) {
			logger.L().Warn(ctx, "Pause restore superseded", zap.Error(err), logger.WithSandboxID(sbx.SandboxID))

			return restoreOutcomeSuperseded
		}

		logger.L().Error(ctx, "Failed to restore refused pause; falling back to removal",
			zap.Error(err),
			logger.WithSandboxID(sbx.SandboxID),
		)

		return restoreOutcomeRestoreFailed
	}

	if err := o.writeSandboxToRoutingTable(ctx, restoredSbx, o.routingCatalog.RestoreSandbox); err != nil {
		if errors.Is(err, e2bcatalog.ErrSandboxExecutionMismatch) {
			return restoreOutcomeSuperseded
		}

		logger.L().Error(ctx, "Failed to restore the refused pause's route; falling back to removal",
			zap.Error(err),
			logger.WithSandboxID(sbx.SandboxID),
		)

		return restoreOutcomeRouteRestoreFailed
	}

	// Record and route use different Redis slots; Add publishes the record first.
	current, err := o.sandboxStore.Get(ctx, sbx.TeamID, sbx.SandboxID)
	if errors.Is(err, sandbox.ErrNotFound) || (err == nil && current.ExecutionID != sbx.ExecutionID) {
		if node := o.GetNode(sbx.ClusterID, sbx.NodeID); node != nil && !node.IsClusterNode() {
			if err := o.routingCatalog.DeleteSandbox(ctx, sbx.SandboxID, sbx.ExecutionID); err != nil {
				logger.L().Error(ctx, "Failed to remove superseded pause route", zap.Error(err), logger.WithSandboxID(sbx.SandboxID))
			}
		}

		return restoreOutcomeSuperseded
	}
	if err != nil {
		logger.L().Warn(ctx, "Failed to verify pause ownership after successful restore", zap.Error(err), logger.WithSandboxID(sbx.SandboxID))
	}

	return restoreOutcomeRestored
}

func (o *Orchestrator) removeSandboxFromNode(
	ctx context.Context,
	sbx sandbox.Sandbox,
	stateAction sandbox.StateAction,
	reason sandbox.KillReason,
	filesystemOnly bool,
	restoreOnRefusal bool,
) error {
	ctx, span := tracer.Start(ctx, "remove-sandbox-from-node")
	defer span.End()

	node := o.getOrConnectNode(ctx, sbx.ClusterID, sbx.NodeID)
	if node == nil {
		fields := []zap.Field{
			logger.WithNodeID(sbx.NodeID),
		}
		if stateAction == sandbox.StateActionKill {
			fields = append(fields, zap.String("kill_reason", reason.String()))
		}

		logger.L().Error(ctx, "failed to get node", fields...)

		return fmt.Errorf("node '%s' not found", sbx.NodeID)
	}

	// For remote cluster nodes we are using gPRC metadata for routing registration instead
	if !node.IsClusterNode() {
		// Remove the sandbox resources after the sandbox is deleted
		err := o.routingCatalog.DeleteSandbox(ctx, sbx.SandboxID, sbx.ExecutionID)
		if err != nil {
			fields := []zap.Field{
				zap.Error(err),
				logger.WithSandboxID(sbx.SandboxID),
			}
			if stateAction == sandbox.StateActionKill {
				fields = append(fields, zap.String("kill_reason", reason.String()))
			}

			logger.L().Error(ctx, "error removing routing record from catalog", fields...)
		}
	}

	sbxlogger.I(sbx).Debug(ctx, "Removing sandbox",
		zap.Bool("auto_pause", sbx.AutoPause),
		zap.String("state_action", stateAction.Name),
	)

	switch stateAction {
	case sandbox.StateActionPause:
		err := o.pauseSandbox(ctx, node, sbx, filesystemOnly, restoreOnRefusal)
		if err != nil {
			if dberrors.IsForeignKeyViolation(err) {
				killErr := o.killSandboxOnNode(ctx, node, sbx.ToNodeSandbox(), sandbox.KillReasonBaseTemplateMissing)
				logger.L().Error(ctx, "Pause failed due to missing base template, killed sandbox as fallback",
					logger.WithSandboxID(sbx.SandboxID),
					zap.String("base_template_id", sbx.BaseTemplateID),
					zap.String("kill_reason", sandbox.KillReasonBaseTemplateMissing.String()),
					zap.NamedError("pause_error", err),
					zap.NamedError("kill_error", killErr),
				)

				return fmt.Errorf("failed to pause sandbox '%s': base template no longer exists: %w", sbx.SandboxID, err)
			}

			return fmt.Errorf("failed to auto pause sandbox '%s': %w", sbx.SandboxID, err)
		}

		return nil
	case sandbox.StateActionKill:
		return o.killSandboxOnNode(ctx, node, sbx.ToNodeSandbox(), reason)
	}

	return nil
}

func (o *Orchestrator) killOrphanSandbox(ctx context.Context, sbx sandbox.NodeSandbox) {
	node := o.GetNode(sbx.ClusterID, sbx.NodeID)
	if node == nil {
		logger.L().Error(ctx, "Node not found for orphan sandbox kill",
			logger.WithSandboxID(sbx.SandboxID),
			logger.WithNodeID(sbx.NodeID),
			zap.String("kill_reason", sandbox.KillReasonOrphaned.String()),
		)

		return
	}

	err := o.killSandboxOnNode(ctx, node, sbx, sandbox.KillReasonOrphaned)
	if err != nil {
		logger.L().Error(ctx, "Failed to kill orphan sandbox on node",
			zap.Error(err),
			logger.WithSandboxID(sbx.SandboxID),
			logger.WithNodeID(sbx.NodeID),
			zap.String("kill_reason", sandbox.KillReasonOrphaned.String()),
		)
	}
}

// compensateInterruptedCreate best-effort kills an instance a node may have
// created for a request whose context was cancelled before the API registered
// it. It runs the same node-side kill as the orphan reconciler, just eagerly on
// the failure path so the leak window is near-zero instead of an orphan grace
// period. A no-op on the node (NotFound) is handled by killSandboxOnNode, so a
// create the node never actually completed costs only a cheap delete RPC. The
// caller must pass a context detached from the cancelled request.
func (o *Orchestrator) compensateInterruptedCreate(ctx context.Context, node *nodemanager.Node, sandboxID, executionID string, vcpu, ramMB int64) {
	ctx, cancel := context.WithTimeout(ctx, interruptedCreateKillTimeout)
	defer cancel()

	nodeSbx := sandbox.NodeSandbox{
		SandboxID:   sandboxID,
		ExecutionID: executionID,
		NodeID:      node.ID,
		ClusterID:   node.ClusterID,
		VCpu:        vcpu,
		RamMB:       ramMB,
	}

	if err := o.killSandboxOnNode(ctx, node, nodeSbx, sandbox.KillReasonOrphaned); err != nil {
		logger.L().Error(ctx, "Failed to compensate interrupted sandbox create on node",
			zap.Error(err),
			logger.WithSandboxID(sandboxID),
			logger.WithNodeID(node.ID),
			zap.String("kill_reason", sandbox.KillReasonOrphaned.String()),
		)
	}
}

func (o *Orchestrator) killSandboxOnNode(
	ctx context.Context,
	node *nodemanager.Node,
	sbx sandbox.NodeSandbox,
	reason sandbox.KillReason,
) error {
	killReason := reason.String()
	req := &orchestrator.SandboxDeleteRequest{
		SandboxId:  sbx.SandboxID,
		KillReason: &killReason,
	}

	client, ctx := node.GetSandboxDeleteCtx(ctx, sbx.SandboxID, sbx.ExecutionID, false)
	_, err := client.Sandbox.Delete(ctx, req)
	st, ok := status.FromError(err)
	if ok && st.Code() == codes.NotFound {
		logger.L().Info(ctx, "Sandbox not found during kill",
			logger.WithSandboxID(sbx.SandboxID),
			logger.WithNodeID(node.ID),
			zap.String("kill_reason", killReason),
		)
	} else if err != nil {
		return fmt.Errorf("failed to delete sandbox: %w", err)
	}

	node.OptimisticRemove(ctx, nodemanager.SandboxResources{
		CPUs:      sbx.VCpu,
		MiBMemory: sbx.RamMB,
	})

	return nil
}
