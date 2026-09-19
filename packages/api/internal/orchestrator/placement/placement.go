package placement

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement")

// PlacementResult carries the outcome of a placement attempt alongside the error.
type PlacementResult struct {
	// Node is the node the sandbox was placed on, or nil on failure.
	Node *nodemanager.Node
	// WarmedNode is the first node that attempted the create before a timeout,
	// whose cache is warmest; nil unless TimedOut.
	WarmedNode *nodemanager.Node
	// TimedOut reports whether placement failed due to context cancellation/deadline.
	TimedOut bool
	// InterruptedNode is the node whose in-flight SandboxCreate was interrupted
	// by the request context being cancelled/timing out. The node may have
	// completed the create server-side even though the RPC returned Canceled, so
	// it can hold an instance the API never registered. Set only on such a
	// failure; callers should issue a best-effort kill of the (sandboxID,
	// executionID) on it to avoid leaving an orphan until reconcile reclaims it.
	InterruptedNode *nodemanager.Node
	// Response is the successful create's RPC response; nil on failure.
	Response *orchestrator.SandboxCreateResponse
}

// Algorithm defines the interface for sandbox placement strategies.
// Implementations should choose an optimal node based on available resources
// and current load distribution.
type Algorithm interface {
	chooseNode(ctx context.Context, nodes []*nodemanager.Node, nodesExcluded map[string]struct{}, requested nodemanager.SandboxResources, cpu CPURequirement, features FeatureRequirement, filterByLabels bool, requiredLabels []string) (*nodemanager.Node, error)
}

// PlaceSandbox derives sbxRequest's orchestrator-capability requirement and
// places it. The requirement is read off the request rather than supplied by
// the caller, so it cannot disagree with the bytes the orchestrator receives.
func PlaceSandbox(
	ctx context.Context,
	algorithm Algorithm,
	clusterNodes []*nodemanager.Node,
	preferredNode *nodemanager.Node,
	sbxRequest *orchestrator.SandboxCreateRequest,
	cpu CPURequirement,
	labelFilteringEnabled bool,
	requiredLabels []string,
) (PlacementResult, error) {
	return placeSandbox(ctx, algorithm, clusterNodes, preferredNode, sbxRequest, cpu, requiredFeatures(sbxRequest), labelFilteringEnabled, requiredLabels)
}

func placeSandbox(
	ctx context.Context,
	algorithm Algorithm,
	clusterNodes []*nodemanager.Node,
	preferredNode *nodemanager.Node,
	sbxRequest *orchestrator.SandboxCreateRequest,
	cpu CPURequirement,
	features FeatureRequirement,
	labelFilteringEnabled bool,
	requiredLabels []string,
) (PlacementResult, error) {
	ctx, span := tracer.Start(ctx, "place-sandbox")
	defer span.End()

	nodesExcluded := make(map[string]struct{})
	var err error

	var node *nodemanager.Node
	// Vetted here rather than trusted: the preferred node skips chooseNode, so
	// affinity would otherwise outrank the CPU and feature requirements on
	// every resume.
	if preferredNode != nil && NodeSatisfiesCPU(preferredNode, cpu) && NodeSatisfiesFeatures(preferredNode, features) {
		node = preferredNode
	}

	// Reported before the loop so a too-old fleet does not reach the caller as a
	// timeout after burning the deadline on nodes that can never serve it. An
	// empty cluster and an already-cancelled request are left to the loop, which
	// classifies them better.
	if node == nil && len(clusterNodes) > 0 && ctx.Err() == nil && !anyNodeSatisfiesFeatures(clusterNodes, features) {
		return PlacementResult{}, UnsupportedFeatureError{Features: features.FeatureNames(), MinVersion: features.MinVersion()}
	}

	// First node that attempted the create (not a fast ResourceExhausted refusal).
	var firstTriedNode *nodemanager.Node

	// Node whose in-flight SandboxCreate returned because the request context
	// was cancelled/timed out. That node may still have completed the create
	// server-side, so it is the one that can hold an unregistered instance.
	var interruptedNode *nodemanager.Node

	var lastCreateErr error

	// failed reports the warming node only when the failure was caused by the
	// request context being cancelled or timing out (ctx.Err() != nil). Hard
	// failures (where the context is still live) carry no node, so callers never
	// pin a retry to a node that genuinely refused the sandbox.
	//
	// It also surfaces the interrupted node so the caller can compensate for a
	// create the node may have finished after the RPC was cancelled.
	//
	// TODO [EN-1099]: We key off ctx.Err() rather than the gRPC status code because
	// the orchestrator currently collapses a timed-out resume into codes.Internal
	// (it folds the deadline cause into the message, not the code),
	// so the code alone cannot tell a timeout apart from a hard failure.
	failed := func(err error) (PlacementResult, error) {
		if ctx.Err() == nil {
			return PlacementResult{}, err
		}

		return PlacementResult{WarmedNode: firstTriedNode, InterruptedNode: interruptedNode, TimedOut: true}, err
	}

	attempt := 0
	refusals := 0

	// Nothing but capacity refusals before the deadline is capacity, not a slow placement.
	deadline := func() (PlacementResult, error) {
		if attempt == 0 && refusals > 0 {
			return failed(NoNodesAvailableError{})
		}

		return failed(PlacementTimeoutError{Attempts: attempt})
	}

	for attempt < maxRetries {
		select {
		case <-ctx.Done():
			return deadline()
		default:
			// Continue
		}

		if node != nil {
			telemetry.ReportEvent(ctx, "Placing sandbox on the preferred node", telemetry.WithNodeID(node.ID))
		} else {
			if len(nodesExcluded) >= len(clusterNodes) {
				if lastCreateErr != nil {
					return failed(SandboxCreateError{Attempts: attempt, LastErr: lastCreateErr})
				}

				return failed(NoNodesAvailableError{})
			}

			node, err = algorithm.chooseNode(ctx, clusterNodes, nodesExcluded, nodemanager.SandboxResources{CPUs: sbxRequest.GetSandbox().GetVcpu(), MiBMemory: sbxRequest.GetSandbox().GetRamMb()}, cpu, features, labelFilteringEnabled, requiredLabels)
			if err != nil {
				// A create was already attempted: its error explains the failure
				// better than the empty candidate set it caused.
				if lastCreateErr != nil {
					return failed(SandboxCreateError{Attempts: attempt, LastErr: lastCreateErr})
				}

				return failed(err)
			}

			telemetry.ReportEvent(ctx, "Placing sandbox on the node", telemetry.WithNodeID(node.ID))
		}

		node.PlacementMetrics.StartPlacing(sbxRequest.GetSandbox().GetSandboxId(), nodemanager.SandboxResources{
			CPUs:      sbxRequest.GetSandbox().GetVcpu(),
			MiBMemory: sbxRequest.GetSandbox().GetRamMb(),
		})

		ctx, span := tracer.Start(ctx, "create-sandbox")
		span.SetAttributes(
			telemetry.WithNodeID(node.ID),
			telemetry.WithClusterID(node.ClusterID),
		)
		resp, err := node.SandboxCreate(ctx, sbxRequest)
		span.End()
		if err == nil {
			node.PlacementMetrics.Success(sbxRequest.GetSandbox().GetSandboxId())

			// Optimistic update: assume resources are occupied after successful creation.
			// Manually update node.metrics with the newly allocated resources.
			// This will be overwritten by the next real Metrics report for auto-correction.
			node.OptimisticAdd(nodemanager.SandboxResources{
				CPUs:      sbxRequest.GetSandbox().GetVcpu(),
				MiBMemory: sbxRequest.GetSandbox().GetRamMb(),
			})

			return PlacementResult{Node: node, Response: resp}, nil
		}

		failedNode := node
		node = nil

		st, ok := status.FromError(err)
		statusCode := codes.Internal
		if ok {
			statusCode = st.Code()
		}

		// Remember the first node that got far enough to actually attempt the
		// sandbox (i.e. did not refuse with ResourceExhausted).
		if statusCode != codes.ResourceExhausted && firstTriedNode == nil {
			firstTriedNode = failedNode
		}

		// If the request context was cancelled while this node's create was in
		// flight, the node may have finished creating the instance even though
		// the RPC returned. A ResourceExhausted refusal never started a create,
		// so it cannot leak. Track the most recent such node as the compensation
		// target; the loop exits on the next ctx.Done() check.
		if ctx.Err() != nil && statusCode != codes.ResourceExhausted {
			interruptedNode = failedNode
		}

		switch statusCode {
		case codes.ResourceExhausted:
			refusals++
			failedNode.PlacementMetrics.Skip(sbxRequest.GetSandbox().GetSandboxId())
			logger.L().Warn(ctx, "Node exhausted, trying another node", logger.WithSandboxID(sbxRequest.GetSandbox().GetSandboxId()), logger.WithNodeID(failedNode.ID), zap.Error(utils.UnwrapGRPCError(err)))
		default:
			nodesExcluded[failedNode.ID] = struct{}{}
			failedNode.PlacementMetrics.Fail(sbxRequest.GetSandbox().GetSandboxId())
			logger.L().Error(ctx, "Failed to create sandbox", logger.WithSandboxID(sbxRequest.GetSandbox().GetSandboxId()), logger.WithTemplateID(sbxRequest.GetSandbox().GetTemplateId()), logger.WithBuildID(sbxRequest.GetSandbox().GetBuildId()), logger.WithNodeID(failedNode.ID), zap.Int("attempt", attempt+1), zap.Error(utils.UnwrapGRPCError(err)))
			lastCreateErr = err
			attempt++
		}
	}

	// A deadline hitting mid-create is a timeout, not a node failure.
	if ctx.Err() != nil {
		return deadline()
	}

	return failed(SandboxCreateError{Attempts: attempt, LastErr: lastCreateErr})
}
