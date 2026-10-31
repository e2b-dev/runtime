package placement

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedutils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
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
	// Response is the successful create's RPC response; nil on failure.
	Response *orchestrator.SandboxCreateResponse
	// MissingHugepageCeiling is why the final memory check admitted Node
	// unbounded (a MissingHugepageCeiling reason), "" when the check bounded
	// it or no limit applied. Taken at admission: the node's metrics may
	// change before the caller reads them.
	MissingHugepageCeiling string
}

// Requirements are what a node must meet to take a sandbox; a zero field
// imposes nothing.
type Requirements struct {
	CPU      CPURequirement
	Features FeatureRequirement
	Memory   MemoryRequirement
}

// Algorithm defines the interface for sandbox placement strategies.
// Implementations should choose an optimal node based on available resources
// and current load distribution.
type Algorithm interface {
	chooseNode(ctx context.Context, nodes []*nodemanager.Node, nodesExcluded map[string]struct{}, requested nodemanager.SandboxResources, requirements Requirements, filterByLabels bool, requiredLabels []string) (*nodemanager.Node, error)
}

// PlaceSandbox places sbxRequest on a node meeting requirements. Features is
// read off the request, replacing any the caller set, so it cannot disagree
// with the bytes the orchestrator receives.
//
// PlaceSandbox places with the caller's context end to end: a create under
// way is cut with the caller.
func PlaceSandbox(
	ctx context.Context,
	algorithm Algorithm,
	clusterNodes []*nodemanager.Node,
	node *nodemanager.Node,
	sbxRequest *orchestrator.SandboxCreateRequest,
	requirements Requirements,
	labelFilteringEnabled bool,
	requiredLabels []string,
) (PlacementResult, error) {
	return PlaceSandboxCompletingStartedCreate(ctx, algorithm, clusterNodes, node, sbxRequest, requirements, labelFilteringEnabled, requiredLabels, time.Time{})
}

// PlaceSandboxCompletingStartedCreate places like PlaceSandbox, except that a
// node create already under way when the caller leaves runs on until
// completeStartedCreateUntil instead of being cut: a resume the control plane
// has begun must not be cancelled half way for a client that comes back. A
// caller that stays keeps its own context's end. The loop between creates
// still ends with the caller, and a node's answer that arrives after the
// caller left is reported as that answer, never as a timeout. A zero time
// cuts the create with the caller.
func PlaceSandboxCompletingStartedCreate(
	ctx context.Context,
	algorithm Algorithm,
	clusterNodes []*nodemanager.Node,
	preferredNode *nodemanager.Node,
	sbxRequest *orchestrator.SandboxCreateRequest,
	requirements Requirements,
	labelFilteringEnabled bool,
	requiredLabels []string,
	completeStartedCreateUntil time.Time,
) (PlacementResult, error) {
	requirements.Features = requiredFeatures(sbxRequest)

	return placeSandbox(ctx, algorithm, clusterNodes, preferredNode, sbxRequest, requirements, labelFilteringEnabled, requiredLabels, defaultRefusalBackoff, completeStartedCreateUntil)
}

func placeSandbox(
	ctx context.Context,
	algorithm Algorithm,
	clusterNodes []*nodemanager.Node,
	preferredNode *nodemanager.Node,
	sbxRequest *orchestrator.SandboxCreateRequest,
	requirements Requirements,
	labelFilteringEnabled bool,
	requiredLabels []string,
	backoff refusalBackoff,
	completeStartedCreateUntil time.Time,
) (PlacementResult, error) {
	ctx, span := tracer.Start(ctx, "place-sandbox")
	defer span.End()

	nodesExcluded := make(map[string]struct{})
	var err error

	resources := nodemanager.SandboxResources{
		CPUs:      sbxRequest.GetSandbox().GetVcpu(),
		MiBMemory: sbxRequest.GetSandbox().GetRamMb(),
		HugePages: sbxRequest.GetSandbox().GetHugePages(),
	}

	var node *nodemanager.Node
	// Vetted here rather than trusted: the preferred node skips chooseNode, so
	// affinity would otherwise outrank the CPU and feature requirements on
	// every resume. Memory is checked when the placement is recorded below.
	if preferredNode != nil && NodeSatisfiesCPU(preferredNode, requirements.CPU) && NodeSatisfiesFeatures(preferredNode, requirements.Features) {
		node = preferredNode
	}

	// Reported before the loop so a too-old fleet does not reach the caller as a
	// timeout after burning the deadline on nodes that can never serve it. An
	// empty cluster and an already-cancelled request are left to the loop, which
	// classifies them better.
	if node == nil && len(clusterNodes) > 0 && ctx.Err() == nil && !anyNodeSatisfiesFeatures(clusterNodes, requirements.Features) {
		return PlacementResult{}, UnsupportedFeatureError{Features: requirements.Features.FeatureNames(), MinVersion: requirements.Features.MinVersion()}
	}

	// First node that attempted the create (not a fast ResourceExhausted refusal).
	var firstTriedNode *nodemanager.Node

	var lastCreateErr error

	// createDetached: the last create ran on past a caller that left, until
	// completeStartedCreateUntil. createCut: it ended with its context, not
	// with a node's answer.
	var createDetached, createCut bool

	// failed reports the warming node only when the failure was caused by the
	// request context being cancelled or timing out (ctx.Err() != nil) or by
	// the create being cut at completeStartedCreateUntil. Hard failures (where
	// the context is still live) carry no node, so callers never pin a retry
	// to a node that genuinely refused the sandbox.
	//
	// TODO [EN-1099]: We key off ctx.Err() rather than the gRPC status code because
	// the orchestrator currently collapses a timed-out resume into codes.Internal
	// (it folds the deadline cause into the message, not the code),
	// so the code alone cannot tell a timeout apart from a hard failure.
	failed := func(err error) (PlacementResult, error) {
		if ctx.Err() == nil && !createCut {
			return PlacementResult{}, err
		}

		return PlacementResult{WarmedNode: firstTriedNode, TimedOut: true}, err
	}

	attempt := 0
	refusals := 0
	// A node refused at the record step is excluded, so chooseNode no longer
	// sees its full pool.
	poolsFull := false

	// answered reports what the nodes said to a caller that left while a
	// detached create ran: never a timeout, so no node is pinned for a remap.
	answered := func() (PlacementResult, error) {
		if attempt == 0 {
			return PlacementResult{}, NoNodesAvailableError{}
		}

		return PlacementResult{}, SandboxCreateError{Attempts: attempt, LastErr: lastCreateErr}
	}

	// Nothing but capacity refusals before the deadline is capacity, not a slow placement.
	deadline := func() (PlacementResult, error) {
		if attempt == 0 && (refusals > 0 || poolsFull) {
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
			if allExcluded(clusterNodes, nodesExcluded) {
				if lastCreateErr != nil {
					return failed(SandboxCreateError{Attempts: attempt, LastErr: lastCreateErr})
				}

				return failed(NoNodesAvailableError{})
			}

			node, err = algorithm.chooseNode(ctx, clusterNodes, nodesExcluded, resources, requirements, labelFilteringEnabled, requiredLabels)
			if err != nil {
				// A create was already attempted: its error explains the failure
				// better than the empty candidate set it caused.
				if lastCreateErr != nil {
					return failed(SandboxCreateError{Attempts: attempt, LastErr: lastCreateErr})
				}

				if poolsFull {
					return failed(NoNodesAvailableError{})
				}

				return failed(err)
			}

			telemetry.ReportEvent(ctx, "Placing sandbox on the node", telemetry.WithNodeID(node.ID))
		}

		chosen := node
		var missingCeiling string
		if !node.PlacementMetrics.StartPlacingIf(sbxRequest.GetSandbox().GetSandboxId(), resources, func() bool {
			ok, missing := MemoryAdmission(chosen, resources, requirements.Memory)
			missingCeiling = missing

			return ok
		}) {
			telemetry.ReportEvent(ctx, "Node's hugepage pool is full", telemetry.WithNodeID(node.ID))
			nodesExcluded[node.ID] = struct{}{}
			node = nil
			poolsFull = true

			continue
		}

		ctx, span := tracer.Start(ctx, "create-sandbox")
		span.SetAttributes(
			telemetry.WithNodeID(node.ID),
			telemetry.WithClusterID(node.ClusterID),
		)
		createCtx, cancelCreate := ctx, context.CancelFunc(func() {})
		createDetached = completeStartedCreateUntil.After(time.Now())
		if createDetached {
			createCtx, cancelCreate = sharedutils.ContextExtendedTo(ctx, completeStartedCreateUntil)
		}
		resp, err := node.SandboxCreate(createCtx, sbxRequest)
		createCut = createCtx.Err() != nil
		cancelCreate()
		span.End()
		if err == nil {
			// Optimistic update: assume resources are occupied after successful creation.
			// Manually update node.metrics with the newly allocated resources.
			// This will be overwritten by the next real Metrics report for auto-correction.
			// Added before the in-flight entry is dropped, so a concurrent
			// memory check always counts this sandbox somewhere.
			node.OptimisticAdd(resources)
			node.PlacementMetrics.Success(sbxRequest.GetSandbox().GetSandboxId())

			return PlacementResult{Node: node, Response: resp, MissingHugepageCeiling: missingCeiling}, nil
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

		switch statusCode {
		case codes.ResourceExhausted:
			refusals++
			failedNode.PlacementMetrics.Skip(sbxRequest.GetSandbox().GetSandboxId())

			delay := backoff.delay(refusals)
			logger.L().Warn(ctx, "Node exhausted, trying another node", logger.WithSandboxID(sbxRequest.GetSandbox().GetSandboxId()), logger.WithNodeID(failedNode.ID), zap.Int("refusals", refusals), zap.Duration("retry_in", delay), zap.Error(utils.UnwrapGRPCError(err)))
			// The loop ends with the caller.
			if createDetached && ctx.Err() != nil {
				return answered()
			}
			if !sleep(ctx, delay) {
				return deadline()
			}
		default:
			// A refusal the node marks as for good is the same on every
			// node and says nothing about this one: no exclusion, no
			// failure sample, no other attempt. A node that predates the
			// mark gets the other attempts. Not reported through failed:
			// a request context that ended meanwhile makes no timeout of
			// it, and no warmed node, or a resume's origin would be
			// remapped to the node that just refused for good.
			if ok && createRefusedForGood(st) {
				failedNode.PlacementMetrics.Skip(sbxRequest.GetSandbox().GetSandboxId())
				logger.L().Warn(ctx, "Sandbox refused by the node for good", logger.WithSandboxID(sbxRequest.GetSandbox().GetSandboxId()), logger.WithTemplateID(sbxRequest.GetSandbox().GetTemplateId()), logger.WithBuildID(sbxRequest.GetSandbox().GetBuildId()), logger.WithNodeID(failedNode.ID), zap.Error(utils.UnwrapGRPCError(err)))

				return PlacementResult{}, SandboxCreateError{Attempts: attempt + 1, LastErr: err}
			}
			// A create cut by its own context (the caller cancelled or its
			// deadline fired) says nothing about the node: it is the client
			// leaving, not a node that refused the sandbox. Don't exclude it,
			// don't count it as a failure sample, and don't log it at ERROR —
			// doing so pollutes the ERROR stream and inflates createFails with
			// client behaviour. Still Skip to drop the in-progress reservation,
			// still count the attempt so deadline() classifies the timeout, and
			// end with the caller through deadline().
			if createCut {
				failedNode.PlacementMetrics.Skip(sbxRequest.GetSandbox().GetSandboxId())
				logger.L().Warn(ctx, "Sandbox create cut by the caller before it completed", logger.WithSandboxID(sbxRequest.GetSandbox().GetSandboxId()), logger.WithTemplateID(sbxRequest.GetSandbox().GetTemplateId()), logger.WithBuildID(sbxRequest.GetSandbox().GetBuildId()), logger.WithNodeID(failedNode.ID), zap.Int("attempt", attempt+1), zap.Error(utils.UnwrapGRPCError(err)))
				lastCreateErr = err
				attempt++

				return deadline()
			}
			nodesExcluded[failedNode.ID] = struct{}{}
			failedNode.PlacementMetrics.Fail(sbxRequest.GetSandbox().GetSandboxId())
			logger.L().Error(ctx, "Failed to create sandbox", logger.WithSandboxID(sbxRequest.GetSandbox().GetSandboxId()), logger.WithTemplateID(sbxRequest.GetSandbox().GetTemplateId()), logger.WithBuildID(sbxRequest.GetSandbox().GetBuildId()), logger.WithNodeID(failedNode.ID), zap.Int("attempt", attempt+1), zap.Error(utils.UnwrapGRPCError(err)))
			lastCreateErr = err
			attempt++

			// The loop ends with the caller.
			if createDetached && ctx.Err() != nil {
				return answered()
			}
		}
	}

	// A deadline hitting mid-create is a timeout, not a node failure.
	if ctx.Err() != nil {
		return deadline()
	}

	return failed(SandboxCreateError{Attempts: attempt, LastErr: lastCreateErr})
}

// createRefusedForGood reports a create the node refused with the typed
// detail that says no other node and no retry changes the answer.
func createRefusedForGood(st *status.Status) bool {
	for _, detail := range st.Details() {
		if userErr, ok := detail.(*orchestrator.UserError); ok && userErr.GetCode() == orchestrator.UserErrorCode_SANDBOX_CREATE_REFUSED {
			return true
		}
	}

	return false
}

// allExcluded reports whether every listed node is excluded. A preferred node
// that has left the list can be excluded as well, so comparing counts would
// end placement before every listed node was tried.
func allExcluded(nodes []*nodemanager.Node, excluded map[string]struct{}) bool {
	for _, n := range nodes {
		if _, ok := excluded[n.ID]; !ok {
			return false
		}
	}

	return true
}
