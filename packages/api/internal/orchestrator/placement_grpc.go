package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	placementpb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/sandbox/placement"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
)

// holdBudgetSlack covers the caller's clock against the API's when it
// chose its deadline from the same budgets.
const holdBudgetSlack = time.Minute

var (
	errNotMigratable   = errors.New("the sandbox cannot be migrated")
	errNodeUnavailable = errors.New("the node is not available for a migration")
	errNodeUnknown     = fmt.Errorf("%w: the node is unknown", errNodeUnavailable)
)

// migrationNodes resolves the running execution's source and the requested
// target as nodes a move may take place between: the source Ready or
// Draining, since a draining node still serves its sandboxes and is the
// usual one to migrate off, the target accepting new requests, another
// process than the source's, the CPU a checkpoint restores on, and room in
// its hugepage pool, the admission a create runs.
func (o *Orchestrator) migrationNodes(ctx context.Context, sbx sandbox.Sandbox, targetNodeID string) (*nodemanager.Node, *nodemanager.Node, error) {
	source, err := o.migrationNode(sbx.ClusterID, sbx.NodeID, func(n *nodemanager.Node) bool {
		status := n.Status()

		return status == api.NodeStatusReady || status == api.NodeStatusDraining
	})
	if err != nil {
		return nil, nil, fmt.Errorf("source: %w", err)
	}
	target, err := o.migrationNode(sbx.ClusterID, targetNodeID, (*nodemanager.Node).CanAcceptNewRequests)
	if err != nil {
		return nil, nil, fmt.Errorf("target: %w", err)
	}
	if source.ID == target.ID || source.Metadata().ServiceInstanceID == target.Metadata().ServiceInstanceID {
		return nil, nil, fmt.Errorf("%w: the target is the node it runs on", errNotMigratable)
	}
	if !source.MachineInfo().IsExactMatch(target.MachineInfo()) {
		return nil, nil, fmt.Errorf("%w: the target's CPU differs from the source's", errNodeUnavailable)
	}
	node := sbx.ToNodeSandbox()
	resources := nodemanager.SandboxResources{CPUs: node.VCpu, MiBMemory: node.RamMB, HugePages: node.HugePages}
	memory := placement.MemoryRequirementFor(ctx, o.featureFlagsClient, sbx.TeamID, sbx.SandboxID)
	if ok, _ := placement.MemoryAdmission(target, resources, memory); !ok {
		return nil, nil, fmt.Errorf("%w: the target has no memory for the execution", errNodeUnavailable)
	}

	return source, target, nil
}

// migrationNode resolves nodeID to the node this client knows for it, when
// usable says it may take part: a node the sync replaces meanwhile is not
// read again.
func (o *Orchestrator) migrationNode(clusterID uuid.UUID, nodeID string, usable func(*nodemanager.Node) bool) (*nodemanager.Node, error) {
	node := o.GetNode(clusterID, nodeID)
	if node == nil {
		return nil, fmt.Errorf("node %q: %w", nodeID, errNodeUnknown)
	}
	if !usable(node) {
		return nil, fmt.Errorf("%w: node %q is %s", errNodeUnavailable, nodeID, node.Status())
	}
	if node.Metadata().ServiceInstanceID == "" {
		return nil, fmt.Errorf("%w: node %q reports no service instance", errNodeUnavailable, nodeID)
	}

	return node, nil
}

// SandboxPlacementService serves the migration manager's reads and holds of
// the lifecycle record on the API's internal listener. Only a chain-verified
// caller on the allow-list is served: the listener alone admits other
// internal callers, which must not hold or move records.
type SandboxPlacementService struct {
	placementpb.UnimplementedSandboxPlacementServiceServer

	orchestrator *Orchestrator
	callers      *mtls.AllowList
}

func NewSandboxPlacementService(o *Orchestrator, callers *mtls.AllowList) *SandboxPlacementService {
	return &SandboxPlacementService{orchestrator: o, callers: callers}
}

func (s *SandboxPlacementService) authorize(ctx context.Context) error {
	peer, ok := mtls.PeerFromContext(ctx)
	if !ok || !peer.ChainVerified {
		return status.Error(codes.Unauthenticated, "verified service identity required")
	}
	if !s.callers.Allows(peer.ID) {
		return status.Error(codes.PermissionDenied, "placement caller is not authorized")
	}

	return nil
}

func (s *SandboxPlacementService) GetSandboxPlacement(ctx context.Context, req *placementpb.GetSandboxPlacementRequest) (*placementpb.GetSandboxPlacementResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	teamID, err := uuid.Parse(req.GetTeamId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "team_id must be a UUID")
	}
	o := s.orchestrator
	sbx, err := o.sandboxStore.Get(ctx, teamID, req.GetSandboxId())
	if errors.Is(err, sandbox.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "sandbox not found")
	}
	if err != nil {
		return nil, storeUnavailable(ctx, err)
	}
	switch {
	case sbx.ClusterID != consts.LocalClusterID:
		return nil, status.Error(codes.FailedPrecondition, "the sandbox is not in the local cluster")
	case sbx.State != sandbox.StateRunning:
		return nil, status.Errorf(codes.FailedPrecondition, "the sandbox is %s", sbx.State)
	case sbx.ExecutionID != req.GetExpectedExecutionId() || sbx.NodeID != req.GetExpectedSourceNodeId():
		return nil, status.Error(codes.FailedPrecondition, "the sandbox's placement changed")
	case sbx.NetworkPlacement != nil && sbx.NetworkPlacement.GetExecutionId() != sbx.ExecutionID:
		return nil, status.Error(codes.FailedPrecondition, "network placement belongs to another execution")
	}
	source, target, err := o.migrationNodes(ctx, sbx, req.GetTargetNodeId())
	if errors.Is(err, errNodeUnknown) {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	return placementpb.GetSandboxPlacementResponse_builder{
		ExecutionId:     sbx.ExecutionID,
		NetworkMode:     sbx.NetworkPlacement.GetNetworkMode(),
		RouterId:        sbx.NetworkPlacement.GetRouterId(),
		Source:          placementNode(source),
		Target:          placementNode(target),
		VolumesAttached: new(len(sbx.VolumeMounts) > 0),
	}.Build(), nil
}

func placementNode(node *nodemanager.Node) *placementpb.Node {
	return placementpb.Node_builder{NodeId: node.ID, ServiceInstanceId: node.Metadata().ServiceInstanceID, IpAddress: node.IPAddress}.Build()
}

// HoldExecution takes the hold until the requested deadline, which may not
// outlive the team's operation and termination budgets: a caller that lost
// its operation cannot keep a record unkillable for longer than the
// budgets the migration flags set.
func (s *SandboxPlacementService) HoldExecution(ctx context.Context, req *placementpb.HoldExecutionRequest) (*placementpb.HoldExecutionResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	hold, err := executionHold(req.GetHold())
	if err != nil {
		return nil, err
	}
	if req.GetHoldUntil().CheckValid() != nil {
		return nil, status.Error(codes.InvalidArgument, "hold_until is not a valid timestamp")
	}
	until := req.GetHoldUntil().AsTime()
	team := featureflags.TeamContext(hold.TeamID.String())
	flags := s.orchestrator.featureFlagsClient
	budget := time.Duration(flags.IntFlag(ctx, featureflags.SandboxMigrationOperationBudgetSeconds, team)+flags.IntFlag(ctx, featureflags.SandboxMigrationTerminationBudgetSeconds, team)) * time.Second
	if until.After(time.Now().Add(budget + holdBudgetSlack)) {
		return nil, status.Errorf(codes.InvalidArgument, "hold_until outlives the migration budgets of %s", budget)
	}
	if err := s.orchestrator.sandboxStore.HoldExecution(ctx, hold, until); err != nil {
		return nil, holdError(ctx, err)
	}

	return &placementpb.HoldExecutionResponse{}, nil
}

// ResolveExecutionHold applies the action once: a repeat that finds its
// effect in place is done. A move finds the execution on the target and
// unheld, which only this hold's move leaves; a release finds the record
// not held, whether by this release or because the hold lapsed; an end
// finds the record gone, whether by this end or an ordinary kill. The
// repeats name the same outcome, so none is told apart from its first
// application.
//
// An end after the hold lapsed, of the execution still recorded running on
// the source, ends the record through the ordinary kill, which reports the
// stop once: the source does not report a stop it was asked for, so nothing
// else would end the record before its timeout. The source check keeps a
// late end off the same execution a later operation moved elsewhere.
func (s *SandboxPlacementService) ResolveExecutionHold(ctx context.Context, req *placementpb.ResolveExecutionHoldRequest) (*placementpb.ResolveExecutionHoldResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	hold, err := executionHold(req.GetHold())
	if err != nil {
		return nil, err
	}
	store := s.orchestrator.sandboxStore
	switch action, target := req.GetAction(), req.GetTargetNodeId(); {
	case action == placementpb.ResolutionAction_RESOLUTION_ACTION_MOVE && target != "":
		_, err = store.MoveHeldExecution(ctx, hold, target)
	case action == placementpb.ResolutionAction_RESOLUTION_ACTION_RELEASE && target == "":
		if err = store.ReleaseHeldExecution(ctx, hold); errors.Is(err, sandbox.ErrNotHeld) {
			err = nil
		}
	case action == placementpb.ResolutionAction_RESOLUTION_ACTION_END && target == "":
		var removed sandbox.Sandbox
		removed, err = store.RemoveHeldExecution(ctx, hold)
		switch {
		case err == nil:
			go s.orchestrator.analyticsRemove(context.WithoutCancel(ctx), removed, sandbox.StateActionKill)
		case errors.Is(err, sandbox.ErrNotFound):
			err = nil
		case errors.Is(err, sandbox.ErrNotHeld):
			err = s.orchestrator.endLapsedHold(ctx, hold)
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "target_node_id is required for move and forbidden otherwise")
	}
	if err != nil {
		return nil, holdError(ctx, err)
	}

	return &placementpb.ResolveExecutionHoldResponse{}, nil
}

// endLapsedHold kills the record of the hold's execution after the hold
// lapsed, while the record still runs that execution on the hold's source.
// The source answers NotFound for the guest it stopped, the record goes and
// the kill reports the stop once.
func (o *Orchestrator) endLapsedHold(ctx context.Context, hold sandbox.Hold) error {
	record, err := o.sandboxStore.Get(ctx, hold.TeamID, hold.SandboxID)
	if errors.Is(err, sandbox.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read the record: %w", err)
	}
	if record.State != sandbox.StateRunning || record.ExecutionID != hold.ExecutionID || record.NodeID != hold.SourceNodeID {
		return fmt.Errorf("the record is %s on node %q, not the held execution on its source: %w", record.State, record.NodeID, sandbox.ErrNotHeld)
	}

	return o.RemoveSandbox(ctx, hold.TeamID, hold.SandboxID, sandbox.RemoveOpts{
		Action:            sandbox.StateActionKill,
		Reason:            sandbox.KillReasonEndedOnNode,
		ExpectExecutionID: hold.ExecutionID,
	})
}

func executionHold(ref *placementpb.ExecutionHold) (sandbox.Hold, error) {
	teamID, err := uuid.Parse(ref.GetTeamId())
	if err != nil {
		return sandbox.Hold{}, status.Error(codes.InvalidArgument, "team_id must be a UUID")
	}

	return sandbox.Hold{TeamID: teamID, SandboxID: ref.GetSandboxId(), ExecutionID: ref.GetExecutionId(), SourceNodeID: ref.GetExpectedSourceNodeId(), ID: ref.GetOperationId()}, nil
}

func holdError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, sandbox.ErrNotFound):
		return status.Error(codes.NotFound, "sandbox not found")
	case errors.Is(err, sandbox.ErrHoldConflict), errors.Is(err, sandbox.ErrNotHeld), errors.Is(err, sandbox.ErrExecutionMismatch):
		return status.Error(codes.FailedPrecondition, err.Error())
	}

	return storeUnavailable(ctx, err)
}

func storeUnavailable(ctx context.Context, err error) error {
	logger.L().Error(ctx, "Sandbox placement store failed", zap.Error(err))

	return status.Error(codes.Unavailable, "sandbox store unavailable")
}
