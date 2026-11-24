package remote

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

type Storage struct {
	client statepb.StateServiceClient
}

var _ sandboxtypes.Storage = (*Storage)(nil)

// NewStorage is the sandbox record store served by the state gRPC service.
func NewStorage(conn grpc.ClientConnInterface) *Storage {
	return &Storage{client: statepb.NewStateServiceClient(conn)}
}

var wireActions = map[sandboxtypes.StateAction]statepb.RemoveAction{
	sandboxtypes.StateActionPause:    statepb.RemoveAction_REMOVE_ACTION_PAUSE,
	sandboxtypes.StateActionKill:     statepb.RemoveAction_REMOVE_ACTION_KILL,
	sandboxtypes.StateActionSnapshot: statepb.RemoveAction_REMOVE_ACTION_SNAPSHOT,
}

func (s *Storage) Add(ctx context.Context, sbx sandboxtypes.Sandbox) error {
	const method = statepb.StateService_Add_FullMethodName
	record, err := encodeRecord(method, sbx)
	if err != nil {
		return err
	}
	resp, err := unary(ctx, method, s.client.Add, statepb.AddRequest_builder{Record: record}.Build())
	if err != nil {
		return err
	}

	return outcomeError(method, resp.GetOutcome())
}

func (s *Storage) Get(ctx context.Context, teamID uuid.UUID, sandboxID string) (sandboxtypes.Sandbox, error) {
	const method = statepb.StateService_Get_FullMethodName
	resp, err := unary(ctx, method, s.client.Get, statepb.GetRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID}.Build())
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	if err := outcomeError(method, resp.GetOutcome()); err != nil {
		return sandboxtypes.Sandbox{}, err
	}

	return decodeRecord(method, resp.GetRecord())
}

func (s *Storage) Remove(ctx context.Context, teamID uuid.UUID, sandboxID, expectedExecutionID string) error {
	const method = statepb.StateService_Remove_FullMethodName
	resp, err := unary(ctx, method, s.client.Remove, statepb.RemoveRequest_builder{
		TeamId: teamID.String(), SandboxId: sandboxID, ExpectedExecutionId: expectedExecutionID,
	}.Build())
	if err != nil {
		return err
	}

	return outcomeError(method, resp.GetOutcome())
}

func (s *Storage) TeamItems(ctx context.Context, teamID uuid.UUID, filter []sandboxtypes.State) ([]sandboxtypes.Sandbox, error) {
	const method = statepb.StateService_TeamItems_FullMethodName
	wanted := make([]statepb.SandboxState, 0, len(filter))
	for _, state := range filter {
		wire, ok := wireStates[state]
		if !ok {
			return nil, fmt.Errorf("%s: unknown state %q", method, state)
		}
		wanted = append(wanted, wire)
	}

	return readRecords(ctx, method, s.client.TeamItems, statepb.TeamItemsRequest_builder{TeamId: teamID.String(), States: wanted}.Build(), []sandboxtypes.Sandbox{})
}

func (s *Storage) ExpiredItems(ctx context.Context) ([]sandboxtypes.Sandbox, error) {
	return readRecords(ctx, statepb.StateService_ExpiredItems_FullMethodName, s.client.ExpiredItems, statepb.ExpiredItemsRequest_builder{}.Build(), nil)
}

func (s *Storage) TeamsWithSandboxCount(ctx context.Context) (map[uuid.UUID]int64, error) {
	const method = statepb.StateService_TeamsWithSandboxCount_FullMethodName
	resp, err := unary(ctx, method, s.client.TeamsWithSandboxCount, statepb.TeamsWithSandboxCountRequest_builder{}.Build())
	if err != nil {
		return nil, err
	}

	counts := make(map[uuid.UUID]int64, len(resp.GetCounts()))
	for team, count := range resp.GetCounts() {
		teamID, err := uuid.Parse(team)
		if err != nil {
			return nil, fmt.Errorf("%s: team ID: %w", method, err)
		}
		counts[teamID] = count
	}

	return counts, nil
}

type reference struct {
	teamID    string
	sandboxID string
}

func (s *Storage) Reconcile(ctx context.Context, sandboxes []sandboxtypes.NodeSandbox, nodeID string) []sandboxtypes.NodeSandbox {
	wire := make([]*statepb.NodeSandbox, 0, len(sandboxes))
	byReference := make(map[reference]sandboxtypes.NodeSandbox, len(sandboxes))
	for _, sbx := range sandboxes {
		wire = append(wire, statepb.NodeSandbox_builder{
			SandboxId:   sbx.SandboxID,
			ExecutionId: sbx.ExecutionID,
			TeamId:      sbx.TeamID.String(),
			NodeId:      sbx.NodeID,
			ClusterId:   sbx.ClusterID.String(),
			StartTime:   timestamppb.New(sbx.StartTime),
			Vcpu:        sbx.VCpu,
			RamMb:       sbx.RamMB,
			HugePages:   sbx.HugePages,
			Ended:       sbx.Ended,
			GuestEvent:  sbx.GuestEvent,
			DiskKept:    sbx.DiskKept,
		}.Build())
		byReference[reference{teamID: sbx.TeamID.String(), sandboxID: sbx.SandboxID}] = sbx
	}

	resp, err := unary(ctx, statepb.StateService_Reconcile_FullMethodName, s.client.Reconcile, statepb.ReconcileRequest_builder{NodeId: nodeID, Sandboxes: wire}.Build())
	if err != nil {
		// No orphans on a failure: the caller kills every orphan it is told of.
		logger.L().Warn(ctx, "Failed to reconcile the node's sandboxes, skipping", logger.WithNodeID(nodeID), zap.Error(err))

		return nil
	}

	var orphans []sandboxtypes.NodeSandbox
	for _, orphan := range resp.GetOrphans() {
		if sbx, ok := byReference[reference{teamID: orphan.GetTeamId(), sandboxID: orphan.GetSandboxId()}]; ok {
			orphans = append(orphans, sbx)
		}
	}

	return orphans
}

func (s *Storage) BeginUpdate(ctx context.Context, teamID uuid.UUID, sandboxID string) (sandboxtypes.Sandbox, sandboxtypes.UpdateToken, error) {
	const method = statepb.StateService_BeginUpdate_FullMethodName
	resp, err := unary(ctx, method, s.client.BeginUpdate, statepb.BeginUpdateRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID}.Build())
	if err != nil {
		return sandboxtypes.Sandbox{}, "", err
	}
	if err := outcomeError(method, resp.GetOutcome()); err != nil {
		return sandboxtypes.Sandbox{}, "", err
	}
	sbx, err := decodeRecord(method, resp.GetRecord())
	if err != nil {
		return sandboxtypes.Sandbox{}, "", err
	}

	return sbx, sandboxtypes.UpdateToken(resp.GetToken()), nil
}

func (s *Storage) CommitUpdate(ctx context.Context, teamID uuid.UUID, sandboxID string, token sandboxtypes.UpdateToken, updated sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
	const method = statepb.StateService_CommitUpdate_FullMethodName
	record, err := encodeRecord(method, updated)
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	resp, err := unary(ctx, method, s.client.CommitUpdate, statepb.CommitUpdateRequest_builder{
		TeamId: teamID.String(), SandboxId: sandboxID, Token: []byte(token), Record: record,
	}.Build())
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	if err := outcomeError(method, resp.GetOutcome()); err != nil {
		return sandboxtypes.Sandbox{}, err
	}

	return decodeRecord(method, resp.GetRecord())
}

func (s *Storage) AbortUpdate(ctx context.Context, teamID uuid.UUID, sandboxID string, token sandboxtypes.UpdateToken) error {
	const method = statepb.StateService_AbortUpdate_FullMethodName
	resp, err := unary(ctx, method, s.client.AbortUpdate, statepb.AbortUpdateRequest_builder{
		TeamId: teamID.String(), SandboxId: sandboxID, Token: []byte(token),
	}.Build())
	if err != nil {
		return err
	}

	return outcomeError(method, resp.GetOutcome())
}

func wireHold(hold sandboxtypes.Hold) *statepb.Hold {
	return statepb.Hold_builder{
		TeamId:       hold.TeamID.String(),
		SandboxId:    hold.SandboxID,
		ExecutionId:  hold.ExecutionID,
		SourceNodeId: hold.SourceNodeID,
		Id:           hold.ID,
	}.Build()
}

func (s *Storage) HoldExecution(ctx context.Context, hold sandboxtypes.Hold, until time.Time) error {
	const method = statepb.StateService_HoldExecution_FullMethodName
	resp, err := unary(ctx, method, s.client.HoldExecution, statepb.HoldExecutionRequest_builder{Hold: wireHold(hold), Until: timestamppb.New(until)}.Build())
	if err != nil {
		return err
	}

	return outcomeError(method, resp.GetOutcome())
}

func (s *Storage) MoveHeldExecution(ctx context.Context, hold sandboxtypes.Hold, nodeID string) (bool, error) {
	const method = statepb.StateService_MoveHeldExecution_FullMethodName
	resp, err := unary(ctx, method, s.client.MoveHeldExecution, statepb.MoveHeldExecutionRequest_builder{Hold: wireHold(hold), NodeId: nodeID}.Build())
	if err != nil {
		return false, err
	}
	if err := outcomeError(method, resp.GetOutcome()); err != nil {
		return false, err
	}

	return resp.GetMoved(), nil
}

func (s *Storage) ReleaseHeldExecution(ctx context.Context, hold sandboxtypes.Hold) error {
	const method = statepb.StateService_ReleaseHeldExecution_FullMethodName
	resp, err := unary(ctx, method, s.client.ReleaseHeldExecution, statepb.ReleaseHeldExecutionRequest_builder{Hold: wireHold(hold)}.Build())
	if err != nil {
		return err
	}

	return outcomeError(method, resp.GetOutcome())
}

func (s *Storage) RemoveHeldExecution(ctx context.Context, hold sandboxtypes.Hold) (sandboxtypes.Sandbox, error) {
	const method = statepb.StateService_RemoveHeldExecution_FullMethodName
	resp, err := unary(ctx, method, s.client.RemoveHeldExecution, statepb.RemoveHeldExecutionRequest_builder{Hold: wireHold(hold)}.Build())
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	if err := outcomeError(method, resp.GetOutcome()); err != nil {
		return sandboxtypes.Sandbox{}, err
	}

	return decodeRecord(method, resp.GetRecord())
}

func (s *Storage) BeginRemoving(ctx context.Context, teamID uuid.UUID, sandboxID string, opts sandboxtypes.RemoveOpts) (sandboxtypes.StateTransition, bool, error) {
	const method = statepb.StateService_BeginRemoving_FullMethodName
	action, ok := wireActions[opts.Action]
	if !ok {
		return sandboxtypes.StateTransition{}, false, fmt.Errorf("%s: unknown action %q", method, opts.Action.Name)
	}
	resp, err := unary(ctx, method, s.client.BeginRemoving, statepb.BeginRemovingRequest_builder{
		TeamId:    teamID.String(),
		SandboxId: sandboxID,
		Options: statepb.RemoveOptions_builder{
			Action:             action,
			Eviction:           opts.Eviction,
			Reason:             string(opts.Reason),
			FilesystemOnly:     opts.FilesystemOnly,
			RequireEnvdHealthy: opts.RequireEnvdHealthy,
			ExpectExecutionId:  opts.ExpectExecutionID,
			BeforeEndTime:      opts.BeforeEndTime,
		}.Build(),
	}.Build())
	if err != nil {
		return sandboxtypes.StateTransition{}, false, err
	}

	var transition sandboxtypes.StateTransition
	if resp.HasTransition() {
		transition, err = decodeTransition(method, resp.GetTransition())
		if err != nil {
			return sandboxtypes.StateTransition{}, false, err
		}
	}

	// A refusal comes with the transition as read: callers read its record after the error.
	return transition, resp.GetAlreadyDone(), transitionError(method, resp.GetOutcome(), resp.GetCurrentState(), opts.Action.TargetState)
}

func (s *Storage) FinishTransition(ctx context.Context, teamID uuid.UUID, sandboxID string, transitionID string, action sandboxtypes.StateAction, outcome sandboxtypes.TransitionOutcome) error {
	const method = statepb.StateService_FinishTransition_FullMethodName
	wireAction, ok := wireActions[action]
	if !ok {
		return fmt.Errorf("%s: unknown action %q", method, action.Name)
	}
	resp, err := unary(ctx, method, s.client.FinishTransition, statepb.FinishTransitionRequest_builder{
		TeamId:       teamID.String(),
		SandboxId:    sandboxID,
		TransitionId: transitionID,
		Action:       wireAction,
		Result: statepb.TransitionResult_builder{
			Failed:       outcome.Failed,
			Error:        outcome.Error,
			SkipRestore:  outcome.SkipRestore,
			Restored:     outcome.Restored,
			RefusalClass: outcome.RefusalClass,
		}.Build(),
	}.Build())
	if err != nil {
		return err
	}

	return outcomeError(method, resp.GetOutcome())
}

func (s *Storage) RestoreRunning(ctx context.Context, transition sandboxtypes.StateTransition, opts sandboxtypes.RestoreOpts) (sandboxtypes.Sandbox, error) {
	const method = statepb.StateService_RestoreRunning_FullMethodName
	wire, err := encodeTransition(method, transition)
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	resp, err := unary(ctx, method, s.client.RestoreRunning, statepb.RestoreRunningRequest_builder{
		Transition: wire,
		Options: statepb.RestoreOptions_builder{
			RetryAfter:  durationpb.New(opts.RetryAfter),
			HoldOnly:    opts.HoldOnly,
			EnvdRefusal: opts.EnvdRefusal,
		}.Build(),
	}.Build())
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	if err := transitionError(method, resp.GetOutcome(), resp.GetCurrentState(), sandboxtypes.StateRunning); err != nil {
		return sandboxtypes.Sandbox{}, err
	}

	return decodeRecord(method, resp.GetRecord())
}

func (s *Storage) RestoreCheckpointed(ctx context.Context, transition sandboxtypes.StateTransition, placement *orchestrator.SandboxNetworkPlacement) (sandboxtypes.Sandbox, error) {
	const method = statepb.StateService_RestoreCheckpointed_FullMethodName
	wire, err := encodeTransition(method, transition)
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	req := statepb.RestoreCheckpointedRequest_builder{Transition: wire}.Build()
	if placement != nil {
		req.SetPlacement(statepb.NetworkPlacement_builder{
			ExecutionId:    placement.GetExecutionId(),
			NetworkMode:    placement.GetNetworkMode(),
			RouterId:       placement.GetRouterId(),
			IngressAddress: placement.GetIngressAddress(),
			IngressPort:    placement.GetIngressPort(),
		}.Build())
	}
	resp, err := unary(ctx, method, s.client.RestoreCheckpointed, req)
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	if err := transitionError(method, resp.GetOutcome(), resp.GetCurrentState(), sandboxtypes.StateRunning); err != nil {
		return sandboxtypes.Sandbox{}, err
	}

	return decodeRecord(method, resp.GetRecord())
}

func (s *Storage) WaitForTransition(ctx context.Context, teamID uuid.UUID, sandboxID string, transitionID string) (string, error) {
	const method = statepb.StateService_WaitForTransition_FullMethodName
	for {
		// The ID the server returned keeps the outcome of a transition that ends between two polls.
		resp, err := longPoll(ctx, method, s.client.WaitForTransition, statepb.WaitForTransitionRequest_builder{
			TeamId: teamID.String(), SandboxId: sandboxID, TransitionId: transitionID,
		}.Build())
		if err != nil {
			return transitionID, err
		}
		transitionID = resp.GetTransitionId()

		switch resp.GetOutcome() {
		case statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_WAITING:
			continue
		case statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FINISHED:
			return transitionID, nil
		case statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED:
			return transitionID, &sandboxtypes.RestoredTransitionError{Refusal: sandboxtypes.RefusalFromClass(resp.GetRefusalClass())}
		case statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FAILED:
			return transitionID, &sandboxtypes.TransitionFailedError{Message: resp.GetFailure()}
		case statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_NOT_FOUND:
			return transitionID, sandboxtypes.ErrNotFound
		default:
			return transitionID, unknownOutcome(method, resp.GetOutcome())
		}
	}
}

func encodeTransition(method string, transition sandboxtypes.StateTransition) (*statepb.StateTransition, error) {
	record, err := encodeRecord(method, transition.Sandbox)
	if err != nil {
		return nil, err
	}
	wire := statepb.StateTransition_builder{Record: record, Id: transition.ID, InFlightId: transition.InFlightID}.Build()
	if transition.OriginalEndTime != nil {
		wire.SetOriginalEndTime(timestamppb.New(*transition.OriginalEndTime))
	}

	return wire, nil
}

func decodeTransition(method string, wire *statepb.StateTransition) (sandboxtypes.StateTransition, error) {
	sbx, err := decodeRecord(method, wire.GetRecord())
	if err != nil {
		return sandboxtypes.StateTransition{}, err
	}
	transition := sandboxtypes.StateTransition{Sandbox: sbx, ID: wire.GetId(), InFlightID: wire.GetInFlightId()}
	if wire.HasOriginalEndTime() {
		end := wire.GetOriginalEndTime().AsTime()
		transition.OriginalEndTime = &end
	}

	return transition, nil
}
