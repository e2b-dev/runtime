package state

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

const (
	internalMessage     = "internal error"
	maxMessageRecords   = 256
	maxMessageRecordLen = 1 << 20
)

var states = map[statepb.SandboxState]sandboxtypes.State{
	statepb.SandboxState_SANDBOX_STATE_RUNNING:      sandboxtypes.StateRunning,
	statepb.SandboxState_SANDBOX_STATE_PAUSING:      sandboxtypes.StatePausing,
	statepb.SandboxState_SANDBOX_STATE_KILLING:      sandboxtypes.StateKilling,
	statepb.SandboxState_SANDBOX_STATE_SNAPSHOTTING: sandboxtypes.StateSnapshotting,
}

type Server struct {
	statepb.UnimplementedStateServiceServer

	storage sandboxtypes.Storage
	maxWait time.Duration
}

var _ statepb.StateServiceServer = (*Server)(nil)

// NewServer's maxWait bounds one WaitForTransition call.
func NewServer(storage sandboxtypes.Storage, maxWait time.Duration) *Server {
	return &Server{storage: storage, maxWait: maxWait}
}

func (s *Server) Add(ctx context.Context, req *statepb.AddRequest) (*statepb.AddResponse, error) {
	var sbx sandboxtypes.Sandbox
	if err := json.Unmarshal(req.GetRecord(), &sbx); err != nil {
		return nil, status.Error(codes.InvalidArgument, "record: must be the JSON of a sandbox record")
	}
	if err := checkRecord(sbx); err != nil {
		return nil, err
	}

	err := s.storage.Add(ctx, sbx)
	outcome, ok := outcomeOf(err)
	if !ok {
		return nil, storeError(ctx, err, logger.WithTeamID(sbx.TeamID.String()), logger.WithSandboxID(sbx.SandboxID))
	}

	return statepb.AddResponse_builder{Outcome: outcome}.Build(), nil
}

func (s *Server) Get(ctx context.Context, req *statepb.GetRequest) (*statepb.GetResponse, error) {
	teamID, err := parseIDs(req.GetTeamId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}

	sbx, err := s.storage.Get(ctx, teamID, req.GetSandboxId())
	outcome, ok := outcomeOf(err)
	if !ok {
		return nil, storeError(ctx, err, logger.WithTeamID(req.GetTeamId()), logger.WithSandboxID(req.GetSandboxId()))
	}
	if outcome != statepb.StateOutcome_STATE_OUTCOME_OK {
		return statepb.GetResponse_builder{Outcome: outcome}.Build(), nil
	}

	record, err := json.Marshal(sbx)
	if err != nil {
		return nil, storeError(ctx, err, logger.WithTeamID(req.GetTeamId()), logger.WithSandboxID(req.GetSandboxId()))
	}

	return statepb.GetResponse_builder{Outcome: outcome, Record: record}.Build(), nil
}

func (s *Server) Remove(ctx context.Context, req *statepb.RemoveRequest) (*statepb.RemoveResponse, error) {
	teamID, err := parseIDs(req.GetTeamId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}

	err = s.storage.Remove(ctx, teamID, req.GetSandboxId(), req.GetExpectedExecutionId())
	outcome, ok := outcomeOf(err)
	if !ok {
		return nil, storeError(ctx, err, logger.WithTeamID(req.GetTeamId()), logger.WithSandboxID(req.GetSandboxId()))
	}

	return statepb.RemoveResponse_builder{Outcome: outcome}.Build(), nil
}

func (s *Server) TeamItems(req *statepb.TeamItemsRequest, stream grpc.ServerStreamingServer[statepb.TeamItemsResponse]) error {
	ctx := stream.Context()
	teamID, err := uuid.Parse(req.GetTeamId())
	if err != nil {
		return status.Error(codes.InvalidArgument, "team_id: must be a valid UUID")
	}
	wanted := make([]sandboxtypes.State, 0, len(req.GetStates()))
	for _, st := range req.GetStates() {
		state, ok := states[st]
		if !ok {
			return status.Error(codes.InvalidArgument, "states: must be defined and not unspecified")
		}
		wanted = append(wanted, state)
	}

	items, err := s.storage.TeamItems(ctx, teamID, wanted)
	if err != nil {
		return storeError(ctx, err, logger.WithTeamID(req.GetTeamId()))
	}

	records, err := encodeRecords(items)
	if err != nil {
		return storeError(ctx, err, logger.WithTeamID(req.GetTeamId()))
	}

	return sendRecords(records, func(chunk [][]byte) error {
		return stream.Send(statepb.TeamItemsResponse_builder{Records: chunk}.Build())
	})
}

func (s *Server) ExpiredItems(_ *statepb.ExpiredItemsRequest, stream grpc.ServerStreamingServer[statepb.ExpiredItemsResponse]) error {
	ctx := stream.Context()
	items, err := s.storage.ExpiredItems(ctx)
	if err != nil {
		return storeError(ctx, err)
	}

	records, err := encodeRecords(items)
	if err != nil {
		return storeError(ctx, err)
	}

	return sendRecords(records, func(chunk [][]byte) error {
		return stream.Send(statepb.ExpiredItemsResponse_builder{Records: chunk}.Build())
	})
}

func (s *Server) TeamsWithSandboxCount(ctx context.Context, _ *statepb.TeamsWithSandboxCountRequest) (*statepb.TeamsWithSandboxCountResponse, error) {
	teams, err := s.storage.TeamsWithSandboxCount(ctx)
	if err != nil {
		return nil, storeError(ctx, err)
	}

	counts := make(map[string]int64, len(teams))
	for teamID, count := range teams {
		counts[teamID.String()] = count
	}

	return statepb.TeamsWithSandboxCountResponse_builder{Counts: counts}.Build(), nil
}

func (s *Server) Reconcile(ctx context.Context, req *statepb.ReconcileRequest) (*statepb.ReconcileResponse, error) {
	sandboxes := make([]sandboxtypes.NodeSandbox, 0, len(req.GetSandboxes()))
	for _, sbx := range req.GetSandboxes() {
		teamID, err := uuid.Parse(sbx.GetTeamId())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "sandboxes.team_id: must be a valid UUID")
		}
		clusterID, err := uuid.Parse(sbx.GetClusterId())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "sandboxes.cluster_id: must be a valid UUID")
		}
		var startTime time.Time
		if sbx.HasStartTime() {
			startTime = sbx.GetStartTime().AsTime()
		}

		sandboxes = append(sandboxes, sandboxtypes.NodeSandbox{
			SandboxID:   sbx.GetSandboxId(),
			ExecutionID: sbx.GetExecutionId(),
			TeamID:      teamID,
			NodeID:      sbx.GetNodeId(),
			ClusterID:   clusterID,
			StartTime:   startTime,
			VCpu:        sbx.GetVcpu(),
			RamMB:       sbx.GetRamMb(),
			HugePages:   sbx.GetHugePages(),
			Ended:       sbx.GetEnded(),
			GuestEvent:  sbx.GetGuestEvent(),
			DiskKept:    sbx.GetDiskKept(),
		})
	}

	orphans := s.storage.Reconcile(ctx, sandboxes, req.GetNodeId())

	refs := make([]*statepb.SandboxReference, 0, len(orphans))
	for _, orphan := range orphans {
		refs = append(refs, statepb.SandboxReference_builder{
			TeamId:    orphan.TeamID.String(),
			SandboxId: orphan.SandboxID,
		}.Build())
	}

	return statepb.ReconcileResponse_builder{Orphans: refs}.Build(), nil
}

func outcomeOf(err error) (statepb.StateOutcome, bool) {
	switch {
	case err == nil:
		return statepb.StateOutcome_STATE_OUTCOME_OK, true
	case errors.Is(err, sandboxtypes.ErrNotFound):
		return statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND, true
	case errors.Is(err, sandboxtypes.ErrLifecycleHeld):
		return statepb.StateOutcome_STATE_OUTCOME_LIFECYCLE_HELD, true
	case errors.Is(err, sandboxtypes.ErrExecutionMismatch):
		return statepb.StateOutcome_STATE_OUTCOME_EXECUTION_MISMATCH, true
	case errors.Is(err, sandboxtypes.ErrLockLost):
		return statepb.StateOutcome_STATE_OUTCOME_LOCK_LOST, true
	case errors.Is(err, sandboxtypes.ErrHoldConflict):
		return statepb.StateOutcome_STATE_OUTCOME_HOLD_CONFLICT, true
	case errors.Is(err, sandboxtypes.ErrNotHeld):
		return statepb.StateOutcome_STATE_OUTCOME_NOT_HELD, true
	case errors.Is(err, sandboxtypes.ErrEvictionInProgress):
		return statepb.StateOutcome_STATE_OUTCOME_EVICTION_IN_PROGRESS, true
	case errors.Is(err, sandboxtypes.ErrEvictionNotNeeded):
		return statepb.StateOutcome_STATE_OUTCOME_EVICTION_NOT_NEEDED, true
	case errors.Is(err, sandboxtypes.ErrRestoreConflict):
		return statepb.StateOutcome_STATE_OUTCOME_RESTORE_CONFLICT, true
	}
	if _, ok := errors.AsType[*sandboxtypes.InvalidStateTransitionError](err); ok {
		return statepb.StateOutcome_STATE_OUTCOME_INVALID_TRANSITION, true
	}

	return statepb.StateOutcome_STATE_OUTCOME_UNSPECIFIED, false
}

// Records are never logged: they carry access tokens.
func encodeRecords(items []sandboxtypes.Sandbox) ([][]byte, error) {
	records := make([][]byte, 0, len(items))
	for _, sbx := range items {
		record, err := json.Marshal(sbx)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	return records, nil
}

func sendRecords(records [][]byte, send func(chunk [][]byte) error) error {
	var chunk [][]byte
	size := 0
	for _, record := range records {
		if len(chunk) > 0 && (len(chunk) == maxMessageRecords || size+len(record) > maxMessageRecordLen) {
			if err := send(chunk); err != nil {
				return err
			}
			chunk, size = nil, 0
		}
		chunk = append(chunk, record)
		size += len(record)
	}
	if len(chunk) > 0 {
		return send(chunk)
	}

	return nil
}

// The store's expiry index member teamID:sandboxID:executionID must parse back, or the record never expires.
func checkRecord(sbx sandboxtypes.Sandbox) error {
	if !validSandboxID(sbx.SandboxID) || strings.Contains(sbx.SandboxID, ":") {
		return status.Error(codes.InvalidArgument, "record: sandboxID must not be empty or contain {, } or :")
	}
	if sbx.TeamID == uuid.Nil {
		return status.Error(codes.InvalidArgument, "record: teamID must not be the nil UUID")
	}
	if executionID, err := uuid.Parse(sbx.ExecutionID); err != nil || executionID.String() != sbx.ExecutionID {
		return status.Error(codes.InvalidArgument, "record: executionID must be a UUID in canonical lower-case form")
	}

	return nil
}

// The store's key builders panic on an empty sandbox ID or one with a brace.
func validSandboxID(sandboxID string) bool {
	return sandboxID != "" && !strings.ContainsAny(sandboxID, "{}")
}

func parseIDs(teamID, sandboxID string) (uuid.UUID, error) {
	team, err := uuid.Parse(teamID)
	if err != nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, "team_id: must be a valid UUID")
	}
	if !validSandboxID(sandboxID) {
		return uuid.Nil, status.Error(codes.InvalidArgument, "sandbox_id: must not be empty or contain { or }")
	}

	return team, nil
}

func storeError(ctx context.Context, err error, fields ...zap.Field) error {
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}

	method, _ := grpc.Method(ctx)
	logger.L().Error(ctx, "state request failed", append([]zap.Field{zap.String("method", method), zap.Error(err)}, fields...)...)

	return status.Error(codes.Internal, internalMessage)
}
