package state

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

func (s *Server) BeginUpdate(ctx context.Context, req *statepb.BeginUpdateRequest) (*statepb.BeginUpdateResponse, error) {
	teamID, err := parseIDs(req.GetTeamId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}

	sbx, token, err := s.storage.BeginUpdate(ctx, teamID, req.GetSandboxId())
	outcome, ok := outcomeOf(err)
	if !ok {
		return nil, storeError(ctx, err, logger.WithTeamID(req.GetTeamId()), logger.WithSandboxID(req.GetSandboxId()))
	}
	if outcome != statepb.StateOutcome_STATE_OUTCOME_OK {
		return statepb.BeginUpdateResponse_builder{Outcome: outcome}.Build(), nil
	}

	record, err := json.Marshal(sbx)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		s.releaseLock(ctx, teamID, req.GetSandboxId(), token)

		return nil, storeError(ctx, err, logger.WithTeamID(req.GetTeamId()), logger.WithSandboxID(req.GetSandboxId()))
	}

	envelope, err := encodeToken(teamID, req.GetSandboxId(), token)
	if err != nil {
		s.releaseLock(ctx, teamID, req.GetSandboxId(), token)

		return nil, storeError(ctx, err, logger.WithTeamID(req.GetTeamId()), logger.WithSandboxID(req.GetSandboxId()))
	}

	return statepb.BeginUpdateResponse_builder{Outcome: outcome, Record: record, Token: envelope}.Build(), nil
}

func (s *Server) CommitUpdate(ctx context.Context, req *statepb.CommitUpdateRequest) (*statepb.CommitUpdateResponse, error) {
	teamID, err := parseIDs(req.GetTeamId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	token, err := decodeToken(req.GetToken(), teamID, req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	sbx, err := commitRecord(req.GetRecord(), teamID, req.GetSandboxId())
	if err != nil {
		// The store releases the lock on every commit; a commit refused here must too.
		s.releaseLock(ctx, teamID, req.GetSandboxId(), token)

		return nil, err
	}

	committed, err := s.storage.CommitUpdate(ctx, teamID, req.GetSandboxId(), token, sbx)
	if errors.Is(err, sandboxtypes.ErrInvalidUpdateToken) {
		return nil, invalidToken()
	}
	outcome, ok := outcomeOf(err)
	if !ok {
		return nil, storeError(ctx, err, logger.WithTeamID(req.GetTeamId()), logger.WithSandboxID(req.GetSandboxId()))
	}
	if outcome != statepb.StateOutcome_STATE_OUTCOME_OK {
		return statepb.CommitUpdateResponse_builder{Outcome: outcome}.Build(), nil
	}

	record, err := json.Marshal(committed)
	if err != nil {
		return nil, storeError(ctx, err, logger.WithTeamID(req.GetTeamId()), logger.WithSandboxID(req.GetSandboxId()))
	}

	return statepb.CommitUpdateResponse_builder{Outcome: outcome, Record: record}.Build(), nil
}

func (s *Server) AbortUpdate(ctx context.Context, req *statepb.AbortUpdateRequest) (*statepb.AbortUpdateResponse, error) {
	teamID, err := parseIDs(req.GetTeamId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	token, err := decodeToken(req.GetToken(), teamID, req.GetSandboxId())
	if err != nil {
		return nil, err
	}

	err = s.storage.AbortUpdate(ctx, teamID, req.GetSandboxId(), token)
	if errors.Is(err, sandboxtypes.ErrInvalidUpdateToken) {
		return nil, invalidToken()
	}
	outcome, ok := outcomeOf(err)
	if !ok {
		return nil, storeError(ctx, err, logger.WithTeamID(req.GetTeamId()), logger.WithSandboxID(req.GetSandboxId()))
	}

	return statepb.AbortUpdateResponse_builder{Outcome: outcome}.Build(), nil
}

func commitRecord(data []byte, teamID uuid.UUID, sandboxID string) (sandboxtypes.Sandbox, error) {
	var sbx sandboxtypes.Sandbox
	if err := json.Unmarshal(data, &sbx); err != nil {
		return sandboxtypes.Sandbox{}, status.Error(codes.InvalidArgument, "record: must be the JSON of a sandbox record")
	}
	if sbx.TeamID != teamID || sbx.SandboxID != sandboxID {
		return sandboxtypes.Sandbox{}, status.Error(codes.InvalidArgument, "record: teamID and sandboxID must be team_id and sandbox_id")
	}
	if err := checkRecord(sbx); err != nil {
		return sandboxtypes.Sandbox{}, err
	}

	return sbx, nil
}

func (s *Server) releaseLock(ctx context.Context, teamID uuid.UUID, sandboxID string, token sandboxtypes.UpdateToken) {
	if err := s.storage.AbortUpdate(context.WithoutCancel(ctx), teamID, sandboxID, token); err != nil {
		logger.L().Warn(ctx, "Failed to release the lock of a refused update", logger.WithTeamID(teamID.String()), logger.WithSandboxID(sandboxID))
	}
}

// The wire token binds the store's token to the team and sandbox BeginUpdate
// verified. Tokens are never logged: one writes under and releases the lock.
type wireToken struct {
	TeamID    string `json:"teamID"`
	SandboxID string `json:"sandboxID"`
	Token     string `json:"token"`
}

func encodeToken(teamID uuid.UUID, sandboxID string, token sandboxtypes.UpdateToken) ([]byte, error) {
	return json.Marshal(wireToken{TeamID: teamID.String(), SandboxID: sandboxID, Token: string(token)})
}

func decodeToken(data []byte, teamID uuid.UUID, sandboxID string) (sandboxtypes.UpdateToken, error) {
	if len(data) == 0 {
		return "", status.Error(codes.InvalidArgument, "token: must not be empty")
	}
	var token wireToken
	if err := json.Unmarshal(data, &token); err != nil || token.TeamID == "" || token.SandboxID == "" || token.Token == "" {
		return "", invalidToken()
	}
	if token.TeamID != teamID.String() || token.SandboxID != sandboxID {
		return "", status.Error(codes.InvalidArgument, "token: issued for another team or sandbox")
	}

	return sandboxtypes.UpdateToken(token.Token), nil
}

func invalidToken() error {
	return status.Error(codes.InvalidArgument, "token: not a token of this store")
}
