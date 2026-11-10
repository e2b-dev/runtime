package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	internalauthteam "github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/team"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	authdb "github.com/e2b-dev/infra/packages/db/pkg/auth"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/service")

type authStoreImpl struct {
	authDB *authdb.Client
}

var _ authStore = (*authStoreImpl)(nil)

func newAuthStore(authDB *authdb.Client) *authStoreImpl {
	return &authStoreImpl{authDB: authDB}
}

func (s *authStoreImpl) GetTeamByHashedAPIKey(ctx context.Context, hashedKey string) (*types.Team, error) {
	ctx, span := tracer.Start(ctx, "get team auth")
	defer span.End()

	// Deleting an API key invalidates its cache entry; reading through the
	// read replica here races replication lag and could re-cache a
	// just-deleted key for the full cache TTL, so key revocation must be
	// read-after-write safe.
	result, err := s.authDB.GetTeamByAPIKey(ctx, hashedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get team from API key: %w", err)
	}

	team := types.NewTeam(authqueries.GetTeamByTeamIDRow(result))
	if err := internalauthteam.CheckTeamBanned(*team.Team); err != nil {
		return nil, err
	}

	if err := internalauthteam.CheckTeamDeleted(*team.Team); err != nil {
		return nil, err
	}

	go func() {
		// Run the update in a separate context to avoid an extra latency
		ctx := context.WithoutCancel(ctx)
		updateErr := s.authDB.UpdateLastTimeUsed(ctx, hashedKey)
		if updateErr != nil {
			logger.L().Error(ctx, "failed to update last time used", zap.Error(updateErr))
		}
	}()

	return team, nil
}

func (s *authStoreImpl) GetTeamByID(ctx context.Context, teamID uuid.UUID) (*types.Team, error) {
	ctx, span := tracer.Start(ctx, "get team by id auth")
	defer span.End()

	result, err := s.authDB.GetTeamByTeamID(ctx, teamID)
	if err != nil {
		return nil, fmt.Errorf("failed to get team from team ID: %w", err)
	}

	team := types.NewTeam(result)
	if err := internalauthteam.CheckTeamBanned(*team.Team); err != nil {
		return nil, err
	}

	return team, nil
}

func (s *authStoreImpl) GetTeamByIDAndUserID(ctx context.Context, userID uuid.UUID, teamID string) (*types.Team, error) {
	ctx, span := tracer.Start(ctx, "get team by id and user id auth")
	defer span.End()

	teamIDParsed, err := uuid.Parse(teamID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse team ID: %w", err)
	}

	result, err := s.authDB.GetTeamByTeamAndUser(ctx, authqueries.GetTeamByTeamAndUserParams{
		ID:     teamIDParsed,
		UserID: userID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get team from teamID and userID key: %w", err)
	}

	team := types.NewTeam(authqueries.GetTeamByTeamIDRow(result))
	if err := internalauthteam.CheckTeamBanned(*team.Team); err != nil {
		return nil, err
	}

	if err := internalauthteam.CheckTeamDeleted(*team.Team); err != nil {
		return nil, err
	}

	return team, nil
}

func (s *authStoreImpl) GetTeamAPIKeyHashes(ctx context.Context, teamID uuid.UUID) ([]string, error) {
	return s.authDB.GetTeamAPIKeyHashes(ctx, teamID)
}

func (s *authStoreImpl) GetTeamMemberIDs(ctx context.Context, teamID uuid.UUID) ([]uuid.UUID, error) {
	return s.authDB.GetTeamMemberIDs(ctx, teamID)
}
