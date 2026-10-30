package handlers

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
)

type noopAuthService struct{}

func (noopAuthService) ValidateAPIKey(context.Context, *gin.Context, string) (*authtypes.Team, *auth.APIError) {
	return nil, nil
}

func (noopAuthService) ValidateAuthProviderToken(context.Context, *gin.Context, string) (uuid.UUID, *auth.APIError) {
	return uuid.Nil, nil
}

func (noopAuthService) ValidateAuthProviderTeam(context.Context, *gin.Context, string) (*authtypes.Team, *auth.APIError) {
	return nil, nil
}

func (noopAuthService) GetTeamByID(context.Context, uuid.UUID) (*authtypes.Team, error) {
	return nil, nil
}

func (noopAuthService) InvalidateTeamMemberCache(context.Context, uuid.UUID, string) {}

func (noopAuthService) InvalidateAPIKeyCache(context.Context, string) {}

func (noopAuthService) InvalidateTeamCache(context.Context, uuid.UUID) error {
	return nil
}

func (noopAuthService) Close(context.Context) error {
	return nil
}
