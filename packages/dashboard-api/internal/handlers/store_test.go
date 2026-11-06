package handlers

import (
	"context"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
)

func TestGetTeamFromAPIKeyUsesAuthService(t *testing.T) {
	t.Parallel()

	team := &authtypes.Team{Team: &dbtypes.Team{ID: uuid.New()}}
	authService := &recordingAPIKeyAuthService{team: team}
	store := &APIStore{authService: authService}

	ginCtx := &gin.Context{}
	got, apiErr := store.GetTeamFromAPIKey(t.Context(), ginCtx, "e2b_test")
	if apiErr != nil {
		t.Fatalf("expected no auth error, got %v", apiErr)
	}
	if got != team {
		t.Fatalf("expected team returned from auth service")
	}
	if authService.apiKey != "e2b_test" {
		t.Fatalf("expected API key to be forwarded, got %q", authService.apiKey)
	}
}

type recordingAPIKeyAuthService struct {
	noopAuthService

	apiKey string
	team   *authtypes.Team
}

func (s *recordingAPIKeyAuthService) ValidateAPIKey(_ context.Context, _ *gin.Context, apiKey string) (*authtypes.Team, *auth.APIError) {
	s.apiKey = apiKey

	return s.team, nil
}
