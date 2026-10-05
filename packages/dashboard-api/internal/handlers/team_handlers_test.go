package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestRequireAuthedTeamMatchesPath_Success(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	teamID := uuid.New()
	auth.SetTeamInfoForTest(t, ctx, &authtypes.Team{
		Team: &authqueries.Team{ID: teamID},
	})

	store := &APIStore{}
	_, ok := store.requireAuthedTeamMatchesPath(ctx, teamID)
	if !ok {
		t.Fatalf("expected team parity check to pass")
	}
}

func TestRequireAuthedTeamMatchesPath_Mismatch(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	auth.SetTeamInfoForTest(t, ctx, &authtypes.Team{
		Team: &authqueries.Team{ID: uuid.New()},
	})

	store := &APIStore{}
	_, ok := store.requireAuthedTeamMatchesPath(ctx, uuid.New())
	if ok {
		t.Fatalf("expected team parity check to fail")
	}
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d", recorder.Code)
	}
}

func insertHandlerTestTeamMember(t *testing.T, db *testutils.Database, userID, teamID uuid.UUID, isDefault bool) {
	t.Helper()

	if isDefault {
		require.NoError(t, db.AuthDB.TestsRawSQL(t.Context(), `
UPDATE public.users_teams
SET is_default = false
WHERE user_id = $1
`, userID))
	}

	require.NoError(t, db.AuthDB.TestsRawSQL(t.Context(), `
INSERT INTO public.users_teams (user_id, team_id, is_default)
VALUES ($1, $2, $3)
`, userID, teamID, isDefault))
}

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
