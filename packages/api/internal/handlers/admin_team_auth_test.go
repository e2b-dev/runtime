package handlers

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/authservice"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// An admin request resolves a banned team, and resolving it must not cache the
// team where the team-ID lookups that refuse a ban would read it.
func TestGetTeamFromAdminTokenResolvesABannedTeam(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	authService, err := authservice.NewAuthService(t.Context(), redisutils.SetupInstance(t), db.AuthDB, sharedauth.ProviderConfig{}, http.DefaultClient)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, authService.Close(context.WithoutCancel(t.Context()))) })
	store := &APIStore{authService: authService}

	teamID := testutils.CreateTestTeam(t, db)
	require.NoError(t, db.AuthDB.TestsRawSQL(t.Context(), "UPDATE public.teams SET is_banned = true WHERE id = $1", teamID))

	team, apiErr := store.GetTeamFromAdminToken(t.Context(), nil, teamID.String())
	require.Nil(t, apiErr)
	assert.Equal(t, teamID, team.ID)
	assert.True(t, team.IsBanned)

	_, err = authService.GetTeamByID(t.Context(), teamID)
	var forbidden *sharedauth.TeamForbiddenError
	assert.ErrorAs(t, err, &forbidden)
}
