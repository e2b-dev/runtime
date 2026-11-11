package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	testqueries "github.com/e2b-dev/infra/packages/db/pkg/testutils/queries"
)

func TestGetTeamsDefaultProject(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		defaults      []bool
		wantDefaults  []bool
		sameCreatedAt bool
	}{
		{name: "no projects"},
		{name: "sole project", defaults: []bool{false}, wantDefaults: []bool{true}},
		{name: "first project fallback", defaults: []bool{false, false}, wantDefaults: []bool{true, false}},
		{name: "existing default first", defaults: []bool{true, false}, wantDefaults: []bool{true, false}},
		{name: "existing default later", defaults: []bool{false, true}, wantDefaults: []bool{false, true}},
		{name: "stable order for equal creation times", defaults: []bool{false, false}, wantDefaults: []bool{true, false}, sameCreatedAt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := testutils.SetupDatabase(t)
			userID := uuid.New()
			require.NoError(t, db.AuthDB.UpsertPublicUser(t.Context(), userID))

			teamIDs := make([]uuid.UUID, len(tc.defaults))
			storedDefaults := make(map[uuid.UUID]bool, len(tc.defaults))
			for i, isDefault := range slices.Backward(tc.defaults) {
				idSuffix := len(tc.defaults) - i
				if tc.sameCreatedAt {
					idSuffix = i + 1
				}
				teamID := uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", idSuffix))
				require.NoError(t, db.TestQueries.InsertTestTeam(t.Context(), testqueries.InsertTestTeamParams{
					ID: teamID, Name: fmt.Sprintf("Project %d", i),
					Email: "user@example.com", Slug: fmt.Sprintf("project-%d", i),
				}))
				createdAt := time.Unix(int64(i), 0)
				if tc.sameCreatedAt {
					createdAt = time.Unix(0, 0)
				}
				require.NoError(t, db.AuthDB.TestsRawSQL(t.Context(),
					"UPDATE public.teams SET created_at = $1 WHERE id = $2", createdAt, teamID))
				teamIDs[i] = teamID
				storedDefaults[teamID] = isDefault
				require.NoError(t, db.AuthDB.CreateTeamMembership(t.Context(), authqueries.CreateTeamMembershipParams{
					UserID: userID, TeamID: teamID, IsDefault: isDefault,
				}))
			}

			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/teams", nil)
			auth.SetUserIDForTest(t, ctx, userID)

			store := &APIStore{authDB: db.AuthDB}
			store.GetTeams(ctx)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			var teams []api.Team
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &teams))
			require.NotNil(t, teams)
			require.Len(t, teams, len(teamIDs))
			for i, team := range teams {
				require.Equal(t, teamIDs[i].String(), team.TeamID)
				require.Equal(t, tc.wantDefaults[i], team.IsDefault)
				require.NotEmpty(t, team.ApiKey)
			}

			memberships, err := db.AuthDB.GetTeamsWithUsersTeams(t.Context(), userID)
			require.NoError(t, err)
			for _, membership := range memberships {
				require.Equal(t, storedDefaults[membership.ID], membership.IsDefault)
			}
		})
	}
}

// A deleted default team keeps its membership, but the listing leaves it out,
// mints it no key, and falls back to the remaining team as the default.
func TestGetTeamsLeavesOutADeletedTeam(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	userID := uuid.New()
	require.NoError(t, db.AuthDB.UpsertPublicUser(t.Context(), userID))
	deletedTeam := testutils.CreateTestTeam(t, db)
	liveTeam := testutils.CreateTestTeam(t, db)
	for teamID, isDefault := range map[uuid.UUID]bool{deletedTeam: true, liveTeam: false} {
		require.NoError(t, db.AuthDB.CreateTeamMembership(t.Context(), authqueries.CreateTeamMembershipParams{
			UserID: userID, TeamID: teamID, IsDefault: isDefault,
		}))
	}
	require.NoError(t, db.AuthDB.TestsRawSQL(t.Context(),
		"UPDATE public.teams SET deleted_at = now(), is_blocked = true WHERE id = $1", deletedTeam))

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/teams", nil)
	auth.SetUserIDForTest(t, ctx, userID)

	(&APIStore{authDB: db.AuthDB}).GetTeams(ctx)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var teams []api.Team
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &teams))
	require.Len(t, teams, 1)
	require.Equal(t, liveTeam.String(), teams[0].TeamID)
	require.True(t, teams[0].IsDefault)
	keys, err := db.AuthDB.GetTeamAPIKeyHashes(t.Context(), deletedTeam)
	require.NoError(t, err)
	require.Empty(t, keys)
}

// An API key carries no role, so a key handed to a viewer would let them
// write. A role this build does not know is held to the same.
func TestGetTeamsIssuesNoAPIKeyToAReadOnlyMember(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	userID := uuid.New()
	require.NoError(t, db.AuthDB.UpsertPublicUser(t.Context(), userID))
	roles := map[uuid.UUID]string{
		testutils.CreateTestTeam(t, db): authtypes.TeamRoleOwner,
		testutils.CreateTestTeam(t, db): authtypes.TeamRoleMember,
		testutils.CreateTestTeam(t, db): authtypes.TeamRoleViewer,
		testutils.CreateTestTeam(t, db): "auditor",
	}
	for teamID, role := range roles {
		require.NoError(t, db.AuthDB.CreateTeamMembership(t.Context(), authqueries.CreateTeamMembershipParams{
			UserID: userID, TeamID: teamID,
		}))
		require.NoError(t, db.AuthDB.TestsRawSQL(t.Context(),
			"UPDATE public.users_teams SET role = $1 WHERE team_id = $2 AND user_id = $3", role, teamID, userID))
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/teams", nil)
	auth.SetUserIDForTest(t, ctx, userID)

	(&APIStore{authDB: db.AuthDB}).GetTeams(ctx)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var teams []api.Team
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &teams))
	require.Len(t, teams, len(roles))
	for _, listed := range teams {
		teamID := uuid.MustParse(listed.TeamID)
		keys, err := db.AuthDB.GetTeamAPIKeyHashes(t.Context(), teamID)
		require.NoError(t, err)
		if role := roles[teamID]; role != authtypes.TeamRoleOwner && role != authtypes.TeamRoleMember {
			require.Empty(t, listed.ApiKey, roles[teamID])
			require.Empty(t, keys, roles[teamID])
		} else {
			require.NotEmpty(t, listed.ApiKey, roles[teamID])
			require.Len(t, keys, 1, roles[teamID])
		}
	}
}
