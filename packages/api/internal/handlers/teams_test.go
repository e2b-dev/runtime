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
					ID: teamID, Name: fmt.Sprintf("Project %d", i), Tier: "base_v1",
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
				require.Equal(t, storedDefaults[membership.Team.ID], membership.IsDefault)
			}
		})
	}
}
