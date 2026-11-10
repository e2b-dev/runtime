package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
)

func TestGetUnstableTeamsTeamIDStatusReportsABannedAndBlockedTeamToAnAdmin(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	require.NoError(t, db.AuthDB.TestsRawSQL(t.Context(),
		`UPDATE public.teams SET is_banned = true, is_blocked = true, blocked_reason = 'manual review' WHERE id = $1`,
		teamID,
	))

	recorder := callTeamRead(t, &APIStore{authDB: db.AuthDB}, (*APIStore).GetUnstableTeamsTeamIDStatus, teamID.String(), nil)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var body api.TeamStatusResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	reason := "manual review"
	require.Equal(t, api.TeamStatusResponse{IsBanned: true, IsBlocked: true, BlockedReason: &reason}, body)
}

func TestGetUnstableTeamsTeamIDStatusReportsNoReasonForAnUnblockedTeam(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)

	recorder := callTeamRead(t, &APIStore{authDB: db.AuthDB}, (*APIStore).GetUnstableTeamsTeamIDStatus, teamID.String(), &teamID)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.JSONEq(t, `{"isBanned":false,"isBlocked":false,"blockedReason":null}`, recorder.Body.String())
}

func TestTeamStatusAndLimitsAnswerOnlyForTheTeamTheRequestMayRead(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	otherTeamID := testutils.CreateTestTeam(t, db)

	for name, tc := range map[string]struct {
		pathID     string
		authedTeam *uuid.UUID
		wantCode   int
	}{
		"a team reads itself by UUID":                {pathID: teamID.String(), authedTeam: &teamID, wantCode: http.StatusOK},
		"a team reads itself by public project ID":   {pathID: id.ProjectID(teamID).String(), authedTeam: &teamID, wantCode: http.StatusOK},
		"a team names another team":                  {pathID: otherTeamID.String(), authedTeam: &teamID, wantCode: http.StatusForbidden},
		"an admin reads any team":                    {pathID: otherTeamID.String(), wantCode: http.StatusOK},
		"an admin names a team that does not exist":  {pathID: uuid.NewString(), wantCode: http.StatusNotFound},
		"the path names something other than a team": {pathID: id.WorkspaceID(teamID).String(), wantCode: http.StatusBadRequest},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			for endpoint, handler := range map[string]func(*APIStore, *gin.Context, api.TeamID){
				"status": (*APIStore).GetUnstableTeamsTeamIDStatus,
				"limits": (*APIStore).GetUnstableTeamsTeamIDLimits,
			} {
				t.Run(endpoint, func(t *testing.T) {
					t.Parallel()

					recorder := callTeamRead(t, &APIStore{authDB: db.AuthDB}, handler, tc.pathID, tc.authedTeam)
					require.Equal(t, tc.wantCode, recorder.Code, recorder.Body.String())
				})
			}
		})
	}
}

// callTeamRead calls handler as an admin when authedTeamID is nil, otherwise
// as a request authenticated for that team.
func callTeamRead(t *testing.T, store *APIStore, handler func(*APIStore, *gin.Context, api.TeamID), pathID string, authedTeamID *uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/unstable/teams/"+pathID, nil)
	if authedTeamID != nil {
		auth.SetTeamInfoForTest(t, c, &types.Team{Team: &dbtypes.Team{ID: *authedTeamID}})
	}

	handler(store, c, pathID)

	return recorder
}
