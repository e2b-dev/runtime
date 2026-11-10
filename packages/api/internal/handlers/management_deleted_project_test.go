package handlers

import (
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

// A deleted project keeps its row, so a delivery that arrives after the
// deletion would still find it. The deleted-project middleware answers every
// write with 404 and leaves the project as the deletion left it.
func TestWritesToADeletedProjectAnswerNotFoundAndChangeNothing(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	clusterID := uuid.New()
	insertCluster(t, db, clusterID, false)

	for name, tc := range map[string]struct {
		method string
		path   string
		body   string
	}{
		"project upsert":     {http.MethodPut, "/unstable/management/projects/%s", `{"name":"restored","slug":"restored","email":"restored@example.test"}`},
		"member delivery":    {http.MethodPut, "/unstable/management/projects/%s/members/" + uuid.NewString(), `{"revision":1,"present":true,"identities":[{"issuer":"https://issuer.test","subject":"member"}]}`},
		"limits delivery":    {http.MethodPut, "/unstable/management/projects/%s/limits", validLimitsBody},
		"block delivery":     {http.MethodPut, "/unstable/management/projects/%s/block", `{"revision":1,"blocked":false}`},
		"ban delivery":       {http.MethodPut, "/unstable/management/projects/%s/ban", `{"revision":1,"banned":true}`},
		"cluster assignment": {http.MethodPut, "/unstable/management/projects/%s/cluster/" + clusterID.String(), ""},
		"cluster detachment": {http.MethodDelete, "/unstable/management/projects/%s/cluster/" + clusterID.String(), ""},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store, auth := newManagementStore(t, db)
			projectID := testutils.CreateTestTeam(t, db)
			markProjectDeleted(t, db, projectID)
			before := readDeletedProjectState(t, db, projectID)

			response := callManagement(t, store, tc.method, strings.Replace(tc.path, "%s", projectID.String(), 1), tc.body)

			require.Equal(t, http.StatusNotFound, response.Code, response.Body.String())
			require.JSONEq(t, `{"code":404,"message":"Project not found"}`, response.Body.String())
			require.Equal(t, before, readDeletedProjectState(t, db, projectID))
			require.Empty(t, *auth.invalidatedTeamIDs)
		})
	}
}

func TestALiveProjectPassesTheDeletedProjectCheck(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	projectID := testutils.CreateTestTeam(t, db)

	response := callManagement(t, store, http.MethodPut, "/unstable/management/projects/"+projectID.String(),
		`{"name":"renamed","slug":"renamed-`+projectID.String()[:8]+`","email":"renamed@example.test"}`)

	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
}

// The deletion itself is exempt, so a retried deletion still repeats its
// cleanup.
func TestADeletedProjectCanStillBeDeletedAgain(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	projectID := testutils.CreateTestTeam(t, db)

	first := callDeleteProject(t, store, projectID)
	retry := callDeleteProject(t, store, projectID)

	require.Equal(t, http.StatusNoContent, first.Code, first.Body.String())
	require.Equal(t, http.StatusNoContent, retry.Code, retry.Body.String())
}

// markProjectDeleted leaves a project the way a deletion does: marked deleted,
// blocked, and released from its cluster.
func markProjectDeleted(t *testing.T, db *testutils.Database, projectID uuid.UUID) {
	t.Helper()

	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.teams SET deleted_at = now(), is_blocked = true, blocked_reason = 'Project deletion', cluster_id = NULL WHERE id = $1`,
		projectID))
}

// readDeletedProjectState reads everything a write to the project could
// change: the team row, its memberships and its limits.
func readDeletedProjectState(t *testing.T, db *testutils.Database, projectID uuid.UUID) string {
	t.Helper()

	var state string
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(), `SELECT jsonb_build_object(
		'team', (SELECT to_jsonb(t) FROM public.teams t WHERE t.id = $1),
		'members', (SELECT count(*) FROM public.users_teams WHERE team_id = $1),
		'limits', (SELECT count(*) FROM public.project_limits WHERE team_id = $1))::text`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&state)
		}, projectID))

	return state
}
