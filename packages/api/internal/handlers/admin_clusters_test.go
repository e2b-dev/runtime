package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	middleware "github.com/oapi-codegen/gin-middleware"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
)

// The cluster routes list every team on a cluster with its email, so a spec
// edit admitting a team credential on one would show each team the others.
func TestEveryAdminClusterOperationAcceptsOnlyAdminCredentials(t *testing.T) {
	t.Parallel()

	swagger, err := api.GetSwagger()
	require.NoError(t, err)

	operations := 0
	for path, item := range swagger.Paths.Map() {
		if !strings.HasPrefix(path, "/unstable/admin/clusters") {
			continue
		}
		for method, operation := range item.Operations() {
			operations++
			require.NotNil(t, operation.Security, "%s %s", method, path)
			require.Equal(t, openapi3.SecurityRequirements{{"AdminApiKeyAuth": {}}, {"AdminJWTAuth": {}}}, *operation.Security, "%s %s", method, path)
		}
	}
	require.NotZero(t, operations, "no operation matched the cluster prefix")
}

func TestAdminListClusterTeamsSortsAndPagesTheAssignedTeams(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store := newAdminClusterStore(t, db)
	clusterID, empty := uuid.New(), uuid.New()
	insertCluster(t, db, clusterID, false)
	insertCluster(t, db, empty, false)
	first, second := assignTeam(t, db, clusterID), assignTeam(t, db, clusterID)
	testutils.CreateTestTeam(t, db)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.teams SET name = CASE WHEN id = $1 THEN 'b-team' ELSE 'a-team' END, is_banned = (id = $2) WHERE id IN ($1, $2)`,
		first, second))
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.teams SET created_at = now() - interval '1 day' WHERE id = $1`, first))

	for name, tc := range map[string]struct {
		clusterID uuid.UUID
		query     string
		want      []uuid.UUID
		total     int64
	}{
		"newest first by default":       {clusterID, "", []uuid.UUID{second, first}, 2},
		"by name ascending":             {clusterID, "?sortBy=name&sortOrder=asc", []uuid.UUID{second, first}, 2},
		"a page":                        {clusterID, "?sortBy=name&limit=1&offset=1", []uuid.UUID{second}, 2},
		"a cluster without teams":       {empty, "", []uuid.UUID{}, 0},
		"by name descending by default": {clusterID, "?sortBy=name", []uuid.UUID{first, second}, 2},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			response := callAdmin(t, store, http.MethodGet, "/unstable/admin/clusters/"+tc.clusterID.String()+"/teams"+tc.query, "")
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			var list api.ClusterTeamListResponse
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &list))
			ids := make([]uuid.UUID, 0, len(list.Items))
			for _, item := range list.Items {
				ids = append(ids, item.Id)
			}
			require.Equal(t, tc.want, ids)
			require.Equal(t, tc.total, list.Total)
		})
	}

	t.Run("an item says whether the team is banned", func(t *testing.T) {
		t.Parallel()

		response := callAdmin(t, store, http.MethodGet, "/unstable/admin/clusters/"+clusterID.String()+"/teams?sortBy=name&sortOrder=asc", "")
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var list api.ClusterTeamListResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &list))
		require.Len(t, list.Items, 2)
		require.True(t, list.Items[0].IsBanned)
		require.Equal(t, "a-team", *list.Items[0].Name)
		require.False(t, list.Items[1].IsBanned)
	})

	t.Run("an unknown cluster", func(t *testing.T) {
		t.Parallel()

		response := callAdmin(t, store, http.MethodGet, "/unstable/admin/clusters/"+uuid.NewString()+"/teams", "")
		require.Equal(t, http.StatusNotFound, response.Code, response.Body.String())
	})
}

// A deleted project normally leaves its cluster at deletion; one deleted
// before that still carries the cluster ID, and the cluster delete ignores it.
func TestAClusterWhoseOnlyTeamIsDeletedShowsNoTeams(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store := newAdminClusterStore(t, db)
	clusterID := uuid.New()
	insertCluster(t, db, clusterID, false)
	teamID := assignTeam(t, db, clusterID)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.teams SET deleted_at = now(), is_blocked = true, blocked_reason = 'Project deletion' WHERE id = $1`, teamID))

	teams := callAdmin(t, store, http.MethodGet, "/unstable/admin/clusters/"+clusterID.String()+"/teams", "")
	require.Equal(t, http.StatusOK, teams.Code, teams.Body.String())
	var page api.ClusterTeamListResponse
	require.NoError(t, json.Unmarshal(teams.Body.Bytes(), &page))
	require.Empty(t, page.Items)
	require.Zero(t, page.Total)
}

// Without a cluster service the API runs on the local cluster alone, and every
// route that reads or writes a cluster record says so.
func TestClusterRoutesAnswer503WithoutAClusterService(t *testing.T) {
	t.Parallel()

	store := &APIStore{}
	clusterPath := "/unstable/admin/clusters/" + uuid.NewString()
	managementPath := "/unstable/management/clusters/" + uuid.NewString()
	type route struct {
		call               func(*testing.T, *APIStore, string, string, string) *httptest.ResponseRecorder
		method, path, body string
	}
	for name, tc := range map[string]route{
		"admin teams":                  {callAdmin, http.MethodGet, clusterPath + "/teams", ""},
		"management delete":            {callManagement, http.MethodDelete, managementPath, ""},
		"management deletion request":  {callManagement, http.MethodPut, managementPath + "/deletion", ""},
		"management destroy readiness": {callManagement, http.MethodGet, managementPath + "/destroy-readiness", ""},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			response := tc.call(t, store, tc.method, tc.path, tc.body)
			require.Equal(t, http.StatusServiceUnavailable, response.Code, response.Body.String())
		})
	}
}

// The local cluster has no record, and the cluster service refuses its nil ID,
// so the routes answer as for an unknown cluster without asking the service.
func TestClusterRoutesTreatTheLocalClusterAsUnknown(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	// No expectation: a call fails the test.
	store, _ := newClusterServiceStore(t)
	store.sqlcDB = db.SqlcClient
	clusterPath := "/unstable/admin/clusters/" + uuid.Nil.String()
	managementPath := "/unstable/management/clusters/" + uuid.Nil.String()
	type route struct {
		call               func(*testing.T, *APIStore, string, string, string) *httptest.ResponseRecorder
		method, path, body string
		want               int
	}
	for name, tc := range map[string]route{
		"admin teams":                  {callAdmin, http.MethodGet, clusterPath + "/teams", "", http.StatusNotFound},
		"management destroy readiness": {callManagement, http.MethodGet, managementPath + "/destroy-readiness", "", http.StatusNoContent},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			response := tc.call(t, store, tc.method, tc.path, tc.body)
			require.Equal(t, tc.want, response.Code, response.Body.String())
		})
	}
}

// newAdminClusterStore serves the admin cluster routes from the test
// database and the clusters recorded for it.
func newAdminClusterStore(t *testing.T, db *testutils.Database) *APIStore {
	t.Helper()

	return &APIStore{sqlcDB: db.SqlcClient, clusterService: clusterServiceFor(t, db)}
}

// callAdmin serves one request the way the server does: through the OpenAPI
// request validator, with authentication taken as passed.
func callAdmin(t *testing.T, store *APIStore, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	swagger, err := api.GetSwagger()
	require.NoError(t, err)
	swagger.Servers = nil
	router := gin.New()
	router.Use(middleware.OapiRequestValidatorWithOptions(swagger, &middleware.Options{
		ErrorHandler: func(c *gin.Context, message string, status int) {
			utils.ErrorHandler(c, message, max(c.Writer.Status(), status))
		},
		Options:               openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc},
		SilenceServersWarning: true,
	}))
	api.RegisterHandlers(router, store)

	request := httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	return recorder
}

func assignTeam(t *testing.T, db *testutils.Database, clusterID uuid.UUID) uuid.UUID {
	t.Helper()

	teamID := testutils.CreateTestTeam(t, db)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET cluster_id = $1 WHERE id = $2`, clusterID, teamID))

	return teamID
}
