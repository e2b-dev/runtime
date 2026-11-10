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
	"github.com/jackc/pgx/v5"
	middleware "github.com/oapi-codegen/gin-middleware"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

// The cluster routes create, reconfigure and delete clusters and rotate their
// tokens, so a spec edit admitting a team credential on any of them would hand
// those writes to every team.
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

func TestAdminCreateClusterCreatesAProtectedClusterWithoutReturningItsToken(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store := newAdminClusterStore(db)
	request := adminClusterCreateRequest()

	response := callAdmin(t, store, http.MethodPost, "/unstable/admin/clusters", mustJSON(t, request))

	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
	require.NotContains(t, response.Body.String(), request.Token)
	var created api.ClusterDetail
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &created))
	require.Equal(t, api.ClusterDetail{
		Id:                 created.Id,
		Name:               request.Name,
		Endpoint:           request.Endpoint,
		EndpointTls:        request.EndpointTls,
		SandboxProxyDomain: request.SandboxProxyDomain,
		AuthOrgId:          request.AuthOrgId,
		DeletionProtection: true,
	}, created)
	require.Equal(t, request.Token, clusterToken(t, db, created.Id))

	// Another cluster cannot take the same auth organization.
	conflict := callAdmin(t, store, http.MethodPost, "/unstable/admin/clusters", mustJSON(t, request))
	require.Equal(t, http.StatusConflict, conflict.Code, conflict.Body.String())
	requireClusterErrorCode(t, conflict, clusterRegistrationConflictCode)
}

func TestAdminCreateClusterReusesAStableIDOnlyForAnIdenticalConfiguration(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store := newAdminClusterStore(db)
	clusterID := uuid.New()
	request := adminClusterCreateRequest()
	request.ClusterId = &clusterID

	for range 2 {
		response := callAdmin(t, store, http.MethodPost, "/unstable/admin/clusters", mustJSON(t, request))
		require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
		var created api.ClusterDetail
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &created))
		require.Equal(t, clusterID, created.Id)
	}

	request.Token = "different-token"
	conflict := callAdmin(t, store, http.MethodPost, "/unstable/admin/clusters", mustJSON(t, request))
	require.Equal(t, http.StatusConflict, conflict.Code, conflict.Body.String())
	requireClusterErrorCode(t, conflict, clusterRegistrationConflictCode)
	require.Equal(t, "cluster-token", clusterToken(t, db, clusterID))
}

func TestAdminCreateClusterRejectsAnInvalidRequest(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store := newAdminClusterStore(db)

	for name, tc := range map[string]struct {
		change func(*api.AdminClusterCreateRequest)
	}{
		"blank name":                 {func(r *api.AdminClusterCreateRequest) { r.Name = "  " }},
		"blank token":                {func(r *api.AdminClusterCreateRequest) { r.Token = "  " }},
		"the local cluster's nil ID": {func(r *api.AdminClusterCreateRequest) { r.ClusterId = new(uuid.Nil) }},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request := adminClusterCreateRequest()
			request.AuthOrgId = new("org_" + uuid.NewString())
			tc.change(&request)

			response := callAdmin(t, store, http.MethodPost, "/unstable/admin/clusters", mustJSON(t, request))

			require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
			requireClusterErrorCode(t, response, clusterRegistrationInvalidCode)
		})
	}
}

func TestAdminDeleteClusterRefusesWhatTheManagementDeleteRefuses(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store := newAdminClusterStore(db)
	created := callAdmin(t, store, http.MethodPost, "/unstable/admin/clusters", mustJSON(t, adminClusterCreateRequest()))
	require.Equal(t, http.StatusCreated, created.Code, created.Body.String())
	var protected api.ClusterDetail
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &protected))
	unprotected := uuid.New()
	insertCluster(t, db, unprotected, false)

	refused := callAdmin(t, store, http.MethodDelete, "/unstable/admin/clusters/"+protected.Id.String(), "")
	require.Equal(t, http.StatusConflict, refused.Code, refused.Body.String())
	require.True(t, clusterExists(t, db, protected.Id))

	local := callAdmin(t, store, http.MethodDelete, "/unstable/admin/clusters/"+uuid.Nil.String(), "")
	require.Equal(t, http.StatusBadRequest, local.Code, local.Body.String())

	for range 2 {
		deleted := callAdmin(t, store, http.MethodDelete, "/unstable/admin/clusters/"+unprotected.String(), "")
		require.Equal(t, http.StatusNoContent, deleted.Code, deleted.Body.String())
	}
	require.False(t, clusterExists(t, db, unprotected))
}

func TestAdminListClustersSearchesSortsAndPages(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store := newAdminClusterStore(db)
	// A prefix only these clusters carry keeps the list to them.
	prefix := "list-" + uuid.NewString()[:8]
	alpha := insertNamedCluster(t, db, prefix+"-alpha", "c.example.test", new("alpha.example.test"))
	bravo := insertNamedCluster(t, db, prefix+"-bravo", "a.example.test", nil)
	charlie := insertNamedCluster(t, db, prefix+"-charlie", "b.example.test", nil)
	for range 2 {
		assignTeam(t, db, charlie)
	}
	assignTeam(t, db, alpha)

	for name, tc := range map[string]struct {
		search string
		params string
		want   []uuid.UUID
		total  int64
	}{
		"by endpoint ascending by default": {prefix, "", []uuid.UUID{bravo, charlie, alpha}, 3},
		"by name descending":               {prefix, "&sortBy=name&sortOrder=desc", []uuid.UUID{charlie, bravo, alpha}, 3},
		"by team count descending":         {prefix, "&sortBy=teamCount&sortOrder=desc", []uuid.UUID{charlie, alpha, bravo}, 3},
		"a page":                           {prefix, "&limit=1&offset=1", []uuid.UUID{charlie}, 3},
		"a name":                           {prefix + "-bravo", "", []uuid.UUID{bravo}, 1},
		"an endpoint":                      {"b.example.test", "", []uuid.UUID{charlie}, 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			response := callAdmin(t, store, http.MethodGet, "/unstable/admin/clusters?search="+tc.search+tc.params, "")
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			var list api.ClusterListResponse
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &list))

			ids := make([]uuid.UUID, 0, len(list.Items))
			for _, item := range list.Items {
				ids = append(ids, item.Id)
			}
			require.Equal(t, tc.want, ids)
			require.Equal(t, tc.total, list.Total)
		})
	}

	t.Run("an item carries the cluster's team count and proxy domain", func(t *testing.T) {
		t.Parallel()

		response := callAdmin(t, store, http.MethodGet, "/unstable/admin/clusters?search="+prefix+"-alpha", "")
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var list api.ClusterListResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &list))
		require.Equal(t, []api.ClusterListItem{{
			Id: alpha, Name: prefix + "-alpha", Endpoint: "c.example.test",
			SandboxProxyDomain: new("alpha.example.test"), TeamCount: 1,
		}}, list.Items)
	})

	t.Run("a search treats LIKE wildcards literally", func(t *testing.T) {
		t.Parallel()

		response := callAdmin(t, store, http.MethodGet, "/unstable/admin/clusters?search="+prefix+"%25", "")
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var list api.ClusterListResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &list))
		require.Empty(t, list.Items)
		require.Zero(t, list.Total)
	})
}

func TestAdminGetClusterReturnsItsTeamsAndLiveTemplateCount(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store := newAdminClusterStore(db)
	clusterID := insertNamedCluster(t, db, "detail", "detail.example.test", nil)
	older, newer := assignTeam(t, db, clusterID), assignTeam(t, db, clusterID)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.teams SET created_at = now() - interval '1 day' WHERE id = $1`, older))
	live := testutils.CreateTestTemplate(t, db, older)
	deleted := testutils.CreateTestTemplate(t, db, older)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.envs SET cluster_id = $1, deleted_at = CASE WHEN id = $3 THEN now() END WHERE id IN ($2, $3)`,
		clusterID, live, deleted))

	response := callAdmin(t, store, http.MethodGet, "/unstable/admin/clusters/"+clusterID.String()+"?teamsLimit=1", "")

	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.NotContains(t, response.Body.String(), "token")
	var detail api.ClusterDetailWithTeams
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &detail))
	require.Equal(t, api.ClusterDetail{
		Id: clusterID, Name: "detail", Endpoint: "detail.example.test", EndpointTls: true, DeletionProtection: false,
	}, detail.Cluster)
	require.Len(t, detail.Teams.Items, 1)
	require.Equal(t, newer, detail.Teams.Items[0].Id, "newest team first")
	require.EqualValues(t, 2, detail.Teams.Total)
	require.EqualValues(t, 2, detail.TeamCount)
	require.EqualValues(t, 1, detail.TemplateCount)

	missing := callAdmin(t, store, http.MethodGet, "/unstable/admin/clusters/"+uuid.NewString(), "")
	require.Equal(t, http.StatusNotFound, missing.Code, missing.Body.String())
}

func TestAdminListClusterTeamsSortsAndPagesTheAssignedTeams(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store := newAdminClusterStore(db)
	clusterID := insertNamedCluster(t, db, "teams", "teams.example.test", nil)
	empty := insertNamedCluster(t, db, "empty", "empty.example.test", nil)
	first, second := assignTeam(t, db, clusterID), assignTeam(t, db, clusterID)
	testutils.CreateTestTeam(t, db)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.teams SET name = CASE WHEN id = $1 THEN 'b-team' ELSE 'a-team' END, is_banned = (id = $2) WHERE id IN ($1, $2)`,
		first, second))

	for name, tc := range map[string]struct {
		clusterID uuid.UUID
		query     string
		want      []uuid.UUID
		total     int64
	}{
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

func TestAdminUpdateClusterWritesOnlyThePropertiesPresent(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store := newAdminClusterStore(db)

	for name, tc := range map[string]struct {
		body string
		want func(api.ClusterDetail) api.ClusterDetail
	}{
		"name": {`{"name":" renamed "}`, func(d api.ClusterDetail) api.ClusterDetail {
			d.Name = "renamed"

			return d
		}},
		"endpoint and TLS": {`{"endpoint":"moved.example.test","endpointTls":false}`, func(d api.ClusterDetail) api.ClusterDetail {
			d.Endpoint, d.EndpointTls = "moved.example.test", false

			return d
		}},
		"proxy domain cleared by null": {`{"sandboxProxyDomain":null}`, func(d api.ClusterDetail) api.ClusterDetail {
			d.SandboxProxyDomain = nil

			return d
		}},
		"auth organization cleared by an empty string": {`{"authOrgId":""}`, func(d api.ClusterDetail) api.ClusterDetail {
			d.AuthOrgId = nil

			return d
		}},
		"deletion protection": {`{"deletionProtection":false}`, func(d api.ClusterDetail) api.ClusterDetail {
			d.DeletionProtection = false

			return d
		}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request := adminClusterCreateRequest()
			request.AuthOrgId = new("org_" + uuid.NewString())
			created := callAdmin(t, store, http.MethodPost, "/unstable/admin/clusters", mustJSON(t, request))
			require.Equal(t, http.StatusCreated, created.Code, created.Body.String())
			var before api.ClusterDetail
			require.NoError(t, json.Unmarshal(created.Body.Bytes(), &before))

			response := callAdmin(t, store, http.MethodPatch, "/unstable/admin/clusters/"+before.Id.String(), tc.body)

			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			var updated api.ClusterDetail
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &updated))
			require.Equal(t, tc.want(before), updated)
			require.Equal(t, request.Token, clusterToken(t, db, before.Id))
		})
	}
}

func TestAdminUpdateClusterRefusesAnInvalidUpdate(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store := newAdminClusterStore(db)
	clusterID := insertNamedCluster(t, db, "refuse", "refuse.example.test", nil)
	other := insertNamedCluster(t, db, "other", "other.example.test", nil)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.clusters SET auth_org_id = 'org_taken' WHERE id = $1`, other))
	deleting := insertNamedCluster(t, db, "deleting", "deleting.example.test", nil)
	require.Equal(t, http.StatusNoContent, callManagementRequestClusterDeletion(t, &APIStore{sqlcDB: db.SqlcClient}, deleting))
	before := readClusters(t, db)

	for name, tc := range map[string]struct {
		clusterID uuid.UUID
		body      string
		want      int
	}{
		"no property":                              {clusterID, `{}`, http.StatusBadRequest},
		"a blank name":                             {clusterID, `{"name":"  "}`, http.StatusBadRequest},
		"a blank endpoint":                         {clusterID, `{"endpoint":"  "}`, http.StatusBadRequest},
		"a null TLS flag":                          {clusterID, `{"endpointTls":null}`, http.StatusBadRequest},
		"a null protection flag":                   {clusterID, `{"deletionProtection":null}`, http.StatusBadRequest},
		"an unknown cluster":                       {uuid.New(), `{"name":"renamed"}`, http.StatusNotFound},
		"another cluster's auth organization":      {clusterID, `{"authOrgId":"org_taken"}`, http.StatusConflict},
		"protection on while deletion is underway": {deleting, `{"deletionProtection":true}`, http.StatusConflict},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			response := callAdmin(t, store, http.MethodPatch, "/unstable/admin/clusters/"+tc.clusterID.String(), tc.body)

			require.Equal(t, tc.want, response.Code, response.Body.String())
			require.Equal(t, before, readClusters(t, db))
		})
	}
}

// The request validator refuses a null flag first; the handler refuses it too
// rather than writing false.
func TestAdminUpdateClusterRefusesANullFlagWithoutTheValidator(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store := newAdminClusterStore(db)
	clusterID := uuid.New()
	insertCluster(t, db, clusterID, true)
	before := readClusters(t, db)
	router := gin.New()
	api.RegisterHandlers(router, store)

	for name, body := range map[string]string{
		"deletion protection": `{"deletionProtection":null}`,
		"TLS":                 `{"endpointTls":null}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request := httptest.NewRequestWithContext(t.Context(), http.MethodPatch, "/unstable/admin/clusters/"+clusterID.String(), strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)

			require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
			require.Equal(t, before, readClusters(t, db))
		})
	}
}

// A deleted project normally leaves its cluster at deletion; one deleted
// before that still carries the cluster ID, and the cluster delete ignores it.
func TestAClusterWhoseOnlyTeamIsDeletedShowsNoTeams(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store := newAdminClusterStore(db)
	name := "deleted-team-" + uuid.NewString()[:8]
	clusterID := insertNamedCluster(t, db, name, "deleted-team.example.test", nil)
	teamID := assignTeam(t, db, clusterID)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.teams SET deleted_at = now(), is_blocked = true, blocked_reason = 'Project deletion' WHERE id = $1`, teamID))

	listed := callAdmin(t, store, http.MethodGet, "/unstable/admin/clusters?search="+name, "")
	require.Equal(t, http.StatusOK, listed.Code, listed.Body.String())
	var list api.ClusterListResponse
	require.NoError(t, json.Unmarshal(listed.Body.Bytes(), &list))
	require.Len(t, list.Items, 1)
	require.Zero(t, list.Items[0].TeamCount)

	got := callAdmin(t, store, http.MethodGet, "/unstable/admin/clusters/"+clusterID.String(), "")
	require.Equal(t, http.StatusOK, got.Code, got.Body.String())
	var detail api.ClusterDetailWithTeams
	require.NoError(t, json.Unmarshal(got.Body.Bytes(), &detail))
	require.Zero(t, detail.TeamCount)
	require.Empty(t, detail.Teams.Items)
	require.Zero(t, detail.Teams.Total)

	teams := callAdmin(t, store, http.MethodGet, "/unstable/admin/clusters/"+clusterID.String()+"/teams", "")
	require.Equal(t, http.StatusOK, teams.Code, teams.Body.String())
	var page api.ClusterTeamListResponse
	require.NoError(t, json.Unmarshal(teams.Body.Bytes(), &page))
	require.Empty(t, page.Items)
	require.Zero(t, page.Total)
}

func TestAdminRotateClusterTokenReplacesOnlyTheToken(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store := newAdminClusterStore(db)
	clusterID := insertNamedCluster(t, db, "rotate", "rotate.example.test", nil)

	rotated := callAdmin(t, store, http.MethodPut, "/unstable/admin/clusters/"+clusterID.String()+"/token", `{"token":" rotated-token "}`)
	require.Equal(t, http.StatusNoContent, rotated.Code, rotated.Body.String())
	require.Equal(t, "rotated-token", clusterToken(t, db, clusterID))

	blank := callAdmin(t, store, http.MethodPut, "/unstable/admin/clusters/"+clusterID.String()+"/token", `{"token":"  "}`)
	require.Equal(t, http.StatusBadRequest, blank.Code, blank.Body.String())
	require.Equal(t, "rotated-token", clusterToken(t, db, clusterID))

	missing := callAdmin(t, store, http.MethodPut, "/unstable/admin/clusters/"+uuid.NewString()+"/token", `{"token":"token"}`)
	require.Equal(t, http.StatusNotFound, missing.Code, missing.Body.String())
}

func newAdminClusterStore(db *testutils.Database) *APIStore {
	return &APIStore{sqlcDB: db.SqlcClient}
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

func adminClusterCreateRequest() api.AdminClusterCreateRequest {
	return api.AdminClusterCreateRequest{
		Name:               "Operator cluster",
		Endpoint:           "api.example.test:5008",
		EndpointTls:        true,
		Token:              "cluster-token",
		SandboxProxyDomain: new("sandbox.example.test"),
		AuthOrgId:          new("org_test"),
	}
}

func insertNamedCluster(t *testing.T, db *testutils.Database, name, endpoint string, sandboxProxyDomain *string) uuid.UUID {
	t.Helper()

	clusterID := uuid.New()
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`INSERT INTO public.clusters (id, name, endpoint, endpoint_tls, token, sandbox_proxy_domain, deletion_protection)
		VALUES ($1, $2, $3, true, 'token', $4, false)`, clusterID, name, endpoint, sandboxProxyDomain))

	return clusterID
}

func assignTeam(t *testing.T, db *testutils.Database, clusterID uuid.UUID) uuid.UUID {
	t.Helper()

	teamID := testutils.CreateTestTeam(t, db)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET cluster_id = $1 WHERE id = $2`, clusterID, teamID))

	return teamID
}

func clusterToken(t *testing.T, db *testutils.Database, clusterID uuid.UUID) string {
	t.Helper()

	var token string
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(), `SELECT token FROM public.clusters WHERE id = $1`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&token)
		}, clusterID))

	return token
}

// readClusters reads every stored cluster, token included, to show a refused
// write changed none of them.
func readClusters(t *testing.T, db *testutils.Database) string {
	t.Helper()

	var state string
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		`SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY id), '[]')::text FROM public.clusters c`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&state)
		}))

	return state
}
