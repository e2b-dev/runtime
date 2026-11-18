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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
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

	request := adminClusterCreateRequest()
	created := createdCluster(uuid.New(), request)
	want := clustersgrpc.CreateClusterRequest_builder{
		Name:               request.Name,
		Endpoint:           request.Endpoint,
		EndpointTls:        new(request.EndpointTls),
		Token:              request.Token,
		SandboxProxyDomain: request.SandboxProxyDomain,
		AuthOrgId:          request.AuthOrgId,
		DeletionProtection: new(true),
	}.Build()
	store, clusterService := newClusterServiceStore(t)
	clusterService.EXPECT().CreateCluster(mock.Anything, mock.MatchedBy(func(req *clustersgrpc.CreateClusterRequest) bool {
		return proto.Equal(want, req)
	})).Return(clustersgrpc.CreateClusterResponse_builder{Cluster: created}.Build(), nil).Once()

	response := callAdmin(t, store, http.MethodPost, "/unstable/admin/clusters", mustJSON(t, request))

	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
	require.NotContains(t, response.Body.String(), request.Token)
	var detail api.ClusterDetail
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &detail))
	require.Equal(t, api.ClusterDetail{
		Id:                 uuid.MustParse(created.GetClusterId()),
		Name:               request.Name,
		Endpoint:           request.Endpoint,
		EndpointTls:        request.EndpointTls,
		SandboxProxyDomain: request.SandboxProxyDomain,
		AuthOrgId:          request.AuthOrgId,
		DeletionProtection: true,
	}, detail)
}

// The cluster service answers a replay of the same ID and configuration with
// the stored cluster, and refuses anything else as a conflict.
func TestAdminCreateClusterSendsTheRequestedIDAndReportsAConflict(t *testing.T) {
	t.Parallel()

	clusterID := uuid.New()
	request := adminClusterCreateRequest()
	request.ClusterId = &clusterID
	store, clusterService := newClusterServiceStore(t)
	isRequestedID := mock.MatchedBy(func(req *clustersgrpc.CreateClusterRequest) bool {
		return req.HasClusterId() && req.GetClusterId() == clusterID.String()
	})
	clusterService.EXPECT().CreateCluster(mock.Anything, isRequestedID).
		Return(clustersgrpc.CreateClusterResponse_builder{Cluster: createdCluster(clusterID, request)}.Build(), nil).Once()
	clusterService.EXPECT().CreateCluster(mock.Anything, isRequestedID).
		Return(nil, status.Error(codes.AlreadyExists, "a different cluster with this ID exists")).Once()

	created := callAdmin(t, store, http.MethodPost, "/unstable/admin/clusters", mustJSON(t, request))
	require.Equal(t, http.StatusCreated, created.Code, created.Body.String())
	var detail api.ClusterDetail
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &detail))
	require.Equal(t, clusterID, detail.Id)

	conflict := callAdmin(t, store, http.MethodPost, "/unstable/admin/clusters", mustJSON(t, request))
	require.Equal(t, http.StatusConflict, conflict.Code, conflict.Body.String())
	requireClusterErrorCode(t, conflict, clusterRegistrationConflictCode)
}

func TestAdminCreateClusterRejectsAnInvalidRequest(t *testing.T) {
	t.Parallel()

	// The cluster service is never asked.
	store, _ := newClusterServiceStore(t)

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
	store := newAdminClusterStore(t, db)
	protected, unprotected := uuid.New(), uuid.New()
	insertCluster(t, db, protected, true)
	insertCluster(t, db, unprotected, false)

	refused := callAdmin(t, store, http.MethodDelete, "/unstable/admin/clusters/"+protected.String(), "")
	require.Equal(t, http.StatusConflict, refused.Code, refused.Body.String())
	require.True(t, clusterExists(t, db, protected))

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
	store := newAdminClusterStore(t, db)
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
	store := newAdminClusterStore(t, db)
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
	store := newAdminClusterStore(t, db)
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

	for name, tc := range map[string]struct {
		body string
		want func(clustersgrpc.UpdateClusterRequest_builder) clustersgrpc.UpdateClusterRequest_builder
	}{
		"name": {`{"name":" renamed "}`, func(u clustersgrpc.UpdateClusterRequest_builder) clustersgrpc.UpdateClusterRequest_builder {
			u.Name = new("renamed")

			return u
		}},
		"endpoint and TLS": {`{"endpoint":"moved.example.test","endpointTls":false}`, func(u clustersgrpc.UpdateClusterRequest_builder) clustersgrpc.UpdateClusterRequest_builder {
			u.Endpoint, u.EndpointTls = new("moved.example.test"), new(false)

			return u
		}},
		"proxy domain cleared by null": {`{"sandboxProxyDomain":null}`, func(u clustersgrpc.UpdateClusterRequest_builder) clustersgrpc.UpdateClusterRequest_builder {
			u.SandboxProxyDomain = clustersgrpc.NullableStringUpdate_builder{}.Build()

			return u
		}},
		"auth organization cleared by an empty string": {`{"authOrgId":""}`, func(u clustersgrpc.UpdateClusterRequest_builder) clustersgrpc.UpdateClusterRequest_builder {
			u.AuthOrgId = clustersgrpc.NullableStringUpdate_builder{}.Build()

			return u
		}},
		"deletion protection": {`{"deletionProtection":false}`, func(u clustersgrpc.UpdateClusterRequest_builder) clustersgrpc.UpdateClusterRequest_builder {
			u.DeletionProtection = new(false)

			return u
		}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			clusterID := uuid.New()
			updated := createdCluster(clusterID, adminClusterCreateRequest())
			want := tc.want(clustersgrpc.UpdateClusterRequest_builder{ClusterId: clusterID.String()}).Build()
			store, clusterService := newClusterServiceStore(t)
			clusterService.EXPECT().UpdateCluster(mock.Anything, mock.MatchedBy(func(req *clustersgrpc.UpdateClusterRequest) bool {
				return proto.Equal(want, req)
			})).Return(clustersgrpc.UpdateClusterResponse_builder{Cluster: updated}.Build(), nil).Once()

			response := callAdmin(t, store, http.MethodPatch, "/unstable/admin/clusters/"+clusterID.String(), tc.body)

			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			require.NotContains(t, response.Body.String(), updated.GetToken())
			var detail api.ClusterDetail
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &detail))
			wantDetail, err := clusterDetail(updated)
			require.NoError(t, err)
			require.Equal(t, wantDetail, detail)
		})
	}
}

func TestAdminUpdateClusterRefusesAnInvalidUpdate(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		body   string
		answer error
		want   int
	}{
		"no property":            {`{}`, nil, http.StatusBadRequest},
		"a blank name":           {`{"name":"  "}`, nil, http.StatusBadRequest},
		"a blank endpoint":       {`{"endpoint":"  "}`, nil, http.StatusBadRequest},
		"a null TLS flag":        {`{"endpointTls":null}`, nil, http.StatusBadRequest},
		"a null protection flag": {`{"deletionProtection":null}`, nil, http.StatusBadRequest},
		"an unknown cluster":     {`{"name":"renamed"}`, status.Error(codes.NotFound, "cluster not found"), http.StatusNotFound},
		"another cluster's auth organization": {
			`{"authOrgId":"org_taken"}`, status.Error(codes.AlreadyExists, "a cluster with this auth organization exists"), http.StatusConflict,
		},
		"protection on while deletion is underway": {
			`{"deletionProtection":true}`, clusterRefusal(clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_DELETION_REQUESTED), http.StatusConflict,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// A body refused before the call never reaches the cluster service.
			store, clusterService := newClusterServiceStore(t)
			if tc.answer != nil {
				clusterService.EXPECT().UpdateCluster(mock.Anything, mock.Anything).Return(nil, tc.answer).Once()
			}

			response := callAdmin(t, store, http.MethodPatch, "/unstable/admin/clusters/"+uuid.NewString(), tc.body)

			require.Equal(t, tc.want, response.Code, response.Body.String())
		})
	}
}

// The request validator refuses a null flag first; the handler refuses it too
// rather than writing false.
func TestAdminUpdateClusterRefusesANullFlagWithoutTheValidator(t *testing.T) {
	t.Parallel()

	// The cluster service is never asked.
	store, _ := newClusterServiceStore(t)
	router := gin.New()
	api.RegisterHandlers(router, store)

	for name, body := range map[string]string{
		"deletion protection": `{"deletionProtection":null}`,
		"TLS":                 `{"endpointTls":null}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request := httptest.NewRequestWithContext(t.Context(), http.MethodPatch, "/unstable/admin/clusters/"+uuid.NewString(), strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)

			require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
		})
	}
}

// A deleted project normally leaves its cluster at deletion; one deleted
// before that still carries the cluster ID, and the cluster delete ignores it.
func TestAClusterWhoseOnlyTeamIsDeletedShowsNoTeams(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store := newAdminClusterStore(t, db)
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

	clusterID := uuid.New()
	store, clusterService := newClusterServiceStore(t)
	want := clustersgrpc.UpdateClusterRequest_builder{ClusterId: clusterID.String(), Token: new("rotated-token")}.Build()
	clusterService.EXPECT().UpdateCluster(mock.Anything, mock.MatchedBy(func(req *clustersgrpc.UpdateClusterRequest) bool {
		return proto.Equal(want, req)
	})).Return(clustersgrpc.UpdateClusterResponse_builder{Cluster: createdCluster(clusterID, adminClusterCreateRequest())}.Build(), nil).Once()
	clusterService.EXPECT().UpdateCluster(mock.Anything, mock.MatchedBy(func(req *clustersgrpc.UpdateClusterRequest) bool {
		return req.GetClusterId() != clusterID.String()
	})).Return(nil, status.Error(codes.NotFound, "cluster not found")).Once()

	rotated := callAdmin(t, store, http.MethodPut, "/unstable/admin/clusters/"+clusterID.String()+"/token", `{"token":" rotated-token "}`)
	require.Equal(t, http.StatusNoContent, rotated.Code, rotated.Body.String())

	blank := callAdmin(t, store, http.MethodPut, "/unstable/admin/clusters/"+clusterID.String()+"/token", `{"token":"  "}`)
	require.Equal(t, http.StatusBadRequest, blank.Code, blank.Body.String())

	missing := callAdmin(t, store, http.MethodPut, "/unstable/admin/clusters/"+uuid.NewString()+"/token", `{"token":"token"}`)
	require.Equal(t, http.StatusNotFound, missing.Code, missing.Body.String())
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
		"admin list":                   {callAdmin, http.MethodGet, "/unstable/admin/clusters", ""},
		"admin create":                 {callAdmin, http.MethodPost, "/unstable/admin/clusters", mustJSON(t, adminClusterCreateRequest())},
		"admin get":                    {callAdmin, http.MethodGet, clusterPath, ""},
		"admin metadata":               {callAdmin, http.MethodGet, clusterPath + "/metadata", ""},
		"admin update":                 {callAdmin, http.MethodPatch, clusterPath, `{"name":"renamed"}`},
		"admin delete":                 {callAdmin, http.MethodDelete, clusterPath, ""},
		"admin teams":                  {callAdmin, http.MethodGet, clusterPath + "/teams", ""},
		"admin token rotation":         {callAdmin, http.MethodPut, clusterPath + "/token", `{"token":"token"}`},
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
		"admin get":            {callAdmin, http.MethodGet, clusterPath, "", http.StatusNotFound},
		"admin metadata":       {callAdmin, http.MethodGet, clusterPath + "/metadata", "", http.StatusNotFound},
		"admin teams":          {callAdmin, http.MethodGet, clusterPath + "/teams", "", http.StatusNotFound},
		"admin update":         {callAdmin, http.MethodPatch, clusterPath, `{"name":"renamed"}`, http.StatusNotFound},
		"admin token rotation": {callAdmin, http.MethodPut, clusterPath + "/token", `{"token":"token"}`, http.StatusNotFound},
		"management telemetry": {
			callManagement, http.MethodPut, managementPath + "/telemetry", `{"telemetry_client_id":"client_test"}`, http.StatusNotFound,
		},
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

// createdCluster is the cluster the cluster service stores for request.
func createdCluster(clusterID uuid.UUID, request api.AdminClusterCreateRequest) *clustersgrpc.Cluster {
	return clustersgrpc.Cluster_builder{
		ClusterId:          clusterID.String(),
		Name:               request.Name,
		Endpoint:           request.Endpoint,
		EndpointTls:        request.EndpointTls,
		Token:              request.Token,
		SandboxProxyDomain: request.SandboxProxyDomain,
		AuthOrgId:          request.AuthOrgId,
		DeletionProtection: true,
		Etag:               "etag-created",
	}.Build()
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
	recordsOf(db).put(clustersgrpc.Cluster_builder{
		ClusterId:          clusterID.String(),
		Name:               name,
		Endpoint:           endpoint,
		EndpointTls:        true,
		Token:              "token",
		SandboxProxyDomain: sandboxProxyDomain,
	}.Build())

	return clusterID
}

func assignTeam(t *testing.T, db *testutils.Database, clusterID uuid.UUID) uuid.UUID {
	t.Helper()

	teamID := testutils.CreateTestTeam(t, db)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET cluster_id = $1 WHERE id = $2`, clusterID, teamID))

	return teamID
}
