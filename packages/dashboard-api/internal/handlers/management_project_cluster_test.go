package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestManagementClusterLifecycleReplaysAndEndsAbsent(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := createClusterAssignmentTestTeam(t, db)
	clusterID := uuid.New()
	store := &APIStore{db: db.SqlcClient, authService: &recordingCacheAuthService{}}
	registration := managementClusterRegistration()

	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, registration).Code)
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, registration).Code)
	require.Equal(t, http.StatusNoContent, callManagementAssignProjectCluster(t, store, projectID, clusterID).Code)
	require.Equal(t, http.StatusNoContent, callManagementAssignProjectCluster(t, store, projectID, clusterID).Code)
	require.Equal(t, http.StatusNoContent, callManagementDetachProjectCluster(t, store, projectID, clusterID).Code)
	require.Equal(t, http.StatusNoContent, callManagementDetachProjectCluster(t, store, projectID, clusterID).Code)
	require.Equal(t, http.StatusNoContent, callManagementDeleteCluster(t, store, clusterID).Code)
	require.Equal(t, http.StatusNoContent, callManagementDeleteCluster(t, store, clusterID).Code)

	var exists bool
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		`SELECT EXISTS (SELECT FROM public.clusters WHERE id = $1)`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&exists)
		}, clusterID))
	require.False(t, exists)
}

func TestManagementRegisterClusterRejectsDescriptorChange(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	clusterID := uuid.New()
	store := &APIStore{db: db.SqlcClient}
	registration := managementClusterRegistration()

	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, registration).Code)
	registration.Endpoint = "changed.example.test:443"
	response := callManagementRegisterCluster(t, store, clusterID, registration)
	require.Equal(t, http.StatusConflict, response.Code)
	requireClusterErrorCode(t, response, api.ClusterRegistrationConflict)
}

func TestManagementRegisterClusterAcceptsTheClusterDescriptor(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	registration := managementClusterRegistration()

	require.Equal(t, http.StatusNoContent,
		callManagementRegisterCluster(t, &APIStore{db: db.SqlcClient}, uuid.New(), registration).Code)
}

func TestManagementRegisterClusterRejectsReservedClusterID(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	response := callManagementRegisterCluster(
		t,
		&APIStore{db: db.SqlcClient},
		uuid.Nil,
		managementClusterRegistration(),
	)

	require.Equal(t, http.StatusBadRequest, response.Code)
	requireClusterErrorCode(t, response, api.ClusterRegistrationInvalid)
}

func TestManagementAssignProjectClusterRejectsReservedClusterID(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := createClusterAssignmentTestTeam(t, db)
	response := callManagementAssignProjectCluster(
		t,
		&APIStore{db: db.SqlcClient},
		projectID,
		uuid.Nil,
	)

	require.Equal(t, http.StatusBadRequest, response.Code)
	requireClusterErrorCode(t, response, api.ClusterAssignmentInvalid)
}

func TestManagementAssignProjectClusterRejectsDifferentAssignment(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := createClusterAssignmentTestTeam(t, db)
	firstID := uuid.New()
	secondID := uuid.New()
	authService := &recordingCacheAuthService{}
	store := &APIStore{db: db.SqlcClient, authService: authService}
	first := managementClusterRegistration()
	second := managementClusterRegistration()
	second.AuthOrgId = new("org_other")

	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, firstID, first).Code)
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, secondID, second).Code)
	require.Equal(t, http.StatusNoContent, callManagementAssignProjectCluster(t, store, projectID, firstID).Code)
	response := callManagementAssignProjectCluster(t, store, projectID, secondID)
	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	requireClusterErrorCode(t, response, api.ClusterAssignmentAlreadyAssigned)
	require.Equal(t, []uuid.UUID{projectID}, authService.invalidated)
}

func TestManagementAssignProjectClusterRejectsNonEnterpriseProjectWithoutMutation(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := createClusterAssignmentTestTeam(t, db)
	clusterID := uuid.New()
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.teams SET tier = 'cluster_assignment_test' WHERE id = $1`,
		projectID,
	))

	authService := &recordingCacheAuthService{}
	store := &APIStore{db: db.SqlcClient, authService: authService}
	require.Equal(t, http.StatusNoContent,
		callManagementRegisterCluster(t, store, clusterID, managementClusterRegistration()).Code)

	response := callManagementAssignProjectCluster(t, store, projectID, clusterID)
	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	requireClusterErrorCode(t, response, api.ClusterAssignmentRequiresEnterprise)
	require.Empty(t, authService.invalidated)

	var assignedClusterID *uuid.UUID
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		`SELECT cluster_id FROM public.teams WHERE id = $1`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&assignedClusterID)
		},
		projectID,
	))
	require.Nil(t, assignedClusterID)
}

func TestManagementAssignProjectClusterReplaysAfterDowngradeButRejectsReplacement(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := createClusterAssignmentTestTeam(t, db)
	clusterID := uuid.New()
	replacementClusterID := uuid.New()
	authService := &recordingCacheAuthService{}
	store := &APIStore{db: db.SqlcClient, authService: authService}
	registration := managementClusterRegistration()
	replacementRegistration := managementClusterRegistration()
	replacementRegistration.AuthOrgId = new("org_replacement")

	require.Equal(t, http.StatusNoContent,
		callManagementRegisterCluster(t, store, clusterID, registration).Code)
	require.Equal(t, http.StatusNoContent,
		callManagementRegisterCluster(t, store, replacementClusterID, replacementRegistration).Code)
	require.Equal(t, http.StatusNoContent,
		callManagementAssignProjectCluster(t, store, projectID, clusterID).Code)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.teams SET tier = 'cluster_assignment_test' WHERE id = $1`,
		projectID,
	))

	replayed := callManagementAssignProjectCluster(t, store, projectID, clusterID)
	require.Equal(t, http.StatusNoContent, replayed.Code, replayed.Body.String())
	replacement := callManagementAssignProjectCluster(t, store, projectID, replacementClusterID)
	require.Equal(t, http.StatusConflict, replacement.Code, replacement.Body.String())
	require.Contains(t, replacement.Body.String(), enterpriseClusterAssignmentPolicyMessage)
	require.Equal(t, []uuid.UUID{projectID, projectID}, authService.invalidated)

	var assignedClusterID uuid.UUID
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		`SELECT cluster_id FROM public.teams WHERE id = $1`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&assignedClusterID)
		},
		projectID,
	))
	require.Equal(t, clusterID, assignedClusterID)
}

func managementClusterRegistration() api.ManagementClusterRegistrationRequest {
	return api.ManagementClusterRegistrationRequest{
		Name:               "Managed cluster",
		Endpoint:           "api.example.test:443",
		EndpointTls:        true,
		Token:              "cluster-token",
		SandboxProxyDomain: new("sandbox.example.test"),
		AuthOrgId:          new("org_example"),
	}
}

func callManagementRegisterCluster(
	t *testing.T,
	store *APIStore,
	clusterID uuid.UUID,
	registration api.ManagementClusterRegistrationRequest,
) *httptest.ResponseRecorder {
	t.Helper()

	body, err := json.Marshal(registration)
	require.NoError(t, err)

	return callManagementClusterHandler(t, http.MethodPut, "/v1/management/clusters/"+clusterID.String(), body,
		func(c *gin.Context) { store.ManagementRegisterCluster(c, clusterID) })
}

func callManagementDeleteCluster(t *testing.T, store *APIStore, clusterID uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()

	return callManagementClusterHandler(t, http.MethodDelete, "/v1/management/clusters/"+clusterID.String(), nil,
		func(c *gin.Context) { store.ManagementDeleteCluster(c, clusterID) })
}

func callManagementAssignProjectCluster(
	t *testing.T,
	store *APIStore,
	projectID uuid.UUID,
	clusterID uuid.UUID,
) *httptest.ResponseRecorder {
	t.Helper()

	return callManagementClusterHandler(t, http.MethodPut,
		"/v1/management/projects/"+projectID.String()+"/cluster/"+clusterID.String(), nil,
		func(c *gin.Context) { store.ManagementAssignProjectCluster(c, projectID, clusterID) })
}

func callManagementDetachProjectCluster(
	t *testing.T,
	store *APIStore,
	projectID uuid.UUID,
	clusterID uuid.UUID,
) *httptest.ResponseRecorder {
	t.Helper()

	return callManagementClusterHandler(t, http.MethodDelete,
		"/v1/management/projects/"+projectID.String()+"/cluster/"+clusterID.String(), nil,
		func(c *gin.Context) { store.ManagementDetachProjectCluster(c, projectID, clusterID) })
}

func callManagementClusterHandler(
	t *testing.T,
	method string,
	path string,
	body []byte,
	handler func(*gin.Context),
) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Request = httptest.NewRequestWithContext(t.Context(), method, path, bytes.NewReader(body))
	ginContext.Request.Header.Set("Content-Type", "application/json")
	handler(ginContext)
	ginContext.Writer.WriteHeaderNow()

	return recorder
}

func requireClusterErrorCode(t *testing.T, response *httptest.ResponseRecorder, code api.ErrorCode) {
	t.Helper()
	var body api.Error
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.NotNil(t, body.ErrorCode)
	require.Equal(t, code, *body.ErrorCode)
	require.EqualValues(t, response.Code, body.Code)
}

func TestManagementAssignmentMissingResourcesHaveStableCodes(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	projectID := createClusterAssignmentTestTeam(t, db)
	store := &APIStore{db: db.SqlcClient}
	for _, test := range []struct {
		name      string
		projectID uuid.UUID
		code      api.ErrorCode
	}{
		{"project", uuid.New(), api.ClusterAssignmentProjectNotFound},
		{"cluster", projectID, api.ClusterAssignmentClusterNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			response := callManagementAssignProjectCluster(t, store, test.projectID, uuid.New())
			require.Equal(t, http.StatusNotFound, response.Code)
			requireClusterErrorCode(t, response, test.code)
		})
	}
}
