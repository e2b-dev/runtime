package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
)

func TestProjectBlockContractAcceptsCombinedHoldReasons(t *testing.T) {
	t.Parallel()

	swagger, err := api.GetSwagger()
	require.NoError(t, err)
	schema := swagger.Components.Schemas["ManagementProjectBlockRequest"].Value
	require.NoError(t, schema.VisitJSON(map[string]any{
		"revision": float64(1),
		"blocked":  true,
		"reason":   strings.Repeat("hold_reason, ", 64),
	}))
}

func TestProjectMemberApplyRequestMatchesTheProjectionShape(t *testing.T) {
	t.Parallel()

	body := `{"revision":4,"present":true,"is_default":true,"identities":[{"issuer":"https://issuer.test","subject":"subject"}]}`
	isDefault := true

	var decoded api.ManagementProjectMemberApplyRequest
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("decoding the payload callers send: %v", err)
	}

	want := api.ManagementProjectMemberApplyRequest{
		Revision:  4,
		Present:   true,
		IsDefault: &isDefault,
		Identities: &[]api.ManagementProjectMemberIdentity{{
			Issuer:  "https://issuer.test",
			Subject: "subject",
		}},
	}
	if decoded.Revision != want.Revision || decoded.Present != want.Present || decoded.IsDefault == nil || *decoded.IsDefault != *want.IsDefault || decoded.Identities == nil || (*decoded.Identities)[0] != (*want.Identities)[0] {
		t.Errorf("decoded %+v, want %+v", decoded, want)
	}
}

func TestProjectUpsertIgnoresARetiredProjectType(t *testing.T) {
	t.Parallel()

	body := `{"name":"Acme","slug":"acme","email":"ops@acme.test","project_type":"enterprise_v3"}`

	var decoded api.ManagementProjectUpsertRequest
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("decoding an upsert that still carries project_type: %v", err)
	}

	want := api.ManagementProjectUpsertRequest{Name: "Acme", Slug: "acme", Email: "ops@acme.test"}
	if decoded != want {
		t.Errorf("decoded %+v, want %+v", decoded, want)
	}
}

func TestManagementClusterRegistrationContainsOnlyClusterDescriptorFields(t *testing.T) {
	t.Parallel()

	swagger, err := api.GetSwagger()
	if err != nil {
		t.Fatalf("loading Dashboard API contract: %v", err)
	}

	schema := swagger.Components.Schemas["ManagementClusterRegistrationRequest"]
	if schema == nil || schema.Value == nil {
		t.Fatal("ManagementClusterRegistrationRequest schema is missing")
	}
	want := map[string]struct{}{
		"name": {}, "endpoint": {}, "endpoint_tls": {}, "token": {},
		"sandbox_proxy_domain": {}, "auth_org_id": {},
	}
	for name := range schema.Value.Properties {
		if _, ok := want[name]; !ok {
			t.Fatalf("management cluster registration exposes unexpected field %q", name)
		}
		delete(want, name)
	}
	if len(want) != 0 {
		t.Fatalf("management cluster registration is missing fields: %v", want)
	}
}

func TestEveryManagementRouteReachesItsHandler(t *testing.T) {
	t.Parallel()

	projectID := uuid.New().String()
	userID := uuid.New().String()
	clusterID := uuid.New().String()
	project := "/v1/management/projects/" + projectID
	cluster := "/v1/management/clusters/" + clusterID
	projectCluster := project + "/cluster/" + clusterID

	for _, tt := range []struct {
		operation string
		method    string
		path      string
		body      string
	}{
		{"upsertProject", http.MethodPut, project, `{"name":"a","slug":"a","project_type":"base_v1"}`},
		{"deleteProject", http.MethodDelete, project, ""},
		{"applyProjectMember", http.MethodPut, project + "/members/" + userID, `{"revision":1,"present":false}`},
		{"upsertLimits", http.MethodPut, project + "/limits", `{}`},
		{"applyProjectBlock", http.MethodPut, project + "/block", `{"revision":1,"blocked":false}`},
		{"registerCluster", http.MethodPut, cluster, `{"name":"a","endpoint":"a:443","endpoint_tls":true,"token":"token"}`},
		{"deleteCluster", http.MethodDelete, cluster, ""},
		{"assignProjectCluster", http.MethodPut, projectCluster, ""},
		{"detachProjectCluster", http.MethodDelete, projectCluster, ""},
	} {
		t.Run(tt.operation, func(t *testing.T) {
			t.Parallel()

			reached := make(chan string, 1)
			router := gin.New()
			api.RegisterHandlers(router, &routeRecorder{reached: reached})

			recorder := httptest.NewRecorder()
			request := httptest.NewRequestWithContext(t.Context(), tt.method, tt.path, strings.NewReader(tt.body))
			request.Header.Set("Content-Type", "application/json")

			router.ServeHTTP(recorder, request)

			select {
			case got := <-reached:
				if got != tt.operation {
					t.Fatalf("request reached %q, want %q", got, tt.operation)
				}
			default:
				t.Fatalf("no handler was reached; status %d", recorder.Code)
			}
		})
	}
}

type routeRecorder struct {
	api.ServerInterface

	reached chan string
}

func (r *routeRecorder) report(c *gin.Context, operation string) {
	r.reached <- operation
	c.Status(http.StatusNoContent)
}

func (r *routeRecorder) ManagementUpsertProject(c *gin.Context, _ api.ProjectID) {
	r.report(c, "upsertProject")
}

func (r *routeRecorder) ManagementDeleteProject(c *gin.Context, _ api.ProjectID) {
	r.report(c, "deleteProject")
}

func (r *routeRecorder) ManagementApplyProjectMember(c *gin.Context, _ api.ProjectID, _ api.UserID) {
	r.report(c, "applyProjectMember")
}

func (r *routeRecorder) ManagementUpsertProjectLimits(c *gin.Context, _ api.ProjectID) {
	r.report(c, "upsertLimits")
}

func (r *routeRecorder) ManagementApplyProjectBlock(c *gin.Context, _ api.ProjectID) {
	r.report(c, "applyProjectBlock")
}

func (r *routeRecorder) ManagementRegisterCluster(c *gin.Context, _ api.ClusterID) {
	r.report(c, "registerCluster")
}

func (r *routeRecorder) ManagementDeleteCluster(c *gin.Context, _ api.ClusterID) {
	r.report(c, "deleteCluster")
}

func (r *routeRecorder) ManagementAssignProjectCluster(c *gin.Context, _ api.ProjectID, _ api.ClusterID) {
	r.report(c, "assignProjectCluster")
}

func (r *routeRecorder) ManagementDetachProjectCluster(c *gin.Context, _ api.ProjectID, _ api.ClusterID) {
	r.report(c, "detachProjectCluster")
}

// Zero disables the list rate limit, so a sender that leaves the rate out must be
// refused by the request validator rather than decoded as zero.
func TestProjectLimitsContractRequiresTheListRate(t *testing.T) {
	t.Parallel()

	swagger, err := api.GetSwagger()
	require.NoError(t, err)
	schema := swagger.Components.Schemas["ManagementProjectLimits"].Value

	var body map[string]any
	require.NoError(t, json.Unmarshal([]byte(validLimitsBody), &body))
	require.NoError(t, schema.VisitJSON(body))

	delete(body, "api_team_rps_list")
	require.ErrorContains(t, schema.VisitJSON(body), "api_team_rps_list")
}
