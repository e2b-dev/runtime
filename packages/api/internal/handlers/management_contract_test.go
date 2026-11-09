package handlers

import (
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
)

// The management writes create, rename and delete projects, so a spec edit
// that admits another scheme on any of them, such as the static admin token,
// would hand those writes to it without failing anything else.
func TestEveryManagementOperationAcceptsOnlyTheServiceJWT(t *testing.T) {
	t.Parallel()

	swagger, err := api.GetSwagger()
	require.NoError(t, err)

	operations := 0
	for path, item := range swagger.Paths.Map() {
		if !strings.HasPrefix(path, "/unstable/management/") || path == "/unstable/management/telemetry/names" {
			continue
		}
		for method, operation := range item.Operations() {
			operations++
			require.NotNil(t, operation.Security, "%s %s", method, path)
			require.Equal(t, openapi3.SecurityRequirements{{"AdminJWTAuth": {}}}, *operation.Security, "%s %s", method, path)
		}
	}
	require.NotZero(t, operations, "no operation matched the management prefix")
}

// A cluster registered through the management API starts unprotected, so the
// registration must not grow a field that sets protection or anything else
// beyond the descriptor.
func TestManagementClusterRegistrationContainsOnlyClusterDescriptorFields(t *testing.T) {
	t.Parallel()

	swagger, err := api.GetSwagger()
	require.NoError(t, err)
	schema := swagger.Components.Schemas["ManagementClusterRegistrationRequest"]
	require.NotNil(t, schema)

	require.ElementsMatch(t,
		[]string{"name", "endpoint", "endpoint_tls", "token", "sandbox_proxy_domain", "auth_org_id", "telemetry_client_id"},
		slices.Collect(maps.Keys(schema.Value.Properties)))
}

// The caller reads a 404 from the readiness check as a server without the
// route, so the check itself never answers 404.
func TestClusterDestroyReadinessNeverAnswersNotFound(t *testing.T) {
	t.Parallel()

	swagger, err := api.GetSwagger()
	require.NoError(t, err)
	path := swagger.Paths.Value("/unstable/management/clusters/{clusterID}/destroy-readiness")
	require.NotNil(t, path)
	require.NotNil(t, path.Get)
	require.Nil(t, path.Get.Responses.Status(http.StatusNotFound))
}
