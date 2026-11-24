package handlers

import (
	"net/http"
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
