package middleware

import (
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
)

// The rule matches on route shape, so a renamed path parameter or a new write
// route changes what it guards without failing anything else. Pinning the
// guarded set against the generated router makes either visible.
func TestRefuseDeletedProjectGuardsExactlyTheProjectWrites(t *testing.T) {
	t.Parallel()

	router := gin.New()
	api.RegisterHandlers(router, api.ServerInterface(nil))
	guarded := map[string]struct{}{}
	for _, route := range router.Routes() {
		if guardsDeletedProject(route.Method, route.Path) {
			guarded[route.Method+" "+route.Path] = struct{}{}
		}
	}

	require.Equal(t, map[string]struct{}{
		"PUT /unstable/management/projects/:projectID":                       {},
		"PUT /unstable/management/projects/:projectID/members/:userID":       {},
		"PUT /unstable/management/projects/:projectID/limits":                {},
		"PUT /unstable/management/projects/:projectID/settings":              {},
		"PUT /unstable/management/projects/:projectID/block":                 {},
		"PUT /unstable/management/projects/:projectID/ban":                   {},
		"PUT /unstable/management/projects/:projectID/cluster/:clusterID":    {},
		"DELETE /unstable/management/projects/:projectID/cluster/:clusterID": {},
	}, guarded)
	require.False(t, guardsDeletedProject(http.MethodDelete, "/unstable/management/projects/:projectID"),
		"a retried deletion must reach its handler")
}
