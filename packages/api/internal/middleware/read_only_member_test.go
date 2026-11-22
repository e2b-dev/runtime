package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	dbtypes "github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

// The denied reads match on route shape, so a renamed path parameter would
// reopen the route to viewers without failing anything else.
func TestReadOnlyDeniedReadsNameRegisteredRoutes(t *testing.T) {
	t.Parallel()

	router := gin.New()
	api.RegisterHandlers(router, api.ServerInterface(nil))
	registered := map[string]struct{}{}
	for _, route := range router.Routes() {
		registered[route.Method+" "+route.Path] = struct{}{}
	}

	for method, routes := range readOnlyDeniedReads {
		for route := range routes {
			require.Contains(t, registered, method+" "+route)
		}
	}
}

// A handler that finds the team itself, after the middleware has run, must
// refuse a viewer the same writes.
func TestCheckTeamAccessForRouteRefusesAViewersWrite(t *testing.T) {
	t.Parallel()

	viewer := &types.Team{Team: &dbtypes.Team{ID: uuid.New()}, Role: types.TeamRoleViewer}

	check := func(method string) error {
		var err error
		r := gin.New()
		r.Handle(method, "/sandboxes", func(c *gin.Context) { err = CheckTeamAccessForRoute(c, viewer) })
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), method, "/sandboxes", nil))

		return err
	}

	require.NoError(t, check(http.MethodGet))
	var forbidden *auth.TeamForbiddenError
	require.ErrorAs(t, check(http.MethodPost), &forbidden)
}
