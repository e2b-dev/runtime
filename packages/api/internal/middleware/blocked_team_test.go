package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
)

// A blocked team keeps read access to its own resources and loses mutations.
// The secrets routes follow the same read-parity rule as every other per-team
// resource: metadata reads stay reachable, create/update/delete do not.
func TestBlockedTeamAllowlistKeepsSecretsReadsAndDeniesMutations(t *testing.T) {
	t.Parallel()

	get := blockedTeamAllowlist[http.MethodGet]
	assert.Contains(t, get, "/secrets")
	assert.Contains(t, get, "/secrets/:secretID")

	for _, method := range []string{http.MethodPost, http.MethodDelete} {
		for route := range blockedTeamAllowlist[method] {
			assert.NotContains(t, []string{"/secrets", "/secrets/:secretID"}, route,
				"secrets mutations must not be allowlisted for blocked teams")
		}
	}
}

func TestBlockedTeamStillReadsItsStatusAndLimits(t *testing.T) {
	t.Parallel()

	reason := "payment required"
	blocked := &types.Team{Team: &dbtypes.Team{ID: uuid.New(), IsBlocked: true, BlockedReason: &reason}}

	for name, route := range map[string]string{
		"status": "/unstable/teams/:teamID/status",
		"limits": "/unstable/teams/:teamID/limits",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			r := gin.New()
			r.Use(func(c *gin.Context) { auth.SetTeamInfoForTest(t, c, blocked) }, EnforceBlockedTeam())
			r.GET(route, func(c *gin.Context) { c.Status(http.StatusOK) })

			recorder := httptest.NewRecorder()
			r.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, strings.Replace(route, ":teamID", blocked.TeamID(), 1), nil))

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		})
	}
}

// A blocked team can still read its builds, as it can read its templates.
func TestBlockedTeamStillReadsItsBuilds(t *testing.T) {
	t.Parallel()

	reason := "payment required"
	blocked := &types.Team{Team: &dbtypes.Team{ID: uuid.New(), IsBlocked: true, BlockedReason: &reason}}

	for name, tc := range map[string]struct {
		route string
		path  string
	}{
		"list":     {route: "/unstable/builds", path: "/unstable/builds"},
		"statuses": {route: "/unstable/builds/statuses", path: "/unstable/builds/statuses?build_ids=" + uuid.NewString()},
		"detail":   {route: "/unstable/builds/:buildID", path: "/unstable/builds/" + uuid.NewString()},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			r := gin.New()
			r.Use(func(c *gin.Context) { auth.SetTeamInfoForTest(t, c, blocked) }, EnforceBlockedTeam())
			r.GET(tc.route, func(c *gin.Context) { c.Status(http.StatusOK) })

			recorder := httptest.NewRecorder()
			r.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, tc.path, nil))

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		})
	}
}

// A blocked team can still read its templates' tags, the default templates and
// the template search, as it can read the templates themselves.
func TestBlockedTeamAllowlistKeepsTemplateTagReads(t *testing.T) {
	t.Parallel()

	get := blockedTeamAllowlist[http.MethodGet]
	for _, route := range []string{
		"/default-templates",
		"/unstable/templates/search",
		"/templates/:templateID/tags",
		"/templates/:templateID/tags/count",
		"/templates/:templateID/tags/exists",
		"/templates/:templateID/tags/groups",
		"/templates/:templateID/tags/:tag/assignments",
	} {
		assert.Contains(t, get, route)
	}
}
