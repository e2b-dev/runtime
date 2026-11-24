package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	dbtypes "github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

// An admin request on a banned team may read and delete, but reach nothing
// that writes or starts a workload, nor a read that hands out a write.
func TestEnforceBannedTeamAllowsOnlyReadsAndDeletes(t *testing.T) {
	t.Parallel()

	banned := &types.Team{Team: &dbtypes.Team{ID: uuid.New(), IsBanned: true}}
	active := &types.Team{Team: &dbtypes.Team{ID: uuid.New()}}

	for name, tc := range map[string]struct {
		team    *types.Team
		method  string
		route   string
		allowed bool
	}{
		"banned team lists sandboxes":          {team: banned, method: http.MethodGet, route: "/sandboxes", allowed: true},
		"banned team reads build logs":         {team: banned, method: http.MethodGet, route: "/templates/:templateID/builds/:buildID/logs", allowed: true},
		"banned team kills a sandbox":          {team: banned, method: http.MethodDelete, route: "/sandboxes/:sandboxID", allowed: true},
		"banned team deletes a secret":         {team: banned, method: http.MethodDelete, route: "/secrets/:secretID", allowed: true},
		"banned team creates a sandbox":        {team: banned, method: http.MethodPost, route: "/sandboxes", allowed: false},
		"banned team resumes a sandbox":        {team: banned, method: http.MethodPost, route: "/sandboxes/:sandboxID/resume", allowed: false},
		"banned team updates a template":       {team: banned, method: http.MethodPatch, route: "/templates/:templateID", allowed: false},
		"banned team gets a build upload link": {team: banned, method: http.MethodGet, route: "/templates/:templateID/files/:hash", allowed: false},
		"active team creates a sandbox":        {team: active, method: http.MethodPost, route: "/sandboxes", allowed: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			r := gin.New()
			r.Use(func(c *gin.Context) { auth.SetTeamInfoForTest(t, c, tc.team) }, EnforceBannedTeam())
			r.Handle(tc.method, tc.route, func(c *gin.Context) { c.Status(http.StatusOK) })

			path := strings.NewReplacer(":templateID", "tpl", ":buildID", "bld", ":sandboxID", "sbx", ":secretID", "sec", ":hash", "abc").Replace(tc.route)
			recorder := httptest.NewRecorder()
			r.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), tc.method, path, nil))

			want := http.StatusForbidden
			if tc.allowed {
				want = http.StatusOK
			}
			assert.Equal(t, want, recorder.Code, recorder.Body.String())
		})
	}
}
