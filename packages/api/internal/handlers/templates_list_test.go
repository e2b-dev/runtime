package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	analyticscollector "github.com/e2b-dev/infra/packages/api/internal/analytics_collector"
	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
)

// The CLI lists templates with an API key and the configured project as the
// teamID query, so the public project ID shown in the dashboard must pass the
// key's team check.
func TestTemplateListsAcceptPublicProjectID(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)

	posthogClient, err := analyticscollector.NewPosthogClient(t.Context(), "")
	require.NoError(t, err)
	store := &APIStore{sqlcDB: db.SqlcClient, posthog: posthogClient}

	for _, tc := range []struct {
		name     string
		teamID   string
		wantCode int
	}{
		{name: "team UUID", teamID: teamID.String(), wantCode: http.StatusOK},
		{name: "public project ID", teamID: id.ProjectID(teamID).String(), wantCode: http.StatusOK},
		{name: "another project's public ID", teamID: id.ProjectID(uuid.New()).String(), wantCode: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			for endpoint, call := range map[string]func(*gin.Context){
				"v1": func(c *gin.Context) { store.GetTemplates(c, api.GetTemplatesParams{TeamID: &tc.teamID}) },
				"v2": func(c *gin.Context) { store.GetV2Templates(c, api.GetV2TemplatesParams{TeamID: &tc.teamID}) },
			} {
				t.Run(endpoint, func(t *testing.T) {
					t.Parallel()

					recorder := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(recorder)
					c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/templates", nil)
					auth.SetTeamInfoForTest(t, c, &types.Team{Team: &authqueries.Team{ID: teamID}})

					call(c)

					require.Equal(t, tc.wantCode, recorder.Code, recorder.Body.String())
				})
			}
		})
	}
}
