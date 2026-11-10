package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	analyticscollector "github.com/e2b-dev/infra/packages/api/internal/analytics_collector"
	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
)

func newTemplateListStore(t *testing.T, db *testutils.Database, capped bool) *APIStore {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.UnpaginatedTemplateListCapFlag.Key()).VariationForAll(capped))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	posthogClient, err := analyticscollector.NewPosthogClient(t.Context(), "")
	require.NoError(t, err)

	return &APIStore{sqlcDB: db.SqlcClient, posthog: posthogClient, featureFlags: ff}
}

// The CLI lists templates with an API key and the configured project as the
// teamID query, so the public project ID shown in the dashboard must pass the
// key's team check.
func TestTemplateListsAcceptPublicProjectID(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store := newTemplateListStore(t, db, featureflags.UnpaginatedTemplateListCapFlag.Fallback())

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
					auth.SetTeamInfoForTest(t, c, &types.Team{Team: &dbtypes.Team{ID: teamID}})

					call(c)

					require.Equal(t, tc.wantCode, recorder.Code, recorder.Body.String())
				})
			}
		})
	}
}

func TestGetTemplatesCap(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		capped    bool
		templates int
		wantCode  int
	}{
		{name: "at the cap", capped: true, templates: templatesUnboundedMaxCount, wantCode: http.StatusOK},
		{name: "above the cap", capped: true, templates: templatesUnboundedMaxCount + 1, wantCode: http.StatusBadRequest},
		{name: "cap off", capped: false, templates: templatesUnboundedMaxCount + 1, wantCode: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := testutils.SetupDatabase(t)
			teamID := testutils.CreateTestTeam(t, db)
			require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
				`INSERT INTO public.envs (id, team_id, public, updated_at, source)
				SELECT 'template-' || g, $1, false, NOW(), 'template' FROM generate_series(1, $2::int) AS g`,
				teamID, tc.templates,
			))
			store := newTemplateListStore(t, db, tc.capped)

			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/templates", nil)
			auth.SetTeamInfoForTest(t, c, &types.Team{Team: &dbtypes.Team{ID: teamID}})

			store.GetTemplates(c, api.GetTemplatesParams{})

			require.Equal(t, tc.wantCode, recorder.Code, recorder.Body.String())
			if tc.wantCode == http.StatusOK {
				var templates []api.Template
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &templates))
				assert.Len(t, templates, tc.templates)
			}
		})
	}
}
