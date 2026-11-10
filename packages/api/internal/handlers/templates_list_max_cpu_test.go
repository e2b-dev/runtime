package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

func TestTemplateListsMaxCpuCount(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)

	resizableID := testutils.CreateTestTemplate(t, db, teamID)
	resizableBuild := testutils.CreateTestBuild(t, t.Context(), db, resizableID, "ready")
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		"UPDATE public.env_builds SET max_vcpu = 4 WHERE id = $1", resizableBuild))

	testutils.CreateTestBuildAssignment(t, t.Context(), db, resizableID, resizableBuild, "default")

	fixedID := testutils.CreateTestTemplate(t, db, teamID)
	fixedBuild := testutils.CreateTestBuild(t, t.Context(), db, fixedID, "ready")
	testutils.CreateTestBuildAssignment(t, t.Context(), db, fixedID, fixedBuild, "default")

	store := newTemplateListStore(t, db, featureflags.UnpaginatedTemplateListCapFlag.Fallback())

	for endpoint, call := range map[string]func(*gin.Context){
		"v1":     func(c *gin.Context) { store.GetTemplates(c, api.GetTemplatesParams{}) },
		"v2":     func(c *gin.Context) { store.GetV2Templates(c, api.GetV2TemplatesParams{}) },
		"search": func(c *gin.Context) { store.GetUnstableTemplatesSearch(c, api.GetUnstableTemplatesSearchParams{}) },
	} {
		t.Run(endpoint, func(t *testing.T) {
			t.Parallel()

			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/templates", nil)
			auth.SetTeamInfoForTest(t, c, &types.Team{Team: &dbtypes.Team{ID: teamID}})

			call(c)
			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

			var templates []map[string]json.RawMessage
			if endpoint == "search" {
				var page struct {
					Data []map[string]json.RawMessage `json:"data"`
				}
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &page))
				templates = page.Data
			} else {
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &templates))
			}

			got := map[string]map[string]json.RawMessage{}
			for _, tmpl := range templates {
				var templateID string
				require.NoError(t, json.Unmarshal(tmpl["templateID"], &templateID))
				got[templateID] = tmpl
			}

			require.Contains(t, got, resizableID)
			var capacity int32
			require.NoError(t, json.Unmarshal(got[resizableID]["maxCpuCount"], &capacity))
			require.Equal(t, int32(4), capacity)
			require.Contains(t, got, fixedID)
			require.NotContains(t, got[fixedID], "maxCpuCount", "fixed-size templates must omit capacity rather than serialize null")
		})
	}

	t.Run("detail", func(t *testing.T) {
		t.Parallel()

		for _, templateID := range []string{resizableID, fixedID} {
			response := serveTemplateGet(t, db, teamID, templateID)
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			var detail map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &detail))
			var builds []map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(detail["builds"], &builds))
			require.Len(t, builds, 1)
			for _, object := range []map[string]json.RawMessage{detail, builds[0]} {
				if templateID == fixedID {
					require.NotContains(t, object, "maxCpuCount")
				} else {
					var capacity int32
					require.NoError(t, json.Unmarshal(object["maxCpuCount"], &capacity))
					require.Equal(t, int32(4), capacity)
				}
			}
		}
	})
}
