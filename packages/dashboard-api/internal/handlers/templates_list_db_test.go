package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func markEnvDefault(t *testing.T, db *testutils.Database, envID string, description *string) {
	t.Helper()
	err := db.SqlcClient.TestsRawSQL(t.Context(),
		`INSERT INTO public.env_defaults (env_id, description) VALUES ($1, $2)`,
		envID, description)
	require.NoError(t, err)
}

func newTemplatesTestStore(db *testutils.Database) *APIStore {
	return &APIStore{db: db.SqlcClient}
}

func teamInfo(teamID uuid.UUID, clusterID *uuid.UUID) *authtypes.Team {
	return &authtypes.Team{Team: &authqueries.Team{ID: teamID, ClusterID: clusterID}}
}

func callGetTemplates(t *testing.T, store *APIStore, team *authtypes.Team, params api.GetTemplatesParams) (int, api.TeamTemplatesResponse) {
	t.Helper()

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/templates", nil)
	auth.SetTeamInfoForTest(t, ctx, team)

	store.GetTemplates(ctx, params)

	var resp api.TeamTemplatesResponse
	if recorder.Code == http.StatusOK {
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	}

	return recorder.Code, resp
}

func templateIDSet(resp api.TeamTemplatesResponse) map[string]bool {
	out := make(map[string]bool, len(resp.Data))
	for _, tmpl := range resp.Data {
		out[tmpl.TemplateID] = true
	}

	return out
}

func TestGetTemplates_ListAndKeysetPaginate(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)

	teamID := testutils.CreateTestTeam(t, testDB)
	want := map[string]bool{}
	for range 3 {
		want[testutils.CreateTestTemplate(t, testDB, teamID)] = true
	}

	store := newTemplatesTestStore(testDB)
	team := teamInfo(teamID, nil)

	limit := int32(2)
	got := map[string]bool{}
	pages := 0
	var cursor *string
	for {
		pages++
		status, resp := callGetTemplates(t, store, team, api.GetTemplatesParams{
			Limit:  &limit,
			Cursor: cursor,
		})
		require.Equal(t, http.StatusOK, status)
		require.LessOrEqual(t, len(resp.Data), int(limit))

		for id := range templateIDSet(resp) {
			require.False(t, got[id], "template %s returned on more than one page", id)
			got[id] = true
		}

		if resp.NextCursor == nil {
			break
		}
		cursor = resp.NextCursor
		require.LessOrEqual(t, pages, 5, "pagination did not terminate")
	}

	assert.Equal(t, want, got, "all templates should be returned exactly once across pages")
	assert.Equal(t, 2, pages, "3 templates at limit 2 should be 2 pages")
}

func TestGetTemplates_DefaultGatingByCluster(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)

	teamID := testutils.CreateTestTeam(t, testDB)
	owned := testutils.CreateTestTemplate(t, testDB, teamID)

	// A default template owned by a different team, exposed via env_defaults.
	otherTeamID := testutils.CreateTestTeam(t, testDB)
	defaultEnv := testutils.CreateTestTemplate(t, testDB, otherTeamID)
	markEnvDefault(t, testDB, defaultEnv, nil)

	store := newTemplatesTestStore(testDB)

	// No cluster -> defaults are included inline.
	_, resp := callGetTemplates(t, store, teamInfo(teamID, nil), api.GetTemplatesParams{})
	ids := templateIDSet(resp)
	assert.True(t, ids[owned], "team-owned template should be listed")
	assert.True(t, ids[defaultEnv], "default template should be listed when team has no cluster")
	for _, tmpl := range resp.Data {
		if tmpl.TemplateID == defaultEnv {
			assert.True(t, tmpl.IsDefault, "default template should be flagged isDefault")
		}
	}

	// Dedicated cluster -> defaults are omitted.
	clusterID := uuid.New()
	_, clusterResp := callGetTemplates(t, store, teamInfo(teamID, &clusterID), api.GetTemplatesParams{})
	clusterIDs := templateIDSet(clusterResp)
	assert.True(t, clusterIDs[owned], "team-owned template should still be listed on a cluster")
	assert.False(t, clusterIDs[defaultEnv], "default template should be omitted when team is on a cluster")
}

func TestGetTemplates_Filters(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)

	teamID := testutils.CreateTestTeam(t, testDB)
	matching := testutils.CreateTestTemplate(t, testDB, teamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, matching, "needle-template", nil)
	other := testutils.CreateTestTemplate(t, testDB, teamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, other, "haystack-template", nil)

	store := newTemplatesTestStore(testDB)
	team := teamInfo(teamID, nil)

	search := "needle"
	_, resp := callGetTemplates(t, store, team, api.GetTemplatesParams{Search: &search})
	ids := templateIDSet(resp)
	assert.True(t, ids[matching], "search should match by alias name")
	assert.False(t, ids[other], "search should exclude non-matching templates")
}
