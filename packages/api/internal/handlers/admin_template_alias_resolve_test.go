package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
)

func TestPostUnstableAdminTemplatesAliasesResolve_ReportsEachNameInRequestOrder(t *testing.T) {
	t.Parallel()

	store, testDB := newAdminTemplatesTestStore(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	otherTeamID := testutils.CreateTestTeam(t, testDB)

	complete := testutils.CreateTestTemplate(t, testDB, teamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, complete, "complete", new("vendor"))
	testutils.CreateTestTemplateAliasWithName(t, testDB, complete, "complete", nil)
	nakedMissing := testutils.CreateTestTemplate(t, testDB, teamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, nakedMissing, "naked-missing", new("vendor"))
	split := testutils.CreateTestTemplate(t, testDB, teamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, split, "split", new("vendor"))
	squatter := testutils.CreateTestTemplate(t, testDB, otherTeamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, squatter, "split", nil)
	elsewhere := testutils.CreateTestTemplate(t, testDB, otherTeamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, elsewhere, "elsewhere", new("someone-else"))
	deleted := testutils.CreateTestTemplate(t, testDB, teamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, deleted, "deleted", new("vendor"))
	softDeleteTestTemplate(t, ctx, testDB, deleted)
	private := testutils.CreateTestTemplate(t, testDB, teamID)
	require.NoError(t, testDB.SqlcClient.TestsRawSQL(ctx, `UPDATE public.envs SET public = false WHERE id = $1`, private))
	testutils.CreateTestTemplateAliasWithName(t, testDB, private, "private", new("vendor"))

	names := []string{"split", "complete", "naked-missing", "elsewhere", "deleted", "private", "unknown"}
	recorder := serveAdminTemplates(t, http.MethodPost, resolveBody(t, "vendor", names), func(c *gin.Context) {
		store.PostUnstableAdminTemplatesAliasesResolve(c)
	})
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var response api.AdminTemplateAliasResolveResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))

	held := func(templateID string, public bool) *api.AdminResolvedTemplate {
		return &api.AdminResolvedTemplate{TemplateID: templateID, Public: public}
	}
	assert.Equal(t, []api.AdminTemplateAliasResolution{
		{Name: "split", Namespaced: held(split, true), Naked: held(squatter, true)},
		{Name: "complete", Namespaced: held(complete, true), Naked: held(complete, true)},
		{Name: "naked-missing", Namespaced: held(nakedMissing, true)},
		{Name: "elsewhere"},
		{Name: "deleted"},
		{Name: "private", Namespaced: held(private, false)},
		{Name: "unknown"},
	}, response.Results)
	assert.Contains(t, recorder.Body.String(), `{"naked":null,"name":"unknown","namespaced":null}`, "a name nobody holds answers null, not an absent field")
}

func TestPostUnstableAdminTemplatesAliasesResolve_Rejections(t *testing.T) {
	t.Parallel()

	store, _ := newAdminTemplatesTestStore(t)
	tooMany := make([]string, adminResolveMaxNames+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("name-%d", i)
	}

	for name, body := range map[string]string{
		"blank namespace": resolveBody(t, "  ", []string{"base"}),
		"no names":        `{"namespace":"vendor","names":[]}`,
		"missing names":   `{"namespace":"vendor"}`,
		"blank name":      resolveBody(t, "vendor", []string{"base", " "}),
		"duplicate name":  resolveBody(t, "vendor", []string{"base", "base"}),
		"too many names":  resolveBody(t, "vendor", tooMany),
		"not a JSON body": `names=base`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			recorder := serveAdminTemplates(t, http.MethodPost, body, func(c *gin.Context) {
				store.PostUnstableAdminTemplatesAliasesResolve(c)
			})
			assert.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		})
	}

	recorder := serveAdminTemplates(t, http.MethodPost, resolveBody(t, "vendor", tooMany[:adminResolveMaxNames]), func(c *gin.Context) {
		store.PostUnstableAdminTemplatesAliasesResolve(c)
	})
	assert.Equal(t, http.StatusOK, recorder.Code, "the limit itself is accepted: %s", strings.TrimSpace(recorder.Body.String()))
}

func resolveBody(t *testing.T, namespace string, names []string) string {
	t.Helper()

	body, err := json.Marshal(api.AdminTemplateAliasResolveRequest{Namespace: namespace, Names: names})
	require.NoError(t, err)

	return string(body)
}
