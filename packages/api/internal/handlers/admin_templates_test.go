package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestPostUnstableAdminTemplatesAliases_AddsAliasAndClearsCachedMiss(t *testing.T) {
	t.Parallel()

	store, testDB := newAdminTemplatesTestStore(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	// A lookup before the alias exists caches the miss.
	_, err := store.templateCache.ResolveAlias(ctx, "promoted-base", "some-team")
	require.ErrorIs(t, err, templatecache.ErrTemplateNotFound)

	recorder := serveAdminTemplates(t, http.MethodPost, `{"templateID":"`+templateID+`","alias":"  Promoted-Base "}`, func(c *gin.Context) {
		store.PostUnstableAdminTemplatesAliases(c)
	})
	require.Equal(t, http.StatusCreated, recorder.Code, recorder.Body.String())

	var created api.AdminTemplateAlias
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &created))
	assert.Equal(t, "promoted-base", created.Alias, "normalized like a template name")
	assert.Equal(t, templateID, created.TemplateID)
	assert.Nil(t, created.Namespace)
	assert.True(t, created.IsRenamable)

	info, err := store.templateCache.ResolveAlias(ctx, "promoted-base", "some-team")
	require.NoError(t, err, "the cached miss is cleared")
	assert.Equal(t, templateID, info.TemplateID)
}

func TestPostUnstableAdminTemplatesAliases_Rejections(t *testing.T) {
	t.Parallel()

	store, testDB := newAdminTemplatesTestStore(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	otherTemplateID := testutils.CreateTestTemplate(t, testDB, teamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, otherTemplateID, "taken", nil)
	deletedTemplateID := testutils.CreateTestTemplate(t, testDB, teamID)
	softDeleteTestTemplate(t, ctx, testDB, deletedTemplateID)

	for _, tc := range []struct {
		name     string
		body     string
		wantCode int
	}{
		{name: "invalid characters", body: `{"templateID":"` + templateID + `","alias":"no spaces"}`, wantCode: http.StatusBadRequest},
		{name: "namespaced", body: `{"templateID":"` + templateID + `","alias":"team/base"}`, wantCode: http.StatusBadRequest},
		{name: "tagged", body: `{"templateID":"` + templateID + `","alias":"base:v1"}`, wantCode: http.StatusBadRequest},
		{name: "unknown template", body: `{"templateID":"missing","alias":"fresh"}`, wantCode: http.StatusNotFound},
		{name: "deleted template", body: `{"templateID":"` + deletedTemplateID + `","alias":"fresh"}`, wantCode: http.StatusNotFound},
		{name: "taken", body: `{"templateID":"` + templateID + `","alias":"taken"}`, wantCode: http.StatusConflict},
		{name: "a template ID", body: `{"templateID":"` + templateID + `","alias":"` + otherTemplateID + `"}`, wantCode: http.StatusConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			recorder := serveAdminTemplates(t, http.MethodPost, tc.body, func(c *gin.Context) {
				store.PostUnstableAdminTemplatesAliases(c)
			})
			assert.Equal(t, tc.wantCode, recorder.Code, recorder.Body.String())
		})
	}
}

func TestDeleteUnstableAdminTemplatesAliasesAliasID_RemovesOnlyAliasesWithoutNamespace(t *testing.T) {
	t.Parallel()

	store, testDB := newAdminTemplatesTestStore(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, templateID, "promoted", nil)
	testutils.CreateTestTemplateAliasWithName(t, testDB, templateID, "scoped", new("acme"))
	nakedID := aliasIDByName(t, testDB, templateID, "promoted")
	namespacedID := aliasIDByName(t, testDB, templateID, "scoped")

	// A lookup caches the hit.
	info, err := store.templateCache.ResolveAlias(ctx, "promoted", "some-team")
	require.NoError(t, err)
	require.Equal(t, templateID, info.TemplateID)

	recorder := serveAdminTemplates(t, http.MethodDelete, "", func(c *gin.Context) {
		store.DeleteUnstableAdminTemplatesAliasesAliasID(c, namespacedID)
	})
	assert.Equal(t, http.StatusNotFound, recorder.Code, "a namespaced alias is not removed")

	recorder = serveAdminTemplates(t, http.MethodDelete, "", func(c *gin.Context) {
		store.DeleteUnstableAdminTemplatesAliasesAliasID(c, nakedID)
	})
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var deleted api.AdminTemplateAliasDeleted
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &deleted))
	assert.Equal(t, api.AdminTemplateAliasDeleted{AliasID: nakedID, TemplateID: templateID, Alias: "promoted"}, deleted)

	_, err = store.templateCache.ResolveAlias(ctx, "promoted", "some-team")
	require.ErrorIs(t, err, templatecache.ErrTemplateNotFound, "the cached hit is cleared")

	recorder = serveAdminTemplates(t, http.MethodDelete, "", func(c *gin.Context) {
		store.DeleteUnstableAdminTemplatesAliasesAliasID(c, nakedID)
	})
	assert.Equal(t, http.StatusNotFound, recorder.Code, "already removed")
}

func TestGetUnstableAdminTemplatesAliases_FindsByTemplateIDOrAliasInAnyNamespace(t *testing.T) {
	t.Parallel()

	store, testDB := newAdminTemplatesTestStore(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	otherTeamID := testutils.CreateTestTeam(t, testDB)

	byID := testutils.CreateTestTemplate(t, testDB, teamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, byID, "zeta", new("acme"))
	testutils.CreateTestTemplateAliasWithName(t, testDB, byID, "alpha", nil)
	sharedName := testutils.CreateTestTemplate(t, testDB, otherTeamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, sharedName, "worker", new("other"))
	sharedNameToo := testutils.CreateTestTemplate(t, testDB, teamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, sharedNameToo, "worker", new("acme"))
	deleted := testutils.CreateTestTemplate(t, testDB, teamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, deleted, "worker", new("third"))
	softDeleteTestTemplate(t, ctx, testDB, deleted)

	results := callAdminAliasSearch(t, store, api.GetUnstableAdminTemplatesAliasesParams{Search: byID})
	require.Len(t, results, 1)
	assert.Equal(t, teamID, results[0].Team.Id)
	require.Len(t, results[0].Aliases, 2)
	assert.Equal(t, "alpha", results[0].Aliases[0].Alias, "aliases without a namespace come first")
	assert.Nil(t, results[0].Aliases[0].Namespace)
	assert.Equal(t, "zeta", results[0].Aliases[1].Alias)

	results = callAdminAliasSearch(t, store, api.GetUnstableAdminTemplatesAliasesParams{Search: "worker"})
	got := make([]string, 0, len(results))
	for _, result := range results {
		got = append(got, result.TemplateID)
	}
	assert.ElementsMatch(t, []string{sharedName, sharedNameToo}, got, "every namespace, deleted templates left out")

	limit := api.PaginationLimit(1)
	recorder := serveAdminTemplates(t, http.MethodGet, "", func(c *gin.Context) {
		store.GetUnstableAdminTemplatesAliases(c, api.GetUnstableAdminTemplatesAliasesParams{Search: "worker", Limit: &limit})
	})
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	token := recorder.Header().Get("X-Next-Token")
	require.NotEmpty(t, token)
	second := callAdminAliasSearch(t, store, api.GetUnstableAdminTemplatesAliasesParams{Search: "worker", Limit: &limit, NextToken: &token})
	require.Len(t, second, 1)

	assert.Empty(t, callAdminAliasSearch(t, store, api.GetUnstableAdminTemplatesAliasesParams{Search: "nothing-matches"}))
}

func TestAdminDefaultTemplates_AddUpdateRemove(t *testing.T) {
	t.Parallel()

	store, testDB := newAdminTemplatesTestStore(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	deletedTemplateID := testutils.CreateTestTemplate(t, testDB, teamID)
	softDeleteTestTemplate(t, ctx, testDB, deletedTemplateID)

	add := func(body string) *httptest.ResponseRecorder {
		return serveAdminTemplates(t, http.MethodPost, body, func(c *gin.Context) { store.PostUnstableAdminTemplatesDefaults(c) })
	}

	recorder := add(`{"templateID":"` + templateID + `","description":"  Base sandbox "}`)
	require.Equal(t, http.StatusCreated, recorder.Code, recorder.Body.String())
	assert.JSONEq(t, `{"templateID":"`+templateID+`","description":"Base sandbox"}`, recorder.Body.String())

	assert.Equal(t, http.StatusConflict, add(`{"templateID":"`+templateID+`"}`).Code, "already a default")
	assert.Equal(t, http.StatusNotFound, add(`{"templateID":"missing"}`).Code)
	assert.Equal(t, http.StatusNotFound, add(`{"templateID":"`+deletedTemplateID+`"}`).Code)

	recorder = serveAdminTemplates(t, http.MethodPatch, `{"description":"   "}`, func(c *gin.Context) {
		store.PatchUnstableAdminTemplatesDefaultsTemplateID(c, templateID)
	})
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	assert.JSONEq(t, `{"templateID":"`+templateID+`","description":null}`, recorder.Body.String(), "blank clears it")

	recorder = serveAdminTemplates(t, http.MethodPatch, `{"description":"Updated"}`, func(c *gin.Context) {
		store.PatchUnstableAdminTemplatesDefaultsTemplateID(c, "missing")
	})
	assert.Equal(t, http.StatusNotFound, recorder.Code)

	defaults := callDefaultTemplatesHandler(t, ctx, testDB, asAdmin)
	require.Len(t, defaults, 1)
	assert.Equal(t, templateID, defaults[0].TemplateID)

	recorder = serveAdminTemplates(t, http.MethodDelete, "", func(c *gin.Context) {
		store.DeleteUnstableAdminTemplatesDefaultsTemplateID(c, templateID)
	})
	require.Equal(t, http.StatusNoContent, recorder.Code, recorder.Body.String())
	assert.Empty(t, callDefaultTemplatesHandler(t, ctx, testDB, asAdmin))

	recorder = serveAdminTemplates(t, http.MethodDelete, "", func(c *gin.Context) {
		store.DeleteUnstableAdminTemplatesDefaultsTemplateID(c, templateID)
	})
	assert.Equal(t, http.StatusNotFound, recorder.Code, "already removed")
}

func TestGetUnstableAdminTemplatesLookup_ResolvesBuildTemplateIDAndNames(t *testing.T) {
	t.Parallel()

	store, testDB := newAdminTemplatesTestStore(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, templateID, "worker", new("acme"))
	testutils.CreateTestTemplateAliasWithName(t, testDB, templateID, "promoted-worker", nil)

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	buildID := insertTestBuild(t, ctx, testDB, templateID, "ready", 2, baseTime)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, buildID, "default", baseTime)

	for _, tc := range []struct {
		query     string
		wantBuild *uuid.UUID
	}{
		{query: buildID.String(), wantBuild: &buildID},
		{query: templateID},
		{query: "acme/worker"},
		{query: "promoted-worker"},
		{query: "/promoted-worker"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			t.Parallel()

			recorder := serveAdminTemplates(t, http.MethodGet, "", func(c *gin.Context) {
				store.GetUnstableAdminTemplatesLookup(c, api.GetUnstableAdminTemplatesLookupParams{Query: tc.query})
			})
			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

			var lookup api.AdminTemplateLookup
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &lookup))
			assert.Equal(t, templateID, lookup.TemplateID)
			assert.Equal(t, teamID, lookup.Team.Id)
			assert.NotEmpty(t, lookup.Team.Email)
			assert.ElementsMatch(t, []string{"acme/worker", "promoted-worker"}, lookup.Names)
			assert.False(t, lookup.IsSnapshot)
			assert.Equal(t, tc.wantBuild, lookup.MatchedBuildID)
			require.Len(t, lookup.Builds, 1)
			assert.Equal(t, buildID, lookup.Builds[0].BuildID)
		})
	}
}

func TestGetUnstableAdminTemplatesLookup_NotFound(t *testing.T) {
	t.Parallel()

	store, testDB := newAdminTemplatesTestStore(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	deletedTemplateID := testutils.CreateTestTemplate(t, testDB, teamID)
	softDeleteTestTemplate(t, ctx, testDB, deletedTemplateID)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, templateID, "worker", new("acme"))

	for _, query := range []string{
		uuid.NewString(),  // a build ID with no template: no fallback to names
		deletedTemplateID, // deleted templates are not found
		"worker",          // a namespaced alias does not match without its namespace
		"other/worker",
	} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()

			recorder := serveAdminTemplates(t, http.MethodGet, "", func(c *gin.Context) {
				store.GetUnstableAdminTemplatesLookup(c, api.GetUnstableAdminTemplatesLookupParams{Query: query})
			})
			assert.Equal(t, http.StatusNotFound, recorder.Code, recorder.Body.String())
		})
	}
}

func newAdminTemplatesTestStore(t *testing.T) (*APIStore, *testutils.Database) {
	t.Helper()

	testDB := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)

	return &APIStore{
		sqlcDB:        testDB.SqlcClient,
		templateCache: templatecache.NewTemplateCache(testDB.SqlcClient, redis),
	}, testDB
}

func serveAdminTemplates(t *testing.T, method string, body string, handle func(*gin.Context)) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), method, "/unstable/admin/templates", bytes.NewBufferString(body))
	if body != "" {
		ginCtx.Request.Header.Set("Content-Type", "application/json")
	}

	handle(ginCtx)
	// gin writes a status set without a body after the handler returns.
	ginCtx.Writer.WriteHeaderNow()

	return recorder
}

func callAdminAliasSearch(t *testing.T, store *APIStore, params api.GetUnstableAdminTemplatesAliasesParams) []api.AdminTemplateAliases {
	t.Helper()

	recorder := serveAdminTemplates(t, http.MethodGet, "", func(c *gin.Context) {
		store.GetUnstableAdminTemplatesAliases(c, params)
	})
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var results []api.AdminTemplateAliases
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &results))

	return results
}

func aliasIDByName(t *testing.T, testDB *testutils.Database, templateID, alias string) uuid.UUID {
	t.Helper()

	var aliasID uuid.UUID
	err := testDB.SqlcClient.TestsRawSQLQuery(t.Context(),
		`SELECT id FROM public.env_aliases WHERE env_id = $1 AND alias = $2`,
		func(rows pgx.Rows) error {
			if rows.Next() {
				return rows.Scan(&aliasID)
			}

			return pgx.ErrNoRows
		},
		templateID, alias,
	)
	require.NoError(t, err)

	return aliasID
}
