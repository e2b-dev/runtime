package handlers

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	ginmiddleware "github.com/oapi-codegen/gin-middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/api/internal/middleware"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// templateBuildsFixture is a template with four builds, oldest first:
//
//	superseded  ready   2 CPUs          was default, no tag now
//	fixed       ready   2 CPUs          default
//	resizable   ready   2 CPUs, max 4   v2, stable
//	failed      failed  2 CPUs          (assigned broken; a failed build carries no tag)
type templateBuildsFixture struct {
	store      *APIStore
	testDB     *testutils.Database
	teamID     uuid.UUID
	teamSlug   string
	templateID string
	alias      string

	superseded uuid.UUID
	fixed      uuid.UUID
	resizable  uuid.UUID
	failed     uuid.UUID
}

func newTemplateBuildsFixture(t *testing.T) templateBuildsFixture {
	t.Helper()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	teamSlug := testutils.GetTeamSlug(t, ctx, testDB, teamID)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	alias := testutils.CreateTestTemplateAliasWithNamespace(t, testDB, templateID, &teamSlug)

	f := templateBuildsFixture{
		store: &APIStore{
			sqlcDB:        testDB.SqlcClient,
			templateCache: templatecache.NewTemplateCache(testDB.SqlcClient, redis_utils.SetupInstance(t)),
		},
		testDB:     testDB,
		teamID:     teamID,
		teamSlug:   teamSlug,
		templateID: templateID,
		alias:      alias,
	}

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	f.superseded = insertTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady, 2, baseTime)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, f.superseded, "default", baseTime)
	f.fixed = insertTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady, 2, baseTime.Add(time.Hour))
	insertTemplateTagAssignment(t, ctx, testDB, templateID, f.fixed, "default", baseTime.Add(time.Hour))
	f.resizable = insertTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady, 2, baseTime.Add(2*time.Hour))
	insertTemplateTagAssignment(t, ctx, testDB, templateID, f.resizable, "v2", baseTime.Add(2*time.Hour))
	insertTemplateTagAssignment(t, ctx, testDB, templateID, f.resizable, "stable", baseTime.Add(2*time.Hour))
	require.NoError(t, testDB.SqlcClient.TestsRawSQL(ctx, `UPDATE public.env_builds SET max_vcpu = 4 WHERE id = $1`, f.resizable))
	f.failed = insertTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupFailed, 2, baseTime.Add(3*time.Hour))
	insertTemplateTagAssignment(t, ctx, testDB, templateID, f.failed, "broken", baseTime.Add(3*time.Hour))

	return f
}

func (f templateBuildsFixture) serve(t *testing.T, teamID uuid.UUID, teamSlug, template string, params api.GetTemplatesTemplateIDBuildsParams) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/templates/template/builds", nil)
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{Team: &dbtypes.Team{ID: teamID, Slug: teamSlug}})

	f.store.GetTemplatesTemplateIDBuilds(ginCtx, template, params)

	return recorder
}

func (f templateBuildsFixture) list(t *testing.T, template string, params api.GetTemplatesTemplateIDBuildsParams) ([]api.TemplateBuildWithTags, string) {
	t.Helper()

	recorder := f.serve(t, f.teamID, f.teamSlug, template, params)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var builds []api.TemplateBuildWithTags
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &builds))

	return builds, recorder.Header().Get("X-Next-Token")
}

func templateBuildIDs(builds []api.TemplateBuildWithTags) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(builds))
	for _, build := range builds {
		ids = append(ids, build.BuildID)
	}

	return ids
}

func TestGetTemplatesTemplateIDBuilds_ListsBuildsNewestFirstWithTagsAndCPUCounts(t *testing.T) {
	t.Parallel()

	f := newTemplateBuildsFixture(t)

	builds, nextToken := f.list(t, f.templateID, api.GetTemplatesTemplateIDBuildsParams{})

	require.Equal(t, []uuid.UUID{f.failed, f.resizable, f.fixed, f.superseded}, templateBuildIDs(builds))
	assert.Empty(t, nextToken)

	failed, resizable, fixed, superseded := builds[0], builds[1], builds[2], builds[3]
	assert.Equal(t, api.TemplateBuildStatusError, failed.Status)
	assert.Empty(t, failed.Tags, "a tag points at a ready build only")

	assert.Equal(t, api.TemplateBuildStatusReady, resizable.Status)
	assert.Equal(t, api.CPUCount(2), resizable.CpuCount)
	assert.Equal(t, new(api.CPUCount(4)), resizable.MaxCpuCount)
	assert.Equal(t, []string{"stable", "v2"}, resizable.Tags)

	assert.Equal(t, api.CPUCount(2), fixed.CpuCount)
	assert.Nil(t, fixed.MaxCpuCount, "a fixed-size build has no max")
	assert.Equal(t, []string{"default"}, fixed.Tags)

	assert.Empty(t, superseded.Tags, "default moved to a newer build")

	body := f.serve(t, f.teamID, f.teamSlug, f.templateID, api.GetTemplatesTemplateIDBuildsParams{}).Body.String()
	assert.Contains(t, body, `"tags":[]`, "a build without tags reports an empty array, not null")
}

func TestGetTemplatesTemplateIDBuilds_AcceptsTemplateIDOrName(t *testing.T) {
	t.Parallel()

	f := newTemplateBuildsFixture(t)
	byID, _ := f.list(t, f.templateID, api.GetTemplatesTemplateIDBuildsParams{})

	for _, name := range []string{f.alias, f.teamSlug + "/" + f.alias} {
		byName, _ := f.list(t, name, api.GetTemplatesTemplateIDBuildsParams{})
		assert.Equal(t, byID, byName, name)
	}
}

func TestGetTemplatesTemplateIDBuilds_FiltersByTags(t *testing.T) {
	t.Parallel()

	f := newTemplateBuildsFixture(t)

	for name, tc := range map[string]struct {
		tags []string
		want []uuid.UUID
	}{
		"one tag":                          {tags: []string{"v2"}, want: []uuid.UUID{f.resizable}},
		"default skips a superseded build": {tags: []string{"default"}, want: []uuid.UUID{f.fixed}},
		"several tags":                     {tags: []string{"v2", "default"}, want: []uuid.UUID{f.resizable, f.fixed}},
		"two tags on one build":            {tags: []string{"v2", "stable"}, want: []uuid.UUID{f.resizable}},
		"unknown tag":                      {tags: []string{"missing"}, want: []uuid.UUID{}},
		"empty list":                       {tags: []string{}, want: []uuid.UUID{f.failed, f.resizable, f.fixed, f.superseded}},
	} {
		builds, _ := f.list(t, f.templateID, api.GetTemplatesTemplateIDBuildsParams{Tags: &tc.tags})
		assert.Equal(t, tc.want, templateBuildIDs(builds), name)
	}
}

func TestGetTemplatesTemplateIDBuilds_FiltersByStatuses(t *testing.T) {
	t.Parallel()

	f := newTemplateBuildsFixture(t)

	for name, tc := range map[string]struct {
		statuses []api.TemplateBuildStatus
		want     []uuid.UUID
	}{
		"error":           {statuses: []api.TemplateBuildStatus{api.TemplateBuildStatusError}, want: []uuid.UUID{f.failed}},
		"ready":           {statuses: []api.TemplateBuildStatus{api.TemplateBuildStatusReady}, want: []uuid.UUID{f.resizable, f.fixed, f.superseded}},
		"ready and error": {statuses: []api.TemplateBuildStatus{api.TemplateBuildStatusReady, api.TemplateBuildStatusError}, want: []uuid.UUID{f.failed, f.resizable, f.fixed, f.superseded}},
		"none match":      {statuses: []api.TemplateBuildStatus{api.TemplateBuildStatusWaiting, api.TemplateBuildStatusBuilding}, want: []uuid.UUID{}},
	} {
		builds, _ := f.list(t, f.templateID, api.GetTemplatesTemplateIDBuildsParams{Statuses: &tc.statuses})
		assert.Equal(t, tc.want, templateBuildIDs(builds), name)
	}

	builds, _ := f.list(t, f.templateID, api.GetTemplatesTemplateIDBuildsParams{
		Statuses: &[]api.TemplateBuildStatus{api.TemplateBuildStatusReady},
		Tags:     &[]string{"broken", "v2"},
	})
	assert.Equal(t, []uuid.UUID{f.resizable}, templateBuildIDs(builds), "statuses and tags combine")
}

func TestGetTemplatesTemplateIDBuilds_PagesThroughEveryBuildOnce(t *testing.T) {
	t.Parallel()

	f := newTemplateBuildsFixture(t)

	var seen []uuid.UUID
	params := api.GetTemplatesTemplateIDBuildsParams{Limit: new(int32(3))}
	for page := 0; ; page++ {
		require.Less(t, page, 3, "paging does not end")

		builds, nextToken := f.list(t, f.templateID, params)
		seen = append(seen, templateBuildIDs(builds)...)
		if nextToken == "" {
			break
		}

		require.Len(t, builds, 3)
		params.NextToken = &nextToken
	}

	assert.Equal(t, []uuid.UUID{f.failed, f.resizable, f.fixed, f.superseded}, seen)

	recorder := f.serve(t, f.teamID, f.teamSlug, f.templateID, api.GetTemplatesTemplateIDBuildsParams{NextToken: new("not-a-token")})
	assert.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestGetTemplatesTemplateIDBuilds_RejectsOtherTeamsAndUnknownTemplates(t *testing.T) {
	t.Parallel()

	f := newTemplateBuildsFixture(t)

	missing := f.serve(t, f.teamID, f.teamSlug, "no-such-template", api.GetTemplatesTemplateIDBuildsParams{})
	assert.Equal(t, http.StatusNotFound, missing.Code, missing.Body.String())

	// Another team's template is answered word for word like a missing one.
	for _, reference := range []string{f.templateID, f.teamSlug + "/" + f.alias} {
		otherTeam := f.serve(t, uuid.New(), "other-team", reference, api.GetTemplatesTemplateIDBuildsParams{})
		assert.Equal(t, http.StatusNotFound, otherTeam.Code, otherTeam.Body.String())
		assert.Equal(t, missing.Body.String(), otherTeam.Body.String())
	}

	invalid := f.serve(t, f.teamID, f.teamSlug, "Not A Name!", api.GetTemplatesTemplateIDBuildsParams{})
	assert.Equal(t, http.StatusBadRequest, invalid.Code, invalid.Body.String())

	// The path names a template; builds are picked with the tags parameter.
	for _, tagged := range []string{f.templateID + ":v2", f.alias + ":default"} {
		recorder := f.serve(t, f.teamID, f.teamSlug, tagged, api.GetTemplatesTemplateIDBuildsParams{})
		assert.Equal(t, http.StatusBadRequest, recorder.Code, tagged)
	}
}

// A next token whose ID is not a build ID is a bad request, not a query error.
func TestGetTemplatesTemplateIDBuilds_RejectsACursorWithoutABuildID(t *testing.T) {
	t.Parallel()

	f := newTemplateBuildsFixture(t)
	token := base64.URLEncoding.EncodeToString([]byte("2026-05-28T10:00:00Z__not-a-build-id"))

	recorder := f.serve(t, f.teamID, f.teamSlug, f.templateID, api.GetTemplatesTemplateIDBuildsParams{NextToken: &token})

	assert.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

// Two assignments of one tag at the same instant: the tag points at the build
// with the greater ID, the order of the assignments index that sandbox
// creation reads.
func TestGetTemplatesTemplateIDBuilds_BreaksTagTiesByBuildID(t *testing.T) {
	t.Parallel()

	f := newTemplateBuildsFixture(t)
	at := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC)
	insertTemplateTagAssignment(t, t.Context(), f.testDB, f.templateID, f.fixed, "tied", at)
	insertTemplateTagAssignment(t, t.Context(), f.testDB, f.templateID, f.resizable, "tied", at)

	want := f.fixed
	if bytes.Compare(f.resizable[:], f.fixed[:]) > 0 {
		want = f.resizable
	}

	builds, _ := f.list(t, f.templateID, api.GetTemplatesTemplateIDBuildsParams{Tags: &[]string{"tied"}})

	assert.Equal(t, []uuid.UUID{want}, templateBuildIDs(builds))
}

// Builds created at the same instant are paged in build ID order, each once.
func TestGetTemplatesTemplateIDBuilds_PagesBuildsWithTheSameCreationTime(t *testing.T) {
	t.Parallel()

	f := newTemplateBuildsFixture(t)
	at := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	want := make([]uuid.UUID, 0, 6)
	for range 6 {
		buildID := insertTestBuild(t, t.Context(), f.testDB, f.templateID, dbtypes.BuildStatusGroupReady, 2, at)
		insertTemplateTagAssignment(t, t.Context(), f.testDB, f.templateID, buildID, "twin", at)
		want = append(want, buildID)
	}
	slices.SortFunc(want, func(a, b uuid.UUID) int { return bytes.Compare(b[:], a[:]) })

	var seen []uuid.UUID
	params := api.GetTemplatesTemplateIDBuildsParams{Limit: new(int32(1))}
	for range 6 {
		builds, nextToken := f.list(t, f.templateID, params)
		seen = append(seen, templateBuildIDs(builds)...)
		params.NextToken = &nextToken
	}

	assert.Equal(t, want, seen)
}

// A rebuild takes its tags when it is registered. Until it is ready, and if it
// fails, the tag stays on the build sandboxes still start from.
func TestGetTemplatesTemplateIDBuilds_ATagStaysOnItsLatestReadyBuild(t *testing.T) {
	t.Parallel()

	f := newTemplateBuildsFixture(t)
	insertTemplateTagAssignment(t, t.Context(), f.testDB, f.templateID, f.failed, "default", time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC))

	builds, _ := f.list(t, f.templateID, api.GetTemplatesTemplateIDBuildsParams{Tags: &[]string{"default"}})
	assert.Equal(t, []uuid.UUID{f.fixed}, templateBuildIDs(builds))

	builds, _ = f.list(t, f.templateID, api.GetTemplatesTemplateIDBuildsParams{
		Tags:     &[]string{"default", "broken"},
		Statuses: &[]api.TemplateBuildStatus{api.TemplateBuildStatusError},
	})
	assert.Empty(t, builds, "no tag points at the failed build")
}

// Tags are stored trimmed and lowercased; the filter is cleaned the same way,
// and a value that cannot be a tag is refused.
func TestGetTemplatesTemplateIDBuilds_CleansAndValidatesTheTagFilter(t *testing.T) {
	t.Parallel()

	f := newTemplateBuildsFixture(t)

	builds, _ := f.list(t, f.templateID, api.GetTemplatesTemplateIDBuildsParams{Tags: &[]string{" V2 ", "v2", ""}})
	assert.Equal(t, []uuid.UUID{f.resizable}, templateBuildIDs(builds))

	// Items that are empty are no tags: no filter.
	builds, _ = f.list(t, f.templateID, api.GetTemplatesTemplateIDBuildsParams{Tags: &[]string{"", " "}})
	assert.Len(t, builds, 4)

	for name, tags := range map[string][]string{
		"not a tag":  {"v2", "bad tag!"},
		"a build ID": {f.resizable.String()},
	} {
		recorder := f.serve(t, f.teamID, f.teamSlug, f.templateID, api.GetTemplatesTemplateIDBuildsParams{Tags: &tags})
		assert.Equal(t, http.StatusBadRequest, recorder.Code, name)
	}
}

// Name resolution is cached, so both routes read the template again: once it
// is deleted, or owned by another team, a warm cache does not keep it readable
// and it is answered like a missing one.
func TestTemplateInfoAndBuilds_ReadTheStoredTemplateBehindAWarmCache(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		change string
		want   int
	}{
		"deleted":         {change: `UPDATE public.envs SET deleted_at = NOW() WHERE id = $1`, want: http.StatusNotFound},
		"moved to a team": {change: `UPDATE public.envs SET team_id = (SELECT id FROM public.teams WHERE id <> team_id LIMIT 1) WHERE id = $1`, want: http.StatusNotFound},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f := newTemplateBuildsFixture(t)
			testutils.CreateTestTeam(t, f.testDB)

			// Warm the name cache.
			require.Equal(t, http.StatusOK, f.serve(t, f.teamID, f.teamSlug, f.alias, api.GetTemplatesTemplateIDBuildsParams{}).Code)
			require.Equal(t, http.StatusOK, serveTemplateInfo(t, f, f.teamID, f.teamSlug, f.alias).Code)

			require.NoError(t, f.testDB.SqlcClient.TestsRawSQL(t.Context(), tc.change, f.templateID))

			builds := f.serve(t, f.teamID, f.teamSlug, f.alias, api.GetTemplatesTemplateIDBuildsParams{})
			assert.Equal(t, tc.want, builds.Code, builds.Body.String())
			info := serveTemplateInfo(t, f, f.teamID, f.teamSlug, f.alias)
			assert.Equal(t, tc.want, info.Code, info.Body.String())
		})
	}
}

// templateRoutes serves the generated routes the way the API does: API key
// authentication, the request validator, the blocked-team check, and raw
// paths, so a namespaced name travels as team%2Fname.
func (f templateBuildsFixture) templateRoutes(t *testing.T, team *authtypes.Team) (*gin.Engine, string) {
	t.Helper()

	apiKey := auth.PrefixAPIKey + team.ID.String()
	swagger, err := api.GetSwagger()
	require.NoError(t, err)
	swagger.Servers = nil
	authenticate := auth.CreateAuthenticationFunc([]auth.Authenticator{
		auth.NewApiKeyAuthenticator(func(_ context.Context, _ *gin.Context, key string) (*authtypes.Team, *auth.APIError) {
			if key != apiKey {
				return nil, &auth.APIError{Code: http.StatusUnauthorized, Err: errors.New("unknown key"), ClientMsg: "Invalid API key"}
			}

			return team, nil
		}),
	}, nil)

	r := gin.New()
	r.UseRawPath = true
	r.Use(ginmiddleware.OapiRequestValidatorWithOptions(swagger, &ginmiddleware.Options{
		ErrorHandler: func(c *gin.Context, message string, fallbackStatusCode int) {
			utils.ErrorHandler(c, message, max(c.Writer.Status(), fallbackStatusCode))
		},
		MultiErrorHandler:     utils.MultiErrorHandler,
		Options:               openapi3filter.Options{AuthenticationFunc: authenticate, MultiError: true},
		SilenceServersWarning: true,
	}))
	r.Use(middleware.EnforceBlockedTeam())
	api.RegisterHandlersWithOptions(r, f.store, api.GinServerOptions{
		ErrorHandler: func(c *gin.Context, err error, statusCode int) { utils.ErrorHandler(c, err.Error(), statusCode) },
	})

	return r, apiKey
}

func TestTemplateInfoAndBuildsRoutes(t *testing.T) {
	t.Parallel()

	f := newTemplateBuildsFixture(t)
	team := minimalTeamInfo()
	team.ID, team.Slug, team.Name = f.teamID, f.teamSlug, f.teamSlug
	router, apiKey := f.templateRoutes(t, team)

	get := func(path, key string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
		if key != "" {
			req.Header.Set(auth.HeaderAPIKey, key)
		}
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req)

		return recorder
	}
	name := f.teamSlug + "%2F" + f.alias

	// Comma-separated lists bind to both filters, by ID and by namespaced name.
	for _, template := range []string{f.templateID, name} {
		recorder := get("/templates/"+template+"/builds?tags=v2,default&statuses=ready,error&limit=1", apiKey)
		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

		var builds []api.TemplateBuildWithTags
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &builds))
		assert.Equal(t, []uuid.UUID{f.resizable}, templateBuildIDs(builds), template)
		assert.NotEmpty(t, recorder.Header().Get("X-Next-Token"), template)
	}

	for _, template := range []string{f.templateID, name} {
		recorder := get("/v2/templates/"+template, apiKey)
		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

		var template api.TemplateV2
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &template))
		assert.Equal(t, f.templateID, template.TemplateID)
	}

	for path, want := range map[string]int{
		"/templates/" + f.templateID + "/builds":                                         http.StatusUnauthorized,
		"/v2/templates/" + f.templateID:                                                  http.StatusUnauthorized,
		"/templates/" + f.templateID + "/builds?statuses=success":                        http.StatusBadRequest,
		"/templates/" + f.templateID + "/builds?statuses=ready,ready":                    http.StatusBadRequest,
		"/templates/" + f.templateID + "/builds?tags=":                                   http.StatusBadRequest,
		"/templates/" + f.templateID + "/builds?limit=0":                                 http.StatusBadRequest,
		"/templates/" + f.templateID + "/builds?limit=101":                               http.StatusBadRequest,
		"/templates/" + f.templateID + "/builds?tags=" + strings.Repeat("t,", 100) + "t": http.StatusBadRequest,
	} {
		key := apiKey
		if want == http.StatusUnauthorized {
			key = ""
		}
		recorder := get(path, key)
		assert.Equal(t, want, recorder.Code, "%s: %s", path, recorder.Body.String())
	}

	// A blocked team keeps both reads, as it keeps the template list.
	reason := "payment required"
	team.IsBlocked, team.BlockedReason = true, &reason
	for _, path := range []string{"/templates/" + f.templateID + "/builds", "/v2/templates/" + f.templateID} {
		recorder := get(path, apiKey)
		assert.Equal(t, http.StatusOK, recorder.Code, "%s: %s", path, recorder.Body.String())
	}
}

// templateBuildStatusGroups is the inverse of the mapping builds are reported
// with, so a status filter returns the builds that report that status.
func TestTemplateBuildStatusGroupsInvertsTheReportedStatus(t *testing.T) {
	t.Parallel()

	for _, group := range []dbtypes.BuildStatusGroup{
		dbtypes.BuildStatusGroupPending,
		dbtypes.BuildStatusGroupInProgress,
		dbtypes.BuildStatusGroupReady,
		dbtypes.BuildStatusGroupFailed,
	} {
		status := getCorrespondingTemplateBuildStatus(t.Context(), group)

		groups, err := templateBuildStatusGroups(&[]api.TemplateBuildStatus{status})
		require.NoError(t, err)
		assert.Equal(t, []string{string(group)}, groups, "status %s", status)
	}

	none, err := templateBuildStatusGroups(nil)
	require.NoError(t, err)
	assert.Empty(t, none, "no status requested: no filter")

	_, err = templateBuildStatusGroups(&[]api.TemplateBuildStatus{"ready", "unknown"})
	require.Error(t, err, "an unknown status must not widen the list")
}

// An unknown status reaching the handler without the request validator is a
// bad request, not an unfiltered list.
func TestGetTemplatesTemplateIDBuilds_RejectsAnUnknownStatus(t *testing.T) {
	t.Parallel()

	f := newTemplateBuildsFixture(t)

	recorder := f.serve(t, f.teamID, f.teamSlug, f.templateID, api.GetTemplatesTemplateIDBuildsParams{
		Statuses: &[]api.TemplateBuildStatus{"unknown"},
	})

	assert.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}
