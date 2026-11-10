package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func TestSearchTemplates_ListsPublicDefaultsFlaggedWithoutUsage(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	platformTeamID := testutils.CreateTestTeam(t, testDB)
	now := time.Now().UTC().Truncate(time.Second)

	own := testutils.CreateTestTemplate(t, testDB, teamID)
	setTemplateTimes(t, ctx, testDB, own, now.Add(-3*time.Hour), now.Add(-3*time.Hour))
	setTemplateUsage(t, ctx, testDB, own, 5, now.Add(-time.Minute), 2)
	ownBuildAt := now.Add(-30 * time.Minute)
	ownBuildID := insertTestBuild(t, ctx, testDB, own, "ready", 2, ownBuildAt)
	insertTemplateTagAssignment(t, ctx, testDB, own, ownBuildID, "default", ownBuildAt)

	ownDefault := testutils.CreateTestTemplate(t, testDB, teamID)
	setTemplateTimes(t, ctx, testDB, ownDefault, now.Add(-4*time.Hour), now.Add(-4*time.Hour))
	setTemplateUsage(t, ctx, testDB, ownDefault, 9, now.Add(-time.Minute), 3)
	markTemplateDefault(t, ctx, testDB, ownDefault, nil)

	platformDefault := testutils.CreateTestTemplate(t, testDB, platformTeamID)
	setTemplateTimes(t, ctx, testDB, platformDefault, now.Add(-2*time.Hour), now.Add(-2*time.Hour))
	setTemplateUsage(t, ctx, testDB, platformDefault, 42, now.Add(-time.Minute), 7)
	setTemplateCreator(t, ctx, testDB, platformDefault)
	markTemplateDefault(t, ctx, testDB, platformDefault, new("Platform base"))

	privateDefault := testutils.CreateTestTemplate(t, testDB, platformTeamID)
	setTemplateTimes(t, ctx, testDB, privateDefault, now.Add(-time.Hour), now.Add(-time.Hour))
	markTemplateDefault(t, ctx, testDB, privateDefault, nil)
	setTemplatePublic(ctx, t, testDB, privateDefault, false)

	deletedDefault := testutils.CreateTestTemplate(t, testDB, platformTeamID)
	markTemplateDefault(t, ctx, testDB, deletedDefault, nil)
	softDeleteTestTemplate(t, ctx, testDB, deletedDefault)

	testutils.CreateTestTemplate(t, testDB, platformTeamID)

	templates, _ := callSearchTemplatesHandler(t, ctx, testDB, teamTestCaller(teamID, nil), api.GetUnstableTemplatesSearchParams{})

	require.Equal(t, []string{platformDefault, own, ownDefault}, templateIDs(templates), "newest first, defaults interleaved")

	byID := templatesByID(templates)
	platform := byID[platformDefault]
	require.NotNil(t, platform.IsDefault)
	assert.True(t, *platform.IsDefault)
	assert.Equal(t, new("Platform base"), platform.Description)
	assert.Zero(t, platform.SpawnCount)
	assert.Zero(t, platform.BuildCount)
	assert.Nil(t, platform.LastSpawnedAt)
	assert.Nil(t, platform.CreatedBy)

	ownTemplate := byID[own]
	require.NotNil(t, ownTemplate.IsDefault)
	assert.False(t, *ownTemplate.IsDefault)
	assert.Nil(t, ownTemplate.Description)
	assert.Equal(t, ownBuildID.String(), ownTemplate.BuildID)
	assert.Equal(t, int64(5), ownTemplate.SpawnCount)
	assert.Equal(t, int32(2), ownTemplate.BuildCount)
	assert.NotNil(t, ownTemplate.LastSpawnedAt)

	ownDefaultTemplate := byID[ownDefault]
	require.NotNil(t, ownDefaultTemplate.IsDefault)
	assert.True(t, *ownDefaultTemplate.IsDefault, "a team's own default is flagged")
	assert.Equal(t, int64(9), ownDefaultTemplate.SpawnCount, "and keeps its usage")
}

func TestSearchTemplates_DedicatedClusterTeamGetsNoDefaults(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	platformTeamID := testutils.CreateTestTeam(t, testDB)

	own := testutils.CreateTestTemplate(t, testDB, teamID)
	platformDefault := testutils.CreateTestTemplate(t, testDB, platformTeamID)
	markTemplateDefault(t, ctx, testDB, platformDefault, nil)

	clusterID := uuid.New()
	templates, _ := callSearchTemplatesHandler(t, ctx, testDB, teamTestCaller(teamID, &clusterID), api.GetUnstableTemplatesSearchParams{})

	assert.Equal(t, []string{own}, templateIDs(templates))
}

func TestSearchTemplates_DefaultSortPaginates(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	platformTeamID := testutils.CreateTestTeam(t, testDB)
	now := time.Now().UTC().Truncate(time.Second)

	want := make([]string, 0, 4)
	for i := range 3 {
		templateID := testutils.CreateTestTemplate(t, testDB, teamID)
		setTemplateTimes(t, ctx, testDB, templateID, now.Add(-time.Duration(2*i+1)*time.Hour), now)
		want = append(want, templateID)
	}
	platformDefault := testutils.CreateTestTemplate(t, testDB, platformTeamID)
	setTemplateTimes(t, ctx, testDB, platformDefault, now.Add(-2*time.Hour), now)
	markTemplateDefault(t, ctx, testDB, platformDefault, nil)
	want = []string{want[0], platformDefault, want[1], want[2]}

	limit := api.TemplatesLimit(2)
	first, token := callSearchTemplatesHandler(t, ctx, testDB, teamTestCaller(teamID, nil), api.GetUnstableTemplatesSearchParams{Limit: &limit})
	require.Len(t, first, 2)
	require.NotEmpty(t, token)

	assert.Equal(t, "created_at_desc|"+first[1].CreatedAt.UTC().Format(time.RFC3339Nano)+"|"+first[1].TemplateID, token)

	second, next := callSearchTemplatesHandler(t, ctx, testDB, teamTestCaller(teamID, nil), api.GetUnstableTemplatesSearchParams{Limit: &limit, Cursor: &token})
	require.Len(t, second, 2)
	assert.Empty(t, next, "last page carries no token")

	assert.Equal(t, want, append(templateIDs(first), templateIDs(second)...))

	sort := api.GetUnstableTemplatesSearchParamsSort(templatesSortCreatedAtDesc)
	explicit, _ := callSearchTemplatesHandler(t, ctx, testDB, teamTestCaller(teamID, nil), api.GetUnstableTemplatesSearchParams{Sort: &sort})
	assert.Equal(t, want, templateIDs(explicit), "created_at_desc is the default sort")
}

func TestSearchTemplates_SortsAndPaginatesBySortColumn(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	now := time.Now().UTC().Truncate(time.Second)

	// created_at order: a, b, c (oldest first); updated_at order: c, a, b.
	a := testutils.CreateTestTemplate(t, testDB, teamID)
	setTemplateTimes(t, ctx, testDB, a, now.Add(-3*time.Hour), now.Add(-2*time.Hour))
	b := testutils.CreateTestTemplate(t, testDB, teamID)
	setTemplateTimes(t, ctx, testDB, b, now.Add(-2*time.Hour), now.Add(-time.Hour))
	c := testutils.CreateTestTemplate(t, testDB, teamID)
	setTemplateTimes(t, ctx, testDB, c, now.Add(-time.Hour), now.Add(-3*time.Hour))

	for _, tc := range []struct {
		sort templatesSort
		want []string
	}{
		{sort: templatesSortCreatedAtDesc, want: []string{c, b, a}},
		{sort: templatesSortCreatedAtAsc, want: []string{a, b, c}},
		{sort: templatesSortUpdatedAtDesc, want: []string{b, a, c}},
		{sort: templatesSortUpdatedAtAsc, want: []string{c, a, b}},
	} {
		t.Run(string(tc.sort), func(t *testing.T) {
			t.Parallel()

			sort := api.GetUnstableTemplatesSearchParamsSort(tc.sort)
			limit := api.TemplatesLimit(1)
			var got []string
			var token *string
			for range len(tc.want) + 1 {
				page, next := callSearchTemplatesHandler(t, ctx, testDB, teamTestCaller(teamID, nil), api.GetUnstableTemplatesSearchParams{
					Sort:   &sort,
					Limit:  &limit,
					Cursor: token,
				})
				got = append(got, templateIDs(page)...)
				if next == "" {
					break
				}
				token = &next
			}

			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSearchTemplates_RejectsATokenFromAnotherSort(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	for range 2 {
		testutils.CreateTestTemplate(t, testDB, teamID)
	}

	limit := api.TemplatesLimit(1)
	updatedAsc := api.GetUnstableTemplatesSearchParamsSort(templatesSortUpdatedAtAsc)
	createdAsc := api.GetUnstableTemplatesSearchParamsSort(templatesSortCreatedAtAsc)

	_, defaultToken := callSearchTemplatesHandler(t, ctx, testDB, teamTestCaller(teamID, nil), api.GetUnstableTemplatesSearchParams{Limit: &limit})
	require.NotEmpty(t, defaultToken)
	_, updatedAscToken := callSearchTemplatesHandler(t, ctx, testDB, teamTestCaller(teamID, nil), api.GetUnstableTemplatesSearchParams{Limit: &limit, Sort: &updatedAsc})
	require.NotEmpty(t, updatedAscToken)

	for name, params := range map[string]api.GetUnstableTemplatesSearchParams{
		"default token, other sort":  {Sort: &updatedAsc, Cursor: &defaultToken},
		"sorted token, default sort": {Cursor: &updatedAscToken},
		"sorted token, another sort": {Sort: &createdAsc, Cursor: &updatedAscToken},
		"not a token":                {Sort: &updatedAsc, Cursor: new("garbage")},
		"cursor with a bad time":     {Cursor: new("created_at_desc|yesterday|tpl")},
		"cursor without an id":       {Cursor: new("created_at_desc|2026-01-01T00:00:00Z|")},
		"bogus sort":                 {Sort: new(api.GetUnstableTemplatesSearchParamsSort("name_asc"))},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			recorder := serveSearchTemplates(t, ctx, testDB, teamTestCaller(teamID, nil), params)
			assert.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		})
	}
}

func TestSearchTemplates_SearchMatchesLiterally(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)

	underscore := testutils.CreateTestTemplate(t, testDB, teamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, underscore, "my_tpl", nil)
	similar := testutils.CreateTestTemplate(t, testDB, teamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, similar, "myxtpl", nil)
	namespaced := testutils.CreateTestTemplate(t, testDB, teamID)
	testutils.CreateTestTemplateAliasWithName(t, testDB, namespaced, "worker", new("acme"))

	for _, tc := range []struct {
		search string
		want   []string
	}{
		{search: "my_", want: []string{underscore}},
		{search: "MYX", want: []string{similar}},
		{search: "%", want: []string{}},
		{search: "acme/work", want: []string{namespaced}},
		{search: underscore[len(underscore)-8:], want: []string{underscore}},
	} {
		t.Run(tc.search, func(t *testing.T) {
			t.Parallel()

			search := tc.search
			templates, _ := callSearchTemplatesHandler(t, ctx, testDB, teamTestCaller(teamID, nil), api.GetUnstableTemplatesSearchParams{Search: &search})
			assert.ElementsMatch(t, tc.want, templateIDs(templates))
		})
	}
}

func TestSearchTemplates_PublicFilterAppliesToDefaults(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	platformTeamID := testutils.CreateTestTeam(t, testDB)

	publicOwn := testutils.CreateTestTemplate(t, testDB, teamID)
	privateOwn := testutils.CreateTestTemplate(t, testDB, teamID)
	setTemplatePublic(ctx, t, testDB, privateOwn, false)
	platformDefault := testutils.CreateTestTemplate(t, testDB, platformTeamID)
	markTemplateDefault(t, ctx, testDB, platformDefault, nil)

	onlyPublic, _ := callSearchTemplatesHandler(t, ctx, testDB, teamTestCaller(teamID, nil), api.GetUnstableTemplatesSearchParams{Public: new(true)})
	assert.ElementsMatch(t, []string{publicOwn, platformDefault}, templateIDs(onlyPublic))

	onlyPrivate, _ := callSearchTemplatesHandler(t, ctx, testDB, teamTestCaller(teamID, nil), api.GetUnstableTemplatesSearchParams{Public: new(false)})
	assert.Equal(t, []string{privateOwn}, templateIDs(onlyPrivate))
}

func teamTestCaller(teamID uuid.UUID, clusterID *uuid.UUID) defaultTemplatesTestCaller {
	return func(t *testing.T, c *gin.Context) {
		t.Helper()

		auth.SetTeamInfoForTest(t, c, &authtypes.Team{Team: &dbtypes.Team{ID: teamID, ClusterID: clusterID}})
	}
}

func serveSearchTemplates(t *testing.T, ctx context.Context, testDB *testutils.Database, caller defaultTemplatesTestCaller, params api.GetUnstableTemplatesSearchParams) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(ctx, http.MethodGet, "/unstable/templates/search", nil)
	caller(t, ginCtx)

	store := &APIStore{sqlcDB: testDB.SqlcClient}
	//nolint:contextcheck // GetUnstableTemplatesSearch reads ctx from ginCtx.Request.Context().
	store.GetUnstableTemplatesSearch(ginCtx, params)

	return recorder
}

func callSearchTemplatesHandler(t *testing.T, ctx context.Context, testDB *testutils.Database, caller defaultTemplatesTestCaller, params api.GetUnstableTemplatesSearchParams) ([]api.Template, string) {
	t.Helper()

	recorder := serveSearchTemplates(t, ctx, testDB, caller, params)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var response api.TemplateSearchResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))

	return response.Data, sharedUtils.DerefOrDefault(response.NextCursor, "")
}

func templateIDs(templates []api.Template) []string {
	ids := make([]string, 0, len(templates))
	for _, template := range templates {
		ids = append(ids, template.TemplateID)
	}

	return ids
}

func setTemplateTimes(t *testing.T, ctx context.Context, testDB *testutils.Database, templateID string, createdAt, updatedAt time.Time) {
	t.Helper()

	err := testDB.SqlcClient.TestsRawSQL(ctx, `UPDATE public.envs SET created_at = $2, updated_at = $3 WHERE id = $1`, templateID, createdAt, updatedAt)
	require.NoError(t, err, "Failed to set template times")
}
