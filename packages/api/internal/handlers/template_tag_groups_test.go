package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
)

func TestGetTemplatesTemplateIDTagsGroups_ReturnsReadyAssignmentsLatestFirst(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	oldBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady)
	newBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady)

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	oldAssignmentID := insertTemplateTagAssignment(t, ctx, testDB, templateID, oldBuildID, "prod", baseTime)
	newAssignmentID := insertTemplateTagAssignment(t, ctx, testDB, templateID, newBuildID, "prod", baseTime.Add(time.Hour))

	response := callTagGroupsHandler(t, ctx, testDB, teamID, templateID, api.GetTemplatesTemplateIDTagsGroupsParams{})

	require.Len(t, response.Tags, 1)
	group := response.Tags[0]
	assert.Equal(t, "prod", group.Tag)
	assert.False(t, group.HasMore)
	require.Len(t, group.Assignments, 2)
	assert.Equal(t, newAssignmentID, group.Assignments[0].AssignmentId)
	assert.Equal(t, newBuildID, group.Assignments[0].BuildId)
	assert.Equal(t, oldAssignmentID, group.Assignments[1].AssignmentId)
	assert.Equal(t, oldBuildID, group.Assignments[1].BuildId)
}

func TestGetTemplatesTemplateIDTagsGroups_LimitsPerTagAndSetsHasMore(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	for i := range 7 {
		buildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady)
		insertTemplateTagAssignment(t, ctx, testDB, templateID, buildID, "prod", baseTime.Add(time.Duration(i)*time.Hour))
	}

	limit := api.TagAssignmentLimit(6)
	response := callTagGroupsHandler(t, ctx, testDB, teamID, templateID, api.GetTemplatesTemplateIDTagsGroupsParams{
		AssignmentLimit: &limit,
	})

	require.Len(t, response.Tags, 1)
	assert.True(t, response.Tags[0].HasMore)
	assert.Len(t, response.Tags[0].Assignments, 6)
}

func TestGetTemplatesTemplateIDTagsGroups_DoesNotDedupeDuplicateBuildAssignments(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady)

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	firstAssignmentID := insertTemplateTagAssignment(t, ctx, testDB, templateID, buildID, "prod", baseTime)
	secondAssignmentID := insertTemplateTagAssignment(t, ctx, testDB, templateID, buildID, "prod", baseTime.Add(time.Hour))

	response := callTagGroupsHandler(t, ctx, testDB, teamID, templateID, api.GetTemplatesTemplateIDTagsGroupsParams{})

	require.Len(t, response.Tags, 1)
	require.Len(t, response.Tags[0].Assignments, 2)
	assert.Equal(t, secondAssignmentID, response.Tags[0].Assignments[0].AssignmentId)
	assert.Equal(t, firstAssignmentID, response.Tags[0].Assignments[1].AssignmentId)
	assert.Equal(t, buildID, response.Tags[0].Assignments[0].BuildId)
	assert.Equal(t, buildID, response.Tags[0].Assignments[1].BuildId)
}

func TestGetTemplatesTemplateIDTagsGroups_OmitsTagsWithoutReadyAssignments(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	failedBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupFailed)
	waitingBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupPending)

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, failedBuildID, "prod", baseTime)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, waitingBuildID, "dev", baseTime.Add(time.Hour))

	response := callTagGroupsHandler(t, ctx, testDB, teamID, templateID, api.GetTemplatesTemplateIDTagsGroupsParams{})

	assert.Empty(t, response.Tags)
}

func TestGetTemplatesTemplateIDTagsGroups_GroupsMultipleTagsByLatestAssignment(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	prodBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady)
	devBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady)

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, prodBuildID, "prod", baseTime)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, devBuildID, "dev", baseTime.Add(time.Hour))

	response := callTagGroupsHandler(t, ctx, testDB, teamID, templateID, api.GetTemplatesTemplateIDTagsGroupsParams{})

	require.Len(t, response.Tags, 2)
	assert.Equal(t, "dev", response.Tags[0].Tag)
	assert.Equal(t, "prod", response.Tags[1].Tag)
}

func TestGetTemplatesTemplateIDTagsGroups_PaginatesAcrossPages(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	const totalTags = 30
	for i := range totalTags {
		buildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady)
		insertTemplateTagAssignment(t, ctx, testDB, templateID, buildID, fmt.Sprintf("tag-%02d", i), baseTime.Add(time.Duration(i)*time.Minute))
	}

	tagsLimit := api.TagGroupsLimit(10)
	seen := make(map[string]struct{})
	var cursor *api.TagGroupsCursor

	for page := range 5 {
		response := callTagGroupsHandler(t, ctx, testDB, teamID, templateID, api.GetTemplatesTemplateIDTagsGroupsParams{
			TagsLimit:  &tagsLimit,
			TagsCursor: cursor,
		})

		for _, group := range response.Tags {
			_, dup := seen[group.Tag]
			require.False(t, dup, "page %d returned duplicate tag %q", page, group.Tag)
			seen[group.Tag] = struct{}{}
		}

		if response.NextCursor == nil || *response.NextCursor == "" {
			break
		}
		cursor = response.NextCursor
	}

	assert.Len(t, seen, totalTags, "expected to walk every tag exactly once")
}

func TestGetTemplatesTemplateIDTagsGroups_StableKeysetOnSameLatest(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	sameTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	for _, tag := range []string{"alpha", "beta", "gamma", "delta"} {
		buildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady)
		insertTemplateTagAssignment(t, ctx, testDB, templateID, buildID, tag, sameTime)
	}

	tagsLimit := api.TagGroupsLimit(2)
	first := callTagGroupsHandler(t, ctx, testDB, teamID, templateID, api.GetTemplatesTemplateIDTagsGroupsParams{
		TagsLimit: &tagsLimit,
	})
	require.Len(t, first.Tags, 2)
	require.NotNil(t, first.NextCursor)

	second := callTagGroupsHandler(t, ctx, testDB, teamID, templateID, api.GetTemplatesTemplateIDTagsGroupsParams{
		TagsLimit:  &tagsLimit,
		TagsCursor: first.NextCursor,
	})
	require.Len(t, second.Tags, 2)

	got := []string{first.Tags[0].Tag, first.Tags[1].Tag, second.Tags[0].Tag, second.Tags[1].Tag}
	assert.Equal(t, []string{"alpha", "beta", "delta", "gamma"}, got, "alphabetical tiebreak on equal latest_assigned_at")
}

func TestGetTemplatesTemplateIDTagsGroups_SortVariants(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	// Each tag gets a unique latest assignment, in non-alphabetical order:
	// beta is the oldest, prod the newest.
	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	for i, tag := range []string{"beta", "alpha", "prod"} {
		buildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady)
		insertTemplateTagAssignment(t, ctx, testDB, templateID, buildID, tag, baseTime.Add(time.Duration(i)*time.Hour))
	}

	cases := []struct {
		name string
		sort api.GetTemplatesTemplateIDTagsGroupsParamsSort
		want []string
	}{
		{"latest_desc", api.GetTemplatesTemplateIDTagsGroupsParamsSortLatestDesc, []string{"prod", "alpha", "beta"}},
		{"latest_asc", api.GetTemplatesTemplateIDTagsGroupsParamsSortLatestAsc, []string{"beta", "alpha", "prod"}},
		{"name_asc", api.GetTemplatesTemplateIDTagsGroupsParamsSortNameAsc, []string{"alpha", "beta", "prod"}},
		{"name_desc", api.GetTemplatesTemplateIDTagsGroupsParamsSortNameDesc, []string{"prod", "beta", "alpha"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sortParam := tc.sort
			response := callTagGroupsHandler(t, ctx, testDB, teamID, templateID, api.GetTemplatesTemplateIDTagsGroupsParams{
				Sort: &sortParam,
			})
			got := make([]string, len(response.Tags))
			for i, g := range response.Tags {
				got[i] = g.Tag
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestGetTemplatesTemplateIDTagsGroups_Search(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	for i, tag := range []string{"prod.v1.0", "prod.v1x0", "a_b", "axb", "staging"} {
		buildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady)
		insertTemplateTagAssignment(t, ctx, testDB, templateID, buildID, tag, baseTime.Add(time.Duration(i)*time.Hour))
	}

	cases := []struct {
		name   string
		search string
		want   []string
	}{
		{"prefix prod", "prod", []string{"prod.v1.0", "prod.v1x0"}},
		{"dot is literal", "v1.0", []string{"prod.v1.0"}},
		{"underscore is literal not wildcard", "a_b", []string{"a_b"}},
		{"no match", "missing", []string{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			searchParam := tc.search
			response := callTagGroupsHandler(t, ctx, testDB, teamID, templateID, api.GetTemplatesTemplateIDTagsGroupsParams{
				Search: &searchParam,
			})
			got := make([]string, 0, len(response.Tags))
			for _, g := range response.Tags {
				got = append(got, g.Tag)
			}
			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestGetTemplatesTemplateIDTagsGroups_InvalidInputs(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	cases := []struct {
		name   string
		params api.GetTemplatesTemplateIDTagsGroupsParams
	}{
		{
			name:   "search contains percent",
			params: api.GetTemplatesTemplateIDTagsGroupsParams{Search: new("%")},
		},
		{
			name:   "search contains at sign",
			params: api.GetTemplatesTemplateIDTagsGroupsParams{Search: new("a@b")},
		},
		{
			name:   "sort is bogus",
			params: api.GetTemplatesTemplateIDTagsGroupsParams{Sort: new(api.GetTemplatesTemplateIDTagsGroupsParamsSort("bogus"))},
		},
		{
			name:   "cursor malformed",
			params: api.GetTemplatesTemplateIDTagsGroupsParams{TagsCursor: new("malformed")},
		},
		{
			name: "cursor sort mismatch",
			params: api.GetTemplatesTemplateIDTagsGroupsParams{
				Sort:       new(api.GetTemplatesTemplateIDTagsGroupsParamsSortLatestDesc),
				TagsCursor: new("name_asc|2026-01-01T00:00:00Z|x"),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			recorder, ginCtx := newTemplateTagsTestContext(t, ctx, teamID)
			store := &APIStore{sqlcDB: testDB.SqlcClient}
			//nolint:contextcheck // GetTemplatesTemplateIDTagsGroups reads ctx from ginCtx.Request.Context().
			store.GetTemplatesTemplateIDTagsGroups(ginCtx, templateID, tc.params)
			assert.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		})
	}
}

func TestGetTemplatesTemplateIDTagsGroups_NextCursorAbsentOnLastPage(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	for i := range 3 {
		buildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady)
		insertTemplateTagAssignment(t, ctx, testDB, templateID, buildID, fmt.Sprintf("tag-%d", i), baseTime.Add(time.Duration(i)*time.Hour))
	}

	tagsLimit := api.TagGroupsLimit(3)
	response := callTagGroupsHandler(t, ctx, testDB, teamID, templateID, api.GetTemplatesTemplateIDTagsGroupsParams{
		TagsLimit: &tagsLimit,
	})

	require.Len(t, response.Tags, 3)
	assert.Nil(t, response.NextCursor)
}

func TestGetTemplatesTemplateIDTagsGroups_NotFoundForOtherTeam(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	ownerTeamID := testutils.CreateTestTeam(t, testDB)
	otherTeamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, ownerTeamID)

	recorder, ginCtx := newTemplateTagsTestContext(t, ctx, otherTeamID)
	store := &APIStore{sqlcDB: testDB.SqlcClient}

	store.GetTemplatesTemplateIDTagsGroups(ginCtx, templateID, api.GetTemplatesTemplateIDTagsGroupsParams{})

	assert.Equal(t, http.StatusNotFound, recorder.Code, "team mismatch should not leak template existence")
}

func TestGetTemplatesTemplateIDTagsGroups_NotFoundForDeletedTemplate(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, buildID, "prod", time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC))
	softDeleteTestTemplate(t, ctx, testDB, templateID)

	recorder, ginCtx := newTemplateTagsTestContext(t, ctx, teamID)
	store := &APIStore{sqlcDB: testDB.SqlcClient}

	store.GetTemplatesTemplateIDTagsGroups(ginCtx, templateID, api.GetTemplatesTemplateIDTagsGroupsParams{})

	assert.Equal(t, http.StatusNotFound, recorder.Code)
}

func TestGetTemplatesTemplateIDTagsCount_ReturnsDistinctReadyTagCount(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	for i := range 5 {
		tag := fmt.Sprintf("tag-%d", i)
		// Two ready builds with the same tag are counted once.
		for range 2 {
			buildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady)
			insertTemplateTagAssignment(t, ctx, testDB, templateID, buildID, tag, baseTime.Add(time.Duration(i)*time.Hour))
		}
	}

	response := callTagCountHandler(t, ctx, testDB, teamID, templateID)
	assert.Equal(t, int64(5), response.Total)
}

func TestGetTemplatesTemplateIDTagsCount_OnlyReadyBuildsCounted(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	readyBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady)
	failedBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupFailed)
	waitingBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupPending)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, readyBuildID, "prod", baseTime)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, failedBuildID, "dev", baseTime)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, waitingBuildID, "staging", baseTime)

	response := callTagCountHandler(t, ctx, testDB, teamID, templateID)
	assert.Equal(t, int64(1), response.Total)
}

func TestGetTemplatesTemplateIDTagsCount_EmptyTemplate(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	response := callTagCountHandler(t, ctx, testDB, teamID, templateID)
	assert.Equal(t, int64(0), response.Total)
}

func TestGetTemplatesTemplateIDTagsCount_NotFoundForOtherTeam(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	ownerTeamID := testutils.CreateTestTeam(t, testDB)
	otherTeamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, ownerTeamID)

	recorder, ginCtx := newTemplateTagsTestContext(t, ctx, otherTeamID)
	store := &APIStore{sqlcDB: testDB.SqlcClient}
	store.GetTemplatesTemplateIDTagsCount(ginCtx, templateID)

	assert.Equal(t, http.StatusNotFound, recorder.Code, "team mismatch should not leak template existence")
}

func TestGetTemplatesTemplateIDTagsExists_ReturnsTrueOnlyForReadyTag(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	readyBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupReady)
	failedBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, dbtypes.BuildStatusGroupFailed)

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, readyBuildID, "prod", baseTime)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, failedBuildID, "dev", baseTime)

	prodExists := callTagExistsHandler(t, ctx, testDB, teamID, templateID, "prod")
	assert.True(t, prodExists.Exists)
	assert.Equal(t, "prod", prodExists.NormalizedTag)

	normalizedProdExists := callTagExistsHandler(t, ctx, testDB, teamID, templateID, " PROD ")
	assert.True(t, normalizedProdExists.Exists)
	assert.Equal(t, "prod", normalizedProdExists.NormalizedTag)

	devExists := callTagExistsHandler(t, ctx, testDB, teamID, templateID, "dev")
	assert.False(t, devExists.Exists)
	assert.Equal(t, "dev", devExists.NormalizedTag)

	missingExists := callTagExistsHandler(t, ctx, testDB, teamID, templateID, "missing")
	assert.False(t, missingExists.Exists)
	assert.Equal(t, "missing", missingExists.NormalizedTag)
}

func TestGetTemplatesTemplateIDTagsExists_InvalidTagReturnsBadRequest(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	recorder, ginCtx := newTemplateTagsTestContext(t, ctx, teamID)
	store := &APIStore{sqlcDB: testDB.SqlcClient}

	store.GetTemplatesTemplateIDTagsExists(ginCtx, templateID, api.GetTemplatesTemplateIDTagsExistsParams{Tag: "bad!tag"})

	assert.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestGetTemplatesTemplateIDTagsExists_NotFoundForOtherTeam(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	ownerTeamID := testutils.CreateTestTeam(t, testDB)
	otherTeamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, ownerTeamID)

	recorder, ginCtx := newTemplateTagsTestContext(t, ctx, otherTeamID)
	store := &APIStore{sqlcDB: testDB.SqlcClient}
	store.GetTemplatesTemplateIDTagsExists(ginCtx, templateID, api.GetTemplatesTemplateIDTagsExistsParams{Tag: "prod"})

	assert.Equal(t, http.StatusNotFound, recorder.Code, "team mismatch should not leak template existence")
}

func TestParseTagGroupsCursor_FormatsAndParses(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC)
	tag := "prod-v1"
	cursor := formatTagGroupsCursor(tagGroupsSortLatestDesc, ts, tag)
	require.True(t, strings.HasPrefix(cursor, "latest_desc|"))

	cursorTime, cursorTag, err := parseTagGroupsCursor(&cursor, tagGroupsSortLatestDesc)
	require.NoError(t, err)
	require.NotNil(t, cursorTime)
	require.NotNil(t, cursorTag)
	assert.True(t, cursorTime.Equal(ts))
	assert.Equal(t, tag, *cursorTag)
}

func TestParseTagGroupsCursor_FirstPageReturnsNilPointers(t *testing.T) {
	t.Parallel()

	for _, sort := range []tagGroupsSort{
		tagGroupsSortLatestDesc, tagGroupsSortLatestAsc,
		tagGroupsSortNameAsc, tagGroupsSortNameDesc,
	} {
		cursorTime, cursorTag, err := parseTagGroupsCursor(nil, sort)
		require.NoError(t, err)
		assert.Nil(t, cursorTime, sort)
		assert.Nil(t, cursorTag, sort)
	}
}

func callTagGroupsHandler(
	t *testing.T,
	ctx context.Context,
	testDB *testutils.Database,
	teamID uuid.UUID,
	templateID string,
	params api.GetTemplatesTemplateIDTagsGroupsParams,
) api.TemplateTagGroupsResponse {
	t.Helper()

	recorder, ginCtx := newTemplateTagsTestContext(t, ctx, teamID)
	store := &APIStore{sqlcDB: testDB.SqlcClient}
	//nolint:contextcheck // GetTemplatesTemplateIDTagsGroups reads ctx from ginCtx.Request.Context().
	store.GetTemplatesTemplateIDTagsGroups(ginCtx, templateID, params)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var response api.TemplateTagGroupsResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))

	return response
}

func callTagCountHandler(t *testing.T, ctx context.Context, testDB *testutils.Database, teamID uuid.UUID, templateID string) api.TemplateTagsCountResponse {
	t.Helper()

	recorder, ginCtx := newTemplateTagsTestContext(t, ctx, teamID)
	store := &APIStore{sqlcDB: testDB.SqlcClient}
	//nolint:contextcheck // GetTemplatesTemplateIDTagsCount reads ctx from ginCtx.Request.Context().
	store.GetTemplatesTemplateIDTagsCount(ginCtx, templateID)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var response api.TemplateTagsCountResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))

	return response
}

func callTagExistsHandler(t *testing.T, ctx context.Context, testDB *testutils.Database, teamID uuid.UUID, templateID string, tag string) api.TemplateTagExistsResponse {
	t.Helper()

	recorder, ginCtx := newTemplateTagsTestContext(t, ctx, teamID)
	store := &APIStore{sqlcDB: testDB.SqlcClient}
	//nolint:contextcheck // GetTemplatesTemplateIDTagsExists reads ctx from ginCtx.Request.Context().
	store.GetTemplatesTemplateIDTagsExists(ginCtx, templateID, api.GetTemplatesTemplateIDTagsExistsParams{Tag: tag})

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var response api.TemplateTagExistsResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))

	return response
}

func newTemplateTagsTestContext(t *testing.T, ctx context.Context, teamID uuid.UUID) (*httptest.ResponseRecorder, *gin.Context) {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{
		Team: &dbtypes.Team{ID: teamID},
	})

	return recorder, ginCtx
}

func insertTemplateTagAssignment(t *testing.T, ctx context.Context, testDB *testutils.Database, templateID string, buildID uuid.UUID, tag string, assignedAt time.Time) uuid.UUID {
	t.Helper()

	var assignmentID uuid.UUID
	err := testDB.SqlcClient.TestsRawSQLQuery(ctx,
		`INSERT INTO public.env_build_assignments (env_id, build_id, tag, source, created_at)
		VALUES ($1, $2, $3, 'app', $4)
		RETURNING id`,
		func(rows pgx.Rows) error {
			if rows.Next() {
				return rows.Scan(&assignmentID)
			}

			return pgx.ErrNoRows
		},
		templateID, buildID, tag, assignedAt,
	)
	require.NoError(t, err, "Failed to create build assignment")

	return assignmentID
}

func softDeleteTestTemplate(t *testing.T, ctx context.Context, testDB *testutils.Database, templateID string) {
	t.Helper()

	err := testDB.SqlcClient.TestsRawSQL(ctx, `UPDATE public.envs SET deleted_at = NOW() WHERE id = $1`, templateID)
	require.NoError(t, err, "Failed to soft-delete template")
}
