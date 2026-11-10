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
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func TestListBuilds_PaginatesNewestFirstWithinTheTeam(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	now := time.Now().UTC().Truncate(time.Second)

	// The two older builds share a second, so the cursor must keep the
	// fraction for the second page to start between them.
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	oldest := insertAssignedTestBuild(t, ctx, testDB, templateID, "success", now.Add(-2*time.Hour+200*time.Millisecond))
	middle := insertAssignedTestBuild(t, ctx, testDB, templateID, "success", now.Add(-2*time.Hour+600*time.Millisecond))
	newest := insertAssignedTestBuild(t, ctx, testDB, templateID, "success", now.Add(-time.Hour))

	otherTemplateID := testutils.CreateTestTemplate(t, testDB, testutils.CreateTestTeam(t, testDB))
	insertAssignedTestBuild(t, ctx, testDB, otherTemplateID, "success", now)

	first, cursor := listBuilds(t, testDB, teamID, api.GetUnstableBuildsParams{Limit: new(int32(2))})
	require.Equal(t, []uuid.UUID{newest, middle}, buildIDs(first))
	require.NotEmpty(t, cursor)

	second, cursor := listBuilds(t, testDB, teamID, api.GetUnstableBuildsParams{Limit: new(int32(2)), Cursor: &cursor})
	assert.Equal(t, []uuid.UUID{oldest}, buildIDs(second))
	assert.Empty(t, cursor, "the last page has no next cursor")
}

// The first page starts from a fixed far-future position, so a build whose
// creation time is ahead of the app's clock is still listed.
func TestListBuilds_FirstPageListsBuildsAheadOfTheAppClock(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)

	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	ahead := insertAssignedTestBuild(t, ctx, testDB, templateID, "building", time.Now().UTC().Add(time.Hour))

	builds, _ := listBuilds(t, testDB, teamID, api.GetUnstableBuildsParams{})

	assert.Equal(t, []uuid.UUID{ahead}, buildIDs(builds))
}

func TestListBuilds_FiltersByBuildIDTemplateIDOrAlias(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	now := time.Now().UTC().Truncate(time.Second)

	plainTemplate := testutils.CreateTestTemplate(t, testDB, teamID)
	plainOld := insertAssignedTestBuild(t, ctx, testDB, plainTemplate, "success", now.Add(-2*time.Hour))
	plainNew := insertAssignedTestBuild(t, ctx, testDB, plainTemplate, "success", now.Add(-time.Hour))

	aliasedTemplate, alias := testutils.CreateTestTemplateWithAlias(t, testDB, teamID)
	aliased := insertAssignedTestBuild(t, ctx, testDB, aliasedTemplate, "success", now)

	for name, tc := range map[string]struct {
		filter       string
		wantIDs      []uuid.UUID
		wantTemplate string
	}{
		"a build ID lists that build":               {filter: plainOld.String(), wantIDs: []uuid.UUID{plainOld}, wantTemplate: plainTemplate},
		"a template ID lists the template's builds": {filter: plainTemplate, wantIDs: []uuid.UUID{plainNew, plainOld}, wantTemplate: plainTemplate},
		"an alias lists its template's builds":      {filter: alias, wantIDs: []uuid.UUID{aliased}, wantTemplate: alias},
		"surrounding whitespace is ignored":         {filter: "  " + alias + " ", wantIDs: []uuid.UUID{aliased}, wantTemplate: alias},
		"a build ID of no team build lists nothing": {filter: uuid.NewString()},
		"a name matching no template lists nothing": {filter: "no-such-template"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			builds, _ := listBuilds(t, testDB, teamID, api.GetUnstableBuildsParams{BuildIdOrTemplate: &tc.filter})

			assert.Equal(t, tc.wantIDs, buildIDs(builds))
			for _, build := range builds {
				assert.Equal(t, tc.wantTemplate, build.Template)
			}
		})
	}
}

func TestListBuilds_FiltersByStatus(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	now := time.Now().UTC().Truncate(time.Second)

	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	failed := insertAssignedTestBuild(t, ctx, testDB, templateID, "failed", now.Add(-4*time.Hour))
	setBuildReasonMessage(t, ctx, testDB, failed, "step 3 failed")
	uploaded := insertAssignedTestBuild(t, ctx, testDB, templateID, "uploaded", now.Add(-3*time.Hour))
	building := insertAssignedTestBuild(t, ctx, testDB, templateID, "building", now.Add(-2*time.Hour))
	pending := insertAssignedTestBuild(t, ctx, testDB, templateID, "pending", now.Add(-time.Hour))

	for name, tc := range map[string]struct {
		statuses   *api.BuildStatuses
		wantIDs    []uuid.UUID
		wantStatus api.BuildStatus
	}{
		"building covers pending and running builds": {statuses: &api.BuildStatuses{api.BuildStatusBuilding}, wantIDs: []uuid.UUID{pending, building}, wantStatus: api.BuildStatusBuilding},
		"success covers uploaded builds":             {statuses: &api.BuildStatuses{api.BuildStatusSuccess}, wantIDs: []uuid.UUID{uploaded}, wantStatus: api.BuildStatusSuccess},
		"failed lists failed builds":                 {statuses: &api.BuildStatuses{api.BuildStatusFailed}, wantIDs: []uuid.UUID{failed}, wantStatus: api.BuildStatusFailed},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			builds, _ := listBuilds(t, testDB, teamID, api.GetUnstableBuildsParams{Statuses: tc.statuses})

			assert.Equal(t, tc.wantIDs, buildIDs(builds))
			for _, build := range builds {
				assert.Equal(t, tc.wantStatus, build.Status)
			}
		})
	}

	t.Run("no status lists every build", func(t *testing.T) {
		t.Parallel()

		builds, _ := listBuilds(t, testDB, teamID, api.GetUnstableBuildsParams{})

		assert.Equal(t, []uuid.UUID{pending, building, uploaded, failed}, buildIDs(builds))
	})

	t.Run("only a failed build carries its failure message", func(t *testing.T) {
		t.Parallel()

		builds, _ := listBuilds(t, testDB, teamID, api.GetUnstableBuildsParams{})

		for _, build := range builds {
			if build.Id == failed {
				assert.Equal(t, new("step 3 failed"), build.StatusMessage)
			} else {
				assert.Nil(t, build.StatusMessage)
			}
		}
	})
}

func TestListBuilds_RejectsMalformedCursors(t *testing.T) {
	t.Parallel()

	for name, cursor := range map[string]string{
		"without a separator": "2026-01-01T00:00:00Z",
		"with a bad time":     "yesterday|" + uuid.NewString(),
		"with a bad build ID": "2026-01-01T00:00:00Z|not-a-uuid",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			recorder := serveBuildsHandler(t, &APIStore{}, teamTestCaller(uuid.New(), nil), func(store *APIStore, c *gin.Context) {
				store.GetUnstableBuilds(c, api.GetUnstableBuildsParams{Cursor: &cursor})
			})

			assert.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		})
	}
}

func TestBuildsStatuses_ReturnsOnlyTheTeamsBuilds(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	now := time.Now().UTC().Truncate(time.Second)

	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	running := insertAssignedTestBuild(t, ctx, testDB, templateID, "building", now.Add(-time.Hour))
	failed := insertAssignedTestBuild(t, ctx, testDB, templateID, "failed", now)
	setBuildReasonMessage(t, ctx, testDB, failed, "out of disk")

	otherTemplateID := testutils.CreateTestTemplate(t, testDB, testutils.CreateTestTeam(t, testDB))
	otherTeams := insertAssignedTestBuild(t, ctx, testDB, otherTemplateID, "building", now)

	recorder := serveBuildsHandler(t, &APIStore{sqlcDB: testDB.SqlcClient}, teamTestCaller(teamID, nil), func(store *APIStore, c *gin.Context) {
		store.GetUnstableBuildsStatuses(c, api.GetUnstableBuildsStatusesParams{BuildIds: []uuid.UUID{running, failed, otherTeams}})
	})
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var response api.BuildsStatusesResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))

	byID := make(map[uuid.UUID]api.BuildStatusItem, len(response.BuildStatuses))
	for _, status := range response.BuildStatuses {
		byID[status.Id] = status
	}
	require.Len(t, byID, 2, "another team's build is left out")
	assert.Equal(t, api.BuildStatusBuilding, byID[running].Status)
	assert.Nil(t, byID[running].StatusMessage)
	assert.Equal(t, api.BuildStatusFailed, byID[failed].Status)
	assert.Equal(t, new("out of disk"), byID[failed].StatusMessage)
}

func TestBuildsStatuses_RejectsMoreThanAHundredIDs(t *testing.T) {
	t.Parallel()

	ids := make([]uuid.UUID, maxBuildStatusesIDs+1)
	for i := range ids {
		ids[i] = uuid.New()
	}

	recorder := serveBuildsHandler(t, &APIStore{}, teamTestCaller(uuid.New(), nil), func(store *APIStore, c *gin.Context) {
		store.GetUnstableBuildsStatuses(c, api.GetUnstableBuildsStatusesParams{BuildIds: ids})
	})

	assert.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestGetBuild_ReturnsTheBuildWithItsTemplateNames(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	createdAt := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)

	templateID, alias := testutils.CreateTestTemplateWithAlias(t, testDB, teamID)
	buildID := insertAssignedTestBuild(t, ctx, testDB, templateID, "failed", createdAt)
	setBuildReasonMessage(t, ctx, testDB, buildID, "step 3 failed")

	recorder := serveGetBuild(t, testDB, teamID, buildID.String())
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var build api.BuildInfo
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &build))
	assert.Equal(t, &[]string{alias}, build.Names)
	assert.True(t, createdAt.Equal(build.CreatedAt))
	assert.Equal(t, api.BuildStatusFailed, build.Status)
	assert.Equal(t, new("step 3 failed"), build.StatusMessage)
}

func TestGetBuild_RefusesBuildsItCannotRead(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)

	otherTemplateID := testutils.CreateTestTemplate(t, testDB, testutils.CreateTestTeam(t, testDB))
	otherTeams := insertAssignedTestBuild(t, ctx, testDB, otherTemplateID, "success", time.Now())

	deletedTemplateID := testutils.CreateTestTemplate(t, testDB, teamID)
	ofDeletedTemplate := insertAssignedTestBuild(t, ctx, testDB, deletedTemplateID, "success", time.Now())
	softDeleteTestTemplate(t, ctx, testDB, deletedTemplateID)

	for name, tc := range map[string]struct {
		buildID  string
		wantCode int
	}{
		"another team's build is not found":        {buildID: otherTeams.String(), wantCode: http.StatusNotFound},
		"a deleted template's build is not found":  {buildID: ofDeletedTemplate.String(), wantCode: http.StatusNotFound},
		"a build ID that is not a UUID is refused": {buildID: "not-a-uuid", wantCode: http.StatusBadRequest},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			recorder := serveGetBuild(t, testDB, teamID, tc.buildID)

			assert.Equal(t, tc.wantCode, recorder.Code, recorder.Body.String())
		})
	}
}

func serveBuildsHandler(t *testing.T, store *APIStore, caller defaultTemplatesTestCaller, call func(*APIStore, *gin.Context)) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/unstable/builds", nil)
	caller(t, ginCtx)

	call(store, ginCtx)

	return recorder
}

func listBuilds(t *testing.T, testDB *testutils.Database, teamID uuid.UUID, params api.GetUnstableBuildsParams) ([]api.ListedBuild, string) {
	t.Helper()

	recorder := serveBuildsHandler(t, &APIStore{sqlcDB: testDB.SqlcClient}, teamTestCaller(teamID, nil), func(store *APIStore, c *gin.Context) {
		store.GetUnstableBuilds(c, params)
	})
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var response api.BuildsListResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))

	return response.Data, sharedUtils.DerefOrDefault(response.NextCursor, "")
}

func serveGetBuild(t *testing.T, testDB *testutils.Database, teamID uuid.UUID, buildID string) *httptest.ResponseRecorder {
	t.Helper()

	return serveBuildsHandler(t, &APIStore{sqlcDB: testDB.SqlcClient}, teamTestCaller(teamID, nil), func(store *APIStore, c *gin.Context) {
		store.GetUnstableBuildsBuildID(c, buildID)
	})
}

func buildIDs(builds []api.ListedBuild) []uuid.UUID {
	var ids []uuid.UUID
	for _, build := range builds {
		ids = append(ids, build.Id)
	}

	return ids
}

func setBuildReasonMessage(t *testing.T, ctx context.Context, testDB *testutils.Database, buildID uuid.UUID, message string) {
	t.Helper()

	err := testDB.SqlcClient.TestsRawSQL(ctx, `UPDATE public.env_builds SET reason = jsonb_build_object('message', $2::text) WHERE id = $1`, buildID, message)
	require.NoError(t, err, "Failed to set build reason")
}

// insertAssignedTestBuild inserts a build and assigns it to its template, as
// the builds queries only list assigned builds.
func insertAssignedTestBuild(t *testing.T, ctx context.Context, testDB *testutils.Database, templateID string, status string, createdAt time.Time) uuid.UUID {
	t.Helper()

	buildID := insertTestBuild(t, ctx, testDB, templateID, status, 2, createdAt)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, buildID, "default", createdAt)

	return buildID
}
