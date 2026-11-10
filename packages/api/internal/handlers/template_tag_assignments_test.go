package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestGetTemplatesTemplateIDTagsTagAssignments_ReturnsLatestFirst(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	oldBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, "ready")
	newBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, "ready")

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	oldAssignmentID := insertTemplateTagAssignment(t, ctx, testDB, templateID, oldBuildID, "prod", baseTime)
	newAssignmentID := insertTemplateTagAssignment(t, ctx, testDB, templateID, newBuildID, "prod", baseTime.Add(time.Hour))

	response := callTagAssignmentsHandler(t, ctx, testDB, teamID, templateID, "prod", api.GetTemplatesTemplateIDTagsTagAssignmentsParams{})

	require.Len(t, response.Data, 2)
	assert.Equal(t, newAssignmentID, response.Data[0].AssignmentId)
	assert.Equal(t, newBuildID, response.Data[0].BuildId)
	assert.Equal(t, oldAssignmentID, response.Data[1].AssignmentId)
	assert.Equal(t, oldBuildID, response.Data[1].BuildId)
	assert.Nil(t, response.NextCursor)
}

func TestGetTemplatesTemplateIDTagsTagAssignments_PaginatesAcrossPages(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	const totalAssignments = 5
	assignmentIDs := make([]uuid.UUID, 0, totalAssignments)
	for i := range totalAssignments {
		buildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, "ready")
		assignmentIDs = append(assignmentIDs, insertTemplateTagAssignment(t, ctx, testDB, templateID, buildID, "prod", baseTime.Add(time.Duration(i)*time.Hour)))
	}

	limit := api.TagAssignmentsLimit(2)
	first := callTagAssignmentsHandler(t, ctx, testDB, teamID, templateID, "prod", api.GetTemplatesTemplateIDTagsTagAssignmentsParams{Limit: &limit})
	require.Len(t, first.Data, 2)
	require.NotNil(t, first.NextCursor)
	assert.Equal(t, assignmentIDs[4], first.Data[0].AssignmentId)
	assert.Equal(t, assignmentIDs[3], first.Data[1].AssignmentId)

	second := callTagAssignmentsHandler(t, ctx, testDB, teamID, templateID, "prod", api.GetTemplatesTemplateIDTagsTagAssignmentsParams{Limit: &limit, Cursor: first.NextCursor})
	require.Len(t, second.Data, 2)
	require.NotNil(t, second.NextCursor)
	assert.Equal(t, assignmentIDs[2], second.Data[0].AssignmentId)
	assert.Equal(t, assignmentIDs[1], second.Data[1].AssignmentId)

	third := callTagAssignmentsHandler(t, ctx, testDB, teamID, templateID, "prod", api.GetTemplatesTemplateIDTagsTagAssignmentsParams{Limit: &limit, Cursor: second.NextCursor})
	require.Len(t, third.Data, 1)
	assert.Nil(t, third.NextCursor)
	assert.Equal(t, assignmentIDs[0], third.Data[0].AssignmentId)
}

func TestGetTemplatesTemplateIDTagsTagAssignments_StableKeysetOnSameTimestamp(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	// Three assignments share a timestamp; the assignment id breaks the tie.
	sameTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	for range 3 {
		buildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, "ready")
		insertTemplateTagAssignment(t, ctx, testDB, templateID, buildID, "prod", sameTime)
	}

	limit := api.TagAssignmentsLimit(2)
	first := callTagAssignmentsHandler(t, ctx, testDB, teamID, templateID, "prod", api.GetTemplatesTemplateIDTagsTagAssignmentsParams{Limit: &limit})
	require.Len(t, first.Data, 2)
	require.NotNil(t, first.NextCursor)

	second := callTagAssignmentsHandler(t, ctx, testDB, teamID, templateID, "prod", api.GetTemplatesTemplateIDTagsTagAssignmentsParams{Limit: &limit, Cursor: first.NextCursor})
	require.Len(t, second.Data, 1)
	assert.Nil(t, second.NextCursor)

	seen := map[uuid.UUID]struct{}{}
	for _, assignment := range append(first.Data, second.Data...) {
		seen[assignment.AssignmentId] = struct{}{}
	}
	assert.Len(t, seen, 3, "every assignment appears exactly once across pages")
}

func TestGetTemplatesTemplateIDTagsTagAssignments_FiltersNonReadyBuilds(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	readyBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, "ready")
	failedBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, "failed")
	waitingBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, "waiting")

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	readyAssignmentID := insertTemplateTagAssignment(t, ctx, testDB, templateID, readyBuildID, "prod", baseTime)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, failedBuildID, "prod", baseTime.Add(time.Hour))
	insertTemplateTagAssignment(t, ctx, testDB, templateID, waitingBuildID, "prod", baseTime.Add(2*time.Hour))

	response := callTagAssignmentsHandler(t, ctx, testDB, teamID, templateID, "prod", api.GetTemplatesTemplateIDTagsTagAssignmentsParams{})

	require.Len(t, response.Data, 1)
	assert.Equal(t, readyAssignmentID, response.Data[0].AssignmentId)
}

func TestGetTemplatesTemplateIDTagsTagAssignments_FiltersOtherTags(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	prodBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, "ready")
	devBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, "ready")

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	prodAssignmentID := insertTemplateTagAssignment(t, ctx, testDB, templateID, prodBuildID, "prod", baseTime)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, devBuildID, "dev", baseTime.Add(time.Hour))

	response := callTagAssignmentsHandler(t, ctx, testDB, teamID, templateID, "prod", api.GetTemplatesTemplateIDTagsTagAssignmentsParams{})

	require.Len(t, response.Data, 1)
	assert.Equal(t, prodAssignmentID, response.Data[0].AssignmentId)
}

func TestGetTemplatesTemplateIDTagsTagAssignments_ReturnsEmptyForUnknownTag(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	readyBuildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, "ready")
	insertTemplateTagAssignment(t, ctx, testDB, templateID, readyBuildID, "prod", time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC))

	response := callTagAssignmentsHandler(t, ctx, testDB, teamID, templateID, "missing", api.GetTemplatesTemplateIDTagsTagAssignmentsParams{})

	assert.Empty(t, response.Data)
	assert.Nil(t, response.NextCursor)
}

func TestGetTemplatesTemplateIDTagsTagAssignments_NormalizesTag(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, testDB, templateID, "ready")
	assignmentID := insertTemplateTagAssignment(t, ctx, testDB, templateID, buildID, "prod", time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC))

	response := callTagAssignmentsHandler(t, ctx, testDB, teamID, templateID, " PROD ", api.GetTemplatesTemplateIDTagsTagAssignmentsParams{})

	require.Len(t, response.Data, 1)
	assert.Equal(t, assignmentID, response.Data[0].AssignmentId)
}

func TestGetTemplatesTemplateIDTagsTagAssignments_NotFoundForOtherTeam(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	ownerTeamID := testutils.CreateTestTeam(t, testDB)
	otherTeamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, ownerTeamID)

	recorder, ginCtx := newTemplateTagsTestContext(t, ctx, otherTeamID)
	store := &APIStore{sqlcDB: testDB.SqlcClient}

	store.GetTemplatesTemplateIDTagsTagAssignments(ginCtx, templateID, "prod", api.GetTemplatesTemplateIDTagsTagAssignmentsParams{})

	assert.Equal(t, http.StatusNotFound, recorder.Code, "team mismatch should not leak template existence")
}

func TestGetTemplatesTemplateIDTagsTagAssignments_NotFoundForUnknownTemplate(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)

	recorder, ginCtx := newTemplateTagsTestContext(t, ctx, teamID)
	store := &APIStore{sqlcDB: testDB.SqlcClient}

	store.GetTemplatesTemplateIDTagsTagAssignments(ginCtx, "does-not-exist", "prod", api.GetTemplatesTemplateIDTagsTagAssignmentsParams{})

	assert.Equal(t, http.StatusNotFound, recorder.Code)
}

func TestGetTemplatesTemplateIDTagsTagAssignments_InvalidTagReturnsBadRequest(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	recorder, ginCtx := newTemplateTagsTestContext(t, ctx, teamID)
	store := &APIStore{sqlcDB: testDB.SqlcClient}

	store.GetTemplatesTemplateIDTagsTagAssignments(ginCtx, templateID, "bad!tag", api.GetTemplatesTemplateIDTagsTagAssignmentsParams{})

	assert.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestGetTemplatesTemplateIDTagsTagAssignments_InvalidCursorReturnsBadRequest(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	recorder, ginCtx := newTemplateTagsTestContext(t, ctx, teamID)
	store := &APIStore{sqlcDB: testDB.SqlcClient}

	badCursor := api.TagAssignmentsCursor("not-a-valid-cursor")
	store.GetTemplatesTemplateIDTagsTagAssignments(ginCtx, templateID, "prod", api.GetTemplatesTemplateIDTagsTagAssignmentsParams{Cursor: &badCursor})

	assert.Equal(t, http.StatusBadRequest, recorder.Code)
}

func callTagAssignmentsHandler(
	t *testing.T,
	ctx context.Context,
	testDB *testutils.Database,
	teamID uuid.UUID,
	templateID string,
	tag api.Tag,
	params api.GetTemplatesTemplateIDTagsTagAssignmentsParams,
) api.TemplateTagAssignmentsResponse {
	t.Helper()

	recorder, ginCtx := newTemplateTagsTestContext(t, ctx, teamID)
	store := &APIStore{sqlcDB: testDB.SqlcClient}
	//nolint:contextcheck // GetTemplatesTemplateIDTagsTagAssignments reads ctx from ginCtx.Request.Context().
	store.GetTemplatesTemplateIDTagsTagAssignments(ginCtx, templateID, tag, params)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var response api.TemplateTagAssignmentsResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))

	return response
}
