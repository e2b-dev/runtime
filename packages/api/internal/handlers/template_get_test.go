package handlers

import (
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
)

func TestGetTemplatesTemplateID_SummarisesTheNewestReadyDefaultBuild(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	setTemplateUsage(t, ctx, testDB, templateID, 1, time.Now(), 3)
	creatorID := setTemplateCreator(t, ctx, testDB, templateID)
	markTemplateDefault(t, ctx, testDB, templateID, new("Base sandbox"))

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	readyBuildID := insertTestBuild(t, ctx, testDB, templateID, "ready", 2, baseTime)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, readyBuildID, "default", baseTime)
	failedBuildID := insertTestBuild(t, ctx, testDB, templateID, "failed", 8, baseTime.Add(time.Hour))
	insertTemplateTagAssignment(t, ctx, testDB, templateID, failedBuildID, "default", baseTime.Add(time.Hour))

	template := callTemplateGetHandler(t, testDB, teamID, templateID)

	assert.Equal(t, readyBuildID.String(), template.BuildID)
	assert.Equal(t, api.CPUCount(2), template.CpuCount)
	assert.Equal(t, api.MemoryMB(2048), template.MemoryMB)
	assert.Equal(t, api.DiskSizeMB(10240), template.DiskSizeMB)
	assert.Equal(t, "0.2.0", template.EnvdVersion)
	assert.Equal(t, api.TemplateBuildStatusError, template.BuildStatus, "the newest default build failed")
	assert.Equal(t, int32(3), template.BuildCount)
	require.NotNil(t, template.CreatedBy)
	assert.Equal(t, creatorID, template.CreatedBy.Id)
	assert.Equal(t, new(true), template.IsDefault)
	assert.Equal(t, new("Base sandbox"), template.Description)
	assert.Len(t, template.Builds, 2, "the build list still covers every build")
}

// Without a ready build tagged default, the build fields carry the zero values
// of the template lists.
func TestGetTemplatesTemplateID_ZeroBuildFieldsWithoutAReadyDefaultBuild(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	readyBetaBuildID := insertTestBuild(t, ctx, testDB, templateID, "ready", 2, baseTime)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, readyBetaBuildID, "beta", baseTime)

	template := callTemplateGetHandler(t, testDB, teamID, templateID)

	assert.Equal(t, uuid.Nil.String(), template.BuildID)
	assert.Zero(t, template.CpuCount)
	assert.Zero(t, template.MemoryMB)
	assert.Zero(t, template.DiskSizeMB)
	assert.Empty(t, template.EnvdVersion)
	assert.Equal(t, api.TemplateBuildStatusWaiting, template.BuildStatus)
	assert.Equal(t, new(false), template.IsDefault)
	assert.Nil(t, template.Description)
	assert.Len(t, template.Builds, 1)
}

func callTemplateGetHandler(t *testing.T, testDB *testutils.Database, teamID uuid.UUID, templateID string) api.TemplateWithBuilds {
	t.Helper()

	recorder := serveTemplateGet(t, testDB, teamID, templateID)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var template api.TemplateWithBuilds
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &template))

	return template
}

func serveTemplateGet(t *testing.T, testDB *testutils.Database, teamID uuid.UUID, templateID string) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/templates/"+templateID, nil)
	asTeam(teamID)(t, ginCtx)

	store := &APIStore{sqlcDB: testDB.SqlcClient}
	store.GetTemplatesTemplateID(ginCtx, templateID, api.GetTemplatesTemplateIDParams{})

	return recorder
}
