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
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
)

func serveTemplateInfo(t *testing.T, f templateBuildsFixture, teamID uuid.UUID, teamSlug, template string) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v2/templates/template", nil)
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{Team: &dbtypes.Team{ID: teamID, Slug: teamSlug}})

	f.store.GetV2TemplatesTemplateID(ginCtx, template)

	return recorder
}

func templateInfo(t *testing.T, f templateBuildsFixture, template string) (api.TemplateV2, map[string]json.RawMessage) {
	t.Helper()

	recorder := serveTemplateInfo(t, f, f.teamID, f.teamSlug, template)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var info api.TemplateV2
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &info))
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &fields))

	return info, fields
}

func TestGetV2TemplatesTemplateID_ReturnsTheTemplateWithItsDefaultBuild(t *testing.T) {
	t.Parallel()

	f := newTemplateBuildsFixture(t)
	// Move default to the resizable build, so the default build carries a max.
	insertTemplateTagAssignment(t, t.Context(), f.testDB, f.templateID, f.resizable, "default", time.Date(2026, 5, 29, 10, 0, 0, 0, time.UTC))

	bodies := make([]string, 0, 3)
	for _, template := range []string{f.templateID, f.alias, f.teamSlug + "/" + f.alias} {
		recorder := serveTemplateInfo(t, f, f.teamID, f.teamSlug, template)
		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		bodies = append(bodies, recorder.Body.String())
	}
	assert.Equal(t, bodies[0], bodies[1], "ID and name give the same template")
	assert.Equal(t, bodies[0], bodies[2], "ID and namespaced name give the same template")

	info, fields := templateInfo(t, f, f.templateID)
	assert.NotContains(t, fields, "builds")
	assert.NotContains(t, fields, "aliases", "the deprecated aliases are not carried to v2")
	assert.Equal(t, f.templateID, info.TemplateID)
	assert.Equal(t, []string{f.teamSlug + "/" + f.alias}, info.Names)
	assert.Equal(t, api.TemplateBuildStatusReady, info.BuildStatus)

	require.NotNil(t, info.DefaultBuild)
	assert.Equal(t, f.resizable, info.DefaultBuild.BuildID)
	assert.Equal(t, api.TemplateBuildStatusReady, info.DefaultBuild.Status)
	assert.Equal(t, api.CPUCount(2), info.DefaultBuild.CpuCount)
	assert.Equal(t, new(api.CPUCount(4)), info.DefaultBuild.MaxCpuCount)
}

// While a rebuild runs, sandboxes still start from the last ready build: that
// build stays the default build, and the build status reports the rebuild.
func TestGetV2TemplatesTemplateID_KeepsTheReadyDefaultBuildDuringARebuild(t *testing.T) {
	t.Parallel()

	f := newTemplateBuildsFixture(t)
	at := time.Date(2026, 5, 29, 10, 0, 0, 0, time.UTC)
	rebuild := insertTestBuild(t, t.Context(), f.testDB, f.templateID, dbtypes.BuildStatusGroupInProgress, 8, at)
	insertTemplateTagAssignment(t, t.Context(), f.testDB, f.templateID, rebuild, "default", at)

	info, _ := templateInfo(t, f, f.templateID)

	assert.Equal(t, api.TemplateBuildStatusBuilding, info.BuildStatus)
	require.NotNil(t, info.DefaultBuild)
	assert.Equal(t, f.fixed, info.DefaultBuild.BuildID)
}

// Until a build tagged default is ready, the template has no default build.
func TestGetV2TemplatesTemplateID_HasNoDefaultBuildBeforeTheFirstReadyBuild(t *testing.T) {
	t.Parallel()

	f := newTemplateBuildsFixture(t)
	templateID := testutils.CreateTestTemplate(t, f.testDB, f.teamID)
	at := time.Date(2026, 5, 29, 10, 0, 0, 0, time.UTC)
	first := insertTestBuild(t, t.Context(), f.testDB, templateID, dbtypes.BuildStatusGroupPending, 2, at)
	insertTemplateTagAssignment(t, t.Context(), f.testDB, templateID, first, "default", at)

	info, fields := templateInfo(t, f, templateID)

	assert.Equal(t, api.TemplateBuildStatusWaiting, info.BuildStatus)
	assert.Nil(t, info.DefaultBuild)
	assert.NotContains(t, fields, "defaultBuild")
}

func TestGetV2TemplatesTemplateID_RejectsOtherTeamsAndUnknownTemplates(t *testing.T) {
	t.Parallel()

	f := newTemplateBuildsFixture(t)

	missing := serveTemplateInfo(t, f, f.teamID, f.teamSlug, "no-such-template")
	assert.Equal(t, http.StatusNotFound, missing.Code, missing.Body.String())

	// Another team's template is answered word for word like a missing one.
	for _, reference := range []string{f.templateID, f.teamSlug + "/" + f.alias} {
		otherTeam := serveTemplateInfo(t, f, uuid.New(), "other-team", reference)
		assert.Equal(t, http.StatusNotFound, otherTeam.Code, otherTeam.Body.String())
		assert.Equal(t, missing.Body.String(), otherTeam.Body.String())
	}

	invalid := serveTemplateInfo(t, f, f.teamID, f.teamSlug, "Not A Name!")
	assert.Equal(t, http.StatusBadRequest, invalid.Code, invalid.Body.String())

	// The path names a template, not one of its builds.
	for _, tagged := range []string{f.templateID + ":v2", f.alias + ":default"} {
		recorder := serveTemplateInfo(t, f, f.teamID, f.teamSlug, tagged)
		assert.Equal(t, http.StatusBadRequest, recorder.Code, tagged)
	}
}
