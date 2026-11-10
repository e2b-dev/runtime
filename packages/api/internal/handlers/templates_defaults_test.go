package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
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
)

func TestGetDefaultTemplates_UsesNewestReadyDefaultBuildForResources(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, teamID)
	markTemplateDefault(t, ctx, testDB, templateID, new("Base sandbox"))

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	readyBuildID := insertTestBuild(t, ctx, testDB, templateID, "ready", 2, baseTime)
	insertTemplateTagAssignment(t, ctx, testDB, templateID, readyBuildID, "default", baseTime)
	// A newer build tagged default is still building: the template keeps
	// running the ready one, and reports the newer one's status.
	buildingBuildID := insertTestBuild(t, ctx, testDB, templateID, "building", 8, baseTime.Add(time.Hour))
	insertTemplateTagAssignment(t, ctx, testDB, templateID, buildingBuildID, "default", baseTime.Add(time.Hour))
	// A newer ready build under another tag does not change the default.
	betaBuildID := insertTestBuild(t, ctx, testDB, templateID, "ready", 4, baseTime.Add(2*time.Hour))
	insertTemplateTagAssignment(t, ctx, testDB, templateID, betaBuildID, "beta", baseTime.Add(2*time.Hour))

	templates := callDefaultTemplatesHandler(t, ctx, testDB, asAdmin)

	require.Len(t, templates, 1)
	template := templates[0]
	assert.Equal(t, templateID, template.TemplateID)
	assert.Equal(t, new(true), template.IsDefault)
	assert.Equal(t, new("Base sandbox"), template.Description)
	assert.Equal(t, readyBuildID.String(), template.BuildID)
	assert.Equal(t, api.CPUCount(2), template.CpuCount)
	assert.Equal(t, api.MemoryMB(2048), template.MemoryMB)
	assert.Equal(t, api.DiskSizeMB(10240), template.DiskSizeMB)
	assert.Equal(t, "0.2.0", template.EnvdVersion)
	assert.Equal(t, api.TemplateBuildStatusBuilding, template.BuildStatus)
}

func TestGetDefaultTemplates_ListsDefaultWithoutReadyBuild(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	noBuildsID := testutils.CreateTestTemplate(t, testDB, teamID)
	markTemplateDefault(t, ctx, testDB, noBuildsID, nil)
	failedOnlyID := testutils.CreateTestTemplate(t, testDB, teamID)
	markTemplateDefault(t, ctx, testDB, failedOnlyID, nil)

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	failedBuildID := insertTestBuild(t, ctx, testDB, failedOnlyID, "failed", 2, baseTime)
	insertTemplateTagAssignment(t, ctx, testDB, failedOnlyID, failedBuildID, "default", baseTime)

	templates := callDefaultTemplatesHandler(t, ctx, testDB, asAdmin)
	byID := templatesByID(templates)

	// Without a ready build the resources are zero, as in the team template list.
	require.Contains(t, byID, noBuildsID)
	noBuilds := byID[noBuildsID]
	assert.Equal(t, uuid.Nil.String(), noBuilds.BuildID)
	assert.Zero(t, noBuilds.CpuCount)
	assert.Zero(t, noBuilds.MemoryMB)
	assert.Zero(t, noBuilds.DiskSizeMB)
	assert.Empty(t, noBuilds.EnvdVersion)
	assert.Nil(t, noBuilds.Description)
	assert.Equal(t, api.TemplateBuildStatusWaiting, noBuilds.BuildStatus)

	require.Contains(t, byID, failedOnlyID)
	failedOnly := byID[failedOnlyID]
	assert.Equal(t, uuid.Nil.String(), failedOnly.BuildID)
	assert.Zero(t, failedOnly.CpuCount)
	assert.Equal(t, api.TemplateBuildStatusError, failedOnly.BuildStatus)
}

func TestGetDefaultTemplates_LeavesOutDeletedAndNonDefaultTemplates(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, testDB)
	firstID := testutils.CreateTestTemplate(t, testDB, teamID)
	markTemplateDefault(t, ctx, testDB, firstID, nil)
	secondID := testutils.CreateTestTemplate(t, testDB, teamID)
	markTemplateDefault(t, ctx, testDB, secondID, nil)
	deletedID := testutils.CreateTestTemplate(t, testDB, teamID)
	markTemplateDefault(t, ctx, testDB, deletedID, nil)
	softDeleteTestTemplate(t, ctx, testDB, deletedID)
	testutils.CreateTestTemplate(t, testDB, teamID)

	templates := callDefaultTemplatesHandler(t, ctx, testDB, asAdmin)

	got := make([]string, 0, len(templates))
	for _, template := range templates {
		got = append(got, template.TemplateID)
	}
	want := []string{firstID, secondID}
	slices.Sort(want)
	assert.Equal(t, want, got, "only live defaults, ordered by template ID")
}

func TestGetDefaultTemplates_ReportsUsageOnlyToOwnerAndAdmin(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	ownerTeamID := testutils.CreateTestTeam(t, testDB)
	otherTeamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, ownerTeamID)
	markTemplateDefault(t, ctx, testDB, templateID, nil)
	lastSpawnedAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	setTemplateUsage(t, ctx, testDB, templateID, 42, lastSpawnedAt, 7)
	creatorID := setTemplateCreator(t, ctx, testDB, templateID)

	cases := []struct {
		name      string
		caller    defaultTemplatesTestCaller
		seesUsage bool
	}{
		{name: "owner team", caller: asTeam(ownerTeamID), seesUsage: true},
		{name: "admin token", caller: asAdmin, seesUsage: true},
		{name: "admin JWT", caller: asServiceIssuer, seesUsage: true},
		{name: "another team", caller: asTeam(otherTeamID)},
		{name: "user without a team", caller: asUser(uuid.New())},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			templates := callDefaultTemplatesHandler(t, ctx, testDB, tc.caller)
			require.Len(t, templates, 1)
			template := templates[0]

			if tc.seesUsage {
				assert.Equal(t, int64(42), template.SpawnCount)
				require.NotNil(t, template.LastSpawnedAt)
				assert.True(t, template.LastSpawnedAt.Equal(lastSpawnedAt))
				assert.Equal(t, int32(7), template.BuildCount)
				require.NotNil(t, template.CreatedBy)
				assert.Equal(t, creatorID, template.CreatedBy.Id)

				return
			}

			assert.Zero(t, template.SpawnCount)
			assert.Nil(t, template.LastSpawnedAt)
			assert.Zero(t, template.BuildCount)
			assert.Nil(t, template.CreatedBy)
		})
	}
}

func TestGetDefaultTemplates_ListsPrivateDefaultsOnlyToOwnerAndAdmin(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	ctx := t.Context()
	ownerTeamID := testutils.CreateTestTeam(t, testDB)
	otherTeamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, ownerTeamID)
	markTemplateDefault(t, ctx, testDB, templateID, nil)
	setTemplatePublic(ctx, t, testDB, templateID, false)

	cases := []struct {
		name   string
		caller defaultTemplatesTestCaller
		listed bool
	}{
		{name: "owner team", caller: asTeam(ownerTeamID), listed: true},
		{name: "admin token", caller: asAdmin, listed: true},
		{name: "another team", caller: asTeam(otherTeamID)},
		{name: "user without a team", caller: asUser(uuid.New())},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			templates := callDefaultTemplatesHandler(t, ctx, testDB, tc.caller)
			if tc.listed {
				require.Len(t, templates, 1)
				assert.False(t, templates[0].Public)

				return
			}

			assert.Empty(t, templates)
		})
	}
}

type defaultTemplatesTestCaller func(t *testing.T, c *gin.Context)

func asAdmin(*testing.T, *gin.Context) {}

func asServiceIssuer(t *testing.T, c *gin.Context) {
	t.Helper()

	auth.SetServiceIssuerForTest(t, c, "https://issuer.example")
}

func asTeam(teamID uuid.UUID) defaultTemplatesTestCaller {
	return func(t *testing.T, c *gin.Context) {
		t.Helper()

		auth.SetTeamInfoForTest(t, c, &authtypes.Team{Team: &dbtypes.Team{ID: teamID}})
	}
}

func asUser(userID uuid.UUID) defaultTemplatesTestCaller {
	return func(t *testing.T, c *gin.Context) {
		t.Helper()

		auth.SetUserIDForTest(t, c, userID)
	}
}

func callDefaultTemplatesHandler(t *testing.T, ctx context.Context, testDB *testutils.Database, caller defaultTemplatesTestCaller) []api.Template {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(ctx, http.MethodGet, "/default-templates", nil)
	caller(t, ginCtx)

	store := &APIStore{sqlcDB: testDB.SqlcClient}
	//nolint:contextcheck // GetDefaultTemplates reads ctx from ginCtx.Request.Context().
	store.GetDefaultTemplates(ginCtx)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var templates []api.Template
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &templates))

	return templates
}

func templatesByID(templates []api.Template) map[string]api.Template {
	byID := make(map[string]api.Template, len(templates))
	for _, template := range templates {
		byID[template.TemplateID] = template
	}

	return byID
}

func markTemplateDefault(t *testing.T, ctx context.Context, testDB *testutils.Database, templateID string, description *string) {
	t.Helper()

	err := testDB.SqlcClient.TestsRawSQL(ctx, `INSERT INTO public.env_defaults (env_id, description) VALUES ($1, $2)`, templateID, description)
	require.NoError(t, err, "Failed to mark template as default")
}

func setTemplateUsage(t *testing.T, ctx context.Context, testDB *testutils.Database, templateID string, spawnCount int64, lastSpawnedAt time.Time, buildCount int32) {
	t.Helper()

	err := testDB.SqlcClient.TestsRawSQL(ctx,
		`UPDATE public.envs SET spawn_count = $2, last_spawned_at = $3, build_count = $4 WHERE id = $1`,
		templateID, spawnCount, lastSpawnedAt, buildCount,
	)
	require.NoError(t, err, "Failed to set template usage")
}

// setTemplateCreator creates a user and records it as the template's creator.
func setTemplateCreator(t *testing.T, ctx context.Context, testDB *testutils.Database, templateID string) uuid.UUID {
	t.Helper()

	userID := uuid.New()
	err := testDB.AuthDB.TestsRawSQL(ctx, `INSERT INTO public.users (id) VALUES ($1)`, userID)
	require.NoError(t, err, "Failed to create the template's creator")

	err = testDB.SqlcClient.TestsRawSQL(ctx, `UPDATE public.envs SET created_by = $2 WHERE id = $1`, templateID, userID)
	require.NoError(t, err, "Failed to set the template's creator")

	return userID
}

// insertTestBuild creates a build with known resources and creation time:
// vcpu as given, 2048 MiB of memory, a 10240 MiB disk and envd 0.2.0.
func insertTestBuild(t *testing.T, ctx context.Context, testDB *testutils.Database, templateID string, status string, vcpu int64, createdAt time.Time) uuid.UUID {
	t.Helper()

	buildID := uuid.New()
	err := testDB.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.env_builds
		(id, env_id, team_id, status, vcpu, ram_mb, free_disk_size_mb, total_disk_size_mb, envd_version, kernel_version, firecracker_version, cluster_node_id, created_at, updated_at)
		VALUES ($1, $2, (SELECT team_id FROM public.envs WHERE id = $2), $3, $4, 2048, 512, 10240, '0.2.0', '6.1.0', '1.4.0', 'test-node', $5, $5)`,
		buildID, templateID, status, vcpu, createdAt,
	)
	require.NoError(t, err, "Failed to create test build")

	return buildID
}
