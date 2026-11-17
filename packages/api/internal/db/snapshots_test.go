package db

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
)

// createTestSnapshot creates a snapshot using UpsertSnapshot and returns the sandbox_id and env_id
func createTestSnapshot(t *testing.T, db *testutils.Database, teamID uuid.UUID, baseEnvID string) (string, string, uuid.UUID) {
	t.Helper()

	sandboxID := "sandbox-" + uuid.New().String()
	envID := "env-" + uuid.New().String()
	kernelVersion := "6.1.0"
	firecrackerVersion := "1.4.0"
	envdVersion := "v1.0.0"
	totalDiskSize := int64(1024)
	allowInternet := true

	// Source build the snapshot copies its CPU info from
	sourceBuildID := testutils.CreateTestBuild(t, t.Context(), db, baseEnvID, types.BuildStatusGroupReady)

	// UpsertSnapshot will create the env automatically, so we pass envID as TemplateID
	params := queries.UpsertSnapshotParams{
		TemplateID:          envID,
		TeamID:              teamID,
		SandboxID:           sandboxID,
		BaseTemplateID:      baseEnvID,
		StartedAt:           pgtype.Timestamptz{Time: time.Now(), Valid: true},
		Vcpu:                2,
		RamMb:               2048,
		TotalDiskSizeMb:     &totalDiskSize,
		Metadata:            types.JSONBStringMap{"key": "value", "type": "test"},
		KernelVersion:       kernelVersion,
		FirecrackerVersion:  firecrackerVersion,
		EnvdVersion:         &envdVersion,
		Secure:              true,
		AllowInternetAccess: &allowInternet,
		AutoPause:           true,
		Config: &types.PausedSandboxConfig{
			Version: types.PausedSandboxConfigVersion,
			Network: &types.SandboxNetworkConfig{
				Egress: &types.SandboxNetworkEgressConfig{
					AllowedAddresses: []string{"192.168.1.0/24"},
				},
			},
		},
		OriginNodeID:  "node-1",
		SourceBuildID: sourceBuildID,
		StatusGroup:   "ready",
	}

	result, err := db.SqlcClient.UpsertSnapshot(t.Context(), params)
	require.NoError(t, err, "Failed to create test snapshot")
	require.NotEqual(t, uuid.Nil, result.BuildID, "BuildID should not be nil")

	// Return sandboxID and the env_id from the result (which is returned as TemplateID)
	return sandboxID, result.TemplateID, result.BuildID
}

// createTestEnvBuild creates an env build for testing
func createTestEnvBuild(t *testing.T, db *testutils.Database, envID string) uuid.UUID {
	t.Helper()

	buildID := uuid.New()
	vcpu := int32(2)
	ramMb := int32(2048)
	freeDisk := int64(512)
	totalDisk := int64(1024)

	err := db.SqlcClient.TestsRawSQL(t.Context(),
		`INSERT INTO public.env_builds
		(id, status_group, vcpu, ram_mb, free_disk_size_mb, total_disk_size_mb, kernel_version, firecracker_version, envd_version, cluster_node_id, created_at, updated_at, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW(), 1)`,
		buildID, types.BuildStatusGroupReady, vcpu, ramMb, freeDisk, totalDisk, "6.1.0", "1.4.0", "v1.0.0", "test-node",
	)
	require.NoError(t, err, "Failed to create test env build")

	// Create the build assignment
	err = db.SqlcClient.TestsRawSQL(t.Context(),
		`INSERT INTO public.env_build_assignments (env_id, build_id, tag)
		VALUES ($1, $2, 'default')`,
		envID, buildID,
	)
	require.NoError(t, err, "Failed to create env build assignment")

	return buildID
}

func deleteBuild(t *testing.T, db *testutils.Database, buildID uuid.UUID) {
	t.Helper()

	err := db.SqlcClient.TestsRawSQL(t.Context(),
		`DELETE FROM public.env_builds
		WHERE id = $1`,
		buildID,
	)
	require.NoError(t, err, "Failed to delete test env builds")
}

func TestGetSnapshotWithBuilds_Success(t *testing.T) {
	t.Parallel()
	// Setup test database with migrations
	db := testutils.SetupDatabase(t)

	// Create test data: team -> base env -> snapshot with env -> env builds
	teamID := testutils.CreateTestTeam(t, db)
	baseEnvID := testutils.CreateTestTemplate(t, db, teamID)
	sandboxID, templateID, _ := createTestSnapshot(t, db, teamID, baseEnvID)

	// Create additional builds for the env (UpsertSnapshot already created one)
	buildID1 := createTestEnvBuild(t, db, templateID)
	buildID2 := createTestEnvBuild(t, db, templateID)

	// Execute GetSnapshotBuilds
	result, err := GetSnapshotBuilds(t.Context(), db.SqlcClient, teamID, sandboxID)
	require.NoError(t, err, "GetSnapshotBuilds should succeed")

	// Verify snapshot data
	assert.Equal(t, templateID, result.TemplateID, "EnvID should match")

	// Verify builds are returned (1 from UpsertSnapshot + 2 additional = 3 total)
	assert.Len(t, result.Builds, 3, "Should have 3 builds total")

	// Verify the additional build IDs are present
	buildIDs := make(map[uuid.UUID]bool)
	for _, build := range result.Builds {
		buildIDs[build.BuildID] = true
	}
	assert.True(t, buildIDs[buildID1], "Should contain first additional build ID")
	assert.True(t, buildIDs[buildID2], "Should contain second additional build ID")
}

func TestGetSnapshotWithBuilds_NoAdditionalBuilds(t *testing.T) {
	t.Parallel()
	// Setup test database with migrations
	db := testutils.SetupDatabase(t)

	// Create test data: team -> base env -> snapshot
	// Note: UpsertSnapshot creates one build automatically
	teamID := testutils.CreateTestTeam(t, db)
	baseEnvID := testutils.CreateTestTemplate(t, db, teamID)
	sandboxID, templateID, _ := createTestSnapshot(t, db, teamID, baseEnvID)

	// Execute GetSnapshotBuilds (only the build created by UpsertSnapshot)
	result, err := GetSnapshotBuilds(t.Context(), db.SqlcClient, teamID, sandboxID)
	require.NoError(t, err, "GetSnapshotBuilds should succeed")

	// Verify snapshot data
	assert.Equal(t, templateID, result.TemplateID, "EnvID should match")

	// Verify the build created by UpsertSnapshot is returned
	assert.Len(t, result.Builds, 1, "Should have exactly 1 build (created by UpsertSnapshot)")
	assert.NotEqual(t, uuid.Nil, result.Builds[0].BuildID, "Build ID should not be nil")
	assert.Equal(t, templateID, result.TemplateID, "Build EnvID should match snapshot EnvID")
}

func TestGetSnapshotWithBuilds_NotFound(t *testing.T) {
	t.Parallel()
	// Setup test database with migrations
	db := testutils.SetupDatabase(t)

	// Create a team but no snapshot
	teamID := testutils.CreateTestTeam(t, db)
	nonExistentSandboxID := "sandbox-does-not-exist"

	// Execute GetSnapshotBuilds with non-existent sandbox ID
	_, err := GetSnapshotBuilds(t.Context(), db.SqlcClient, teamID, nonExistentSandboxID)

	// Verify error is returned
	require.Error(t, err, "GetSnapshotBuilds should return an error")
	assert.ErrorIs(t, err, ErrSnapshotNotFound, "Error should be ErrSnapshotNotFound")
}

func TestGetSnapshotWithBuilds_WrongTeamID(t *testing.T) {
	t.Parallel()
	// Setup test database with migrations
	db := testutils.SetupDatabase(t)

	// Create test data with one team
	teamID := testutils.CreateTestTeam(t, db)
	baseEnvID := testutils.CreateTestTemplate(t, db, teamID)
	sandboxID, _, _ := createTestSnapshot(t, db, teamID, baseEnvID)

	// Create a different team
	differentTeamID := testutils.CreateTestTeam(t, db)

	// Execute GetSnapshotBuilds with wrong team ID
	_, err := GetSnapshotBuilds(t.Context(), db.SqlcClient, differentTeamID, sandboxID)

	// Verify error is returned (snapshot exists but doesn't belong to this team)
	require.Error(t, err, "GetSnapshotBuilds should return an error for wrong team")
	assert.ErrorIs(t, err, ErrSnapshotNotFound, "Error should be ErrSnapshotNotFound")
}

func TestGetSnapshotWithBuilds_NoBuilds(t *testing.T) {
	t.Parallel()
	// Setup test database with migrations
	db := testutils.SetupDatabase(t)

	// Create test data
	teamID := testutils.CreateTestTeam(t, db)
	baseEnvID := testutils.CreateTestTemplate(t, db, teamID)
	sandboxID, _, buildID := createTestSnapshot(t, db, teamID, baseEnvID)
	deleteBuild(t, db, buildID)

	// Execute GetSnapshotBuilds
	result, err := GetSnapshotBuilds(t.Context(), db.SqlcClient, teamID, sandboxID)
	require.NoError(t, err, "GetSnapshotBuilds should succeed")
	assert.Empty(t, result.Builds, "Should have 0 builds after deletion")
}
