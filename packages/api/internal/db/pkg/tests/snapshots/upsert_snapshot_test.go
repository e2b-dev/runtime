package snapshots

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
)

func TestUpsertSnapshot_NewSnapshot(t *testing.T) {
	t.Parallel()
	// Setup test database with migrations
	client := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create a test team first (required by foreign key constraint)
	teamID := testutils.CreateTestTeam(t, client)
	// Create a base env (required by foreign key constraint on snapshots table)
	baseTemplateID := testutils.CreateTestTemplate(t, client, teamID)
	// Source build the snapshot copies its CPU info from
	sourceBuildID := testutils.CreateTestBuild(t, ctx, client, baseTemplateID, types.BuildStatusGroupReady)

	// Prepare test data for a new snapshot
	templateID := "test-template-" + uuid.New().String()
	sandboxID := "sandbox-" + uuid.New().String()
	originNodeID := "node-1"
	envdVersion := "v1.0.0"
	kernelVersion := "6.1.0"
	firecrackerVersion := "1.4.0"
	totalDiskSize := int64(1024)
	allowInternet := true

	params := queries.UpsertSnapshotParams{
		TemplateID:          templateID,
		TeamID:              teamID,
		SandboxID:           sandboxID,
		BaseTemplateID:      baseTemplateID,
		StartedAt:           pgtype.Timestamptz{Time: time.Now(), Valid: true},
		Vcpu:                2,
		RamMb:               2048,
		TotalDiskSizeMb:     &totalDiskSize,
		Metadata:            types.JSONBStringMap{"key": "value"},
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
		OriginNodeID:  originNodeID,
		SourceBuildID: sourceBuildID,
		StatusGroup:   "in_progress",
	}

	// Execute UpsertSnapshot for a new snapshot
	result, err := client.SqlcClient.UpsertSnapshot(ctx, params)
	require.NoError(t, err, "Failed to create new snapshot")

	// Verify the result
	assert.NotEqual(t, uuid.Nil, result.BuildID, "BuildID should not be nil")
	assert.Equal(t, templateID, result.TemplateID, "TemplateID should match the input for a new snapshot")

	// Verify we can retrieve the snapshot using the sandboxID
	// Note: GetLastSnapshot requires status IN ('success', 'uploaded'), so we can't use it here
	// Instead, we verify the returned IDs are valid
	assert.NotEmpty(t, result.TemplateID, "TemplateID should not be empty")
	assert.NotEqual(t, uuid.Nil, result.BuildID, "BuildID should not be nil")

	// Verify metadata was stored correctly
	storedMetadata := testutils.GetSnapshotMetadata(t, ctx, client, sandboxID)
	assert.Equal(t, params.Metadata, storedMetadata, "Stored metadata should match the input metadata")
	assert.Equal(t, "value", storedMetadata["key"], "Metadata key 'key' should have value 'value'")

	// Verify envs entry was created
	envExists := testutils.GetEnvByID(t, ctx, client, result.TemplateID)
	assert.True(t, envExists, "Env entry should exist in envs table")

	// Verify env_builds entry was created
	buildExists := testutils.GetEnvBuildByID(t, ctx, client, result.BuildID)
	assert.True(t, buildExists, "Build entry should exist in env_builds table")

	// Verify env_build_assignments entry was created
	assignment := testutils.GetBuildAssignmentByBuildID(t, ctx, client, result.BuildID)
	require.NotNil(t, assignment, "Build assignment should exist for the new build")
	assert.Equal(t, result.TemplateID, assignment.EnvID, "Assignment env_id should match template_id")
	assert.Equal(t, result.BuildID, assignment.BuildID, "Assignment build_id should match")
	assert.Equal(t, "default", assignment.Tag, "Assignment tag should be 'default'")
}

func TestUpsertSnapshot_ExistingSnapshot(t *testing.T) {
	t.Parallel()
	// Setup test database with migrations
	client := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create a test team first (required by foreign key constraint)
	teamID := testutils.CreateTestTeam(t, client)
	// Create a base env (required by foreign key constraint on snapshots table)
	baseTemplateID := testutils.CreateTestTemplate(t, client, teamID)
	// Source build the snapshots copy their CPU info from
	sourceBuildID := testutils.CreateTestBuild(t, ctx, client, baseTemplateID, types.BuildStatusGroupReady)

	// Prepare test data for the first snapshot creation
	templateID := "test-template-" + uuid.New().String()
	sandboxID := "sandbox-" + uuid.New().String()
	originNodeID := "node-1"
	envdVersion := "v1.0.0"
	kernelVersion := "6.1.0"
	firecrackerVersion := "1.4.0"
	totalDiskSize := int64(1024)
	allowInternet := true

	initialParams := queries.UpsertSnapshotParams{
		TemplateID:          templateID,
		TeamID:              teamID,
		SandboxID:           sandboxID,
		BaseTemplateID:      baseTemplateID,
		StartedAt:           pgtype.Timestamptz{Time: time.Now().Add(-1 * time.Hour), Valid: true},
		Vcpu:                2,
		RamMb:               2048,
		TotalDiskSizeMb:     &totalDiskSize,
		Metadata:            types.JSONBStringMap{"key": "initial_value"},
		KernelVersion:       kernelVersion,
		FirecrackerVersion:  firecrackerVersion,
		EnvdVersion:         &envdVersion,
		Secure:              true,
		AllowInternetAccess: &allowInternet,
		AutoPause:           false,
		Config: &types.PausedSandboxConfig{
			Version: types.PausedSandboxConfigVersion,
		},
		OriginNodeID:  originNodeID,
		SourceBuildID: sourceBuildID,
		StatusGroup:   "in_progress",
	}

	// Create the initial snapshot
	initialResult, err := client.SqlcClient.UpsertSnapshot(ctx, initialParams)
	require.NoError(t, err, "Failed to create initial snapshot")
	initialBuildID := initialResult.BuildID
	initialTemplateID := initialResult.TemplateID

	// Verify initial results
	assert.NotEqual(t, uuid.Nil, initialBuildID, "Initial BuildID should not be nil")
	assert.Equal(t, templateID, initialTemplateID, "Initial TemplateID should match input")

	// Verify initial metadata was stored correctly
	initialStoredMetadata := testutils.GetSnapshotMetadata(t, ctx, client, sandboxID)
	assert.Equal(t, initialParams.Metadata, initialStoredMetadata, "Initial metadata should match")
	assert.Equal(t, "initial_value", initialStoredMetadata["key"], "Initial metadata key should have correct value")

	// Verify initial envs, env_builds, and build assignment were created
	assert.True(t, testutils.GetEnvByID(t, ctx, client, initialTemplateID), "Initial env should exist")
	assert.True(t, testutils.GetEnvBuildByID(t, ctx, client, initialBuildID), "Initial build should exist")
	initialAssignment := testutils.GetBuildAssignmentByBuildID(t, ctx, client, initialBuildID)
	require.NotNil(t, initialAssignment, "Initial build assignment should exist")
	assert.Equal(t, initialTemplateID, initialAssignment.EnvID, "Initial assignment env_id should match")
	assert.Equal(t, "default", initialAssignment.Tag, "Initial assignment tag should be 'default'")

	// Prepare updated data for the existing snapshot (same sandbox_id)
	updatedOriginNodeID := "node-2"
	newStartTime := time.Now()
	updatedMetadata := types.JSONBStringMap{"key": "updated_value", "new_key": "new_value"}
	updatedConfig := &types.PausedSandboxConfig{
		Version: types.PausedSandboxConfigVersion,
		Network: &types.SandboxNetworkConfig{
			Egress: &types.SandboxNetworkEgressConfig{
				AllowedAddresses: []string{"10.0.0.0/8"},
			},
		},
	}

	updatedParams := queries.UpsertSnapshotParams{
		TemplateID:          "new-template-id-should-be-ignored", // This should be ignored for existing snapshots
		TeamID:              teamID,
		SandboxID:           sandboxID, // Same sandbox_id as initial - this is the key
		BaseTemplateID:      baseTemplateID,
		StartedAt:           pgtype.Timestamptz{Time: newStartTime, Valid: true},
		Vcpu:                4,    // Updated from 2
		RamMb:               4096, // Updated from 2048
		TotalDiskSizeMb:     &totalDiskSize,
		Metadata:            updatedMetadata, // Updated metadata
		KernelVersion:       kernelVersion,
		FirecrackerVersion:  firecrackerVersion,
		EnvdVersion:         &envdVersion,
		Secure:              true,
		AllowInternetAccess: &allowInternet,
		AutoPause:           true,                // Updated from false
		Config:              updatedConfig,       // Updated config
		OriginNodeID:        updatedOriginNodeID, // Updated from node-1
		SourceBuildID:       sourceBuildID,
		StatusGroup:         "in_progress",
	}

	// Execute UpsertSnapshot for the existing snapshot (same sandbox_id)
	updatedResult, err := client.SqlcClient.UpsertSnapshot(ctx, updatedParams)
	require.NoError(t, err, "Failed to update existing snapshot")

	// Verify the key behavior of upserting an existing snapshot:
	// 1. TemplateID should remain the same (from the first insert)
	assert.Equal(t, initialTemplateID, updatedResult.TemplateID,
		"TemplateID should remain the same as the initial snapshot when upserting")

	// 2. BuildID should be different (a new build is created each time)
	assert.NotEqual(t, initialBuildID, updatedResult.BuildID,
		"BuildID should be different - a new build is created on each upsert")

	// 3. The new BuildID should be valid
	assert.NotEqual(t, uuid.Nil, updatedResult.BuildID,
		"New BuildID should not be nil")

	// 4. Verify metadata was updated correctly
	updatedStoredMetadata := testutils.GetSnapshotMetadata(t, ctx, client, sandboxID)
	assert.Equal(t, updatedParams.Metadata, updatedStoredMetadata, "Updated metadata should match")
	assert.Equal(t, "updated_value", updatedStoredMetadata["key"], "Updated metadata key should have new value")
	assert.Equal(t, "new_value", updatedStoredMetadata["new_key"], "New metadata key should be present")
	assert.NotEqual(t, initialStoredMetadata, updatedStoredMetadata, "Updated metadata should be different from initial")

	// 5. Verify the second build and its assignment were created
	assert.True(t, testutils.GetEnvBuildByID(t, ctx, client, updatedResult.BuildID), "Second build should exist")
	secondAssignment := testutils.GetBuildAssignmentByBuildID(t, ctx, client, updatedResult.BuildID)
	require.NotNil(t, secondAssignment, "Second build assignment should exist")
	assert.Equal(t, initialTemplateID, secondAssignment.EnvID, "Second assignment env_id should match original template")
	assert.Equal(t, "default", secondAssignment.Tag, "Second assignment tag should be 'default'")

	// 6. Verify we now have 2 build assignments for the same env_id
	allAssignments := testutils.GetBuildAssignments(t, ctx, client, initialTemplateID)
	assert.GreaterOrEqual(t, len(allAssignments), 2, "Should have at least 2 build assignments after second upsert")

	// Test calling upsert a third time to ensure consistent behavior
	thirdParams := queries.UpsertSnapshotParams{
		TemplateID:          "yet-another-template-id",
		TeamID:              teamID,
		SandboxID:           sandboxID, // Same sandbox_id again
		BaseTemplateID:      baseTemplateID,
		StartedAt:           pgtype.Timestamptz{Time: time.Now(), Valid: true},
		Vcpu:                8,
		RamMb:               8192,
		TotalDiskSizeMb:     &totalDiskSize,
		Metadata:            types.JSONBStringMap{"key": "third_value"},
		KernelVersion:       kernelVersion,
		FirecrackerVersion:  firecrackerVersion,
		EnvdVersion:         &envdVersion,
		Secure:              true,
		AllowInternetAccess: &allowInternet,
		AutoPause:           true,
		Config:              updatedConfig,
		OriginNodeID:        "node-3",
		SourceBuildID:       sourceBuildID,
		StatusGroup:         "in_progress",
	}

	thirdResult, err := client.SqlcClient.UpsertSnapshot(ctx, thirdParams)
	require.NoError(t, err, "Failed to update snapshot a third time")

	// Verify consistent behavior on third upsert
	assert.Equal(t, initialTemplateID, thirdResult.TemplateID,
		"TemplateID should still be the same as the initial snapshot")
	assert.NotEqual(t, updatedResult.BuildID, thirdResult.BuildID,
		"BuildID should be different from the second upsert")
	assert.NotEqual(t, initialBuildID, thirdResult.BuildID,
		"BuildID should be different from the first upsert")

	// Verify metadata was updated again
	thirdStoredMetadata := testutils.GetSnapshotMetadata(t, ctx, client, sandboxID)
	assert.Equal(t, thirdParams.Metadata, thirdStoredMetadata, "Third metadata should match")
	assert.Equal(t, "third_value", thirdStoredMetadata["key"], "Third metadata key should have latest value")
	assert.NotEqual(t, updatedStoredMetadata, thirdStoredMetadata, "Third metadata should be different from second")

	// Verify the third build and its assignment were created
	assert.True(t, testutils.GetEnvBuildByID(t, ctx, client, thirdResult.BuildID), "Third build should exist")
	thirdAssignment := testutils.GetBuildAssignmentByBuildID(t, ctx, client, thirdResult.BuildID)
	require.NotNil(t, thirdAssignment, "Third build assignment should exist")
	assert.Equal(t, initialTemplateID, thirdAssignment.EnvID, "Third assignment env_id should match original template")
	assert.Equal(t, "default", thirdAssignment.Tag, "Third assignment tag should be 'default'")

	// Verify we now have 3 build assignments for the same env_id
	finalAssignments := testutils.GetBuildAssignments(t, ctx, client, initialTemplateID)
	assert.GreaterOrEqual(t, len(finalAssignments), 3, "Should have at least 3 build assignments after third upsert")
}

func TestUpsertSnapshot_CopiesMaxVcpuFromSourceBuild(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sourceMax *int64
	}{
		{name: "source with max_vcpu", sourceMax: new(int64(8))},
		{name: "source without max_vcpu", sourceMax: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := testutils.SetupDatabase(t)
			ctx := t.Context()

			teamID := testutils.CreateTestTeam(t, client)
			baseTemplateID := testutils.CreateTestTemplate(t, client, teamID)
			sourceBuildID := testutils.CreateTestBuild(t, ctx, client, baseTemplateID, types.BuildStatusGroupReady)

			err := client.SqlcClient.TestsRawSQL(ctx,
				"UPDATE public.env_builds SET max_vcpu = $1 WHERE id = $2", tt.sourceMax, sourceBuildID)
			require.NoError(t, err)

			totalDiskSize := int64(1024)
			params := queries.UpsertSnapshotParams{
				TemplateID:         "test-template-" + uuid.New().String(),
				TeamID:             teamID,
				SandboxID:          "sandbox-" + uuid.New().String(),
				BaseTemplateID:     baseTemplateID,
				StartedAt:          pgtype.Timestamptz{Time: time.Now(), Valid: true},
				Vcpu:               2,
				RamMb:              2048,
				TotalDiskSizeMb:    &totalDiskSize,
				Metadata:           types.JSONBStringMap{},
				KernelVersion:      "6.1.0",
				FirecrackerVersion: "1.4.0",
				Config:             &types.PausedSandboxConfig{Version: types.PausedSandboxConfigVersion},
				OriginNodeID:       "node-1",
				SourceBuildID:      sourceBuildID,
				StatusGroup:        "in_progress",
			}

			for range 2 {
				result, err := client.SqlcClient.UpsertSnapshot(ctx, params)
				require.NoError(t, err)

				var got *int64
				var online int64
				err = client.SqlcClient.TestsRawSQLQuery(ctx,
					"SELECT max_vcpu, vcpu FROM public.env_builds WHERE id = $1",
					func(rows pgx.Rows) error {
						require.True(t, rows.Next())

						return rows.Scan(&got, &online)
					},
					result.BuildID,
				)
				require.NoError(t, err)
				assert.Equal(t, tt.sourceMax, got)
				assert.Equal(t, params.Vcpu, online)
				params.SourceBuildID = result.BuildID
				if tt.sourceMax != nil {
					params.Vcpu = 4
				}
			}
		})
	}
}

func TestUpsertSnapshot_CopiesMaxRamMbFromSourceBuild(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sourceMax *int64
	}{
		{name: "source with max_ram_mb", sourceMax: new(int64(6144))},
		{name: "source without max_ram_mb", sourceMax: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := testutils.SetupDatabase(t)
			ctx := t.Context()

			teamID := testutils.CreateTestTeam(t, client)
			baseTemplateID := testutils.CreateTestTemplate(t, client, teamID)
			sourceBuildID := testutils.CreateTestBuild(t, ctx, client, baseTemplateID, types.BuildStatusGroupReady)

			err := client.SqlcClient.TestsRawSQL(ctx,
				"UPDATE public.env_builds SET max_ram_mb = $1 WHERE id = $2", tt.sourceMax, sourceBuildID)
			require.NoError(t, err)

			totalDiskSize := int64(1024)
			params := queries.UpsertSnapshotParams{
				TemplateID:         "test-template-" + uuid.New().String(),
				TeamID:             teamID,
				SandboxID:          "sandbox-" + uuid.New().String(),
				BaseTemplateID:     baseTemplateID,
				StartedAt:          pgtype.Timestamptz{Time: time.Now(), Valid: true},
				Vcpu:               2,
				RamMb:              2048,
				TotalDiskSizeMb:    &totalDiskSize,
				Metadata:           types.JSONBStringMap{},
				KernelVersion:      "6.1.0",
				FirecrackerVersion: "1.4.0",
				Config:             &types.PausedSandboxConfig{Version: types.PausedSandboxConfigVersion},
				OriginNodeID:       "node-1",
				SourceBuildID:      sourceBuildID,
				StatusGroup:        "in_progress",
			}

			for range 2 {
				result, err := client.SqlcClient.UpsertSnapshot(ctx, params)
				require.NoError(t, err)

				var got *int64
				var bootRAM int64
				err = client.SqlcClient.TestsRawSQLQuery(ctx,
					"SELECT max_ram_mb, ram_mb FROM public.env_builds WHERE id = $1",
					func(rows pgx.Rows) error {
						require.True(t, rows.Next())

						return rows.Scan(&got, &bootRAM)
					},
					result.BuildID,
				)
				require.NoError(t, err)
				assert.Equal(t, tt.sourceMax, got)
				assert.Equal(t, params.RamMb, bootRAM)
				params.SourceBuildID = result.BuildID
			}
		})
	}
}

// A pause records the sandbox's whole size on its new build, and the resume
// reads it from the latest ready build: a pause whose build never becomes ready
// leaves the size of the last ready build in force.
func TestUpsertSnapshot_StartMemoryFollowsTheReadyBuild(t *testing.T) {
	t.Parallel()

	client := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, client)
	baseTemplateID := testutils.CreateTestTemplate(t, client, teamID)
	sourceBuildID := testutils.CreateTestBuild(t, ctx, client, baseTemplateID, types.BuildStatusGroupReady)
	require.NoError(t, client.SqlcClient.TestsRawSQL(ctx,
		"UPDATE public.env_builds SET max_ram_mb = 4096, start_memory_mb = 1024 WHERE id = $1", sourceBuildID))

	totalDiskSize := int64(1024)
	envdVersion := "v1.0.0"
	sandboxID := "sandbox-" + uuid.New().String()
	params := queries.UpsertSnapshotParams{
		TemplateID:         "test-template-" + uuid.New().String(),
		TeamID:             teamID,
		SandboxID:          sandboxID,
		BaseTemplateID:     baseTemplateID,
		StartedAt:          pgtype.Timestamptz{Time: time.Now(), Valid: true},
		Vcpu:               2,
		RamMb:              1024,
		TotalDiskSizeMb:    &totalDiskSize,
		Metadata:           types.JSONBStringMap{},
		KernelVersion:      "6.1.0",
		FirecrackerVersion: "1.4.0",
		EnvdVersion:        &envdVersion,
		Config:             &types.PausedSandboxConfig{Version: types.PausedSandboxConfigVersion},
		OriginNodeID:       "node-1",
		SourceBuildID:      sourceBuildID,
		StatusGroup:        types.BuildStatusGroupReady,
		StartMemoryMb:      new(int64(2048)),
	}

	first, err := client.SqlcClient.UpsertSnapshot(ctx, params)
	require.NoError(t, err)
	assert.Equal(t, new(int64(4096)), first.MaxRamMb, "the upsert returns the new build's maximum")
	last, err := client.SqlcClient.GetLastSnapshot(ctx, sandboxID)
	require.NoError(t, err)
	assert.Equal(t, first.BuildID, last.EnvBuild.ID)
	assert.Equal(t, new(int64(2048)), last.EnvBuild.StartMemoryMb)
	assert.Equal(t, new(int64(4096)), last.EnvBuild.MaxRamMb)
	assert.Equal(t, int64(1024), last.EnvBuild.RamMb)

	// The sandbox grew to 4096 MiB and its next pause failed: the new build
	// stays in progress and the resume keeps the ready one's 2048 MiB.
	params.SourceBuildID = first.BuildID
	params.StatusGroup = types.BuildStatusGroupInProgress
	params.StartMemoryMb = new(int64(4096))
	second, err := client.SqlcClient.UpsertSnapshot(ctx, params)
	require.NoError(t, err)
	require.NotEqual(t, first.BuildID, second.BuildID)

	last, err = client.SqlcClient.GetLastSnapshot(ctx, sandboxID)
	require.NoError(t, err)
	assert.Equal(t, first.BuildID, last.EnvBuild.ID)
	assert.Equal(t, new(int64(2048)), last.EnvBuild.StartMemoryMb)

	// Once the new build is ready, its size is the sandbox's.
	require.NoError(t, client.SqlcClient.TestsRawSQL(ctx,
		"UPDATE public.env_builds SET status_group = 'ready' WHERE id = $1", second.BuildID))
	last, err = client.SqlcClient.GetLastSnapshot(ctx, sandboxID)
	require.NoError(t, err)
	assert.Equal(t, second.BuildID, last.EnvBuild.ID)
	assert.Equal(t, new(int64(4096)), last.EnvBuild.StartMemoryMb)
}

// A snapshot build an older writer made without max_ram_mb loses the
// maximum for that build only: the next upsert falls back to the snapshot's
// newest build that has one, so the sandbox keeps its memory maximum.
func TestUpsertSnapshot_MaxRamMbHealsAfterAnOlderWriter(t *testing.T) {
	t.Parallel()

	client := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, client)
	baseTemplateID := testutils.CreateTestTemplate(t, client, teamID)
	sourceBuildID := testutils.CreateTestBuild(t, ctx, client, baseTemplateID, types.BuildStatusGroupReady)
	require.NoError(t, client.SqlcClient.TestsRawSQL(ctx,
		"UPDATE public.env_builds SET max_ram_mb = 4096, start_memory_mb = 1024 WHERE id = $1", sourceBuildID))

	totalDiskSize := int64(1024)
	envdVersion := "v1.0.0"
	sandboxID := "sandbox-" + uuid.New().String()
	params := queries.UpsertSnapshotParams{
		TemplateID:         "test-template-" + uuid.New().String(),
		TeamID:             teamID,
		SandboxID:          sandboxID,
		BaseTemplateID:     baseTemplateID,
		StartedAt:          pgtype.Timestamptz{Time: time.Now(), Valid: true},
		Vcpu:               2,
		RamMb:              1024,
		TotalDiskSizeMb:    &totalDiskSize,
		Metadata:           types.JSONBStringMap{},
		KernelVersion:      "6.1.0",
		FirecrackerVersion: "1.4.0",
		EnvdVersion:        &envdVersion,
		Config:             &types.PausedSandboxConfig{Version: types.PausedSandboxConfigVersion},
		OriginNodeID:       "node-1",
		SourceBuildID:      sourceBuildID,
		StatusGroup:        types.BuildStatusGroupReady,
		StartMemoryMb:      new(int64(2048)),
	}

	first, err := client.SqlcClient.UpsertSnapshot(ctx, params)
	require.NoError(t, err)
	require.Equal(t, new(int64(4096)), first.MaxRamMb)

	// An older API pauses the sandbox: its build carries neither column.
	params.SourceBuildID = first.BuildID
	params.StartMemoryMb = nil
	older, err := client.SqlcClient.UpsertSnapshot(ctx, params)
	require.NoError(t, err)
	require.NoError(t, client.SqlcClient.TestsRawSQL(ctx,
		"UPDATE public.env_builds SET max_ram_mb = NULL, start_memory_mb = NULL WHERE id = $1", older.BuildID))

	// The next pause by this version starts from that build and restores the maximum.
	params.SourceBuildID = older.BuildID
	healed, err := client.SqlcClient.UpsertSnapshot(ctx, params)
	require.NoError(t, err)
	assert.Equal(t, new(int64(4096)), healed.MaxRamMb, "the maximum comes back from the snapshot's newest build that has one")
	last, err := client.SqlcClient.GetLastSnapshot(ctx, sandboxID)
	require.NoError(t, err)
	assert.Equal(t, healed.BuildID, last.EnvBuild.ID)
	assert.Equal(t, new(int64(4096)), last.EnvBuild.MaxRamMb)
	assert.Nil(t, last.EnvBuild.StartMemoryMb, "the size stays unknown; the start reports it and bounds it by the maximum")
}
