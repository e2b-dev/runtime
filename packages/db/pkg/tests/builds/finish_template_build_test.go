package builds

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
)

// seededKernel / seededFC match the defaults baked into testutils.CreateTestBuild.
const (
	seededKernel = "6.1.0"
	seededFC     = "1.4.0"
)

// getBuildVersions returns the kernel_version, firecracker_version and envd_version
// currently stored on the env_builds row. envd_version is nullable, so it is
// returned as *string.
func getBuildVersions(t *testing.T, ctx context.Context, db *testutils.Database, buildID uuid.UUID) (kernel, firecracker string, envd *string) {
	t.Helper()

	err := db.SqlcClient.TestsRawSQLQuery(ctx,
		"SELECT kernel_version, firecracker_version, envd_version FROM public.env_builds WHERE id = $1",
		func(rows pgx.Rows) error {
			if !rows.Next() {
				return nil
			}

			return rows.Scan(&kernel, &firecracker, &envd)
		},
		buildID,
	)
	require.NoError(t, err, "Failed to query build versions")

	return kernel, firecracker, envd
}

func TestFinishTemplateBuild_OverwritesVersionsWhenProvided(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupInProgress)

	// Sanity check the seeded row matches the fixture constants.
	kernel, firecracker, _ := getBuildVersions(t, ctx, db, buildID)
	require.Equal(t, seededKernel, kernel)
	require.Equal(t, seededFC, firecracker)

	totalDisk := int64(4096)
	envdVersion := "v9.9.9"
	reportedKernel := "vmlinux-reported"
	reportedFC := "v9.9.9_reported"

	err := db.SqlcClient.FinishTemplateBuild(ctx, queries.FinishTemplateBuildParams{
		BuildID:            buildID,
		StatusGroup:        types.BuildStatusGroupReady,
		TotalDiskSizeMb:    &totalDisk,
		EnvdVersion:        &envdVersion,
		KernelVersion:      reportedKernel,
		FirecrackerVersion: reportedFC,
	})
	require.NoError(t, err)

	kernel, firecracker, envd := getBuildVersions(t, ctx, db, buildID)
	assert.Equal(t, reportedKernel, kernel, "kernel_version should be overwritten with the template-manager reported value")
	assert.Equal(t, reportedFC, firecracker, "firecracker_version should be overwritten with the template-manager reported value")
	if assert.NotNil(t, envd) {
		assert.Equal(t, envdVersion, *envd)
	}
}

func TestFinishTemplateBuild_PreservesVersionsWhenEmpty(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupInProgress)

	// Simulate an old template-manager that does not populate kernel / firecracker
	// versions on the response: both params empty, but the rest of the update
	// (status, envd version, disk size) still applies.
	totalDisk := int64(2048)
	envdVersion := "v1.2.3"

	err := db.SqlcClient.FinishTemplateBuild(ctx, queries.FinishTemplateBuildParams{
		BuildID:            buildID,
		StatusGroup:        types.BuildStatusGroupReady,
		TotalDiskSizeMb:    &totalDisk,
		EnvdVersion:        &envdVersion,
		KernelVersion:      "",
		FirecrackerVersion: "",
	})
	require.NoError(t, err)

	kernel, firecracker, envd := getBuildVersions(t, ctx, db, buildID)
	assert.Equal(t, seededKernel, kernel, "kernel_version should fall back to the seeded value when the reported value is empty")
	assert.Equal(t, seededFC, firecracker, "firecracker_version should fall back to the seeded value when the reported value is empty")
	if assert.NotNil(t, envd) {
		assert.Equal(t, envdVersion, *envd)
	}
}

func TestFinishTemplateBuild_PreservesSingleVersionWhenOnlyOneEmpty(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupInProgress)

	reportedKernel := "vmlinux-reported"

	err := db.SqlcClient.FinishTemplateBuild(ctx, queries.FinishTemplateBuildParams{
		BuildID:            buildID,
		StatusGroup:        types.BuildStatusGroupReady,
		KernelVersion:      reportedKernel,
		FirecrackerVersion: "",
	})
	require.NoError(t, err)

	kernel, firecracker, _ := getBuildVersions(t, ctx, db, buildID)
	assert.Equal(t, reportedKernel, kernel, "kernel_version should be overwritten when a non-empty value is provided")
	assert.Equal(t, seededFC, firecracker, "firecracker_version should fall back to the seeded value when only kernel was reported")
}
