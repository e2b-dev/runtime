package handlers

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// createTestSnapshotForHandler inserts a snapshot and returns (sandboxID, templateID).
// Uses UpsertSnapshot which also creates the env and env_build_assignment rows that
// GetExclusiveBuildsForTemplateDeletion relies on.
func createTestSnapshotForHandler(t *testing.T, db *testutils.Database, teamID uuid.UUID, baseEnvID string) (string, string) {
	t.Helper()

	sandboxID := "sbx-" + uuid.New().String()[:8]
	templateID := "env-" + uuid.New().String()
	envdVersion := "v1.0.0"
	totalDisk := int64(1024)
	allowInternet := true

	sourceBuildID := testutils.CreateTestBuild(t, t.Context(), db, baseEnvID, "success")

	result, err := db.SqlcClient.UpsertSnapshot(t.Context(), queries.UpsertSnapshotParams{
		TemplateID:          templateID,
		TeamID:              teamID,
		SandboxID:           sandboxID,
		BaseTemplateID:      baseEnvID,
		StartedAt:           pgtype.Timestamptz{Time: time.Now(), Valid: true},
		Vcpu:                2,
		RamMb:               2048,
		TotalDiskSizeMb:     &totalDisk,
		Metadata:            dbtypes.JSONBStringMap{},
		KernelVersion:       "6.1.0",
		FirecrackerVersion:  "1.4.0",
		EnvdVersion:         &envdVersion,
		Secure:              true,
		AllowInternetAccess: &allowInternet,
		AutoPause:           false,
		OriginNodeID:        "test-node",
		SourceBuildID:       sourceBuildID,
		Status:              "success",
	})
	require.NoError(t, err)

	return sandboxID, result.TemplateID
}

// TestDeleteTemplateArtifacts_SnapshotBuildIsDeleted verifies that the snapshot
// path (paused sandbox deletion) also triggers artifact cleanup.
// UpsertSnapshot creates an env + env_build_assignment row, so the SQL query
// picks up the build and the deleter is called.
func TestDeleteTemplateArtifacts_SnapshotBuildIsDeleted(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	baseEnvID := testutils.CreateTestTemplate(t, db, teamID)
	_, snapshotTemplateID := createTestSnapshotForHandler(t, db, teamID, baseEnvID)

	// Simulate what deleteSnapshot does: soft-delete then call the artifact cleanup.
	err := db.SqlcClient.TestsRawSQL(ctx,
		`UPDATE public.envs SET deleted_at = NOW() WHERE id = $1`,
		snapshotTemplateID,
	)
	require.NoError(t, err)

	store := &APIStore{
		sqlcDB:        db.SqlcClient,
		templateCache: templatecache.NewTemplateCache(db.SqlcClient, redis),
	}

	var mu sync.Mutex
	deleted := []uuid.UUID{}
	deleteFn := func(_ context.Context, buildID uuid.UUID, _ string, _ uuid.UUID, _ string) error {
		mu.Lock()
		deleted = append(deleted, buildID)
		mu.Unlock()

		return nil
	}

	store.deleteTemplateArtifactsWithDeleter(ctx, snapshotTemplateID, deleteFn)

	assert.Len(t, deleted, 1, "snapshot build should be passed to the deleter")
}
