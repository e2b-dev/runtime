package handlers

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// TestDeleteTemplateArtifacts_ExclusiveBuildsAreDeleted verifies that
// deleteTemplateArtifactsWithDeleter calls the deleter for each build that has a
// ClusterNodeID (i.e. is materialized on a node) and that it skips builds without
// a ClusterNodeID.
func TestDeleteTemplateArtifacts_ExclusiveBuildsAreDeleted(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	// Build with a node: should be passed to the deleter
	buildWithNode := testutils.CreateTestBuild(t, ctx, db, templateID, "success")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildWithNode, "v1")

	// Build without a node: CreateTestBuild uses cluster_node_id = 'test-node',
	// so create a second one without cluster_node_id via raw SQL.
	buildNoNode := uuid.New()
	err := db.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.env_builds
		(id, env_id, status, vcpu, ram_mb, free_disk_size_mb, kernel_version, firecracker_version, created_at, updated_at)
		VALUES ($1, $2, 'success', 2, 2048, 512, '6.1.0', '1.4.0', NOW(), NOW())`,
		buildNoNode, templateID,
	)
	require.NoError(t, err)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildNoNode, "v2")

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

	store.deleteTemplateArtifactsWithDeleter(ctx, templateID, deleteFn)

	assert.Equal(t, []uuid.UUID{buildWithNode}, deleted,
		"only the build with a cluster node should be passed to the deleter")
}

// TestDeleteTemplateArtifacts_SharedBuildsSkipped verifies that builds assigned
// to multiple templates are not passed to the deleter.
func TestDeleteTemplateArtifacts_SharedBuildsSkipped(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateA := testutils.CreateTestTemplate(t, db, teamID)
	templateB := testutils.CreateTestTemplate(t, db, teamID)

	// Shared build: belongs to both templates
	shared := testutils.CreateTestBuild(t, ctx, db, templateA, "success")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateA, shared, "latest")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateB, shared, "latest")

	// Exclusive build: only on templateA
	exclusive := testutils.CreateTestBuild(t, ctx, db, templateA, "success")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateA, exclusive, "v2")

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

	store.deleteTemplateArtifactsWithDeleter(ctx, templateA, deleteFn)

	assert.Equal(t, []uuid.UUID{exclusive}, deleted,
		"shared build must not be deleted; only the exclusive build should be")
}

// TestDeleteTemplateArtifacts_DeleterErrorIsNonFatal verifies that when the
// deleter returns an error the function continues and does not panic or propagate
// the error (artifact deletion is best-effort after DB soft-delete).
func TestDeleteTemplateArtifacts_DeleterErrorIsNonFatal(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	build1 := testutils.CreateTestBuild(t, ctx, db, templateID, "success")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, build1, "v1")

	build2 := testutils.CreateTestBuild(t, ctx, db, templateID, "success")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, build2, "v2")

	store := &APIStore{
		sqlcDB:        db.SqlcClient,
		templateCache: templatecache.NewTemplateCache(db.SqlcClient, redis),
	}

	var mu sync.Mutex
	attempted := []uuid.UUID{}
	deleteFn := func(_ context.Context, buildID uuid.UUID, _ string, _ uuid.UUID, _ string) error {
		mu.Lock()
		attempted = append(attempted, buildID)
		mu.Unlock()

		return errors.New("storage backend unavailable")
	}

	// Must not panic or return early; all builds should be attempted.
	assert.NotPanics(t, func() {
		store.deleteTemplateArtifactsWithDeleter(ctx, templateID, deleteFn)
	})

	assert.Len(t, attempted, 2, "both builds should be attempted even when the first fails")
}

// TestDeleteTemplateArtifacts_WorksAfterSoftDelete verifies that the artifact
// cleanup still finds builds after the template row has been soft-deleted.
func TestDeleteTemplateArtifacts_WorksAfterSoftDelete(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, "success")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, "latest")

	// Simulate what softDeleteTemplate does
	err := db.SqlcClient.TestsRawSQL(ctx,
		`UPDATE public.envs SET deleted_at = NOW() WHERE id = $1`,
		templateID,
	)
	require.NoError(t, err)

	store := &APIStore{
		sqlcDB:        db.SqlcClient,
		templateCache: templatecache.NewTemplateCache(db.SqlcClient, redis),
	}

	var mu sync.Mutex
	deleted := []uuid.UUID{}
	deleteFn := func(_ context.Context, id uuid.UUID, _ string, _ uuid.UUID, _ string) error {
		mu.Lock()
		deleted = append(deleted, id)
		mu.Unlock()

		return nil
	}

	store.deleteTemplateArtifactsWithDeleter(ctx, templateID, deleteFn)

	assert.Equal(t, []uuid.UUID{buildID}, deleted,
		"build should still be found and deleted after the template is soft-deleted")
}
