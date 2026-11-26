package template_manager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	clustersshared "github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestSetFinished_TemplateResolvesToTheNewBuild(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	// InvalidateAllTags scans by key prefix, which SetupInstance cannot isolate.
	redis := redis_utils.SetupContainerInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	previous := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupReady)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, previous, id.DefaultTag)
	rebuild := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupInProgress)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, rebuild, id.DefaultTag)

	templateCache := templatecache.NewTemplateCache(db.SqlcClient, redis)
	buildCache := templatecache.NewTemplateBuildCache(db.SqlcClient, redis)
	t.Cleanup(func() {
		closeCtx := context.WithoutCancel(ctx)
		require.NoError(t, templateCache.Close(closeCtx))
		require.NoError(t, buildCache.Close(closeCtx))
	})

	clusterID := clustersshared.WithClusterFallback(nil)

	// A sandbox start during the rebuild caches the previous ready build.
	_, build, err := templateCache.Get(ctx, templateID, nil, teamID, clusterID)
	require.NoError(t, err)
	require.Equal(t, previous, build.ID)

	// The poller recording the rebuild can belong to any instance, not the one
	// that started it.
	tm := &TemplateManager{sqlcDB: db.SqlcClient, buildCache: buildCache, templateCache: templateCache}
	require.NoError(t, tm.SetFinished(ctx, rebuild, templateID, 100, "0.5.0", "", ""))

	_, build, err = templateCache.Get(ctx, templateID, nil, teamID, clusterID)
	require.NoError(t, err)
	require.Equal(t, rebuild, build.ID, "a build recorded ready must replace the cached one")
}
