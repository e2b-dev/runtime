package template_manager

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	templatemanagergrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// buildDeleteTimeout bounds the node-side delete once the build has been
// recorded as failed.
const buildDeleteTimeout = 30 * time.Second

type buildCanceller interface {
	SetTerminalStatus(ctx context.Context, buildID uuid.UUID, statusGroup dbtypes.BuildStatusGroup, reason *templatemanagergrpc.TemplateBuildStatusReason) (bool, error)
	DeleteBuild(ctx context.Context, buildID uuid.UUID, templateID string, clusterID uuid.UUID, nodeID string) error
}

// CancelTeamBuilds cancels every cancellable build of the team, ten at a time,
// recording reason as each build's status message. A failed cancellation is
// counted, not returned: the only error is failing to list the team's builds.
func (tm *TemplateManager) CancelTeamBuilds(ctx context.Context, teamID uuid.UUID, reason string) (cancelled, failed int, err error) {
	ctx, span := tracer.Start(ctx, "cancel-team-builds")
	defer span.End()

	builds, err := tm.sqlcDB.GetCancellableTemplateBuildsByTeam(ctx, teamID)
	if err != nil {
		return 0, 0, fmt.Errorf("list team builds: %w", err)
	}

	logger.L().Info(ctx, "Found builds to cancel",
		logger.WithTeamID(teamID.String()),
		zap.Int("count", len(builds)),
	)

	cancelledCount := atomic.Int64{}
	failedCount := atomic.Int64{}

	wg := errgroup.Group{}
	wg.SetLimit(10)

	for _, b := range builds {
		wg.Go(func() error {
			err := cancelBuild(ctx, tm, b, reason)
			if err != nil {
				logger.L().Error(ctx, "Failed to cancel build",
					zap.String("buildID", b.BuildID.String()),
					zap.String("templateID", b.TemplateID),
					logger.WithTeamID(teamID.String()),
					zap.Error(err))
				failedCount.Add(1)

				return nil
			}

			logger.L().Debug(ctx, "Successfully cancelled build",
				zap.String("buildID", b.BuildID.String()),
				zap.String("templateID", b.TemplateID),
				logger.WithTeamID(teamID.String()))
			cancelledCount.Add(1)

			return nil
		})
	}

	_ = wg.Wait()

	return int(cancelledCount.Load()), int(failedCount.Load()), nil
}

// cancelBuild ends a build and stops it on its node. The node-side delete takes
// the build's artifacts with it, so it only follows a write that ended the
// build: one that finished on its own between the listing and here keeps both
// its outcome and the artifacts that outcome refers to.
func cancelBuild(ctx context.Context, tm buildCanceller, build queries.GetCancellableTemplateBuildsByTeamRow, reason string) error {
	recorded, err := tm.SetTerminalStatus(ctx, build.BuildID, dbtypes.BuildStatusGroupFailed, &templatemanagergrpc.TemplateBuildStatusReason{
		Message: reason,
	})
	if err != nil {
		return fmt.Errorf("failed to set build status to failed: %w", err)
	}

	if !recorded || build.ClusterNodeID == nil {
		return nil
	}

	// The write above dropped the build from every listing, so nothing retries
	// this stop, and the request context dies with the caller or its deadline.
	deleteCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), buildDeleteTimeout)
	defer cancel()

	err = tm.DeleteBuild(deleteCtx, build.BuildID, build.TemplateID, clusters.WithClusterFallback(build.ClusterID), *build.ClusterNodeID)
	if err != nil {
		return fmt.Errorf("failed to delete build on node: %w", err)
	}

	return nil
}
