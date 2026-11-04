package handlers

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	// artifactCleanupTimeout bounds the whole background cleanup so a stuck
	// DeleteBuild RPC cannot leak a goroutine forever. It is a deadline, not a
	// cancellation of the request: the HTTP response has already been sent.
	artifactCleanupTimeout = 2 * time.Minute
	// artifactCleanupConcurrency caps concurrent DeleteBuild RPCs so deleting a
	// template with many builds (or a burst of deletions) cannot fan out
	// unboundedly against the cluster.
	artifactCleanupConcurrency = 8
)

// deleteBuildFn deletes one build's storage artifacts on its cluster node. It
// matches TemplateManager.DeleteBuild; injectable so tests can observe calls.
type deleteBuildFn func(ctx context.Context, buildID uuid.UUID, templateID string, clusterID uuid.UUID, nodeID string) error

// artifactCleanupCounters hold the outcome metrics; either may be nil (metrics
// unavailable), in which case counting is skipped but cleanup still runs.
type artifactCleanupCounters struct {
	deleted metric.Int64Counter
	failed  metric.Int64Counter
}

// startTemplateArtifactCleanup runs deleteTemplateArtifacts in a detached,
// panic-safe, deadline-bounded goroutine. It is called AFTER the template's DB
// row is soft-deleted and the HTTP response is sent: storage-cleanup failures
// must never affect the already-committed deletion, so they are only logged and
// counted (not surfaced to the caller). ctx should be the request ctx; it is
// detached from cancellation here so the cleanup survives the response.
func (a *APIStore) startTemplateArtifactCleanup(ctx context.Context, templateID string) {
	// Detach from the request lifecycle but keep its trace/values, then bound it.
	base := context.WithoutCancel(ctx)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.L().Error(base, "panic in template artifact cleanup",
					logger.WithTemplateID(templateID),
				)
			}
		}()

		cleanupCtx, cancel := context.WithTimeout(base, artifactCleanupTimeout)
		defer cancel()

		a.deleteTemplateArtifacts(cleanupCtx, templateID, a.templateManager.DeleteBuild)
	}()
}

// deleteTemplateArtifacts queries the builds exclusively owned by templateID
// (the SQL guard excludes builds still shared with other templates) and hands
// them to processExclusiveBuilds. deleteFn is injected for testing.
func (a *APIStore) deleteTemplateArtifacts(ctx context.Context, templateID string, deleteFn deleteBuildFn) {
	builds, err := a.sqlcDB.GetExclusiveBuildsForTemplateDeletion(ctx, templateID)
	if err != nil {
		logger.L().Error(ctx, "failed to query exclusive builds for artifact cleanup",
			logger.WithTemplateID(templateID),
			logger.WithError(err),
		)

		return
	}

	processExclusiveBuilds(ctx, templateID, builds, deleteFn, a.artifactCleanupCounters(ctx, templateID))
}

// artifactCleanupCounters resolves the outcome counters, logging (not failing)
// if the meter cannot provide them.
func (a *APIStore) artifactCleanupCounters(ctx context.Context, templateID string) artifactCleanupCounters {
	meter := a.Telemetry.MeterProvider.Meter("github.com/e2b-dev/infra/packages/api/internal/handlers")
	deleted, derr := telemetry.GetCounter(meter, telemetry.ApiTemplateArtifactsDeleted)
	failed, ferr := telemetry.GetCounter(meter, telemetry.ApiTemplateArtifactsDeleteFailed)
	if derr != nil || ferr != nil {
		logger.L().Error(ctx, "failed to create template artifact cleanup counters",
			logger.WithTemplateID(templateID),
			logger.WithError(firstErr(derr, ferr)),
		)
	}

	return artifactCleanupCounters{deleted: deleted, failed: failed}
}

// processExclusiveBuilds deletes each build's artifacts best-effort: builds not
// yet materialized on a node (nil ClusterNodeID) are skipped, each delete is
// attempted independently with bounded concurrency, a failure on one never
// aborts the others, and every outcome is counted so orphaned storage is
// observable. It takes no DB dependency so it is unit-testable directly.
func processExclusiveBuilds(
	ctx context.Context,
	templateID string,
	builds []queries.GetExclusiveBuildsForTemplateDeletionRow,
	deleteFn deleteBuildFn,
	counters artifactCleanupCounters,
) {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(artifactCleanupConcurrency)

	for _, b := range builds {
		if b.ClusterNodeID == nil {
			// Build was never materialized on a node; nothing to delete.
			continue
		}

		g.Go(func() error {
			clusterID := clusters.WithClusterFallback(b.ClusterID)
			if err := deleteFn(gctx, b.BuildID, templateID, clusterID, *b.ClusterNodeID); err != nil {
				logger.L().Warn(gctx, "failed to delete build artifacts",
					logger.WithTemplateID(templateID),
					logger.WithBuildID(b.BuildID.String()),
					logger.WithError(err),
				)
				addCount(gctx, counters.failed)

				return nil // best-effort: never abort siblings
			}

			logger.L().Info(gctx, "deleted build artifacts",
				logger.WithTemplateID(templateID),
				logger.WithBuildID(b.BuildID.String()),
			)
			addCount(gctx, counters.deleted)

			return nil
		})
	}

	_ = g.Wait() // goroutines never return a non-nil error
}

func addCount(ctx context.Context, c metric.Int64Counter) {
	if c != nil {
		c.Add(ctx, 1)
	}
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}

	return nil
}
