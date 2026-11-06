package handlers

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
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

// artifactCleanupCounters hold the reconciliation metrics. Any may be nil
// (metrics unavailable), in which case counting is skipped but cleanup still
// runs. The invariant that makes orphaned storage observable is:
//
//	candidates == deleted + failed + skippedNilNode + guardKept + skippedTooNew  (success path)
//	candidates with failClosed > 0                                               (nothing deleted)
//
// so "how many artifacts did we fail to reclaim" is derivable, not guessed.
type artifactCleanupCounters struct {
	candidates     metric.Int64Counter // builds the DB considered exclusive (the denominator)
	deleted        metric.Int64Counter // artifacts actually deleted
	failed         metric.Int64Counter // delete attempted, RPC failed (potential orphan)
	skippedNilNode metric.Int64Counter // build never materialized on a node; nothing to delete
	guardKept      metric.Int64Counter // reachability guard protected a still-referenced ancestor
	skippedTooNew  metric.Int64Counter // materialized but younger than the grace window; deferred to the offline sweeper
	failClosed     metric.Int64Counter // guard could not prove reachability; whole cleanup aborted
}

// startTemplateArtifactCleanup runs deleteTemplateArtifacts in a detached,
// panic-safe, deadline-bounded goroutine. It is called AFTER the template's DB
// row is soft-deleted and the HTTP response is sent: storage-cleanup failures
// must never affect the already-committed deletion, so they are only logged and
// counted (not surfaced to the caller). ctx should be the request ctx; it is
// detached from cancellation here so the cleanup survives the response.
func (a *APIStore) startTemplateArtifactCleanup(ctx context.Context, teamID uuid.UUID, templateID string) {
	// Detach from the request lifecycle but keep its trace/values, then bound it.
	base := context.WithoutCancel(ctx)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.L().Error(base, "panic in template artifact cleanup",
					logger.WithTemplateID(templateID),
					logger.WithTeamID(teamID.String()),
				)
			}
		}()

		cleanupCtx, cancel := context.WithTimeout(base, artifactCleanupTimeout)
		defer cancel()

		a.deleteTemplateArtifacts(cleanupCtx, teamID, templateID, a.templateManager.DeleteBuild)
	}()
}

// deleteTemplateArtifacts queries the builds exclusively owned by templateID
// (the SQL guard excludes builds still shared with other templates), runs the
// reachability guard to drop any build still reachable from a live root's
// header closure, then hands the survivors to processExclusiveBuilds. deleteFn
// is injected for testing.
//
// Being DB-exclusive is NOT sufficient for deletion: intermediate cache layers
// are never written to the DB, so a build that is DB-exclusive to this template
// can still be a header-mapping ancestor of another live build. Deleting its
// storage prefix would orphan that ancestor and surface later as a silent,
// unrecoverable "Input/output error". The reachability guard is what prevents
// that; if reachability cannot be proven the whole cleanup fails closed.
func (a *APIStore) deleteTemplateArtifacts(ctx context.Context, teamID uuid.UUID, templateID string, deleteFn deleteBuildFn) {
	counters := a.artifactCleanupCounters(ctx, templateID)

	builds, err := a.sqlcDB.GetExclusiveBuildsForTemplateDeletion(ctx, templateID)
	if err != nil {
		logger.L().Error(ctx, "failed to query exclusive builds for artifact cleanup",
			logger.WithTemplateID(templateID),
			logger.WithTeamID(teamID.String()),
			logger.WithError(err),
		)

		return
	}

	addCount(ctx, counters.candidates, int64(len(builds)))

	keep, err := a.reachabilityKeepSet(ctx, teamID, templateID, builds)
	if err != nil {
		reason := failClosedReason(err)
		logger.L().Warn(ctx, "skipping template artifact cleanup: could not prove build reachability (failing closed, nothing deleted)",
			logger.WithTemplateID(templateID),
			logger.WithTeamID(teamID.String()),
			logger.WithError(err),
		)
		addCountAttrs(ctx, counters.failClosed, int64(len(builds)), attribute.String("reason", reason))

		return
	}

	// The online grace window defers very-new candidates to the offline sweeper
	// (narrows the live-root-query -> DeleteBuild TOCTOU). Flag in seconds;
	// <=0 disables the window. Fallback 24h.
	minAge := time.Duration(a.featureFlags.IntFlag(ctx, featureflags.TemplateArtifactMinAgeSeconds)) * time.Second

	processExclusiveBuilds(ctx, templateID, builds, keep, deleteFn, counters, time.Now(), minAge)
}

// artifactCleanupCounters resolves the reconciliation counters, logging (not
// failing) if the meter cannot provide any of them.
func (a *APIStore) artifactCleanupCounters(ctx context.Context, templateID string) artifactCleanupCounters {
	meter := a.Telemetry.MeterProvider.Meter("github.com/e2b-dev/infra/packages/api/internal/handlers")

	candidates, e1 := telemetry.GetCounter(meter, telemetry.ApiTemplateArtifactsCandidates)
	deleted, e2 := telemetry.GetCounter(meter, telemetry.ApiTemplateArtifactsDeleted)
	failed, e3 := telemetry.GetCounter(meter, telemetry.ApiTemplateArtifactsDeleteFailed)
	skipped, e4 := telemetry.GetCounter(meter, telemetry.ApiTemplateArtifactsSkippedNilNode)
	kept, e5 := telemetry.GetCounter(meter, telemetry.ApiTemplateArtifactsGuardKept)
	failClosed, e6 := telemetry.GetCounter(meter, telemetry.ApiTemplateArtifactsFailClosed)
	tooNew, e7 := telemetry.GetCounter(meter, telemetry.ApiTemplateArtifactsSkippedTooNew)

	if err := firstErr(e1, e2, e3, e4, e5, e6, e7); err != nil {
		logger.L().Error(ctx, "failed to create template artifact cleanup counters",
			logger.WithTemplateID(templateID),
			logger.WithError(err),
		)
	}

	return artifactCleanupCounters{
		candidates:     candidates,
		deleted:        deleted,
		failed:         failed,
		skippedNilNode: skipped,
		guardKept:      kept,
		skippedTooNew:  tooNew,
		failClosed:     failClosed,
	}
}

// processExclusiveBuilds deletes each candidate build's artifacts best-effort.
// A candidate lands in EXACTLY ONE bucket so the counts reconcile:
//   - protected by the reachability guard (still reachable from a live root)  -> guardKept, skip
//   - not yet materialized on a node (nil ClusterNodeID)                       -> skippedNilNode, skip
//   - materialized but younger than minAge (grace window)                      -> skippedTooNew, skip
//   - deleted                                                                  -> deleted
//   - delete attempted but failed                                             -> failed (potential orphan)
//
// The bucket order encodes precedence: guardKept (correctness) first, then
// nil-node (nothing to delete regardless of age), then the grace window, then
// delete. Each delete is attempted independently with bounded concurrency; a
// failure on one never aborts the others. It takes no DB dependency so it is
// unit-testable directly: keep, now and minAge are passed in. A minAge of 0
// disables the grace window.
func processExclusiveBuilds(
	ctx context.Context,
	templateID string,
	builds []queries.GetExclusiveBuildsForTemplateDeletionRow,
	keep map[uuid.UUID]struct{},
	deleteFn deleteBuildFn,
	counters artifactCleanupCounters,
	now time.Time,
	minAge time.Duration,
) {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(artifactCleanupConcurrency)

	for _, b := range builds {
		if _, protected := keep[b.BuildID]; protected {
			// Still reachable from a live root's header closure; deleting it
			// would orphan a shared ancestor. Keep it (space not reclaimed).
			logger.L().Warn(gctx, "keeping build: still referenced by a live build's header closure",
				logger.WithTemplateID(templateID),
				logger.WithBuildID(b.BuildID.String()),
			)
			addCount(gctx, counters.guardKept, 1)

			continue
		}

		if b.ClusterNodeID == nil {
			// Build was never materialized on a node; nothing to delete. Count
			// it (not silent) so candidates still reconciles.
			logger.L().Info(gctx, "skipping build with no cluster node (not materialized)",
				logger.WithTemplateID(templateID),
				logger.WithBuildID(b.BuildID.String()),
			)
			addCount(gctx, counters.skippedNilNode, 1)

			continue
		}

		if minAge > 0 && now.Sub(b.CreatedAt) < minAge {
			// Materialized but younger than the grace window. A freshly created
			// build may have just been reused as a cache-hit ancestor by a
			// concurrent build whose protecting row/header has not landed yet;
			// deleting it now risks orphaning that reuse. Defer to the offline
			// sweeper, which re-derives liveness and age from ground truth.
			logger.L().Info(gctx, "skipping build younger than the online grace window (deferred to offline sweep)",
				logger.WithTemplateID(templateID),
				logger.WithBuildID(b.BuildID.String()),
			)
			addCount(gctx, counters.skippedTooNew, 1)

			continue
		}

		g.Go(func() error {
			clusterID := clusters.WithClusterFallback(b.ClusterID)
			clusterAttr := attribute.String("cluster", clusterID.String())

			if err := deleteFn(gctx, b.BuildID, templateID, clusterID, *b.ClusterNodeID); err != nil {
				logger.L().Warn(gctx, "failed to delete build artifacts",
					logger.WithTemplateID(templateID),
					logger.WithBuildID(b.BuildID.String()),
					logger.WithError(err),
				)
				addCountAttrs(gctx, counters.failed, 1, clusterAttr)

				return nil // best-effort: never abort siblings
			}

			logger.L().Info(gctx, "deleted build artifacts",
				logger.WithTemplateID(templateID),
				logger.WithBuildID(b.BuildID.String()),
			)
			addCountAttrs(gctx, counters.deleted, 1, clusterAttr)

			return nil
		})
	}

	_ = g.Wait() // goroutines never return a non-nil error
}

func addCount(ctx context.Context, c metric.Int64Counter, n int64) {
	if c != nil && n != 0 {
		c.Add(ctx, n)
	}
}

func addCountAttrs(ctx context.Context, c metric.Int64Counter, n int64, attrs ...attribute.KeyValue) {
	if c != nil && n != 0 {
		c.Add(ctx, n, metric.WithAttributes(attrs...))
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
