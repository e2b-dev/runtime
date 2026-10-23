package handlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// softDeleteTemplate soft-deletes the env, releases its aliases, and clears its
// active build rows, in a transaction. The env-locking soft-delete runs first so
// a concurrent build registration commits before the alias/active-build cleanup,
// whose fresh per-statement snapshots then see those rows. Returns the released
// alias cache keys.
func (a *APIStore) softDeleteTemplate(ctx context.Context, teamID uuid.UUID, templateID string) ([]string, error) {
	txDB, tx, err := a.sqlcDB.WithTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if _, err := txDB.SoftDeleteTemplate(ctx, queries.SoftDeleteTemplateParams{TemplateID: templateID, TeamID: teamID}); err != nil {
		if dberrors.IsNotFoundError(err) {
			return nil, nil // already deleted or not owned by the team
		}

		return nil, fmt.Errorf("soft delete template: %w", err)
	}

	aliasKeys, err := txDB.ReleaseTemplateAliases(ctx, templateID)
	if err != nil {
		return nil, fmt.Errorf("release aliases: %w", err)
	}

	if err := txDB.DeleteActiveTemplateBuilds(ctx, templateID); err != nil {
		return nil, fmt.Errorf("delete active builds: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	return aliasKeys, nil
}

type deleteBuildFn func(ctx context.Context, buildID uuid.UUID, templateID string, clusterID uuid.UUID, nodeID string) error

// deleteTemplateArtifacts removes GCS/S3 build artifacts for builds that are
// exclusively owned by the given template. It is called after the template has
// been soft-deleted from the database; failures are logged but do not affect the
// HTTP response because the DB deletion is already committed.
func (a *APIStore) deleteTemplateArtifacts(ctx context.Context, teamID uuid.UUID, templateID string) {
	a.deleteTemplateArtifactsWithDeleter(ctx, teamID, templateID, a.templateManager.DeleteBuild)
}

func (a *APIStore) deleteTemplateArtifactsWithDeleter(ctx context.Context, teamID uuid.UUID, templateID string, deleteFn deleteBuildFn) {
	builds, err := a.sqlcDB.GetExclusiveBuildsForTemplateDeletion(ctx, templateID)
	if err != nil {
		logger.L().Error(ctx, "Failed to query exclusive builds for artifact cleanup",
			logger.WithTemplateID(templateID),
			zap.Error(err),
		)

		return
	}

	// A build being "exclusive" in the database (no env_build_assignments row
	// for any other template) does NOT make it safe to delete: intermediate
	// cache layers are never written to the DB, so a build that is DB-exclusive
	// to this template can still be a header-mapping ancestor of another live
	// template's build. Deleting its storage prefix would orphan that ancestor
	// and surface later as a silent, unrecoverable "Input/output error" when
	// the other template's image is reconstructed. Guard against that by only
	// deleting candidates that are NOT reachable from a live root's header
	// closure; fail closed (delete nothing) if reachability cannot be proven.
	candidateIDs := make([]uuid.UUID, 0, len(builds))
	for _, b := range builds {
		candidateIDs = append(candidateIDs, b.BuildID)
	}

	deletable, err := a.deletableAfterReachabilityGuard(ctx, teamID, templateID, candidateIDs)
	if err != nil {
		logger.L().Warn(ctx, "Skipping template artifact cleanup: could not prove build reachability",
			logger.WithTemplateID(templateID),
			logger.WithTeamID(teamID.String()),
			zap.Error(err),
		)

		return
	}

	for _, b := range builds {
		if b.ClusterNodeID == nil {
			continue
		}

		if _, ok := deletable[b.BuildID]; !ok {
			logger.L().Warn(ctx, "Keeping build: still referenced by a live template's header closure",
				logger.WithTemplateID(templateID),
				logger.WithBuildID(b.BuildID.String()),
			)

			continue
		}

		clusterID := clusters.WithClusterFallback(b.ClusterID)
		if err := deleteFn(ctx, b.BuildID, templateID, clusterID, *b.ClusterNodeID); err != nil {
			logger.L().Warn(ctx, "Failed to delete build artifacts",
				logger.WithTemplateID(templateID),
				zap.String("build_id", b.BuildID.String()),
				zap.Error(err),
			)
		} else {
			logger.L().Info(ctx, "Deleted build artifacts",
				logger.WithTemplateID(templateID),
				zap.String("build_id", b.BuildID.String()),
			)
		}
	}
}

// deletableAfterReachabilityGuard returns the subset of candidate builds that
// are safe to physically delete: those NOT reachable from the header closure of
// any other live template of the team. It fails closed (returns an error) when
// the ancestor reader is unavailable or any live root's headers cannot be read,
// so the caller deletes nothing rather than risk corrupting a shared ancestor.
func (a *APIStore) deletableAfterReachabilityGuard(ctx context.Context, teamID uuid.UUID, templateID string, candidates []uuid.UUID) (map[uuid.UUID]struct{}, error) {
	if len(candidates) == 0 {
		return map[uuid.UUID]struct{}{}, nil
	}

	if a.buildAncestors == nil {
		return nil, fmt.Errorf("build ancestor reader not configured; cannot prove reachability")
	}

	liveRoots, err := a.collectLiveRoots(ctx, teamID, templateID)
	if err != nil {
		return nil, err
	}

	keep, err := reachableKeepSet(ctx, a.buildAncestors, liveRoots)
	if err != nil {
		return nil, err
	}

	deletable, _ := partitionDeletableBuilds(candidates, keep)

	out := make(map[uuid.UUID]struct{}, len(deletable))
	for _, b := range deletable {
		out[b] = struct{}{}
	}

	return out, nil
}

// liveSandboxStates are the sandbox states whose build must be protected: any
// state in which the sandbox may still read the build's data. A sandbox in one
// of these states can pin a build that is no longer any template's current
// assignment.
var liveSandboxStates = []sandbox.State{
	sandbox.StateRunning,
	sandbox.StatePausing,
	sandbox.StateSnapshotting,
	sandbox.StateKilling,
}

// collectLiveRoots unions the live-root build IDs from all three sources whose
// data a deletion must not orphan: (1) builds assigned to the team's other
// active templates (DB), (2) the team's in-progress builds still writing to
// storage (DB), and (3) builds pinned by the team's live sandboxes in the
// orchestrator (Redis) — which may reference a build no longer tracked as any
// template's current assignment. Any source failing returns an error so the
// caller fails closed.
func (a *APIStore) collectLiveRoots(ctx context.Context, teamID uuid.UUID, templateID string) ([]uuid.UUID, error) {
	seen := make(map[uuid.UUID]struct{})
	var roots []uuid.UUID
	add := func(id uuid.UUID) {
		if _, ok := seen[id]; ok {
			return
		}

		seen[id] = struct{}{}
		roots = append(roots, id)
	}

	dbRoots, err := a.sqlcDB.GetLiveBuildRootsForTeamExcludingTemplate(ctx, queries.GetLiveBuildRootsForTeamExcludingTemplateParams{
		TeamID:     teamID,
		TemplateID: templateID,
	})
	if err != nil {
		return nil, fmt.Errorf("query live build roots: %w", err)
	}
	for _, id := range dbRoots {
		add(id)
	}

	inProgress, err := a.sqlcDB.GetInProgressBuildRootsForTeam(ctx, teamID)
	if err != nil {
		return nil, fmt.Errorf("query in-progress build roots: %w", err)
	}
	for _, id := range inProgress {
		add(id)
	}

	if a.runtimeSandboxes == nil {
		return nil, fmt.Errorf("runtime sandbox lister not configured; cannot prove reachability")
	}

	sandboxes, err := a.runtimeSandboxes.GetSandboxes(ctx, teamID, liveSandboxStates)
	if err != nil {
		return nil, fmt.Errorf("list live sandboxes: %w", err)
	}
	for _, sbx := range sandboxes {
		add(sbx.BuildID)
	}

	return roots, nil
}

// DeleteTemplatesTemplateID serves to delete a template (e.g. in CLI)
func (a *APIStore) DeleteTemplatesTemplateID(c *gin.Context, aliasOrTemplateID api.TemplateID) {
	ctx := c.Request.Context()

	identifier, _, err := id.ParseName(aliasOrTemplateID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Invalid template ID: %s", err))
		telemetry.ReportCriticalError(ctx, "invalid template ID", err)

		return
	}

	// Resolve template and get the owning team
	team, aliasInfo, apiErr := a.resolveTemplateAndTeam(ctx, c, identifier)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		if apiErr.Code != http.StatusNotFound {
			telemetry.ReportCriticalError(ctx, "error resolving template", apiErr.Err)
		}

		return
	}

	templateID := aliasInfo.TemplateID

	telemetry.SetAttributes(ctx,
		attribute.String("env.team.id", team.ID.String()),
		attribute.String("env.team.name", team.Name),
		telemetry.WithTemplateID(templateID),
	)

	// Check if there are running sandboxes that use this template as their base
	sandboxes, err := a.orchestrator.GetSandboxes(ctx, team.ID, []sandbox.State{sandbox.StateRunning, sandbox.StatePausing, sandbox.StateSnapshotting})
	if err != nil {
		telemetry.ReportError(ctx, "error when checking for running sandboxes", err)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when checking for running sandboxes")

		return
	}

	for _, sbx := range sandboxes {
		if sbx.BaseTemplateID == templateID {
			a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("cannot delete template '%s' because there are running sandboxes using it", templateID))

			return
		}
	}

	// check if base template has snapshots
	hasSnapshots, err := a.sqlcDB.ExistsTemplateSnapshots(ctx, templateID)
	if err != nil {
		telemetry.ReportError(ctx, "error when checking if base template has snapshots", err)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when checking if template has snapshots")

		return
	}

	if hasSnapshots {
		telemetry.ReportError(ctx, "base template has paused sandboxes", nil, telemetry.WithTemplateID(templateID))
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("cannot delete template '%s' because there are paused sandboxes using it", templateID))

		return
	}

	aliasKeys, err := a.softDeleteTemplate(ctx, team.ID, templateID)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error when deleting template from db", err)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when deleting template")

		return
	}

	a.templateCache.InvalidateAllTags(context.WithoutCancel(ctx), templateID)
	a.templateCache.InvalidateAliasesByTemplateID(context.WithoutCancel(ctx), templateID, aliasKeys)

	// Delete GCS/S3 artifacts in the background; DB deletion is already committed
	// so artifact cleanup failures must not block or fail the HTTP response.
	go a.deleteTemplateArtifacts(context.WithoutCancel(ctx), team.ID, templateID)

	telemetry.ReportEvent(ctx, "deleted template from db")

	properties := a.posthog.GetPackageToPosthogProperties(&c.Request.Header)
	a.posthog.IdentifyAnalyticsTeam(ctx, team.ID.String(), team.Name)
	a.posthog.CreateAnalyticsTeamEvent(ctx, team.ID.String(), "deleted environment", properties.Set("environment", templateID))

	logger.L().Info(ctx, "Deleted template", logger.WithTemplateID(templateID), logger.WithTeamID(team.ID.String()))

	c.Status(http.StatusNoContent)
}
