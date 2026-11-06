package handlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	dashboardqueries "github.com/e2b-dev/infra/packages/db/pkg/dashboard/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/pkg/outbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const projectDeletionReason = "Project deletion"

// ManagementDeleteProject soft-deletes a project and hands the teardown of its
// workloads to the outbox. Repeating it answers 204 again, repeats the cleanup,
// and enqueues another teardown, which is a no-op while one is still open.
func (a *APIStore) ManagementDeleteProject(c *gin.Context, projectID uuid.UUID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithTeamID(projectID.String())}
	telemetry.SetAttributes(ctx, attrs...)

	keyHashes, err := a.deleteProject(ctx, projectID)
	if dberrors.IsNotFoundError(err) {
		telemetry.ReportErrorByCode(ctx, http.StatusNotFound, "delete project failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusNotFound, "Project not found")

		return
	}
	if err != nil {
		telemetry.ReportCriticalError(ctx, "delete project failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error deleting project")

		return
	}

	// Best-effort: the team's other cached entries refresh or expire on their
	// own, and the auth lookups refuse a deleted team once they do.
	for _, hash := range keyHashes {
		a.authService.InvalidateAPIKeyCache(ctx, hash)
	}

	c.Status(http.StatusNoContent)
}

// deleteProject enqueues the teardown in the transaction that marks the project
// deleted, so the teardown runs exactly when the deletion commits. It returns
// the hashes of the API keys it deleted.
func (a *APIStore) deleteProject(ctx context.Context, projectID uuid.UUID) ([]string, error) {
	txDB, tx, err := a.sqlcDB.WithTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin project deletion: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if _, err := txDB.Dashboard.LockProjectForDeletion(ctx, projectID); err != nil {
		return nil, fmt.Errorf("lock project: %w", err)
	}

	keyHashes, err := softDeleteProject(ctx, txDB, projectID)
	if err != nil {
		return nil, err
	}

	if _, err := a.riverClient.InsertTx(ctx, tx, outbox.TeardownTeamResources{TeamID: projectID}, nil); err != nil {
		return nil, fmt.Errorf("enqueue team teardown: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit project deletion: %w", err)
	}

	return keyHashes, nil
}

// softDeleteProject keeps the team row, deletes its memberships and API keys,
// and soft-deletes its templates and snapshots the way a template deletion
// does. Every step is safe to repeat, so a retry also undoes what raced the
// first deletion: a membership, key or snapshot created since, or a block
// reason or cluster assignment written by a delivery.
func softDeleteProject(ctx context.Context, txDB *sqlcdb.Client, projectID uuid.UUID) ([]string, error) {
	if err := txDB.Dashboard.MarkProjectDeleted(ctx, dashboardqueries.MarkProjectDeletedParams{
		ProjectID:     projectID,
		BlockedReason: projectDeletionReason,
	}); err != nil {
		return nil, fmt.Errorf("mark project deleted: %w", err)
	}

	envIDs, err := txDB.Dashboard.SoftDeleteProjectEnvironments(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("soft delete environments: %w", err)
	}
	if len(envIDs) > 0 {
		if err := txDB.Dashboard.ReleaseEnvironmentsAliases(ctx, envIDs); err != nil {
			return nil, fmt.Errorf("release environment aliases: %w", err)
		}
		if err := txDB.Dashboard.DeleteEnvironmentsActiveBuilds(ctx, envIDs); err != nil {
			return nil, fmt.Errorf("delete environment active builds: %w", err)
		}
	}

	if err := txDB.Dashboard.DeleteProjectMembers(ctx, projectID); err != nil {
		return nil, fmt.Errorf("delete members: %w", err)
	}

	keyHashes, err := txDB.Dashboard.DeleteProjectAPIKeys(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("delete API keys: %w", err)
	}

	return keyHashes, nil
}

// IsProjectDeleted reports whether a project has been deleted. Its team row
// remains, so the management writes check this before they apply.
func (a *APIStore) IsProjectDeleted(ctx context.Context, projectID uuid.UUID) (bool, error) {
	return a.sqlcDB.Dashboard.IsProjectDeleted(ctx, projectID)
}
