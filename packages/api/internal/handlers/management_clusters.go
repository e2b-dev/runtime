package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	openapi_types "github.com/oapi-codegen/runtime/types"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusterassign"
	"github.com/e2b-dev/infra/packages/api/internal/management"
	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/pkg/outbox"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const clusterAssignmentRevisionRequiredCode = "cluster_assignment_revision_required"

var (
	errLocalClusterDeletion       = errors.New("the local cluster cannot be deleted")
	errProtectedClusterDeletion   = errors.New("the cluster has deletion protection")
	errClusterInUse               = errors.New("a team still uses the cluster")
	errClusterAssignmentsSettling = errors.New("the cluster service has not yet dropped every team's assignment")
)

func (a *APIStore) ManagementDeleteCluster(c *gin.Context, clusterID api.ClusterID) {
	a.managementRemoveCluster(c, clusterID, false)
}

func (a *APIStore) ManagementRemoveClusterRegistration(c *gin.Context, clusterID api.ClusterID) {
	a.managementRemoveCluster(c, clusterID, true)
}

func (a *APIStore) managementRemoveCluster(c *gin.Context, clusterID uuid.UUID, orphanProjects bool) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithClusterID(clusterID)}

	if !a.requireClusterService(c) {
		return
	}

	err := a.deleteCluster(ctx, clusterID, orphanProjects)
	switch {
	case errors.Is(err, errLocalClusterDeletion):
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "delete cluster failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusBadRequest, "The local cluster cannot be deleted")
	case errors.Is(err, errProtectedClusterDeletion):
		telemetry.ReportErrorByCode(ctx, http.StatusConflict, "delete cluster failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusConflict, "Cluster has deletion protection turned on")
	case errors.Is(err, errClusterInUse):
		telemetry.ReportErrorByCode(ctx, http.StatusConflict, "delete cluster failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusConflict, "Cluster is still referenced by a team or environment")
	case errors.Is(err, errClusterAssignmentsSettling):
		telemetry.ReportErrorByCode(ctx, http.StatusServiceUnavailable, "delete cluster failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusServiceUnavailable, "Cluster assignments are still being cleared, retry the delete")
	case status.Code(err) == codes.Unavailable:
		telemetry.ReportError(ctx, "delete cluster temporarily unavailable", err, attrs...)
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusServiceUnavailable, ClientMsg: "Retry the cluster deletion", Retryable: apierrors.CanRetry})
	case err != nil:
		telemetry.ReportCriticalError(ctx, "delete cluster failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to delete cluster")
	default:
		c.Status(http.StatusNoContent)
	}
}

// clusterDeleteTimeout bounds a cluster delete, which runs detached from the
// request: a caller that leaves must not end it after the cluster service has
// removed the record but before the templates are released.
const clusterDeleteTimeout = time.Minute

// deleteCluster refuses a protected cluster. Then, in one transaction holding
// the cluster's assignment lock, it refuses a cluster a live team uses,
// releases the cluster's templates and has the cluster service remove the
// record. An assignment requested meanwhile waits and is then refused. An
// absent cluster still has its templates released.
func (a *APIStore) deleteCluster(ctx context.Context, clusterID uuid.UUID, orphanProjects bool) error {
	// The local cluster's teams and environments store a NULL cluster_id. The
	// queries below match by equality, which never selects NULL, and its nil ID
	// is refused outright so a request for it never reaches them.
	if clusterID == consts.LocalClusterID {
		return errLocalClusterDeletion
	}

	cluster, err := a.lookUpCluster(ctx, &clusterID)
	if err != nil {
		return fmt.Errorf("read cluster: %w", err)
	}
	if cluster.GetDeletionProtection() {
		return errProtectedClusterDeletion
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), clusterDeleteTimeout)
	defer cancel()
	if orphanProjects {
		return a.removeCluster(ctx, clusterID, true)
	}
	txDB, tx, err := a.sqlcDB.WithTx(ctx)
	if err != nil {
		return fmt.Errorf("begin cluster delete: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := clusterassign.Lock(ctx, tx, clusterID); err != nil {
		return err
	}
	teamIDs, err := txDB.ListClusterTeamIDs(ctx, clusterID)
	if err != nil {
		return fmt.Errorf("list cluster teams: %w", err)
	}
	if len(teamIDs) > 0 {
		return errClusterInUse
	}
	if err := releaseClusterTemplates(ctx, txDB, clusterID); err != nil {
		return err
	}
	if cluster != nil {
		if err := a.removeCluster(ctx, clusterID, false); err != nil {
			return err
		}
		// A sandbox still running on the cluster may have recorded a template
		// while the cluster service worked.
		if err := releaseClusterTemplates(ctx, txDB, clusterID); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit cluster delete: %w", err)
	}

	return nil
}

// removeCluster records the cluster's deletion with the cluster service before
// having it remove the record, so a removal that fails, or that completes
// after the caller gave up on it, still leaves every later assignment refused.
func (a *APIStore) removeCluster(ctx context.Context, clusterID uuid.UUID, orphanProjects bool) error {
	if err := a.recordClusterDeletion(ctx, clusterID); err != nil {
		return err
	}

	_, err := a.clusterService.DeleteCluster(ctx, clustersgrpc.DeleteClusterRequest_builder{ClusterId: clusterID.String(), OrphanProjects: orphanProjects}.Build())
	switch clusterErrorReason(err) {
	case clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_DELETION_PROTECTED:
		return errProtectedClusterDeletion
	// The caller found no live team on the cluster under its lock, so the
	// cluster service's copy is behind: a detached or deleted team's sync has
	// not run yet. That clears on its own, unlike a team still on the cluster.
	case clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_IN_USE:
		return errClusterAssignmentsSettling
	}
	if err != nil {
		return fmt.Errorf("delete cluster: %w", err)
	}

	return nil
}

// releaseClusterTemplates soft-deletes the cluster's live templates and clears
// the cluster from deleted teams and templates, so no row points at it once it
// is deleted.
func releaseClusterTemplates(ctx context.Context, txDB *sqlcdb.Client, clusterID uuid.UUID) error {
	envIDs, err := txDB.SoftDeleteClusterEnvironments(ctx, clusterID)
	if err != nil {
		return fmt.Errorf("soft delete cluster templates: %w", err)
	}
	if len(envIDs) > 0 {
		if err := txDB.ReleaseEnvironmentsAliases(ctx, envIDs); err != nil {
			return fmt.Errorf("release cluster template aliases: %w", err)
		}
		if err := txDB.DeleteEnvironmentsActiveBuilds(ctx, envIDs); err != nil {
			return fmt.Errorf("delete cluster template active builds: %w", err)
		}
	}
	if err := txDB.DetachDeletedTemplatesFromCluster(ctx, clusterID); err != nil {
		return fmt.Errorf("detach deleted templates: %w", err)
	}
	if err := txDB.DetachDeletedTeamsFromCluster(ctx, clusterID); err != nil {
		return fmt.Errorf("detach deleted teams: %w", err)
	}

	return nil
}

func (a *APIStore) ManagementListClusterProjects(c *gin.Context, clusterID api.ClusterID) {
	ctx := c.Request.Context()

	projectIDs, err := a.sqlcDB.ListClusterTeamIDs(ctx, clusterID)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "list cluster projects failed", err, telemetry.WithClusterID(clusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to list cluster projects")

		return
	}
	if projectIDs == nil {
		projectIDs = []uuid.UUID{}
	}

	c.JSON(http.StatusOK, api.ManagementClusterProjects{ProjectIds: projectIDs})
}

func (a *APIStore) ManagementRequestClusterDeletion(c *gin.Context, clusterID api.ClusterID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithClusterID(clusterID)}

	if !a.requireClusterService(c) {
		return
	}

	err := a.requestClusterDeletion(ctx, clusterID)
	switch {
	case errors.Is(err, errLocalClusterDeletion):
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "request cluster deletion failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusBadRequest, "The local cluster cannot be deleted")
	case errors.Is(err, errProtectedClusterDeletion):
		telemetry.ReportErrorByCode(ctx, http.StatusConflict, "request cluster deletion failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusConflict, "Cluster has deletion protection turned on")
	case status.Code(err) == codes.Unavailable:
		telemetry.ReportError(ctx, "record cluster deletion temporarily unavailable", err, attrs...)
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusServiceUnavailable, ClientMsg: "Retry the cluster deletion", Retryable: apierrors.CanRetry})
	case err != nil:
		telemetry.ReportCriticalError(ctx, "request cluster deletion failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to record cluster deletion")
	default:
		c.Status(http.StatusNoContent)
	}
}

// requestClusterDeletion records that the cluster is being deleted. Once it
// returns, no team is newly assigned to the cluster: an assignment in flight
// has committed, and every later one is refused.
func (a *APIStore) requestClusterDeletion(ctx context.Context, clusterID uuid.UUID) error {
	if clusterID == consts.LocalClusterID {
		return errLocalClusterDeletion
	}
	if err := a.recordClusterDeletion(ctx, clusterID); err != nil {
		return err
	}

	return clusterassign.Settle(ctx, a.sqlcDB, clusterID)
}

// recordClusterDeletion has the cluster service record the deletion. It
// refuses a protected cluster, and afterwards refuses turning protection on.
// An absent cluster has nothing to record.
func (a *APIStore) recordClusterDeletion(ctx context.Context, clusterID uuid.UUID) error {
	_, err := a.clusterService.UpdateCluster(ctx, clustersgrpc.UpdateClusterRequest_builder{
		ClusterId:       clusterID.String(),
		RequestDeletion: true,
	}.Build())
	switch {
	case status.Code(err) == codes.NotFound:
		return nil
	case clusterErrorReason(err) == clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_DELETION_PROTECTED:
		return errProtectedClusterDeletion
	case err != nil:
		return fmt.Errorf("record cluster deletion: %w", err)
	}

	return nil
}

func (a *APIStore) ManagementClusterDestroyReadiness(c *gin.Context, clusterID api.ClusterID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithClusterID(clusterID)}

	if !a.requireClusterService(c) {
		return
	}

	// The cluster delete refuses a protected cluster, so refuse here too, before
	// the caller detaches teams or destroys infrastructure.
	cluster, err := a.lookUpCluster(ctx, &clusterID)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "check cluster deletion protection failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to check cluster destroy readiness")

		return
	}
	if cluster.GetDeletionProtection() {
		a.sendAPIStoreError(c, http.StatusConflict, "Cluster has deletion protection turned on.")

		return
	}

	shared, err := a.sqlcDB.ClusterUsedByMultipleTeams(ctx, clusterID)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "check cluster destroy readiness failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to check cluster destroy readiness")

		return
	}
	if shared {
		a.sendAPIStoreError(c, http.StatusConflict, "Cluster is used by more than one team. Move the other teams and their templates off it before destroying the deployment.")

		return
	}

	c.Status(http.StatusNoContent)
}

// enqueueTeamClusterSync has the outbox copy the team's cluster to the cluster
// service once tx commits. Call it with every write of a team's cluster.
func (a *APIStore) enqueueTeamClusterSync(ctx context.Context, tx pgx.Tx, teamID uuid.UUID) error {
	if _, err := a.riverClient.InsertTx(ctx, tx, outbox.SyncTeamClusterAssignment{TeamID: teamID}, nil); err != nil {
		return fmt.Errorf("enqueue team cluster sync: %w", err)
	}

	return nil
}

func (a *APIStore) ManagementDetachProjectCluster(c *gin.Context, projectID openapi_types.UUID, clusterID api.ClusterID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithTeamID(projectID.String()), telemetry.WithClusterID(clusterID)}

	detached, err := a.detachTeamCluster(ctx, projectID, clusterID)
	if dberrors.IsNotFoundError(err) {
		a.sendAPIStoreError(c, http.StatusNotFound, "Team not found")

		return
	}
	if errors.Is(err, management.ErrPlacementConflict) {
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusConflict, ErrorCode: clusterAssignmentRevisionRequiredCode, ClientMsg: "Cluster assignment requires a revision"})

		return
	}
	if err != nil {
		telemetry.ReportCriticalError(ctx, "detach cluster from project failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to detach cluster from team")

		return
	}
	if !detached {
		a.sendAPIStoreError(c, http.StatusConflict, "Team is assigned to a different cluster")

		return
	}

	if err := a.authService.InvalidateTeamCache(ctx, projectID); err != nil {
		telemetry.ReportCriticalError(ctx, "invalidate team cache after cluster detachment failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to invalidate team cache after cluster detachment")

		return
	}

	c.Status(http.StatusNoContent)
}

// A team already on the local cluster satisfies a repeated detach.
func (a *APIStore) detachTeamCluster(ctx context.Context, teamID, clusterID uuid.UUID) (bool, error) {
	txDB, tx, err := a.sqlcDB.WithTx(ctx)
	if err != nil {
		return false, fmt.Errorf("begin cluster detachment: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	current, err := txDB.LockTeamCluster(ctx, teamID)
	if err != nil {
		return false, err
	}
	if current == nil || *current != clusterID {
		return current == nil, nil
	}
	if err := requireUnversionedPlacement(ctx, txDB, teamID); err != nil {
		return false, err
	}
	if err := txDB.DetachTeamCluster(ctx, queries.DetachTeamClusterParams{TeamID: teamID, ClusterID: clusterID}); err != nil {
		return false, err
	}
	if err := a.enqueueTeamClusterSync(ctx, tx, teamID); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit cluster detachment: %w", err)
	}

	return true, nil
}

func requireUnversionedPlacement(ctx context.Context, txDB *sqlcdb.Client, teamID uuid.UUID) error {
	revision, err := txDB.GetProjectPlacementRevision(ctx, teamID)
	if err != nil {
		return fmt.Errorf("read project placement revision: %w", err)
	}
	if revision > 0 {
		return management.ErrPlacementConflict
	}

	return nil
}

// optionalOf reads an optional proto field as a pointer.
func optionalOf(present bool, value string) *string {
	if !present {
		return nil
	}

	return &value
}
