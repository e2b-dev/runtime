package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	openapi_types "github.com/oapi-codegen/runtime/types"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	dashboardqueries "github.com/e2b-dev/infra/packages/db/pkg/dashboard/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	clusterRegistrationInvalidCode       = "cluster_registration_invalid"
	clusterRegistrationConflictCode      = "cluster_registration_conflict"
	clusterAssignmentInvalidCode         = "cluster_assignment_invalid"
	clusterAssignmentProjectNotFoundCode = "cluster_assignment_project_not_found"
	clusterAssignmentClusterNotFoundCode = "cluster_assignment_cluster_not_found"
	clusterAssignmentAlreadyAssignedCode = "cluster_assignment_already_assigned"
	clusterAssignmentClusterDeletingCode = "cluster_assignment_cluster_deleting"
)

var (
	errInvalidClusterRegistration = errors.New("invalid cluster registration")
	errLocalClusterDeletion       = errors.New("the local cluster cannot be deleted")
	errProtectedClusterDeletion   = errors.New("the cluster has deletion protection")
)

type clusterRegistration struct {
	ClusterID          *uuid.UUID
	Name               string
	Endpoint           string
	EndpointTLS        bool
	Token              string
	SandboxProxyDomain *string
	AuthOrgID          *string
	DeletionProtection bool
	RefreshToken       bool
}

func (a *APIStore) ManagementRegisterCluster(c *gin.Context, clusterID api.ClusterID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithClusterID(clusterID)}

	body, err := ginutils.ParseBody[api.ManagementClusterRegistrationRequest](ctx, c)
	if err != nil {
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "register cluster failed", err, attrs...)
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ErrorCode: clusterRegistrationInvalidCode, ClientMsg: "Invalid cluster registration"})

		return
	}

	_, err = a.createCluster(ctx, clusterRegistration{
		ClusterID:          &clusterID,
		Name:               body.Name,
		Endpoint:           body.Endpoint,
		EndpointTLS:        body.EndpointTls,
		Token:              body.Token,
		SandboxProxyDomain: body.SandboxProxyDomain,
		AuthOrgID:          body.AuthOrgId,
		// An authorized replacement keeps the descriptor and assignments but
		// generates a new edge credential. Generic admin creation stays strict.
		RefreshToken: true,
		// A cluster registered through the management API belongs to a
		// deployment its caller also tears down, so it starts unprotected.
		// Registering an existing cluster again keeps its stored protection.
		DeletionProtection: false,
	})
	switch {
	case errors.Is(err, errInvalidClusterRegistration):
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "register cluster failed", err, attrs...)
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ErrorCode: clusterRegistrationInvalidCode, ClientMsg: "Invalid cluster registration"})
	case dberrors.IsUniqueConstraintViolation(err), dberrors.IsNotFoundError(err):
		telemetry.ReportErrorByCode(ctx, http.StatusConflict, "register cluster failed", err, attrs...)
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusConflict, ErrorCode: clusterRegistrationConflictCode, ClientMsg: "Cluster conflicts with stored state"})
	case err != nil:
		telemetry.ReportCriticalError(ctx, "register cluster failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to register cluster")
	default:
		c.Status(http.StatusNoContent)
	}
}

// createCluster stores a new cluster, or answers for an existing one with the
// same ID when its configuration matches. Without RefreshToken the token is
// part of that configuration; with it, a matching cluster takes the new token
// and connection details.
func (a *APIStore) createCluster(ctx context.Context, registration clusterRegistration) (uuid.UUID, error) {
	registration.Name = strings.TrimSpace(registration.Name)
	registration.Endpoint = strings.TrimSpace(registration.Endpoint)
	registration.Token = strings.TrimSpace(registration.Token)
	registration.SandboxProxyDomain = trimmedOptional(registration.SandboxProxyDomain)
	registration.AuthOrgID = trimmedOptional(registration.AuthOrgID)
	if registration.Name == "" || registration.Endpoint == "" || registration.Token == "" ||
		registration.ClusterID != nil && *registration.ClusterID == uuid.Nil {
		return uuid.Nil, errInvalidClusterRegistration
	}

	params := dashboardqueries.CreateClusterParams{
		ClusterID:          registration.ClusterID,
		Name:               registration.Name,
		Endpoint:           registration.Endpoint,
		EndpointTls:        registration.EndpointTLS,
		Token:              registration.Token,
		SandboxProxyDomain: registration.SandboxProxyDomain,
		AuthOrgID:          registration.AuthOrgID,
		DeletionProtection: registration.DeletionProtection,
	}
	if registration.RefreshToken {
		return a.sqlcDB.Dashboard.RegisterManagedCluster(ctx, dashboardqueries.RegisterManagedClusterParams(params))
	}

	return a.sqlcDB.Dashboard.CreateCluster(ctx, params)
}

func (a *APIStore) ManagementDeleteCluster(c *gin.Context, clusterID api.ClusterID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithClusterID(clusterID)}

	err := a.deleteCluster(ctx, clusterID)
	switch {
	case errors.Is(err, errLocalClusterDeletion):
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "delete cluster failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusBadRequest, "The local cluster cannot be deleted")
	case errors.Is(err, errProtectedClusterDeletion):
		telemetry.ReportErrorByCode(ctx, http.StatusConflict, "delete cluster failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusConflict, "Cluster has deletion protection turned on")
	case dberrors.IsForeignKeyViolation(err):
		telemetry.ReportErrorByCode(ctx, http.StatusConflict, "delete cluster failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusConflict, "Cluster is still referenced by a team or environment")
	case err != nil:
		telemetry.ReportCriticalError(ctx, "delete cluster failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to delete cluster")
	default:
		c.Status(http.StatusNoContent)
	}
}

// deleteCluster soft-deletes the cluster's environments and removes the
// cluster in one transaction. A team still assigned to the cluster fails the
// final delete on its foreign key, which rolls the environments back too.
func (a *APIStore) deleteCluster(ctx context.Context, clusterID uuid.UUID) error {
	// The local cluster's teams and environments store a NULL cluster_id. The
	// queries below match by equality, which never selects NULL, and its nil ID
	// is refused outright so a request for it never reaches them.
	if clusterID == consts.LocalClusterID {
		return errLocalClusterDeletion
	}

	txDB, tx, err := a.sqlcDB.WithTx(ctx)
	if err != nil {
		return fmt.Errorf("begin cluster deletion: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	protected, err := txDB.Dashboard.ClusterDeletionProtected(ctx, clusterID)
	if err != nil {
		return fmt.Errorf("check cluster deletion protection: %w", err)
	}
	if protected {
		return errProtectedClusterDeletion
	}

	envIDs, err := txDB.Dashboard.SoftDeleteClusterEnvironments(ctx, clusterID)
	if err != nil {
		return fmt.Errorf("soft delete cluster environments: %w", err)
	}
	if len(envIDs) > 0 {
		if err := txDB.Dashboard.ReleaseEnvironmentsAliases(ctx, envIDs); err != nil {
			return fmt.Errorf("release cluster environment aliases: %w", err)
		}
		if err := txDB.Dashboard.DeleteEnvironmentsActiveBuilds(ctx, envIDs); err != nil {
			return fmt.Errorf("delete cluster environment active builds: %w", err)
		}
	}
	if err := txDB.Dashboard.DetachDeletedTemplatesFromCluster(ctx, clusterID); err != nil {
		return fmt.Errorf("detach deleted templates: %w", err)
	}
	if err := txDB.Dashboard.DetachDeletedTeamsFromCluster(ctx, clusterID); err != nil {
		return fmt.Errorf("detach deleted teams: %w", err)
	}
	if _, err := txDB.Dashboard.DeleteCluster(ctx, clusterID); err != nil {
		return fmt.Errorf("delete cluster: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit cluster deletion: %w", err)
	}

	return nil
}

func (a *APIStore) ManagementListClusterProjects(c *gin.Context, clusterID api.ClusterID) {
	ctx := c.Request.Context()

	projectIDs, err := a.sqlcDB.Dashboard.ListClusterTeamIDs(ctx, clusterID)
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

	err := a.requestClusterDeletion(ctx, clusterID)
	switch {
	case errors.Is(err, errLocalClusterDeletion):
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "request cluster deletion failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusBadRequest, "The local cluster cannot be deleted")
	case errors.Is(err, errProtectedClusterDeletion):
		telemetry.ReportErrorByCode(ctx, http.StatusConflict, "request cluster deletion failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusConflict, "Cluster has deletion protection turned on")
	case err != nil:
		telemetry.ReportCriticalError(ctx, "request cluster deletion failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to record cluster deletion")
	default:
		c.Status(http.StatusNoContent)
	}
}

// requestClusterDeletion records, under the cluster row's lock, that the
// cluster is being deleted. A protection change in flight commits first and is
// seen here; once recorded, a check constraint refuses turning protection on.
func (a *APIStore) requestClusterDeletion(ctx context.Context, clusterID uuid.UUID) error {
	if clusterID == consts.LocalClusterID {
		return errLocalClusterDeletion
	}

	txDB, tx, err := a.sqlcDB.WithTx(ctx)
	if err != nil {
		return fmt.Errorf("begin cluster deletion request: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	protected, err := txDB.Dashboard.LockClusterDeletionProtection(ctx, clusterID)
	if dberrors.IsNotFoundError(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lock cluster for deletion: %w", err)
	}
	if protected {
		return errProtectedClusterDeletion
	}
	if err := txDB.Dashboard.RequestClusterDeletion(ctx, clusterID); err != nil {
		return fmt.Errorf("record cluster deletion: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit cluster deletion request: %w", err)
	}

	return nil
}

func (a *APIStore) ManagementClusterDestroyReadiness(c *gin.Context, clusterID api.ClusterID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithClusterID(clusterID)}

	// The cluster delete refuses a protected cluster, so refuse here too, before
	// the caller detaches teams or destroys infrastructure.
	protected, err := a.sqlcDB.Dashboard.ClusterDeletionProtected(ctx, clusterID)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "check cluster deletion protection failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to check cluster destroy readiness")

		return
	}
	if protected {
		a.sendAPIStoreError(c, http.StatusConflict, "Cluster has deletion protection turned on.")

		return
	}

	shared, err := a.sqlcDB.Dashboard.ClusterUsedByMultipleTeams(ctx, clusterID)
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

func (a *APIStore) ManagementAssignProjectCluster(c *gin.Context, projectID openapi_types.UUID, clusterID api.ClusterID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithTeamID(projectID.String()), telemetry.WithClusterID(clusterID)}

	if clusterID == uuid.Nil {
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ErrorCode: clusterAssignmentInvalidCode, ClientMsg: "cluster_id is required"})

		return
	}

	result, err := a.sqlcDB.Dashboard.AssignTeamCluster(ctx, dashboardqueries.AssignTeamClusterParams{
		ClusterID:        clusterID,
		TeamID:           projectID,
		PreserveExisting: true,
	})
	switch {
	case dberrors.IsNotFoundError(err):
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusNotFound, ErrorCode: clusterAssignmentProjectNotFoundCode, ClientMsg: "Team not found"})

		return
	case dberrors.IsForeignKeyViolation(err):
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusNotFound, ErrorCode: clusterAssignmentClusterNotFoundCode, ClientMsg: "Cluster not found"})

		return
	case dberrors.ConstraintName(err) == clusterDeletingConstraint:
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusConflict, ErrorCode: clusterAssignmentClusterDeletingCode, ClientMsg: "Cluster is being deleted"})

		return
	case err != nil:
		telemetry.ReportCriticalError(ctx, "assign cluster to project failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to assign cluster to team")

		return
	}
	if !result.Assigned {
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusConflict, ErrorCode: clusterAssignmentAlreadyAssignedCode, ClientMsg: "Team is already assigned to a different cluster"})

		return
	}

	// Sandbox placement reads the cluster from the cached team. A failed
	// eviction is reported so the caller retries; the replay is idempotent.
	if err := a.authService.InvalidateTeamCache(ctx, projectID); err != nil {
		telemetry.ReportCriticalError(ctx, "invalidate team cache after cluster assignment failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to invalidate team cache after cluster assignment")

		return
	}

	c.Status(http.StatusNoContent)
}

func (a *APIStore) ManagementDetachProjectCluster(c *gin.Context, projectID openapi_types.UUID, clusterID api.ClusterID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithTeamID(projectID.String()), telemetry.WithClusterID(clusterID)}

	result, err := a.sqlcDB.Dashboard.DetachTeamCluster(ctx, dashboardqueries.DetachTeamClusterParams{
		TeamID:    projectID,
		ClusterID: clusterID,
	})
	if dberrors.IsNotFoundError(err) {
		a.sendAPIStoreError(c, http.StatusNotFound, "Team not found")

		return
	}
	if err != nil {
		telemetry.ReportCriticalError(ctx, "detach cluster from project failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to detach cluster from team")

		return
	}
	if result.ClusterID != nil && !result.Detached {
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

func trimmedOptional(value *string) *string {
	if value == nil {
		return nil
	}

	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}

	return &trimmed
}
