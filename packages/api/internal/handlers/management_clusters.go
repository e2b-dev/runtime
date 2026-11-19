package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	openapi_types "github.com/oapi-codegen/runtime/types"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusterassign"
	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
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
	errInvalidClusterRegistration  = errors.New("invalid cluster registration")
	errClusterRegistrationConflict = errors.New("cluster conflicts with stored state")
	errLocalClusterDeletion        = errors.New("the local cluster cannot be deleted")
	errProtectedClusterDeletion    = errors.New("the cluster has deletion protection")
	errClusterInUse                = errors.New("a team still uses the cluster")
)

// clusterRefreshAttempts bounds the create, read and update a re-registration
// repeats when the stored cluster changes or disappears in between.
const clusterRefreshAttempts = 3

type clusterRegistration struct {
	TelemetryClientID  *string
	ClusterID          *uuid.UUID
	Name               string
	Endpoint           string
	EndpointTLS        bool
	Token              string
	SandboxProxyDomain *string
	AuthOrgID          *string
	DeletionProtection bool
}

// normalize trims every value and refuses a registration missing a required
// one or naming the local cluster.
func (r *clusterRegistration) normalize() error {
	r.Name = strings.TrimSpace(r.Name)
	r.Endpoint = strings.TrimSpace(r.Endpoint)
	r.Token = strings.TrimSpace(r.Token)
	r.SandboxProxyDomain = trimmedOptional(r.SandboxProxyDomain)
	r.AuthOrgID = trimmedOptional(r.AuthOrgID)
	r.TelemetryClientID = trimmedOptional(r.TelemetryClientID)
	if r.Name == "" || r.Endpoint == "" || r.Token == "" ||
		r.ClusterID != nil && *r.ClusterID == uuid.Nil {
		return errInvalidClusterRegistration
	}

	return nil
}

func (a *APIStore) ManagementRegisterCluster(c *gin.Context, clusterID api.ClusterID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithClusterID(clusterID)}

	if !a.requireClusterService(c) {
		return
	}

	body, err := ginutils.ParseBody[api.ManagementClusterRegistrationRequest](ctx, c)
	if err != nil {
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "register cluster failed", err, attrs...)
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ErrorCode: clusterRegistrationInvalidCode, ClientMsg: "Invalid cluster registration"})

		return
	}

	err = a.registerManagedCluster(ctx, clusterRegistration{
		ClusterID:          &clusterID,
		Name:               body.Name,
		Endpoint:           body.Endpoint,
		EndpointTLS:        body.EndpointTls,
		Token:              body.Token,
		SandboxProxyDomain: body.SandboxProxyDomain,
		AuthOrgID:          body.AuthOrgId,
		TelemetryClientID:  body.TelemetryClientId,
		// A cluster registered through the management API belongs to a
		// deployment its caller also tears down, so it starts unprotected.
		// Registering an existing cluster again keeps its stored protection.
		DeletionProtection: false,
	})
	switch {
	case errors.Is(err, errInvalidClusterRegistration):
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "register cluster failed", err, attrs...)
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ErrorCode: clusterRegistrationInvalidCode, ClientMsg: "Invalid cluster registration"})
	case errors.Is(err, errClusterRegistrationConflict):
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
// same ID when every value but deletion protection matches.
func (a *APIStore) createCluster(ctx context.Context, registration clusterRegistration) (*clustersgrpc.Cluster, error) {
	if err := registration.normalize(); err != nil {
		return nil, err
	}

	req := clustersgrpc.CreateClusterRequest_builder{
		Name:               registration.Name,
		Endpoint:           registration.Endpoint,
		EndpointTls:        &registration.EndpointTLS,
		Token:              registration.Token,
		SandboxProxyDomain: registration.SandboxProxyDomain,
		AuthOrgId:          registration.AuthOrgID,
		TelemetryClientId:  registration.TelemetryClientID,
		DeletionProtection: &registration.DeletionProtection,
	}
	if registration.ClusterID != nil {
		req.ClusterId = new(registration.ClusterID.String())
	}

	resp, err := a.clusterService.CreateCluster(ctx, req.Build())
	switch status.Code(err) {
	case codes.OK:
		return resp.GetCluster(), nil
	case codes.AlreadyExists:
		return nil, fmt.Errorf("%w: %w", errClusterRegistrationConflict, err)
	case codes.InvalidArgument:
		return nil, fmt.Errorf("%w: %w", errInvalidClusterRegistration, err)
	default:
		return nil, fmt.Errorf("create cluster: %w", err)
	}
}

// registerManagedCluster stores the cluster, or refreshes the stored one with
// the same ID and auth organization: it takes the new edge credential,
// endpoint, TLS setting and sandbox domain, then a telemetry identity reported
// for the first time, and keeps the name, deletion protection and assignments.
func (a *APIStore) registerManagedCluster(ctx context.Context, registration clusterRegistration) error {
	if err := registration.normalize(); err != nil {
		return err
	}

	for range clusterRefreshAttempts {
		_, err := a.createCluster(ctx, registration)
		if !errors.Is(err, errClusterRegistrationConflict) {
			return err
		}

		// An unknown ID means the conflict is with another cluster's auth
		// organization or telemetry identity, or the cluster went away since
		// the create; the next attempt tells the two apart.
		stored, err := a.lookUpCluster(ctx, registration.ClusterID)
		if err != nil {
			return fmt.Errorf("read registered cluster: %w", err)
		}
		if stored == nil {
			continue
		}
		storedAuthOrgID := optionalOf(stored.HasAuthOrgId(), stored.GetAuthOrgId())
		if !sameOptional(storedAuthOrgID, registration.AuthOrgID) {
			return fmt.Errorf("%w: the cluster belongs to another auth organization", errClusterRegistrationConflict)
		}

		_, err = a.clusterService.UpdateCluster(ctx, clustersgrpc.UpdateClusterRequest_builder{
			ClusterId:          registration.ClusterID.String(),
			Endpoint:           &registration.Endpoint,
			EndpointTls:        &registration.EndpointTLS,
			Token:              &registration.Token,
			SandboxProxyDomain: clustersgrpc.NullableStringUpdate_builder{Value: registration.SandboxProxyDomain}.Build(),
			Etag:               new(stored.GetEtag()),
		}.Build())
		switch status.Code(err) {
		case codes.OK:
			return a.registerTelemetryClient(ctx, *registration.ClusterID, registration.TelemetryClientID, stored)
		case codes.Aborted, codes.NotFound:
			// Changed or removed since the read: start over.
			continue
		case codes.AlreadyExists, codes.FailedPrecondition:
			return fmt.Errorf("%w: %w", errClusterRegistrationConflict, err)
		case codes.InvalidArgument:
			return fmt.Errorf("%w: %w", errInvalidClusterRegistration, err)
		default:
			return fmt.Errorf("refresh registered cluster: %w", err)
		}
	}

	return fmt.Errorf("%w: the cluster kept changing during registration", errClusterRegistrationConflict)
}

// registerTelemetryClient sets a telemetry identity the stored cluster does not
// have yet. The cluster service sets an identity once, so a different one is a
// conflict; it is written after the connection details so that the conflict
// never costs the cluster its new credential.
func (a *APIStore) registerTelemetryClient(ctx context.Context, clusterID uuid.UUID, telemetryClientID *string, stored *clustersgrpc.Cluster) error {
	storedTelemetryClientID := optionalOf(stored.HasTelemetryClientId(), stored.GetTelemetryClientId())
	if telemetryClientID == nil || sameOptional(storedTelemetryClientID, telemetryClientID) {
		return nil
	}

	_, err := a.clusterService.UpdateCluster(ctx, clustersgrpc.UpdateClusterRequest_builder{
		ClusterId:         clusterID.String(),
		TelemetryClientId: telemetryClientID,
	}.Build())
	switch {
	case err == nil,
		// Telemetry names leave out a cluster being deleted, so its identity
		// would never be read.
		clusterErrorReason(err) == clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_DELETION_REQUESTED:
		return nil
	case status.Code(err) == codes.AlreadyExists,
		status.Code(err) == codes.NotFound,
		clusterErrorReason(err) == clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_TELEMETRY_CLIENT_ASSIGNED:
		return fmt.Errorf("%w: telemetry identity: %w", errClusterRegistrationConflict, err)
	case status.Code(err) == codes.InvalidArgument:
		return fmt.Errorf("%w: %w", errInvalidClusterRegistration, err)
	default:
		return fmt.Errorf("register telemetry identity: %w", err)
	}
}

func (a *APIStore) ManagementDeleteCluster(c *gin.Context, clusterID api.ClusterID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithClusterID(clusterID)}

	if !a.requireClusterService(c) {
		return
	}

	err := a.deleteCluster(ctx, clusterID)
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
func (a *APIStore) deleteCluster(ctx context.Context, clusterID uuid.UUID) error {
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
		if err := a.removeCluster(ctx, clusterID); err != nil {
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
func (a *APIStore) removeCluster(ctx context.Context, clusterID uuid.UUID) error {
	if err := a.recordClusterDeletion(ctx, clusterID); err != nil {
		return err
	}

	_, err := a.clusterService.DeleteCluster(ctx, clustersgrpc.DeleteClusterRequest_builder{ClusterId: clusterID.String()}.Build())
	switch clusterErrorReason(err) {
	case clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_DELETION_PROTECTED:
		return errProtectedClusterDeletion
	case clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_IN_USE:
		return errClusterInUse
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

func (a *APIStore) ManagementAssignProjectCluster(c *gin.Context, projectID openapi_types.UUID, clusterID api.ClusterID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithTeamID(projectID.String()), telemetry.WithClusterID(clusterID)}

	if clusterID == uuid.Nil {
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ErrorCode: clusterAssignmentInvalidCode, ClientMsg: "cluster_id is required"})

		return
	}
	// Without a cluster service the pool never loads a remote cluster.
	if !a.requireClusterService(c) {
		return
	}

	assigned, err := a.assignTeamCluster(ctx, projectID, clusterID)
	switch {
	case dberrors.IsNotFoundError(err):
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusNotFound, ErrorCode: clusterAssignmentProjectNotFoundCode, ClientMsg: "Team not found"})

		return
	case errors.Is(err, clusterassign.ErrNotFound):
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusNotFound, ErrorCode: clusterAssignmentClusterNotFoundCode, ClientMsg: "Cluster not found"})

		return
	case errors.Is(err, clusterassign.ErrDeleting):
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusConflict, ErrorCode: clusterAssignmentClusterDeletingCode, ClientMsg: "Cluster is being deleted"})

		return
	case err != nil:
		telemetry.ReportCriticalError(ctx, "assign cluster to project failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to assign cluster to team")

		return
	}
	if !assigned {
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

// assignTeamCluster assigns the team to the cluster and reports false when it
// is on a different one. A new assignment needs a cluster the cluster service
// knows and is not deleting; keeping the team's current cluster is not one.
func (a *APIStore) assignTeamCluster(ctx context.Context, teamID, clusterID uuid.UUID) (bool, error) {
	txDB, tx, err := a.sqlcDB.WithTx(ctx)
	if err != nil {
		return false, fmt.Errorf("begin cluster assignment: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if err := clusterassign.Lock(ctx, tx, clusterID); err != nil {
		return false, err
	}
	current, err := txDB.LockTeamCluster(ctx, teamID)
	if err != nil {
		return false, err
	}
	switch {
	case current == nil:
		if err := clusterassign.Check(ctx, a.clusterService, clusterID); err != nil {
			return false, err
		}
	case *current != clusterID:
		return false, nil
	}

	result, err := txDB.AssignTeamCluster(ctx, queries.AssignTeamClusterParams{
		ClusterID:        clusterID,
		TeamID:           teamID,
		PreserveExisting: true,
	})
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit cluster assignment: %w", err)
	}

	return result.Assigned, nil
}

func (a *APIStore) ManagementDetachProjectCluster(c *gin.Context, projectID openapi_types.UUID, clusterID api.ClusterID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithTeamID(projectID.String()), telemetry.WithClusterID(clusterID)}

	result, err := a.sqlcDB.DetachTeamCluster(ctx, queries.DetachTeamClusterParams{
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

// optionalOf reads an optional proto field as a pointer.
func optionalOf(present bool, value string) *string {
	if !present {
		return nil
	}

	return &value
}

func sameOptional(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}

	return *a == *b
}
