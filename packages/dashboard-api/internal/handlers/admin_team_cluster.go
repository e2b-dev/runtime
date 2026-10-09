package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	dashboardqueries "github.com/e2b-dev/infra/packages/db/pkg/dashboard/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

var (
	errInvalidClusterRegistration = errors.New("invalid cluster registration")
	errLocalClusterDeletion       = errors.New("the local cluster cannot be deleted")
	errProtectedClusterDeletion   = errors.New("the cluster has deletion protection")
)

const enterpriseClusterAssignmentPolicyMessage = "only teams on an enterprise tier can be assigned to a BYOC cluster; upgrade the team to enterprise first"

type clusterRegistration struct {
	ClusterID          *uuid.UUID
	Name               string
	Endpoint           string
	EndpointTLS        bool
	Token              string
	SandboxProxyDomain *string
	AuthOrgID          *string
	DeletionProtection bool
}

func (s *APIStore) PostAdminClusters(c *gin.Context) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.AdminClusterCreateRequest](ctx, c)
	if err != nil {
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ErrorCode: string(api.ClusterRegistrationInvalid), ClientMsg: fmt.Sprintf("Error when parsing request: %s", err)})

		return
	}

	clusterID, err := s.createCluster(ctx, clusterRegistration{
		ClusterID:          body.ClusterId,
		Name:               body.Name,
		Endpoint:           body.Endpoint,
		EndpointTLS:        body.EndpointTls,
		Token:              body.Token,
		SandboxProxyDomain: body.SandboxProxyDomain,
		AuthOrgID:          body.AuthOrgId,
		DeletionProtection: true,
	})
	if errors.Is(err, errInvalidClusterRegistration) {
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ErrorCode: string(api.ClusterRegistrationInvalid), ClientMsg: "name, endpoint and token are required"})

		return
	}
	if err != nil {
		if dberrors.IsUniqueConstraintViolation(err) || dberrors.IsNotFoundError(err) {
			apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusConflict, ErrorCode: string(api.ClusterRegistrationConflict), ClientMsg: "Cluster ID or auth organization is already registered"})
		} else {
			s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to create cluster")
		}

		return
	}

	logger.L().Info(ctx, "admin cluster created", logger.WithClusterID(clusterID))
	c.JSON(http.StatusCreated, api.AdminClusterCreateResponse{ClusterId: clusterID})
}

func (s *APIStore) createCluster(ctx context.Context, registration clusterRegistration) (uuid.UUID, error) {
	registration.Name = strings.TrimSpace(registration.Name)
	registration.Endpoint = strings.TrimSpace(registration.Endpoint)
	registration.Token = strings.TrimSpace(registration.Token)
	registration.SandboxProxyDomain = trimmedOptional(registration.SandboxProxyDomain)
	registration.AuthOrgID = trimmedOptional(registration.AuthOrgID)
	if registration.Name == "" || registration.Endpoint == "" || registration.Token == "" ||
		registration.ClusterID != nil && *registration.ClusterID == uuid.Nil {
		return uuid.Nil, errInvalidClusterRegistration
	}

	return s.db.Dashboard.CreateCluster(ctx, dashboardqueries.CreateClusterParams{
		ClusterID:          registration.ClusterID,
		Name:               registration.Name,
		Endpoint:           registration.Endpoint,
		EndpointTls:        registration.EndpointTLS,
		Token:              registration.Token,
		SandboxProxyDomain: registration.SandboxProxyDomain,
		AuthOrgID:          registration.AuthOrgID,
		DeletionProtection: registration.DeletionProtection,
	})
}

func (s *APIStore) DeleteAdminClustersClusterID(c *gin.Context, clusterID api.ClusterID) {
	ctx := c.Request.Context()

	err := s.deleteCluster(ctx, clusterID)
	if err != nil {
		switch {
		case errors.Is(err, errLocalClusterDeletion):
			s.sendAPIStoreError(c, http.StatusBadRequest, "The local cluster cannot be deleted")
		case errors.Is(err, errProtectedClusterDeletion):
			s.sendAPIStoreError(c, http.StatusConflict, "Cluster has deletion protection turned on")
		case dberrors.IsForeignKeyViolation(err):
			s.sendAPIStoreError(c, http.StatusConflict, "Cluster is still referenced by a team or environment")
		default:
			s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to delete cluster")
		}

		return
	}

	logger.L().Info(ctx, "admin cluster deleted", logger.WithClusterID(clusterID))
	c.Status(http.StatusNoContent)
}

func (s *APIStore) deleteCluster(ctx context.Context, clusterID uuid.UUID) error {
	// The local cluster's teams and environments store a NULL cluster_id. The
	// queries below match by equality, which never selects NULL, and its nil ID
	// is refused outright so a request for it never reaches them.
	if clusterID == consts.LocalClusterID {
		return errLocalClusterDeletion
	}

	db, tx, err := s.db.WithTx(ctx)
	if err != nil {
		return fmt.Errorf("begin cluster deletion: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	protected, err := db.Dashboard.ClusterDeletionProtected(ctx, clusterID)
	if err != nil {
		return fmt.Errorf("check cluster deletion protection: %w", err)
	}
	if protected {
		return errProtectedClusterDeletion
	}

	envIDs, err := db.Dashboard.SoftDeleteClusterEnvironments(ctx, clusterID)
	if err != nil {
		return fmt.Errorf("soft delete cluster environments: %w", err)
	}
	if len(envIDs) > 0 {
		if err := db.Dashboard.ReleaseEnvironmentsAliases(ctx, envIDs); err != nil {
			return fmt.Errorf("release cluster environment aliases: %w", err)
		}
		if err := db.Dashboard.DeleteEnvironmentsActiveBuilds(ctx, envIDs); err != nil {
			return fmt.Errorf("delete cluster environment active builds: %w", err)
		}
	}
	if err := db.Dashboard.DetachDeletedTemplatesFromCluster(ctx, clusterID); err != nil {
		return fmt.Errorf("detach deleted templates: %w", err)
	}
	if _, err := db.Dashboard.DeleteCluster(ctx, clusterID); err != nil {
		return fmt.Errorf("delete cluster: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit cluster deletion: %w", err)
	}

	return nil
}

func (s *APIStore) GetAdminTeamsTeamIDCluster(c *gin.Context, teamID api.TeamID) {
	clusterID, err := s.db.Dashboard.TeamClusterAssignment(
		c.Request.Context(),
		teamID,
	)
	if dberrors.IsNotFoundError(err) {
		s.sendAPIStoreError(c, http.StatusNotFound, "Team cluster assignment not found")

		return
	}
	if err != nil {
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to load team cluster assignment")

		return
	}

	if clusterID == nil {
		s.sendAPIStoreError(c, http.StatusNotFound, "Team cluster assignment not found")

		return
	}

	c.JSON(http.StatusOK, api.AdminTeamClusterAssignmentResponse{ClusterId: *clusterID})
}

func (s *APIStore) assignTeamCluster(c *gin.Context, teamID, clusterID uuid.UUID, preserveExisting bool) {
	if clusterID == uuid.Nil {
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ErrorCode: string(api.ClusterAssignmentInvalid), ClientMsg: "cluster_id is required"})

		return
	}

	ctx := c.Request.Context()
	result, err := s.db.Dashboard.AssignTeamCluster(ctx, dashboardqueries.AssignTeamClusterParams{
		ClusterID:        clusterID,
		TeamID:           teamID,
		PreserveExisting: preserveExisting,
	})
	if dberrors.IsNotFoundError(err) {
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusNotFound, ErrorCode: string(api.ClusterAssignmentProjectNotFound), ClientMsg: "Team not found"})

		return
	}
	if err != nil {
		if dberrors.IsForeignKeyViolation(err) {
			apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusNotFound, ErrorCode: string(api.ClusterAssignmentClusterNotFound), ClientMsg: "Cluster not found"})
		} else {
			s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to assign cluster to team")
		}

		return
	}
	if !result.AssignmentEligible {
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusConflict, ErrorCode: string(api.ClusterAssignmentRequiresEnterprise), ClientMsg: enterpriseClusterAssignmentPolicyMessage})

		return
	}
	if !result.Assigned {
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusConflict, ErrorCode: string(api.ClusterAssignmentAlreadyAssigned), ClientMsg: "Team is already assigned to a different cluster"})

		return
	}

	logger.L().Info(ctx, "admin team cluster assigned",
		logger.WithTeamID(teamID.String()), logger.WithClusterID(clusterID))

	if err := s.authService.InvalidateTeamCache(ctx, teamID); err != nil {
		logger.L().Error(ctx, "invalidating team cache after cluster assignment",
			logger.WithTeamID(teamID.String()), zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to invalidate team cache after cluster assignment")

		return
	}

	c.Status(http.StatusNoContent)
}

func (s *APIStore) detachTeamCluster(
	c *gin.Context,
	teamID api.TeamID,
	clusterID api.ClusterID,
) {
	ctx := c.Request.Context()

	result, err := s.db.Dashboard.DetachTeamCluster(ctx, dashboardqueries.DetachTeamClusterParams{
		TeamID:    teamID,
		ClusterID: clusterID,
	})
	if dberrors.IsNotFoundError(err) {
		s.sendAPIStoreError(c, http.StatusNotFound, "Team not found")

		return
	}
	if err != nil {
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to detach cluster from team")

		return
	}
	if result.ClusterID != nil && !result.Detached {
		s.sendAPIStoreError(c, http.StatusConflict, "Team is assigned to a different cluster")

		return
	}

	logger.L().Info(ctx, "admin team cluster detached",
		logger.WithTeamID(teamID.String()), logger.WithClusterID(clusterID))

	if err := s.authService.InvalidateTeamCache(ctx, teamID); err != nil {
		logger.L().Error(ctx, "invalidating team cache after cluster detachment",
			logger.WithTeamID(teamID.String()), zap.Error(err))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to invalidate team cache after cluster detachment")

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
