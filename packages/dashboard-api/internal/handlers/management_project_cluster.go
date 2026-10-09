package handlers

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
)

func (s *APIStore) ManagementRegisterCluster(c *gin.Context, clusterID api.ClusterID) {
	body, err := ginutils.ParseBody[api.ManagementClusterRegistrationRequest](c.Request.Context(), c)
	if err != nil {
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ErrorCode: string(api.ClusterRegistrationInvalid), ClientMsg: "Invalid cluster registration"})

		return
	}
	_, err = s.createCluster(c.Request.Context(), clusterRegistration{
		ClusterID:          &clusterID,
		Name:               body.Name,
		Endpoint:           body.Endpoint,
		EndpointTLS:        body.EndpointTls,
		Token:              body.Token,
		SandboxProxyDomain: body.SandboxProxyDomain,
		AuthOrgID:          body.AuthOrgId,
		// A cluster registered through the management API belongs to a
		// deployment its caller also tears down, so it starts unprotected.
		// Registering an existing cluster again keeps its stored protection.
		DeletionProtection: false,
	})
	switch {
	case errors.Is(err, errInvalidClusterRegistration):
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ErrorCode: string(api.ClusterRegistrationInvalid), ClientMsg: "Invalid cluster registration"})
	case dberrors.IsUniqueConstraintViolation(err), dberrors.IsNotFoundError(err):
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusConflict, ErrorCode: string(api.ClusterRegistrationConflict), ClientMsg: "Cluster conflicts with stored state"})
	case err != nil:
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to register cluster")
	default:
		c.Status(http.StatusNoContent)
	}
}

func (s *APIStore) ManagementAssignProjectCluster(c *gin.Context, projectID api.ProjectID, clusterID api.ClusterID) {
	s.assignTeamCluster(c, projectID, clusterID, true)
}

func (s *APIStore) ManagementDetachProjectCluster(c *gin.Context, projectID api.ProjectID, clusterID api.ClusterID) {
	s.detachTeamCluster(c, projectID, clusterID)
}

func (s *APIStore) ManagementDeleteCluster(c *gin.Context, clusterID api.ClusterID) {
	s.DeleteAdminClustersClusterID(c, clusterID)
}
