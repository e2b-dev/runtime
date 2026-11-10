package handlers

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) GetNodes(c *gin.Context, params api.GetNodesParams) {
	clusterID := clusters.WithClusterFallback(params.ClusterID)
	result, err := a.orchestrator.AdminNodes(clusterID)
	if err != nil {
		telemetry.ReportCriticalError(c.Request.Context(), "error when getting nodes", err)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting nodes")

		return
	}

	c.JSON(http.StatusOK, result)
}

func (a *APIStore) GetNodesNodeID(c *gin.Context, nodeID api.NodeID, params api.GetNodesNodeIDParams) {
	clusterID := clusters.WithClusterFallback(params.ClusterID)
	result, err := a.orchestrator.AdminNodeDetail(clusterID, nodeID)
	if err != nil {
		if errors.Is(err, orchestrator.ErrNodeNotFound) {
			c.Status(http.StatusNotFound)

			return
		}

		telemetry.ReportCriticalError(c.Request.Context(), "error when getting node details", err)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting node details")

		return
	}

	c.JSON(http.StatusOK, result)
}

func (a *APIStore) GetNodesNodeIDSandboxes(c *gin.Context, nodeID api.NodeID, params api.GetNodesNodeIDSandboxesParams) {
	ctx := c.Request.Context()
	clusterID := clusters.WithClusterFallback(params.ClusterID)
	result, err := a.orchestrator.AdminNodeSandboxes(ctx, clusterID, nodeID)
	if errors.Is(err, orchestrator.ErrNodeNotFound) {
		c.Status(http.StatusNotFound)

		return
	}
	if err != nil {
		telemetry.ReportError(ctx, "error when listing the node's sandboxes", err)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when listing the node's sandboxes")

		return
	}

	c.JSON(http.StatusOK, result)
}

func (a *APIStore) PostNodesNodeID(c *gin.Context, nodeId api.NodeID) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.PostNodesNodeIDJSONRequestBody](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		telemetry.ReportCriticalError(ctx, "error when parsing request", err)

		return
	}

	clusterID := clusters.WithClusterFallback(body.ClusterID)
	node := a.orchestrator.GetNode(clusterID, nodeId)
	if node == nil {
		c.Status(http.StatusNotFound)

		return
	}

	change := nodemanager.StatusChange{Status: body.Status}
	if body.Owner != nil {
		change.Owner = *body.Owner
	}
	if body.Force != nil {
		change.Force = *body.Force
	}

	err = node.SendStatusChange(ctx, change)
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			a.sendAPIStoreError(c, http.StatusConflict, status.Convert(err).Message())

			return
		}
		if status.Code(err) == codes.InvalidArgument {
			a.sendAPIStoreError(c, http.StatusBadRequest, status.Convert(err).Message())

			return
		}

		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error when sending status change: %s", err))

		telemetry.ReportCriticalError(ctx, "error when sending status change", err)

		return
	}

	c.Status(http.StatusNoContent)
}
