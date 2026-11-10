package handlers

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/management"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// ManagementUpsertProject creates or reconciles a project from a
// caller-supplied id, answering 201 or 200 to say which happened.
func (a *APIStore) ManagementUpsertProject(c *gin.Context, projectID uuid.UUID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithTeamID(projectID.String())}
	telemetry.SetAttributes(ctx, attrs...)

	body, err := ginutils.ParseBody[api.ManagementProjectUpsertRequest](ctx, c)
	if err != nil {
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "upsert project failed",
			fmt.Errorf("parse project upsert request: %w", err), attrs...)
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid request body")

		return
	}

	stored, created, err := a.managementService.UpsertProject(ctx, management.Project{
		ClusterID: body.ClusterId,
		ID:        projectID,
		Name:      body.Name,
		Slug:      body.Slug,
		Email:     body.Email,
	})
	if err != nil {
		a.sendProjectUpsertError(c, err, attrs...)

		return
	}

	c.JSON(upsertStatus(created), api.ManagementProject{
		ClusterId: stored.ClusterID,
		Id:        stored.ID,
		Name:      stored.Name,
		Slug:      stored.Slug,
		Email:     stored.Email,
	})
}

func (a *APIStore) sendProjectUpsertError(c *gin.Context, err error, attrs ...attribute.KeyValue) {
	ctx := c.Request.Context()

	if message := projectConflictMessage(err); message != "" {
		telemetry.ReportErrorByCode(ctx, http.StatusConflict, "upsert project failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusConflict, message)

		return
	}

	telemetry.ReportCriticalError(ctx, "upsert project failed", err, attrs...)
	a.sendAPIStoreError(c, http.StatusInternalServerError, "Error upserting project")
}

// clusterDeletingConstraint is raised by the teams trigger that refuses a new
// assignment to a cluster whose deletion is recorded.
const clusterDeletingConstraint = "teams_cluster_not_deleting"

// projectConflictMessage names the conflict an upsert lost to, or "" when the
// failure is not one the caller can resolve by changing the request.
func projectConflictMessage(err error) string {
	switch {
	case errors.Is(err, management.ErrProjectSlugTaken):
		return "Slug is already taken on this control plane"
	case errors.Is(err, management.ErrProjectRaced):
		return "Project was created concurrently, retry"
	case dberrors.ConstraintName(err) == clusterDeletingConstraint:
		return "Cluster is being deleted"
	}

	return ""
}

// upsertStatus distinguishes a project this request created from one it found.
func upsertStatus(created bool) int {
	if created {
		return http.StatusCreated
	}

	return http.StatusOK
}
