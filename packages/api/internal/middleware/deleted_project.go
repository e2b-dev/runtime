package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// deletedProjectPrefix is the route tree whose writes address a project by its
// ID.
const deletedProjectPrefix = "/unstable/management/"

// deletedProjectExempt lists writes that must still reach their handler for a
// deleted project. A retried deletion repeats its cleanup.
var deletedProjectExempt = map[string]map[string]struct{}{
	http.MethodDelete: {
		"/unstable/management/projects/:projectID": {},
	},
}

// RefuseDeletedProject answers 404 to a write addressed to a deleted project.
// A deleted project keeps its team row, so without this the handlers would
// still find it and apply the write.
func RefuseDeletedProject(isProjectDeleted func(ctx context.Context, projectID uuid.UUID) (bool, error)) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !guardsDeletedProject(c.Request.Method, c.FullPath()) {
			c.Next()

			return
		}

		projectID, err := uuid.Parse(c.Param("projectID"))
		if err != nil {
			// The request validator and the handler own malformed IDs.
			c.Next()

			return
		}

		ctx := c.Request.Context()
		deleted, err := isProjectDeleted(ctx, projectID)
		if err != nil {
			telemetry.ReportCriticalError(ctx, "check project deletion failed", err, telemetry.WithTeamID(projectID.String()))
			apierrors.SendAPIStoreError(c, http.StatusInternalServerError, "Error checking project")
			c.Abort()

			return
		}
		if deleted {
			apierrors.SendAPIStoreError(c, http.StatusNotFound, "Project not found")
			c.Abort()

			return
		}

		c.Next()
	}
}

// guardsDeletedProject reports whether RefuseDeletedProject checks a route:
// a write under the management tree that names a project ID and is not exempt.
func guardsDeletedProject(method, route string) bool {
	if method == http.MethodGet {
		return false
	}
	if _, exempt := deletedProjectExempt[method][route]; exempt {
		return false
	}

	return strings.HasPrefix(route, deletedProjectPrefix) && strings.Contains(route, "/:projectID")
}
