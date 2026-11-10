package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// GetUnstableBuildsBuildID returns one of a team's builds with the names of
// its template (e.g. for the dashboard's build page).
func (a *APIStore) GetUnstableBuildsBuildID(c *gin.Context, buildID api.BuildID) {
	ctx := c.Request.Context()

	buildUUID, err := uuid.Parse(buildID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid build id")

		return
	}

	team, apiErr := a.GetTeam(ctx, c, nil)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(ctx, "error when getting team and tier", apiErr.Err)

		return
	}

	telemetry.SetAttributes(ctx, telemetry.WithTeamID(team.ID.String()), telemetry.WithBuildID(buildID))

	row, err := a.sqlcDB.GetBuildInfoByTeamAndBuildID(ctx, queries.GetBuildInfoByTeamAndBuildIDParams{
		TeamID:  team.ID,
		BuildID: buildUUID,
	})
	if dberrors.IsNotFoundError(err) {
		a.sendAPIStoreError(c, http.StatusNotFound, "Build not found or you don't have access to it")

		return
	}
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting build info")
		telemetry.ReportCriticalError(ctx, "error when getting build info", err)

		return
	}

	status := buildStatusFromStatus(row.Status)
	c.JSON(http.StatusOK, api.BuildInfo{
		Names:         &row.Names,
		CreatedAt:     row.CreatedAt,
		FinishedAt:    row.FinishedAt,
		Status:        status,
		StatusMessage: buildStatusMessage(status, row.Reason),
	})
}
