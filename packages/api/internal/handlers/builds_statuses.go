package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const maxBuildStatusesIDs = 100

// GetUnstableBuildsStatuses returns the statuses of a team's builds (e.g. for
// the dashboard to poll running builds). IDs of other teams' builds are left
// out.
func (a *APIStore) GetUnstableBuildsStatuses(c *gin.Context, params api.GetUnstableBuildsStatusesParams) {
	ctx := c.Request.Context()

	team, apiErr := a.GetTeam(ctx, c, nil)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(ctx, "error when getting team and tier", apiErr.Err)

		return
	}

	telemetry.SetAttributes(ctx, telemetry.WithTeamID(team.ID.String()))

	if len(params.BuildIds) > maxBuildStatusesIDs {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Too many build IDs")

		return
	}

	rows, err := a.sqlcDB.GetBuildsStatusesByTeam(ctx, queries.GetBuildsStatusesByTeamParams{
		TeamID:   team.ID,
		BuildIds: params.BuildIds,
	})
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting build statuses")
		telemetry.ReportCriticalError(ctx, "error when getting build statuses", err)

		return
	}

	statuses := make([]api.BuildStatusItem, 0, len(rows))
	for _, row := range rows {
		status := buildStatusFromStatusGroup(row.StatusGroup)
		statuses = append(statuses, api.BuildStatusItem{
			Id:            row.ID,
			Status:        status,
			FinishedAt:    row.FinishedAt,
			StatusMessage: buildStatusMessage(status, row.Reason),
		})
	}

	c.JSON(http.StatusOK, api.BuildsStatusesResponse{BuildStatuses: statuses})
}
