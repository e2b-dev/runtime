package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) GetTemplatesTemplateIDBuildsBuildIDLogs(c *gin.Context, templateID api.TemplateID, buildID api.BuildID, params api.GetTemplatesTemplateIDBuildsBuildIDLogsParams) {
	ctx := c.Request.Context()

	buildUUID, err := uuid.Parse(buildID)
	if err != nil {
		telemetry.ReportError(ctx, "error when parsing build id", err)
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid build id")

		return
	}

	buildInfo, err := a.templateBuildsCache.Get(ctx, buildUUID, templateID)
	if err != nil {
		if errors.Is(err, templatecache.ErrTemplateBuildInfoNotFound) {
			a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Build '%s' not found", buildUUID))

			return
		}

		telemetry.ReportError(ctx, "error when getting template", err)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting template")

		return
	}

	infoTeamID := buildInfo.TeamID.String()
	team, apiErr := a.GetTeam(ctx, c, &infoTeamID)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(ctx, "error when getting team and tier", apiErr.Err)

		return
	}

	if team.ID != buildInfo.TeamID {
		telemetry.ReportError(ctx, "user doesn't have access to env", fmt.Errorf("user doesn't have access to env '%s'", templateID), telemetry.WithTemplateID(templateID))
		a.sendAPIStoreError(c, http.StatusForbidden, fmt.Sprintf("You don't have access to this sandbox template (%s)", templateID))

		return
	}

	// early return if still waiting for build start
	if buildInfo.BuildStatus == types.BuildStatusGroupPending {
		c.JSON(http.StatusOK, api.TemplateBuildLogsResponse{
			Logs: []api.BuildLogEntry{},
		})

		return
	}

	cluster, ok := a.clusters.GetClusterById(buildInfo.ClusterID)
	if !ok {
		telemetry.ReportError(ctx, "error when getting cluster", fmt.Errorf("cluster with ID '%s' not found", buildInfo.ClusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting cluster")

		return
	}

	limit := maxLogEntriesPerRequest
	if params.Limit != nil && *params.Limit < maxLogEntriesPerRequest {
		limit = *params.Limit
	}

	direction := api.LogsDirectionForward
	if params.Direction != nil && *params.Direction == api.LogsDirectionBackward {
		direction = api.LogsDirectionBackward
	}

	var cursor *time.Time
	if params.Cursor != nil {
		cursor = new(time.UnixMilli(*params.Cursor))
	}

	logs, apiErr := cluster.GetResources().GetBuildLogs(ctx, buildInfo.NodeID, templateID, buildID, 0, limit, apiToLogLevel(params.Level), cursor, direction, params.Source)
	if apiErr != nil {
		telemetry.ReportCriticalError(ctx, "error when getting build logs", apiErr.Err, telemetry.WithTemplateID(templateID), telemetry.WithBuildID(buildID))
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)

		return
	}

	logEntries := make([]api.BuildLogEntry, len(logs))
	for i, entry := range logs {
		logEntries[i] = getAPILogEntry(entry)
	}

	c.JSON(http.StatusOK, api.TemplateBuildLogsResponse{Logs: logEntries})
}
