package handlers

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/templates"
)

func (a *APIStore) GetTemplatesTemplateIDFilesHash(c *gin.Context, templateID api.TemplateID, hash string) {
	ctx := c.Request.Context()

	if err := templates.ValidateFilesHash(hash); err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid files hash")
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "invalid files hash", err, telemetry.WithTemplateID(templateID), attribute.String("hash", hash))

		return
	}

	// Resolve via the active-envs view so a soft-deleted template is not found.
	templateDB, err := a.sqlcDB.GetTemplateById(ctx, templateID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusNotFound, fmt.Sprintf("Error when getting template: %s", err))
		telemetry.ReportCriticalError(ctx, "error when getting env", err, telemetry.WithTemplateID(templateID))

		return
	}

	dbTeamID := templateDB.TeamID.String()
	team, apiErr := a.GetTeam(ctx, c, &dbTeamID)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(ctx, "error when getting team and tier", apiErr.Err)

		return
	}

	// Check if the user has access to the template
	if team.ID != templateDB.TeamID {
		telemetry.ReportCriticalError(ctx, "error when getting template", err, telemetry.WithTemplateID(templateID))
		a.sendAPIStoreError(c, http.StatusNotFound, fmt.Sprintf("Error when getting template: %s", err))

		return
	}

	node, err := a.templateManager.GetAvailableBuildClient(ctx, clusters.WithClusterFallback(templateDB.ClusterID))
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error when getting available build client", err, telemetry.WithTemplateID(templateID))
		a.sendAPIStoreError(c, http.StatusServiceUnavailable, "Error when getting available build client")

		return
	}

	resp, err := a.templateManager.InitLayerFileUpload(ctx, clusters.WithClusterFallback(templateDB.ClusterID), node.NodeID, team.ID, templateID, hash)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error when requesting layer files upload", err, telemetry.WithTemplateID(templateID), attribute.String("hash", hash))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when requesting layer files upload")

		return
	}

	upload := api.TemplateBuildFileUpload{
		Present: resp.GetPresent(),
		Url:     resp.Url,
	}
	if headers := resp.GetHeaders(); len(headers) > 0 {
		upload.Headers = &headers
	}

	c.JSON(http.StatusCreated, &upload)
}
