package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) GetDefaultTemplates(c *gin.Context) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "list default templates")

	rows, err := a.sqlcDB.ListDefaultTemplates(ctx)
	if err != nil {
		logger.L().Error(ctx, "Error listing default templates", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when listing default templates")

		return
	}

	caller := defaultTemplatesCallerFrom(c)
	templates := make([]api.Template, 0, len(rows))
	for _, row := range rows {
		seesUsage := caller.admin || caller.owns(row.TeamID)
		if !row.Public && !seesUsage {
			continue
		}

		templates = append(templates, templateToAPI(ctx, row, seesUsage))
	}

	c.JSON(http.StatusOK, templates)
}

// defaultTemplatesCaller is who listed the default templates. The route admits
// an API key (a team), a user token (a user, no team), the admin token or an
// admin JWT. The two admin credentials put neither a team nor a user on the
// context, so a request with neither was authenticated as an admin.
type defaultTemplatesCaller struct {
	teamID *uuid.UUID
	admin  bool
}

func defaultTemplatesCallerFrom(c *gin.Context) defaultTemplatesCaller {
	if team, ok := auth.GetTeamInfo(c); ok && team != nil && team.Team != nil {
		return defaultTemplatesCaller{teamID: &team.ID}
	}

	if _, ok := auth.GetUserID(c); ok {
		return defaultTemplatesCaller{}
	}

	return defaultTemplatesCaller{admin: true}
}

func (c defaultTemplatesCaller) owns(teamID uuid.UUID) bool {
	return c.teamID != nil && *c.teamID == teamID
}
