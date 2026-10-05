package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	templatesDefaultLimit = int32(100)
	templatesMaxLimit     = int32(100)
)

// GetV2Templates lists a team's templates with cursor pagination (e.g. in the CLI).
func (a *APIStore) GetV2Templates(c *gin.Context, params api.GetV2TemplatesParams) {
	ctx := c.Request.Context()

	team, apiErr := a.GetTeam(ctx, c, params.TeamID)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(ctx, "error when getting team and tier", apiErr.Err)

		return
	}

	if params.TeamID != nil && !teamIDMatches(team.ID, *params.TeamID) {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Team ID param mismatch with the API key")
		telemetry.ReportError(ctx, "team param mismatch with the API key", nil, telemetry.WithTeamID(team.ID.String()))

		return
	}

	telemetry.SetAttributes(ctx,
		telemetry.WithTeamID(team.ID.String()),
	)

	pagination, err := utils.NewPagination[*api.Template](
		utils.PaginationParams{
			Limit:     params.Limit,
			NextToken: params.NextToken,
		},
		utils.PaginationConfig{
			DefaultLimit: templatesDefaultLimit,
			MaxLimit:     templatesMaxLimit,
			DefaultID:    utils.MaxTemplateID,
		},
	)
	if err != nil {
		telemetry.ReportError(ctx, "error parsing pagination cursor", err)
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid next token")

		return
	}

	rows, err := a.sqlcDB.GetTeamTemplatesWithCursor(ctx, queries.GetTeamTemplatesWithCursorParams{
		TeamID:          team.ID,
		CursorCreatedAt: pagination.CursorTime(),
		CursorID:        pagination.CursorID(),
		LimitPlusOne:    pagination.QueryLimit(),
	})
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting templates")
		telemetry.ReportCriticalError(ctx, "error when getting templates", err)

		return
	}

	telemetry.ReportEvent(ctx, "listed environments")

	a.posthog.IdentifyAnalyticsTeam(ctx, team.ID.String(), team.Name)
	properties := a.posthog.GetPackageToPosthogProperties(&c.Request.Header)
	a.posthog.CreateAnalyticsTeamEvent(ctx, team.ID.String(), "listed environments", properties)

	templates := make([]*api.Template, 0, len(rows))
	for _, item := range rows {
		var createdBy *api.TeamUser
		if item.CreatorID != nil {
			createdBy = &api.TeamUser{
				Id:    *item.CreatorID,
				Email: nil,
			}
		}

		envdVersion := ""
		if item.BuildEnvdVersion != nil {
			envdVersion = *item.BuildEnvdVersion
		}

		diskMB := int64(0)
		if item.BuildTotalDiskSizeMb != nil {
			diskMB = *item.BuildTotalDiskSizeMb
		}

		templates = append(templates, &api.Template{
			TemplateID:    item.TemplateID,
			BuildID:       item.BuildID.String(),
			CpuCount:      api.CPUCount(item.BuildVcpu),
			MemoryMB:      api.MemoryMB(item.BuildRamMb),
			DiskSizeMB:    api.DiskSizeMB(diskMB),
			Public:        item.Public,
			Aliases:       item.Aliases,
			Names:         item.Names,
			CreatedAt:     item.CreatedAt,
			UpdatedAt:     item.UpdatedAt,
			LastSpawnedAt: item.LastSpawnedAt,
			SpawnCount:    item.SpawnCount,
			BuildCount:    item.BuildCount,
			BuildStatus:   getCorrespondingTemplateBuildStatus(ctx, item.BuildStatus),
			CreatedBy:     createdBy,
			EnvdVersion:   envdVersion,
		})
	}

	templates = pagination.ProcessResultsWithHeader(c, templates, func(t *api.Template) (time.Time, string) {
		return t.CreatedAt, t.TemplateID
	})

	c.JSON(http.StatusOK, templates)
}
