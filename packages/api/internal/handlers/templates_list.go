package handlers

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// templatesUnboundedMaxCount caps GET /templates, which returns every template
// in one response; larger teams must page through GET /v2/templates.
const templatesUnboundedMaxCount = 5000

// GetTemplates serves to list templates (e.g. in CLI)
func (a *APIStore) GetTemplates(c *gin.Context, params api.GetTemplatesParams) {
	ctx := c.Request.Context()

	team, apiErr := a.GetTeam(ctx, c, params.TeamID)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(ctx, "error when getting team and tier", apiErr.Err)

		return
	}

	if params.TeamID != nil {
		if !teamIDMatches(team.ID, *params.TeamID) {
			a.sendAPIStoreError(c, http.StatusBadRequest, "Team ID param mismatch with the API key")
			telemetry.ReportError(ctx, "team param mismatch with the API key", nil, telemetry.WithTeamID(team.ID.String()))

			return
		}
	}

	telemetry.SetAttributes(ctx,
		telemetry.WithTeamID(team.ID.String()),
	)

	capped := a.featureFlags.BoolFlag(ctx, featureflags.UnpaginatedTemplateListCapFlag, featureflags.TeamContext(team.ID.String()))

	// A nil limit reads every template.
	var limitPlusOne *int32
	if capped {
		limit := int32(templatesUnboundedMaxCount + 1)
		limitPlusOne = &limit
	}

	envs, err := a.sqlcDB.GetTeamTemplates(ctx, queries.GetTeamTemplatesParams{
		TeamID:       team.ID,
		LimitPlusOne: limitPlusOne,
	})
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting templates")
		telemetry.ReportCriticalError(ctx, "error when getting templates", err)

		return
	}

	if capped && len(envs) > templatesUnboundedMaxCount {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Team has more than %d templates; update to the latest E2B SDK or list them with GET /v2/templates", templatesUnboundedMaxCount))
		telemetry.ReportEvent(ctx, "template list exceeds the unbounded limit")

		return
	}

	telemetry.ReportEvent(ctx, "listed environments")

	a.posthog.IdentifyAnalyticsTeam(ctx, team.ID.String(), team.Name)
	properties := a.posthog.GetPackageToPosthogProperties(&c.Request.Header)
	a.posthog.CreateAnalyticsTeamEvent(ctx, team.ID.String(), "listed environments", properties)

	templates := make([]*api.Template, 0, len(envs))
	for _, item := range envs {
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
			TemplateID:    item.ActiveEnv.ID,
			BuildID:       item.BuildID.String(),
			CpuCount:      api.CPUCount(item.BuildVcpu),
			MaxCpuCount:   sharedUtils.CastPtr(item.BuildMaxVcpu, func(v int64) api.CPUCount { return api.CPUCount(v) }),
			MemoryMB:      api.MemoryMB(item.BuildRamMb),
			DiskSizeMB:    api.DiskSizeMB(diskMB),
			Public:        item.ActiveEnv.Public,
			Aliases:       item.Aliases,
			Names:         item.Names,
			CreatedAt:     item.ActiveEnv.CreatedAt,
			UpdatedAt:     item.ActiveEnv.UpdatedAt,
			LastSpawnedAt: item.ActiveEnv.LastSpawnedAt,
			SpawnCount:    item.ActiveEnv.SpawnCount,
			BuildCount:    item.ActiveEnv.BuildCount,
			BuildStatus:   getCorrespondingTemplateBuildStatus(ctx, item.BuildStatus),
			CreatedBy:     createdBy,
			EnvdVersion:   envdVersion,
		})
	}

	c.JSON(http.StatusOK, templates)
}
