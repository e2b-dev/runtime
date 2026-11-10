package handlers

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	templateBuildsDefaultLimit = int32(100)
	templateBuildsMaxLimit     = int32(100)
)

var maxBuildID = uuid.Max.String()

func (a *APIStore) GetTemplatesTemplateID(c *gin.Context, templateID api.TemplateID, params api.GetTemplatesTemplateIDParams) {
	ctx := c.Request.Context()

	team, apiErr := a.GetTeam(ctx, c, nil)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(ctx, "error when getting team and tier", apiErr.Err)

		return
	}

	telemetry.SetAttributes(ctx,
		telemetry.WithTeamID(team.ID.String()),
	)

	template, err := a.sqlcDB.GetTemplateByIDWithAliases(ctx, templateID)
	switch {
	case dberrors.IsNotFoundError(err):
		a.sendAPIStoreError(c, http.StatusNotFound, fmt.Sprintf("Template %s not found", templateID))
		telemetry.ReportError(ctx, "template not found", err, telemetry.WithTemplateID(templateID))

		return
	case err != nil:
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting template")
		telemetry.ReportCriticalError(ctx, "error when getting template", err)

		return
	case template.TeamID != team.ID:
		telemetry.ReportError(ctx, "user doesn't have access to the template", nil, telemetry.WithTemplateID(templateID))
		a.sendAPIStoreError(c, http.StatusForbidden, fmt.Sprintf("You don't have access to this sandbox template (%s)", templateID))

		return
	}

	// Initialize pagination
	pagination, err := utils.NewPagination[queries.EnvBuild](
		utils.PaginationParams{
			Limit:     params.Limit,
			NextToken: params.NextToken,
		},
		utils.PaginationConfig{
			DefaultLimit: templateBuildsDefaultLimit,
			MaxLimit:     templateBuildsMaxLimit,
			DefaultID:    maxBuildID,
		},
	)
	if err != nil {
		telemetry.ReportError(ctx, "error parsing pagination cursor", err)
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid next token")

		return
	}

	builds, err := a.sqlcDB.GetTemplateBuilds(ctx, queries.GetTemplateBuildsParams{
		TemplateID: templateID,
		CursorTime: pagination.CursorTime(),
		CursorID:   pagination.CursorID(),
		BuildLimit: pagination.QueryLimit(),
	})
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting builds")
		telemetry.ReportCriticalError(ctx, "error when getting builds", err)

		return
	}

	defaultBuild, err := a.sqlcDB.GetTemplateDefaultBuild(ctx, templateID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting the template's default build")
		telemetry.ReportCriticalError(ctx, "error when getting the template's default build", err)

		return
	}

	builds = pagination.ProcessResultsWithHeader(c, builds, func(b queries.EnvBuild) (time.Time, string) {
		return b.CreatedAt, b.ID.String()
	})

	res := templateWithBuildsToAPI(ctx, templateRow{
		TemplateID:           template.ID,
		TeamID:               template.TeamID,
		CreatedAt:            template.CreatedAt,
		UpdatedAt:            template.UpdatedAt,
		Public:               template.Public,
		BuildCount:           template.BuildCount,
		SpawnCount:           template.SpawnCount,
		LastSpawnedAt:        template.LastSpawnedAt,
		CreatorID:            template.CreatedBy,
		IsDefault:            defaultBuild.IsDefault,
		BuildID:              defaultBuild.BuildID,
		BuildVcpu:            defaultBuild.BuildVcpu,
		BuildMaxVcpu:         defaultBuild.BuildMaxVcpu,
		BuildRamMb:           defaultBuild.BuildRamMb,
		BuildTotalDiskSizeMb: defaultBuild.BuildTotalDiskSizeMb,
		BuildEnvdVersion:     defaultBuild.BuildEnvdVersion,
		BuildStatus:          defaultBuild.BuildStatus,
		Aliases:              template.Aliases,
		Names:                template.Names,
		DefaultDescription:   defaultBuild.DefaultDescription,
	}, builds)

	c.JSON(http.StatusOK, res)
}

func templateBuildToAPI(ctx context.Context, build queries.EnvBuild) api.TemplateBuild {
	return api.TemplateBuild{
		BuildID:     build.ID,
		Status:      getCorrespondingTemplateBuildStatus(ctx, build.StatusGroup),
		CreatedAt:   build.CreatedAt,
		UpdatedAt:   build.UpdatedAt,
		FinishedAt:  build.FinishedAt,
		CpuCount:    api.CPUCount(build.Vcpu),
		MaxCpuCount: sharedUtils.CastPtr(build.MaxVcpu, func(v int64) api.CPUCount { return api.CPUCount(v) }),
		MemoryMB:    api.MemoryMB(build.RamMb),
		DiskSizeMB:  sharedUtils.CastPtr(build.TotalDiskSizeMb, func(v int64) api.DiskSizeMB { return api.DiskSizeMB(v) }),
		EnvdVersion: build.EnvdVersion,
	}
}
