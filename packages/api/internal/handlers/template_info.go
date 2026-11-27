package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/postgres/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// templateNotFoundMessage answers a template that is missing, deleted or owned
// by another team alike, so the answer does not reveal other teams' templates.
const templateNotFoundMessage = "Template not found"

// GetV2TemplatesTemplateID returns a template given by ID or name, with its
// default build.
func (a *APIStore) GetV2TemplatesTemplateID(c *gin.Context, aliasOrTemplateID api.TemplateID) {
	ctx := c.Request.Context()
	team := auth.MustGetTeamInfo(c)

	templateID, ok := a.resolveTemplateID(c, team, aliasOrTemplateID)
	if !ok {
		return
	}

	identity := []attribute.KeyValue{telemetry.WithTeamID(team.ID.String()), telemetry.WithTemplateID(templateID)}

	template, err := a.sqlcDB.GetTeamTemplateInfo(ctx, queries.GetTeamTemplateInfoParams{TemplateID: templateID, TeamID: team.ID})
	switch {
	case dberrors.IsNotFoundError(err):
		a.sendTemplateError(c, http.StatusNotFound, templateNotFoundMessage, err, identity...)

		return
	case err != nil:
		a.sendTemplateError(c, http.StatusInternalServerError, "Error when getting template", err, identity...)

		return
	}

	c.JSON(http.StatusOK, templateInfoToAPI(ctx, template))
}

// resolveTemplateID resolves a template ID or name from a path. A reference
// with a tag is refused: these paths name a template, not one of its builds.
// The caller reads the template for its team. On failure it has sent the
// error response and returns false.
func (a *APIStore) resolveTemplateID(c *gin.Context, team *types.Team, reference string) (string, bool) {
	ctx := c.Request.Context()
	identity := []attribute.KeyValue{telemetry.WithTeamID(team.ID.String()), telemetry.WithTemplateID(reference)}

	if strings.Contains(reference, id.TagSeparator) {
		a.sendTemplateError(c, http.StatusBadRequest, "Invalid template ID: a tag is not accepted here", nil, identity...)

		return "", false
	}

	identifier, _, err := id.ParseName(reference)
	if err != nil {
		a.sendTemplateError(c, http.StatusBadRequest, fmt.Sprintf("Invalid template ID: %s", err), err, identity...)

		return "", false
	}

	aliasInfo, err := a.templateCache.ResolveAlias(ctx, identifier, team.Slug)
	switch {
	case errors.Is(err, templatecache.ErrTemplateNotFound):
		a.sendTemplateError(c, http.StatusNotFound, templateNotFoundMessage, err, identity...)

		return "", false
	case err != nil:
		a.sendTemplateError(c, http.StatusInternalServerError, "Error when getting template", err, identity...)

		return "", false
	}

	telemetry.SetAttributes(ctx, telemetry.WithTeamID(team.ID.String()), telemetry.WithTemplateID(aliasInfo.TemplateID))

	return aliasInfo.TemplateID, true
}

// sendTemplateError sends an error response and reports it: a server error
// as critical, any other as an error.
func (a *APIStore) sendTemplateError(c *gin.Context, code int, message string, err error, attrs ...attribute.KeyValue) {
	a.sendAPIStoreError(c, code, message)
	telemetry.ReportErrorByCode(c.Request.Context(), code, message, err, attrs...)
}

func templateInfoToAPI(ctx context.Context, row queries.GetTeamTemplateInfoRow) api.TemplateV2 {
	template := api.TemplateV2{
		TemplateID:    row.ID,
		Names:         row.Names,
		Public:        row.Public,
		CreatedAt:     row.CreatedAt,
		UpdatedAt:     row.UpdatedAt,
		LastSpawnedAt: row.LastSpawnedAt,
		SpawnCount:    row.SpawnCount,
		BuildCount:    row.BuildCount,
		IsDefault:     row.IsDefault,
		Description:   row.DefaultDescription,
		BuildStatus:   getCorrespondingTemplateBuildStatus(ctx, row.BuildStatus),
	}

	if row.CreatedBy != nil {
		template.CreatedBy = &api.TeamUser{Id: *row.CreatedBy}
	}

	// The query reads the default build's columns from an outer join: either
	// all of them are set or none is.
	if row.DefaultBuildID != nil {
		template.DefaultBuild = &api.TemplateBuild{
			BuildID:     *row.DefaultBuildID,
			Status:      api.TemplateBuildStatusReady,
			CreatedAt:   sharedUtils.DerefOrDefault(row.DefaultBuildCreatedAt, time.Time{}),
			UpdatedAt:   sharedUtils.DerefOrDefault(row.DefaultBuildUpdatedAt, time.Time{}),
			FinishedAt:  row.DefaultBuildFinishedAt,
			CpuCount:    api.CPUCount(sharedUtils.DerefOrDefault(row.DefaultBuildVcpu, 0)),
			MaxCpuCount: sharedUtils.CastPtr(row.DefaultBuildMaxVcpu, func(v int64) api.CPUCount { return api.CPUCount(v) }),
			MaxMemoryMB: sharedUtils.CastPtr(row.DefaultBuildMaxRamMb, func(v int64) api.MemoryMB { return api.MemoryMB(v) }),
			MemoryMB:    templateMemoryMB(sharedUtils.DerefOrDefault(row.DefaultBuildRamMb, 0), row.DefaultBuildMaxRamMb, row.DefaultBuildStartMemoryMb),
			DiskSizeMB:  sharedUtils.CastPtr(row.DefaultBuildTotalDiskSizeMb, func(v int64) api.DiskSizeMB { return api.DiskSizeMB(v) }),
			EnvdVersion: row.DefaultBuildEnvdVersion,
		}
	}

	return template
}
