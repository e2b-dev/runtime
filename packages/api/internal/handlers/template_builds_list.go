package handlers

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// GetTemplatesTemplateIDBuilds lists the builds of a template given by ID or
// name, newest first, with the tags that point at each build.
func (a *APIStore) GetTemplatesTemplateIDBuilds(c *gin.Context, aliasOrTemplateID api.TemplateID, params api.GetTemplatesTemplateIDBuildsParams) {
	ctx := c.Request.Context()
	team := auth.MustGetTeamInfo(c)

	templateID, ok := a.resolveTemplateID(c, team, aliasOrTemplateID)
	if !ok {
		return
	}

	identity := []attribute.KeyValue{telemetry.WithTeamID(team.ID.String()), telemetry.WithTemplateID(templateID)}

	exists, err := a.sqlcDB.CheckTeamTemplateExists(ctx, queries.CheckTeamTemplateExistsParams{TemplateID: templateID, TeamID: team.ID})
	if err != nil {
		a.sendTemplateError(c, http.StatusInternalServerError, "Error when getting template", err, identity...)

		return
	}
	if !exists {
		a.sendTemplateError(c, http.StatusNotFound, templateNotFoundMessage, nil, identity...)

		return
	}

	// Tags are stored trimmed and lowercased, so the filter is cleaned the
	// same way. An empty item is no tag, as in "tags=v2,".
	requestedTags := slices.DeleteFunc(slices.Clone(sharedUtils.DerefOrDefault(params.Tags, nil)), func(tag string) bool {
		return strings.TrimSpace(tag) == ""
	})
	tags, err := id.ValidateAndDeduplicateTags(requestedTags)
	if err != nil {
		a.sendTemplateError(c, http.StatusBadRequest, "Invalid tag", err, identity...)

		return
	}

	statusGroups, err := templateBuildStatusGroups(params.Statuses)
	if err != nil {
		a.sendTemplateError(c, http.StatusBadRequest, fmt.Sprintf("Invalid status: %s", err), err, identity...)

		return
	}

	pagination, err := utils.NewPagination[queries.ListTemplateBuildsRow](
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
		a.sendTemplateError(c, http.StatusBadRequest, "Invalid next token", err, identity...)

		return
	}

	cursorID, err := uuid.Parse(pagination.CursorID())
	if err != nil {
		a.sendTemplateError(c, http.StatusBadRequest, "Invalid next token", err, identity...)

		return
	}

	rows, err := a.sqlcDB.ListTemplateBuilds(ctx, queries.ListTemplateBuildsParams{
		TemplateID: templateID,
		CursorTime: pagination.CursorTime(),
		CursorID:   cursorID,
		Statuses:   statusGroups,
		Tags:       tags,
		BuildLimit: pagination.QueryLimit(),
	})
	if err != nil {
		a.sendTemplateError(c, http.StatusInternalServerError, "Error when getting builds", err, identity...)

		return
	}

	rows = pagination.ProcessResultsWithHeader(c, rows, func(r queries.ListTemplateBuildsRow) (time.Time, string) {
		return r.CreatedAt, r.ID.String()
	})

	builds := make([]api.TemplateBuildWithTags, 0, len(rows))
	for _, row := range rows {
		builds = append(builds, templateBuildRowToAPI(ctx, row))
	}

	c.JSON(http.StatusOK, builds)
}

func templateBuildRowToAPI(ctx context.Context, row queries.ListTemplateBuildsRow) api.TemplateBuildWithTags {
	return api.TemplateBuildWithTags{
		BuildID:     row.ID,
		Status:      getCorrespondingTemplateBuildStatus(ctx, row.StatusGroup),
		CreatedAt:   row.CreatedAt,
		UpdatedAt:   row.UpdatedAt,
		FinishedAt:  row.FinishedAt,
		CpuCount:    api.CPUCount(row.Vcpu),
		MaxCpuCount: sharedUtils.CastPtr(row.MaxVcpu, func(v int64) api.CPUCount { return api.CPUCount(v) }),
		MaxMemoryMB: sharedUtils.CastPtr(row.MaxRamMb, func(v int64) api.MemoryMB { return api.MemoryMB(v) }),
		MemoryMB:    templateMemoryMB(row.RamMb, row.MaxRamMb, row.StartMemoryMb),
		DiskSizeMB:  sharedUtils.CastPtr(row.TotalDiskSizeMb, func(v int64) api.DiskSizeMB { return api.DiskSizeMB(v) }),
		EnvdVersion: row.EnvdVersion,
		Tags:        row.Tags,
	}
}

// templateBuildStatusGroups returns the status groups of the requested
// statuses, and none when no status is requested: the list is then not
// filtered by status. It is the inverse of getCorrespondingTemplateBuildStatus.
// A status it does not know is an error, so an unknown filter never widens
// the list.
func templateBuildStatusGroups(statuses *api.TemplateBuildStatuses) ([]string, error) {
	var groups []string

	for _, status := range sharedUtils.DerefOrDefault(statuses, nil) {
		switch status {
		case api.TemplateBuildStatusWaiting:
			groups = append(groups, string(types.BuildStatusGroupPending))
		case api.TemplateBuildStatusBuilding:
			groups = append(groups, string(types.BuildStatusGroupInProgress))
		case api.TemplateBuildStatusReady:
			groups = append(groups, string(types.BuildStatusGroupReady))
		case api.TemplateBuildStatusError:
			groups = append(groups, string(types.BuildStatusGroupFailed))
		default:
			return nil, fmt.Errorf("unknown build status %q", status)
		}
	}

	return groups, nil
}
