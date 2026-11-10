package handlers

import (
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	openapi_types "github.com/oapi-codegen/runtime/types"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sharedutils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const adminTeamListDefaultLimit = 50

// likePatternEscaper makes LIKE treat its wildcards and escape character
// literally.
var likePatternEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

func (a *APIStore) PostUnstableAdminTeamsQuery(c *gin.Context) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.AdminTeamListRequest](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		return
	}

	// Only the running sort reads the counts, so other lists don't depend on
	// the cache that holds them.
	sortBy := sharedutils.FromPtr(body.SortBy)
	byRunning := sortBy == api.AdminTeamListSortFieldRunningSandboxes
	runningTeamIDs := []uuid.UUID{}
	runningSandboxCounts := []int64{}
	if byRunning {
		counts, err := a.teamSandboxCounter.TeamRunningSandboxCounts(ctx)
		if err != nil {
			logger.L().Error(ctx, "Failed to count running sandboxes", zap.Error(err))
			a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to count running sandboxes")

			return
		}

		for teamID, count := range counts {
			runningTeamIDs = append(runningTeamIDs, teamID)
			runningSandboxCounts = append(runningSandboxCounts, count)
		}
	}

	filter := adminTeamListFilter(body)

	rows, err := a.sqlcReadDB.ListAdminTeams(ctx, queries.ListAdminTeamsParams{
		ByProjectIds:         filter.ByProjectIds,
		ProjectIds:           filter.ProjectIds,
		BySearchID:           filter.BySearchID,
		SearchID:             filter.SearchID,
		BySearchText:         filter.BySearchText,
		SearchText:           filter.SearchText,
		HideBanned:           filter.HideBanned,
		HideBlocked:          filter.HideBlocked,
		ByCreatedAfter:       filter.ByCreatedAfter,
		CreatedAfter:         filter.CreatedAfter,
		ByCreatedBefore:      filter.ByCreatedBefore,
		CreatedBefore:        filter.CreatedBefore,
		SortBy:               string(sortBy),
		Descending:           sharedutils.DerefOrDefault(body.SortOrder, api.Desc) == api.Desc,
		OffsetCount:          int64(sharedutils.FromPtr(body.Offset)),
		PageSize:             int64(sharedutils.DerefOrDefault(body.Limit, adminTeamListDefaultLimit)),
		RunningTeamIds:       runningTeamIDs,
		RunningSandboxCounts: runningSandboxCounts,
	})
	if err != nil {
		logger.L().Error(ctx, "Failed to list teams", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to list teams")

		return
	}

	total, err := a.sqlcReadDB.CountAdminTeams(ctx, filter)
	if err != nil {
		logger.L().Error(ctx, "Failed to count teams", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to count teams")

		return
	}

	items := make([]api.AdminTeamListItem, 0, len(rows))
	for _, row := range rows {
		item := api.AdminTeamListItem{
			Id:          row.ID,
			Email:       row.Email,
			Name:        row.Name,
			IsBanned:    row.IsBanned,
			IsBlocked:   row.IsBlocked,
			CreatedAt:   row.CreatedAt,
			MemberCount: row.MemberCount,
		}
		if byRunning {
			item.RunningSandboxCount = &row.RunningSandboxCount
		}

		items = append(items, item)
	}

	c.JSON(http.StatusOK, api.AdminTeamList{Items: items, Total: total})
}

// adminTeamListFilter turns the request's filters into the arguments both
// team list queries share. A search that parses as a team ID matches that
// team only; any other search matches text.
func adminTeamListFilter(body api.AdminTeamListRequest) queries.CountAdminTeamsParams {
	filter := queries.CountAdminTeamsParams{
		ByProjectIds:    body.ProjectIds != nil,
		ProjectIds:      sharedutils.FromPtr(body.ProjectIds),
		HideBanned:      sharedutils.FromPtr(body.HideBanned),
		HideBlocked:     sharedutils.FromPtr(body.HideBlocked),
		ByCreatedAfter:  body.CreatedAfter != nil,
		CreatedAfter:    sharedutils.FromPtr(body.CreatedAfter),
		ByCreatedBefore: body.CreatedBefore != nil,
		CreatedBefore:   sharedutils.FromPtr(body.CreatedBefore),
	}

	search := strings.TrimSpace(sharedutils.FromPtr(body.Search))
	if search == "" {
		return filter
	}

	if teamID, err := id.ParseTeamID(search); err == nil {
		filter.BySearchID = true
		filter.SearchID = teamID

		return filter
	}

	filter.BySearchText = true
	filter.SearchText = likePatternEscaper.Replace(search)

	return filter
}

func (a *APIStore) GetUnstableAdminTeamsTeamID(c *gin.Context, teamID openapi_types.UUID) {
	ctx := c.Request.Context()

	// The primary, so a team just labelled or banned reads back as written.
	row, err := a.sqlcDB.GetAdminTeam(ctx, teamID)
	if dberrors.IsNotFoundError(err) {
		a.sendAPIStoreError(c, http.StatusNotFound, "Team not found")

		return
	}
	if err != nil {
		logger.L().Error(ctx, "Failed to get team", zap.Error(err), logger.WithTeamID(teamID.String()))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get team")

		return
	}

	c.JSON(http.StatusOK, api.AdminTeam{
		Id:                      row.ID,
		Email:                   row.Email,
		Name:                    row.Name,
		Slug:                    row.Slug,
		CreatedAt:               row.CreatedAt,
		IsBlocked:               row.IsBlocked,
		IsBanned:                row.IsBanned,
		BlockedReason:           row.BlockedReason,
		SandboxSchedulingLabels: row.SandboxSchedulingLabels,
		ClusterId:               row.ClusterID,
		ClusterName:             row.ClusterName,
	})
}

func (a *APIStore) PutUnstableAdminTeamsTeamIDSchedulingLabels(c *gin.Context, teamID openapi_types.UUID) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.AdminTeamSchedulingLabels](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		return
	}

	labels := make([]string, 0, len(body.Labels))
	for _, label := range body.Labels {
		if !slices.Contains(labels, label) {
			labels = append(labels, label)
		}
	}

	rows, err := a.sqlcDB.SetTeamSchedulingLabels(ctx, queries.SetTeamSchedulingLabelsParams{Labels: labels, TeamID: teamID})
	if err != nil {
		logger.L().Error(ctx, "Failed to update scheduling labels", zap.Error(err), logger.WithTeamID(teamID.String()))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to update scheduling labels")

		return
	}
	if rows == 0 {
		a.sendAPIStoreError(c, http.StatusNotFound, "Team not found")

		return
	}

	// Sandbox creation reads the labels from the cached team. The labels are
	// written, so a failed eviction is logged rather than reported: the
	// cached team picks them up when its entry expires.
	if err := a.authService.InvalidateTeamCache(ctx, teamID); err != nil {
		logger.L().Error(ctx, "Failed to invalidate auth cache for team", zap.Error(err), logger.WithTeamID(teamID.String()))
	}

	c.JSON(http.StatusOK, api.AdminTeamSchedulingLabels{Labels: labels})
}
