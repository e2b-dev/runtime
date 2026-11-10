package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// GetUnstableTemplatesSearch searches a team's templates, together with the
// platform's public default templates, with sorts, filters and cursor
// pagination (e.g. in the dashboard).
func (a *APIStore) GetUnstableTemplatesSearch(c *gin.Context, params api.GetUnstableTemplatesSearchParams) {
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

	sort, err := parseTemplatesSort(params.Sort)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sort parameter")

		return
	}

	cursorTime, cursorID, err := parseTemplatesCursor(params.Cursor, sort)
	if err != nil {
		if errors.Is(err, errTemplatesCursorSortMismatch) {
			a.sendAPIStoreError(c, http.StatusBadRequest, "Cursor was issued for a different sort; clear the cursor and restart")
		} else {
			a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid cursor")
		}

		return
	}

	limit := normalizeTemplatesLimit(params.Limit)

	rows, err := a.searchTeamTemplates(ctx, sort, queries.SearchTeamTemplatesByCreatedAtDescParams{
		TeamID:     team.ID,
		Public:     params.Public,
		Search:     templatesSearchPattern(params.Search),
		CursorTime: cursorTime,
		CursorID:   cursorID,
		// Default templates belong on the shared cluster; a team on a
		// dedicated cluster cannot start them.
		IncludeDefaults: team.ClusterID == nil,
		LimitPlusOne:    limit + 1,
	})
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting templates")
		telemetry.ReportCriticalError(ctx, "error when getting templates", err)

		return
	}

	telemetry.ReportEvent(ctx, "searched templates")

	var nextCursor *string
	if int32(len(rows)) > limit {
		rows = rows[:limit]
		nextCursor = new(formatTemplatesCursor(sort, rows[len(rows)-1]))
	}

	templates := make([]api.Template, 0, len(rows))
	for _, row := range rows {
		// A template the team does not own (a default template of another
		// team) is listed without its usage.
		templates = append(templates, templateToAPI(ctx, row, row.TeamID == team.ID))
	}

	c.JSON(http.StatusOK, api.TemplateSearchResponse{
		Data:       templates,
		NextCursor: nextCursor,
	})
}

func (a *APIStore) searchTeamTemplates(ctx context.Context, sort templatesSort, params queries.SearchTeamTemplatesByCreatedAtDescParams) ([]templateRow, error) {
	switch sort {
	case templatesSortCreatedAtDesc:
		rows, err := a.sqlcDB.SearchTeamTemplatesByCreatedAtDesc(ctx, params)

		return convertTemplateRows(rows, err, func(r queries.SearchTeamTemplatesByCreatedAtDescRow) templateRow {
			return templateRow(r)
		})
	case templatesSortCreatedAtAsc:
		rows, err := a.sqlcDB.SearchTeamTemplatesByCreatedAtAsc(ctx, queries.SearchTeamTemplatesByCreatedAtAscParams(params))

		return convertTemplateRows(rows, err, func(r queries.SearchTeamTemplatesByCreatedAtAscRow) templateRow {
			return templateRow(r)
		})
	case templatesSortUpdatedAtDesc:
		rows, err := a.sqlcDB.SearchTeamTemplatesByUpdatedAtDesc(ctx, queries.SearchTeamTemplatesByUpdatedAtDescParams(params))

		return convertTemplateRows(rows, err, func(r queries.SearchTeamTemplatesByUpdatedAtDescRow) templateRow {
			return templateRow(r)
		})
	case templatesSortUpdatedAtAsc:
		rows, err := a.sqlcDB.SearchTeamTemplatesByUpdatedAtAsc(ctx, queries.SearchTeamTemplatesByUpdatedAtAscParams(params))

		return convertTemplateRows(rows, err, func(r queries.SearchTeamTemplatesByUpdatedAtAscRow) templateRow {
			return templateRow(r)
		})
	default:
		return nil, fmt.Errorf("unsupported sort: %q", sort)
	}
}

func convertTemplateRows[R any](rows []R, err error, convert func(R) templateRow) ([]templateRow, error) {
	if err != nil {
		return nil, err
	}

	out := make([]templateRow, len(rows))
	for i, row := range rows {
		out[i] = convert(row)
	}

	return out, nil
}
