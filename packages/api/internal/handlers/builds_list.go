package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	buildsDefaultLimit = int32(50)
	buildsMaxLimit     = int32(100)
)

var (
	errInvalidBuildsCursor = errors.New("invalid cursor")

	// The first page starts from the same far-future sentinel as the template
	// search, so clock skew between the app and the database cannot drop
	// builds created during the request.
	buildsFirstPageTime = maxTemplatesCursorTime
	buildsFirstPageID   = uuid.Max
)

// GetUnstableBuilds lists a team's builds, newest first, with cursor
// pagination (e.g. in the dashboard).
func (a *APIStore) GetUnstableBuilds(c *gin.Context, params api.GetUnstableBuildsParams) {
	ctx := c.Request.Context()

	team, apiErr := a.GetTeam(ctx, c, nil)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(ctx, "error when getting team and tier", apiErr.Err)

		return
	}

	telemetry.SetAttributes(ctx, telemetry.WithTeamID(team.ID.String()))

	cursorTime, cursorID, err := parseBuildsCursor(params.Cursor)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid cursor")

		return
	}

	limit := min(max(sharedUtils.DerefOrDefault(params.Limit, buildsDefaultLimit), 1), buildsMaxLimit)

	rows, err := a.listBuildRows(ctx, team.ID, params.BuildIdOrTemplate, buildStatusGroupsFilter(params.Statuses), cursorTime, cursorID, limit+1)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting builds")
		telemetry.ReportCriticalError(ctx, "error when getting builds", err)

		return
	}

	telemetry.ReportEvent(ctx, "listed builds")

	var nextCursor *string
	if int32(len(rows)) > limit {
		rows = rows[:limit]
		last := rows[len(rows)-1]
		nextCursor = new(fmt.Sprintf("%s|%s", last.CreatedAt.UTC().Format(time.RFC3339Nano), last.ID))
	}

	builds := make([]api.ListedBuild, 0, len(rows))
	for _, row := range rows {
		template := row.TemplateAlias
		if template == "" {
			template = row.TemplateID
		}

		status := buildStatusFromStatusGroup(row.StatusGroup)
		builds = append(builds, api.ListedBuild{
			Id:            row.ID,
			Template:      template,
			TemplateId:    row.TemplateID,
			Status:        status,
			StatusMessage: buildStatusMessage(status, row.Reason),
			CreatedAt:     row.CreatedAt,
			FinishedAt:    row.FinishedAt,
			CpuCount:      row.Vcpu,
			MemoryMB:      row.RamMb,
			DiskSizeMB:    row.TotalDiskSizeMb,
			EnvdVersion:   row.EnvdVersion,
		})
	}

	c.JSON(http.StatusOK, api.BuildsListResponse{
		Data:       builds,
		NextCursor: nextCursor,
	})
}

// listBuildRow has the columns every GetTeamBuildsPage* query returns.
type listBuildRow struct {
	ID              uuid.UUID
	StatusGroup     types.BuildStatusGroup
	Reason          types.BuildReason
	CreatedAt       time.Time
	FinishedAt      *time.Time
	Vcpu            int64
	RamMb           int64
	TotalDiskSizeMb *int64
	EnvdVersion     *string
	TemplateID      string
	TemplateAlias   string
}

// listBuildRows reads a page of the team's builds. A filter that parses as a
// UUID is tried as a build ID, any other as a template ID; both fall back to
// a template alias when nothing matches.
func (a *APIStore) listBuildRows(
	ctx context.Context,
	teamID uuid.UUID,
	buildIDOrTemplate *string,
	statuses []string,
	cursorTime time.Time,
	cursorID uuid.UUID,
	limitPlusOne int32,
) ([]listBuildRow, error) {
	filter := strings.TrimSpace(sharedUtils.DerefOrDefault(buildIDOrTemplate, ""))
	if filter == "" {
		rows, err := a.sqlcDB.GetTeamBuildsPage(ctx, queries.GetTeamBuildsPageParams{
			TeamID:          teamID,
			CursorCreatedAt: cursorTime,
			CursorID:        cursorID,
			Statuses:        statuses,
			LimitPlusOne:    limitPlusOne,
		})

		return convertBuildRows(rows, err, func(r queries.GetTeamBuildsPageRow) listBuildRow { return listBuildRow(r) })
	}

	buildID, parseErr := uuid.Parse(filter)
	if parseErr == nil {
		byBuildID, err := a.sqlcDB.GetTeamBuildsPageByBuildID(ctx, queries.GetTeamBuildsPageByBuildIDParams{
			TeamID:          teamID,
			BuildID:         buildID,
			CursorCreatedAt: cursorTime,
			CursorID:        cursorID,
			Statuses:        statuses,
			LimitPlusOne:    limitPlusOne,
		})
		if err != nil || len(byBuildID) > 0 {
			return convertBuildRows(byBuildID, err, func(r queries.GetTeamBuildsPageByBuildIDRow) listBuildRow { return listBuildRow(r) })
		}
	} else {
		// Template IDs are never UUIDs.
		byTemplateID, err := a.sqlcDB.GetTeamBuildsPageByTemplateID(ctx, queries.GetTeamBuildsPageByTemplateIDParams{
			TemplateID:      filter,
			TeamID:          teamID,
			CursorCreatedAt: cursorTime,
			CursorID:        cursorID,
			Statuses:        statuses,
			LimitPlusOne:    limitPlusOne,
		})
		if err != nil || len(byTemplateID) > 0 {
			return convertBuildRows(byTemplateID, err, func(r queries.GetTeamBuildsPageByTemplateIDRow) listBuildRow { return listBuildRow(r) })
		}
	}

	rows, err := a.sqlcDB.GetTeamBuildsPageByTemplateAlias(ctx, queries.GetTeamBuildsPageByTemplateAliasParams{
		TemplateAlias:   filter,
		TeamID:          teamID,
		CursorCreatedAt: cursorTime,
		CursorID:        cursorID,
		Statuses:        statuses,
		LimitPlusOne:    limitPlusOne,
	})

	return convertBuildRows(rows, err, func(r queries.GetTeamBuildsPageByTemplateAliasRow) listBuildRow { return listBuildRow(r) })
}

func convertBuildRows[R any](rows []R, err error, convert func(R) listBuildRow) ([]listBuildRow, error) {
	if err != nil {
		return nil, err
	}

	out := make([]listBuildRow, len(rows))
	for i, row := range rows {
		out[i] = convert(row)
	}

	return out, nil
}

// parseBuildsCursor parses a `{created_at}|{build_id}` cursor. It returns the
// first page's position for an empty cursor.
func parseBuildsCursor(cursor *api.BuildsCursor) (time.Time, uuid.UUID, error) {
	if cursor == nil || *cursor == "" {
		return buildsFirstPageTime, buildsFirstPageID, nil
	}

	parts := strings.SplitN(*cursor, "|", 2)
	if len(parts) != 2 {
		return time.Time{}, uuid.Nil, errInvalidBuildsCursor
	}

	cursorTime, err := parseTemplatesCursorTime(parts[0])
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: %w", errInvalidBuildsCursor, err)
	}

	cursorID, err := uuid.Parse(parts[1])
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: %w", errInvalidBuildsCursor, err)
	}

	return cursorTime, cursorID, nil
}
