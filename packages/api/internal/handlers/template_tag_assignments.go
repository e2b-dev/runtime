package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) GetTemplatesTemplateIDTagsTagAssignments(c *gin.Context, templateID api.TemplateID, tag api.Tag, params api.GetTemplatesTemplateIDTagsTagAssignmentsParams) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "list template tag assignments")

	teamID := auth.MustGetTeamID(c)
	telemetry.SetAttributes(ctx, telemetry.WithTeamID(teamID.String()), telemetry.WithTemplateID(templateID))

	if !a.requireTeamTemplate(c, templateID, teamID) {
		return
	}

	normalizedTags, err := id.ValidateAndDeduplicateTags([]string{tag})
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid tag")

		return
	}
	normalizedTag := normalizedTags[0]

	limit := normalizeTagAssignmentsPageLimit(params.Limit)
	cursorTime, cursorID, err := parseTagAssignmentsCursor(params.Cursor)
	if err != nil {
		logger.L().Warn(ctx, "invalid tag assignments cursor", zap.Error(err), logger.WithTeamID(teamID.String()), logger.WithTemplateID(templateID))
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid cursor")

		return
	}

	rows, err := a.sqlcDB.ListTemplateTagAssignmentsByTag(ctx, queries.ListTemplateTagAssignmentsByTagParams{
		TemplateID:         templateID,
		Tag:                normalizedTag,
		CursorAssignedAt:   cursorTime,
		CursorAssignmentID: cursorID,
		LimitPlusOne:       limit + 1,
	})
	if err != nil {
		logger.L().Error(ctx, "Error getting template tag assignments", zap.Error(err), logger.WithTeamID(teamID.String()), logger.WithTemplateID(templateID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting template tag assignments")

		return
	}

	hasMore := int32(len(rows)) > limit
	if hasMore {
		rows = rows[:limit]
	}

	assignments := make([]api.TemplateTagAssignment, 0, len(rows))
	for _, row := range rows {
		assignments = append(assignments, api.TemplateTagAssignment{
			AssignmentId:    row.AssignmentID,
			BuildId:         row.BuildID,
			AssignedAt:      row.AssignedAt.Time,
			BuildCreatedAt:  row.BuildCreatedAt,
			BuildFinishedAt: row.BuildFinishedAt,
		})
	}

	var nextCursor *string
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		cursor := formatTagAssignmentsCursor(last.AssignedAt.Time, last.AssignmentID)
		nextCursor = &cursor
	}

	c.JSON(http.StatusOK, api.TemplateTagAssignmentsResponse{
		Data:       assignments,
		NextCursor: nextCursor,
	})
}
