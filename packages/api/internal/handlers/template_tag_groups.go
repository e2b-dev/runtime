package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) GetTemplatesTemplateIDTagsGroups(c *gin.Context, templateID api.TemplateID, params api.GetTemplatesTemplateIDTagsGroupsParams) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "list template tag groups")

	teamID := auth.MustGetTeamID(c)
	telemetry.SetAttributes(ctx, telemetry.WithTeamID(teamID.String()), telemetry.WithTemplateID(templateID))

	if !a.requireTeamTemplate(c, templateID, teamID) {
		return
	}

	sort, err := parseTagGroupsSort(params.Sort)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sort parameter")

		return
	}

	search, err := normalizeTagGroupsSearch(params.Search)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid search parameter")

		return
	}

	cursorTime, cursorTag, err := parseTagGroupsCursor(params.TagsCursor, sort)
	if err != nil {
		switch {
		case errors.Is(err, errTagGroupsCursorSortMismatch):
			a.sendAPIStoreError(c, http.StatusBadRequest, "Cursor was issued for a different sort; clear the cursor and restart")
		default:
			a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid cursor")
		}

		return
	}

	assignmentLimit := normalizeAssignmentsPerGroupLimit(params.AssignmentLimit)
	tagsLimit := normalizeTagGroupsLimit(params.TagsLimit)

	rows, err := a.listTemplateTagGroups(
		ctx,
		sort,
		templateID,
		search,
		cursorTime,
		cursorTag,
		tagsLimit+1,
		assignmentLimit+1,
	)
	if err != nil {
		logger.L().Error(
			ctx,
			"error listing template tag groups",
			zap.Error(err),
			zap.String("sort", string(sort)),
			logger.WithTeamID(teamID.String()),
			logger.WithTemplateID(templateID),
		)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting template tag groups")

		return
	}

	groups, nextCursor := buildTagGroups(rows, sort, assignmentLimit, tagsLimit)

	c.JSON(http.StatusOK, api.TemplateTagGroupsResponse{
		Tags:       groups,
		NextCursor: nextCursor,
	})
}

func (a *APIStore) GetTemplatesTemplateIDTagsCount(c *gin.Context, templateID api.TemplateID) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "count template tags")

	teamID := auth.MustGetTeamID(c)
	telemetry.SetAttributes(ctx, telemetry.WithTeamID(teamID.String()), telemetry.WithTemplateID(templateID))

	if !a.requireTeamTemplate(c, templateID, teamID) {
		return
	}

	total, err := a.sqlcDB.CountTemplateTags(ctx, templateID)
	if err != nil {
		logger.L().Error(
			ctx,
			"error counting template tags",
			zap.Error(err),
			logger.WithTeamID(teamID.String()),
			logger.WithTemplateID(templateID),
		)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when counting template tags")

		return
	}

	c.JSON(http.StatusOK, api.TemplateTagsCountResponse{Total: total})
}

func (a *APIStore) GetTemplatesTemplateIDTagsExists(c *gin.Context, templateID api.TemplateID, params api.GetTemplatesTemplateIDTagsExistsParams) {
	ctx := c.Request.Context()
	telemetry.ReportEvent(ctx, "check template tag exists")

	teamID := auth.MustGetTeamID(c)
	telemetry.SetAttributes(ctx, telemetry.WithTeamID(teamID.String()), telemetry.WithTemplateID(templateID))

	if !a.requireTeamTemplate(c, templateID, teamID) {
		return
	}

	tags, err := id.ValidateAndDeduplicateTags([]string{params.Tag})
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid tag")

		return
	}

	normalizedTag := tags[0]
	exists, err := a.sqlcDB.CheckReadyTemplateTagExists(ctx, queries.CheckReadyTemplateTagExistsParams{
		TemplateID: templateID,
		Tag:        normalizedTag,
	})
	if err != nil {
		logger.L().Error(
			ctx,
			"error checking template tag existence",
			zap.Error(err),
			logger.WithTeamID(teamID.String()),
			logger.WithTemplateID(templateID),
		)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when checking template tag existence")

		return
	}

	c.JSON(http.StatusOK, api.TemplateTagExistsResponse{
		Exists:        exists,
		NormalizedTag: normalizedTag,
	})
}

// requireTeamTemplate answers 404 for a template that is missing, deleted or
// owned by another team, so the tag routes do not reveal other teams'
// templates.
func (a *APIStore) requireTeamTemplate(c *gin.Context, templateID api.TemplateID, teamID uuid.UUID) bool {
	ctx := c.Request.Context()

	_, err := a.sqlcDB.GetTeamTemplate(ctx, queries.GetTeamTemplateParams{
		ID:     templateID,
		TeamID: teamID,
	})
	if err != nil {
		if dberrors.IsNotFoundError(err) {
			a.sendAPIStoreError(c, http.StatusNotFound, "Template not found")

			return false
		}

		logger.L().Error(ctx, "Error getting template", zap.Error(err), logger.WithTeamID(teamID.String()), logger.WithTemplateID(templateID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting template")

		return false
	}

	return true
}

type tagGroupRow struct {
	AssignmentID     uuid.UUID
	Tag              string
	BuildID          uuid.UUID
	AssignedAt       time.Time
	BuildCreatedAt   time.Time
	BuildFinishedAt  *time.Time
	LatestAssignedAt time.Time
}

func (a *APIStore) listTemplateTagGroups(
	ctx context.Context,
	sort tagGroupsSort,
	templateID api.TemplateID,
	search string,
	cursorTime *time.Time,
	cursorTag *string,
	tagsLimitPlusOne int32,
	assignmentLimitPlusOne int32,
) ([]tagGroupRow, error) {
	switch sort {
	case tagGroupsSortLatestDesc:
		rows, err := a.sqlcDB.ListTemplateTagGroupsByLatestDesc(ctx, queries.ListTemplateTagGroupsByLatestDescParams{
			TemplateID:             templateID,
			Search:                 search,
			CursorTime:             cursorTime,
			CursorTag:              cursorTag,
			TagsLimitPlusOne:       tagsLimitPlusOne,
			AssignmentLimitPlusOne: assignmentLimitPlusOne,
		})
		if err != nil {
			return nil, fmt.Errorf("listing tag groups (latest_desc): %w", err)
		}
		out := make([]tagGroupRow, len(rows))
		for i, r := range rows {
			out[i] = tagGroupRow{
				AssignmentID:     r.AssignmentID,
				Tag:              r.Tag,
				BuildID:          r.BuildID,
				AssignedAt:       r.AssignedAt.Time,
				BuildCreatedAt:   r.BuildCreatedAt,
				BuildFinishedAt:  r.BuildFinishedAt,
				LatestAssignedAt: r.LatestAssignedAt,
			}
		}

		return out, nil

	case tagGroupsSortLatestAsc:
		rows, err := a.sqlcDB.ListTemplateTagGroupsByLatestAsc(ctx, queries.ListTemplateTagGroupsByLatestAscParams{
			TemplateID:             templateID,
			Search:                 search,
			CursorTime:             cursorTime,
			CursorTag:              cursorTag,
			TagsLimitPlusOne:       tagsLimitPlusOne,
			AssignmentLimitPlusOne: assignmentLimitPlusOne,
		})
		if err != nil {
			return nil, fmt.Errorf("listing tag groups (latest_asc): %w", err)
		}
		out := make([]tagGroupRow, len(rows))
		for i, r := range rows {
			out[i] = tagGroupRow{
				AssignmentID:     r.AssignmentID,
				Tag:              r.Tag,
				BuildID:          r.BuildID,
				AssignedAt:       r.AssignedAt.Time,
				BuildCreatedAt:   r.BuildCreatedAt,
				BuildFinishedAt:  r.BuildFinishedAt,
				LatestAssignedAt: r.LatestAssignedAt,
			}
		}

		return out, nil

	case tagGroupsSortNameAsc:
		rows, err := a.sqlcDB.ListTemplateTagGroupsByNameAsc(ctx, queries.ListTemplateTagGroupsByNameAscParams{
			TemplateID:             templateID,
			Search:                 search,
			CursorTag:              cursorTag,
			TagsLimitPlusOne:       tagsLimitPlusOne,
			AssignmentLimitPlusOne: assignmentLimitPlusOne,
		})
		if err != nil {
			return nil, fmt.Errorf("listing tag groups (name_asc): %w", err)
		}
		out := make([]tagGroupRow, len(rows))
		for i, r := range rows {
			out[i] = tagGroupRow{
				AssignmentID:     r.AssignmentID,
				Tag:              r.Tag,
				BuildID:          r.BuildID,
				AssignedAt:       r.AssignedAt.Time,
				BuildCreatedAt:   r.BuildCreatedAt,
				BuildFinishedAt:  r.BuildFinishedAt,
				LatestAssignedAt: r.LatestAssignedAt,
			}
		}

		return out, nil

	case tagGroupsSortNameDesc:
		rows, err := a.sqlcDB.ListTemplateTagGroupsByNameDesc(ctx, queries.ListTemplateTagGroupsByNameDescParams{
			TemplateID:             templateID,
			Search:                 search,
			CursorTag:              cursorTag,
			TagsLimitPlusOne:       tagsLimitPlusOne,
			AssignmentLimitPlusOne: assignmentLimitPlusOne,
		})
		if err != nil {
			return nil, fmt.Errorf("listing tag groups (name_desc): %w", err)
		}
		out := make([]tagGroupRow, len(rows))
		for i, r := range rows {
			out[i] = tagGroupRow{
				AssignmentID:     r.AssignmentID,
				Tag:              r.Tag,
				BuildID:          r.BuildID,
				AssignedAt:       r.AssignedAt.Time,
				BuildCreatedAt:   r.BuildCreatedAt,
				BuildFinishedAt:  r.BuildFinishedAt,
				LatestAssignedAt: r.LatestAssignedAt,
			}
		}

		return out, nil

	default:
		return nil, fmt.Errorf("unsupported sort: %q", sort)
	}
}

// buildTagGroups assembles API groups from the flat row stream, trims any
// (tagsLimit+1)th tag, and returns the nextCursor pointing at the last
// surviving group when one was dropped.
func buildTagGroups(
	rows []tagGroupRow,
	sort tagGroupsSort,
	assignmentLimit int32,
	tagsLimit int32,
) ([]api.TemplateTagGroup, *string) {
	groups := make([]api.TemplateTagGroup, 0)
	groupIndexByTag := map[string]int{}
	latestByTag := map[string]time.Time{}

	for _, row := range rows {
		groupIndex, ok := groupIndexByTag[row.Tag]
		if !ok {
			if int32(len(groups)) >= tagsLimit {
				// (tagsLimit+1)-th tag arrived — skip its rows. We've already
				// captured latest_assigned_at for the previous, surviving tag.
				continue
			}

			groupIndex = len(groups)
			groupIndexByTag[row.Tag] = groupIndex
			latestByTag[row.Tag] = row.LatestAssignedAt
			groups = append(groups, api.TemplateTagGroup{
				Tag:         row.Tag,
				Assignments: []api.TemplateTagAssignment{},
			})
		}

		if int32(len(groups[groupIndex].Assignments)) >= assignmentLimit {
			groups[groupIndex].HasMore = true

			continue
		}

		groups[groupIndex].Assignments = append(groups[groupIndex].Assignments, api.TemplateTagAssignment{
			AssignmentId:    row.AssignmentID,
			BuildId:         row.BuildID,
			AssignedAt:      row.AssignedAt,
			BuildCreatedAt:  row.BuildCreatedAt,
			BuildFinishedAt: row.BuildFinishedAt,
		})
	}

	var nextCursor *string
	if hasMore := tagsLimitWasExceeded(rows, tagsLimit); hasMore && len(groups) > 0 {
		last := groups[len(groups)-1]
		cursor := formatTagGroupsCursor(sort, latestByTag[last.Tag], last.Tag)
		nextCursor = &cursor
	}

	return groups, nextCursor
}

// tagsLimitWasExceeded reports whether more than `tagsLimit` distinct tags
// appeared in `rows`, signalling a next page exists.
func tagsLimitWasExceeded(rows []tagGroupRow, tagsLimit int32) bool {
	seen := make(map[string]struct{}, tagsLimit+1)
	for _, r := range rows {
		seen[r.Tag] = struct{}{}
		if int32(len(seen)) > tagsLimit {
			return true
		}
	}

	return false
}
