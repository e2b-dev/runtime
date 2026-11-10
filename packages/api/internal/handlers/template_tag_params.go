package handlers

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	defaultAssignmentsPerGroup = int32(6)
	maxAssignmentsPerGroup     = int32(25)

	defaultTagGroupsLimit = int32(25)
	maxTagGroupsLimit     = int32(100)
	maxTagGroupsSearchLen = 64

	defaultTagAssignmentsPageSize = int32(50)
	maxTagAssignmentsPageSize     = int32(100)
)

var (
	tagGroupsSearchRegex = regexp.MustCompile(`^[a-z0-9._-]*$`)

	// The first page of a newest-first assignment list starts from a fixed
	// far-future sentinel instead of time.Now(), so clock skew between the
	// app and the database cannot drop rows inserted during the request.
	tagAssignmentsFirstPageTime = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	tagAssignmentsFirstPageID   = uuid.Max

	errTagGroupsInvalidSort           = errors.New("invalid sort")
	errTagGroupsInvalidSearch         = errors.New("invalid search")
	errTagGroupsInvalidCursorFormat   = errors.New("invalid cursor format")
	errTagGroupsCursorSortMismatch    = errors.New("cursor sort mismatch")
	errTagGroupsInvalidCursorTime     = errors.New("invalid cursor time")
	errTagGroupsCursorTagOutOfCharset = errors.New("invalid cursor tag")
	errTagAssignmentsInvalidCursor    = errors.New("invalid cursor")
)

type tagGroupsSort string

const (
	tagGroupsSortLatestDesc tagGroupsSort = "latest_desc"
	tagGroupsSortLatestAsc  tagGroupsSort = "latest_asc"
	tagGroupsSortNameAsc    tagGroupsSort = "name_asc"
	tagGroupsSortNameDesc   tagGroupsSort = "name_desc"
)

func normalizeAssignmentsPerGroupLimit(limit *api.TagAssignmentLimit) int32 {
	return clampTagLimit(sharedUtils.DerefOrDefault(limit, defaultAssignmentsPerGroup), maxAssignmentsPerGroup)
}

func normalizeTagGroupsLimit(limit *api.TagGroupsLimit) int32 {
	return clampTagLimit(sharedUtils.DerefOrDefault(limit, defaultTagGroupsLimit), maxTagGroupsLimit)
}

func normalizeTagAssignmentsPageLimit(limit *api.TagAssignmentsLimit) int32 {
	return clampTagLimit(sharedUtils.DerefOrDefault(limit, defaultTagAssignmentsPageSize), maxTagAssignmentsPageSize)
}

func clampTagLimit(value, maxValue int32) int32 {
	return min(max(value, 1), maxValue)
}

func parseTagGroupsSort(value *api.GetTemplatesTemplateIDTagsGroupsParamsSort) (tagGroupsSort, error) {
	if value == nil {
		return tagGroupsSortLatestDesc, nil
	}

	switch tagGroupsSort(*value) {
	case tagGroupsSortLatestDesc, tagGroupsSortLatestAsc, tagGroupsSortNameAsc, tagGroupsSortNameDesc:
		return tagGroupsSort(*value), nil
	default:
		return "", fmt.Errorf("%w: %q", errTagGroupsInvalidSort, *value)
	}
}

func normalizeTagGroupsSearch(value *api.TagGroupsSearch) (string, error) {
	cleaned := strings.ToLower(strings.TrimSpace(sharedUtils.DerefOrDefault(value, "")))
	if cleaned == "" {
		return "", nil
	}

	if len(cleaned) > maxTagGroupsSearchLen {
		return "", fmt.Errorf("%w: too long", errTagGroupsInvalidSearch)
	}

	if !tagGroupsSearchRegex.MatchString(cleaned) {
		return "", fmt.Errorf("%w: charset", errTagGroupsInvalidSearch)
	}

	return cleaned, nil
}

// parseTagGroupsCursor decodes the {sort}|{latest_assigned_at}|{tag} cursor.
// No cursor means the first page and returns nil pointers.
func parseTagGroupsCursor(cursor *api.TagGroupsCursor, sort tagGroupsSort) (*time.Time, *string, error) {
	if cursor == nil || *cursor == "" {
		return nil, nil, nil
	}

	parts := strings.SplitN(*cursor, "|", 3)
	if len(parts) != 3 {
		return nil, nil, errTagGroupsInvalidCursorFormat
	}

	if parts[0] != string(sort) {
		return nil, nil, errTagGroupsCursorSortMismatch
	}

	cursorTime, err := parseTagCursorTime(parts[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", errTagGroupsInvalidCursorTime, err)
	}

	// A real next-page cursor always pins a concrete tag, so an empty one is
	// rejected along with any character a tag cannot contain.
	if !tagGroupsSearchRegex.MatchString(parts[2]) || parts[2] == "" {
		return nil, nil, errTagGroupsCursorTagOutOfCharset
	}

	cursorTag := parts[2]

	return &cursorTime, &cursorTag, nil
}

func formatTagGroupsCursor(sort tagGroupsSort, latestAt time.Time, tag string) string {
	return fmt.Sprintf("%s|%s|%s", sort, latestAt.UTC().Format(time.RFC3339Nano), tag)
}

// parseTagAssignmentsCursor decodes the {assigned_at}|{assignment_id} cursor.
// No cursor means the first page.
func parseTagAssignmentsCursor(cursor *api.TagAssignmentsCursor) (time.Time, uuid.UUID, error) {
	if cursor == nil || *cursor == "" {
		return tagAssignmentsFirstPageTime, tagAssignmentsFirstPageID, nil
	}

	parts := strings.SplitN(*cursor, "|", 2)
	if len(parts) != 2 {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: bad format", errTagAssignmentsInvalidCursor)
	}

	ts, err := parseTagCursorTime(parts[0])
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: %w", errTagAssignmentsInvalidCursor, err)
	}

	assignmentID, err := uuid.Parse(parts[1])
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: %w", errTagAssignmentsInvalidCursor, err)
	}

	return ts, assignmentID, nil
}

func formatTagAssignmentsCursor(assignedAt time.Time, assignmentID uuid.UUID) string {
	return fmt.Sprintf("%s|%s", assignedAt.UTC().Format(time.RFC3339Nano), assignmentID.String())
}

func parseTagCursorTime(value string) (time.Time, error) {
	cursorTime, err := time.Parse(time.RFC3339Nano, value)
	if err == nil {
		return cursorTime, nil
	}

	return time.Parse(time.RFC3339, value)
}
