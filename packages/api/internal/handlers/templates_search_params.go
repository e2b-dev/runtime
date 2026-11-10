package handlers

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

type templatesSort string

const (
	templatesSortCreatedAtDesc templatesSort = "created_at_desc"
	templatesSortCreatedAtAsc  templatesSort = "created_at_asc"
	templatesSortUpdatedAtDesc templatesSort = "updated_at_desc"
	templatesSortUpdatedAtAsc  templatesSort = "updated_at_asc"
)

var (
	errTemplatesInvalidSort = errors.New("invalid sort")

	likeWildcardEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
)

func parseTemplatesSort(value *api.GetUnstableTemplatesSearchParamsSort) (templatesSort, error) {
	if value == nil {
		return templatesSortCreatedAtDesc, nil
	}

	switch templatesSort(*value) {
	case templatesSortCreatedAtDesc, templatesSortCreatedAtAsc, templatesSortUpdatedAtDesc, templatesSortUpdatedAtAsc:
		return templatesSort(*value), nil
	default:
		return "", fmt.Errorf("%w: %q", errTemplatesInvalidSort, *value)
	}
}

func (s templatesSort) ascending() bool {
	return s == templatesSortCreatedAtAsc || s == templatesSortUpdatedAtAsc
}

func (s templatesSort) byUpdatedAt() bool {
	return s == templatesSortUpdatedAtDesc || s == templatesSortUpdatedAtAsc
}

// Pages use keyset cursors of the form `{sort}|{timestamp}|{template ID}`,
// where the timestamp is the sorted column of the page's last row.

const (
	templatesSearchDefaultLimit = int32(50)
	templatesSearchMaxLimit     = int32(100)
)

var (
	errInvalidTemplatesCursor      = errors.New("invalid cursor")
	errTemplatesCursorSortMismatch = errors.New("cursor sort mismatch")

	// The first page of a newest-first listing starts from a far-future
	// sentinel rather than now, so clock skew between the app and the
	// database cannot drop rows written during the request.
	maxTemplatesCursorTime = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	minTemplatesCursorTime = time.Time{}
)

func normalizeTemplatesLimit(limit *api.TemplatesLimit) int32 {
	return min(max(sharedUtils.DerefOrDefault(limit, templatesSearchDefaultLimit), 1), templatesSearchMaxLimit)
}

// parseTemplatesCursor parses a `{sort}|{value}|{id}` cursor and verifies that
// it was issued for the same sort as the current request. It returns the
// first page's position for an empty cursor.
func parseTemplatesCursor(cursor *api.TemplatesCursor, sort templatesSort) (time.Time, string, error) {
	if cursor == nil || *cursor == "" {
		if sort.ascending() {
			return minTemplatesCursorTime, "", nil
		}

		return maxTemplatesCursorTime, "", nil
	}

	parts := strings.SplitN(*cursor, "|", 3)
	if len(parts) != 3 {
		return time.Time{}, "", errInvalidTemplatesCursor
	}

	if parts[0] != string(sort) {
		return time.Time{}, "", errTemplatesCursorSortMismatch
	}

	// A real next-page cursor always pins a concrete template id.
	if parts[2] == "" {
		return time.Time{}, "", errInvalidTemplatesCursor
	}

	value, err := parseTemplatesCursorTime(parts[1])
	if err != nil {
		return time.Time{}, "", fmt.Errorf("%w: %w", errInvalidTemplatesCursor, err)
	}

	return value, parts[2], nil
}

func parseTemplatesCursorTime(value string) (time.Time, error) {
	cursorTime, err := time.Parse(time.RFC3339Nano, value)
	if err == nil {
		return cursorTime, nil
	}

	return time.Parse(time.RFC3339, value)
}

func formatTemplatesCursor(sort templatesSort, row templateRow) string {
	value := row.CreatedAt
	if sort.byUpdatedAt() {
		value = row.UpdatedAt
	}

	return fmt.Sprintf("%s|%s|%s", sort, value.UTC().Format(time.RFC3339Nano), row.TemplateID)
}

// templatesSearchPattern returns the search term with LIKE wildcards escaped,
// so % and _ match themselves; the query wraps it in % on both sides.
func templatesSearchPattern(search *api.TemplatesSearch) string {
	if search == nil {
		return ""
	}

	return likeWildcardEscaper.Replace(strings.TrimSpace(*search))
}
