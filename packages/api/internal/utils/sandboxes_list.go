package utils

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	MaxSandboxID = "zzzzzzzzzzzzzzzzzzzz"
)

// PaginatedSandbox extends the api.ListedSandbox with a timestamp to use for pagination
type PaginatedSandbox struct {
	api.ListedSandbox

	PaginationTimestamp time.Time `json:"-"`
}

func ParseMetadata(ctx context.Context, metadata *string) (*map[string]string, error) {
	// Parse metadata filter (query) if provided
	var metadataFilter *map[string]string
	if metadata != nil {
		parsedMetadataFilter, err := parseFilters(*metadata)
		if err != nil {
			logger.L().Warn(ctx, "Error parsing metadata", zap.Error(err))

			return nil, fmt.Errorf("error parsing metadata: %w", err)
		}

		metadataFilter = &parsedMetadataFilter
	}

	return metadataFilter, nil
}

// ParseCursor decodes a cursor token into its timestamp, ID and sort direction.
// A two-part token is a descending cursor (the original format, still emitted for
// the default order); a three-part token carries CursorDirectionAsc.
func ParseCursor(cursor string) (time.Time, string, SortDirection, error) {
	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", SortDesc, fmt.Errorf("error decoding cursor: %w", err)
	}

	parts := strings.Split(string(decoded), "__")
	if len(parts) < 2 || len(parts) > 3 {
		return time.Time{}, "", SortDesc, errors.New("invalid cursor format")
	}

	order := SortDesc
	if len(parts) == 3 {
		if parts[2] != CursorDirectionAsc {
			return time.Time{}, "", SortDesc, errors.New("invalid sort direction in cursor")
		}

		order = SortAsc
	}

	cursorTime, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", SortDesc, fmt.Errorf("invalid timestamp format in cursor: %w", err)
	}

	return cursorTime, parts[1], order, nil
}

// FilterBasedOnCursor keeps only the sandboxes that fall after the cursor in the
// requested order. It compares on PaginationTimestamp (the microsecond-aligned keyset
// value), not the public StartedAt, so running and paused sandboxes share the same
// precision as the SQL predicate. Descending order pages through
// (timestamp DESC, sandbox_id ASC); ascending order is the exact reverse
// (timestamp ASC, sandbox_id DESC).
func FilterBasedOnCursor(sandboxes []PaginatedSandbox, cursorTime time.Time, cursorID string, order SortDirection) []PaginatedSandbox {
	var filteredSandboxes []PaginatedSandbox
	for _, sandbox := range sandboxes {
		if isAfterCursor(sandbox, cursorTime, cursorID, order) {
			filteredSandboxes = append(filteredSandboxes, sandbox)
		}
	}

	return filteredSandboxes
}

// FilterUpToCursor keeps the sandboxes FilterBasedOnCursor drops: those at or before the
// cursor in the order.
func FilterUpToCursor(sandboxes []PaginatedSandbox, cursorTime time.Time, cursorID string, order SortDirection) []PaginatedSandbox {
	filteredSandboxes := make([]PaginatedSandbox, 0, len(sandboxes))
	for _, sandbox := range sandboxes {
		if !isAfterCursor(sandbox, cursorTime, cursorID, order) {
			filteredSandboxes = append(filteredSandboxes, sandbox)
		}
	}

	return filteredSandboxes
}

func isAfterCursor(sandbox PaginatedSandbox, cursorTime time.Time, cursorID string, order SortDirection) bool {
	if order == SortAsc {
		return sandbox.PaginationTimestamp.After(cursorTime) ||
			(sandbox.PaginationTimestamp.Equal(cursorTime) && sandbox.SandboxID < cursorID)
	}

	return sandbox.PaginationTimestamp.Before(cursorTime) ||
		(sandbox.PaginationTimestamp.Equal(cursorTime) && sandbox.SandboxID > cursorID)
}

// SortPaginatedSandboxes sorts the sandboxes by PaginationTimestamp then SandboxID for
// stable pagination. It uses PaginationTimestamp (the microsecond-aligned keyset value),
// not the public StartedAt, so the order matches the cursor filter and SQL predicate.
// Descending order is timestamp DESC, SandboxID ASC; ascending order is the exact
// reverse (timestamp ASC, SandboxID DESC) so it maps onto a backward scan of the
// (team_id, sandbox_started_at DESC, sandbox_id) index.
func SortPaginatedSandboxes(sandboxes []PaginatedSandbox, order SortDirection) {
	slices.SortFunc(sandboxes, func(a, b PaginatedSandbox) int {
		if !a.PaginationTimestamp.Equal(b.PaginationTimestamp) {
			if order == SortAsc {
				return a.PaginationTimestamp.Compare(b.PaginationTimestamp)
			}

			return b.PaginationTimestamp.Compare(a.PaginationTimestamp)
		}

		if order == SortAsc {
			return strings.Compare(b.SandboxID, a.SandboxID)
		}

		return strings.Compare(a.SandboxID, b.SandboxID)
	})
}

// SortPaginatedSandboxesDesc preserves the descending-only entry point used by the
// legacy (v1) list endpoint.
func SortPaginatedSandboxesDesc(sandboxes []PaginatedSandbox) {
	SortPaginatedSandboxes(sandboxes, SortDesc)
}

// FilterSandboxesOnStartedAtAndTemplate applies the startedAfter lower bound
// (inclusive) and the exact-template filter.
//
// The bound is compared against PaginationTimestamp, the microsecond-aligned keyset
// value, not the public StartedAt, so a kept row can never sort before the bound it
// satisfies. Callers pass a bound already truncated onto that same microsecond grid
// (see parseSandboxListStartedAfter), which is what keeps the boundary inclusive here
// and in the SQL predicate alike -- a sandbox does not enter or leave a filtered list
// purely because it paused.
//
// The input is left intact: filtering in place would rewrite the caller's slice, and
// this one already returns it unmodified when no filter is active, so a destructive
// contract would only hold for some arguments.
func FilterSandboxesOnStartedAtAndTemplate(sandboxes []PaginatedSandbox, startedAfter time.Time, templateID *string) []PaginatedSandbox {
	if startedAfter.IsZero() && templateID == nil {
		return sandboxes
	}

	filtered := make([]PaginatedSandbox, 0, len(sandboxes))
	for _, sbx := range sandboxes {
		if !startedAfter.IsZero() && sbx.PaginationTimestamp.Before(startedAfter) {
			continue
		}

		if templateID != nil && sbx.TemplateID != *templateID {
			continue
		}

		filtered = append(filtered, sbx)
	}

	return filtered
}

func FilterSandboxesOnMetadata(sandboxes []PaginatedSandbox, metadata *map[string]string) []PaginatedSandbox {
	if metadata == nil {
		return sandboxes
	}

	// Filter instances to match all metadata
	n := 0
	for _, sbx := range sandboxes {
		if sbx.Metadata == nil {
			continue
		}

		matchesAll := true
		for key, value := range *metadata {
			if metadataValue, ok := (*sbx.Metadata)[key]; !ok || metadataValue != value {
				matchesAll = false

				break
			}
		}

		if matchesAll {
			sandboxes[n] = sbx
			n++
		}
	}

	// Trim slice
	sandboxes = sandboxes[:n]

	return sandboxes
}

func parseFilters(query string) (map[string]string, error) {
	query, err := url.QueryUnescape(query)
	if err != nil {
		return nil, fmt.Errorf("error when unescaping query: %w", err)
	}

	// Parse filters, both key and value are also unescaped
	filters := make(map[string]string)

	for filter := range strings.SplitSeq(query, "&") {
		parts := strings.Split(filter, "=")
		if len(parts) != 2 {
			return nil, errors.New("invalid key value pair in query")
		}

		key, err := url.QueryUnescape(parts[0])
		if err != nil {
			return nil, fmt.Errorf("error when unescaping key: %w", err)
		}

		value, err := url.QueryUnescape(parts[1])
		if err != nil {
			return nil, fmt.Errorf("error when unescaping value: %w", err)
		}

		filters[key] = value
	}

	return filters, nil
}
