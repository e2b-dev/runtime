package utils

import (
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
)

// CursorDirectionAsc marks an ascending cursor as the token's third component.
// Descending cursors keep the original two-part form, so tokens minted before
// ordering existed stay readable and tokens minted for the default order stay
// readable by instances that predate it.
const CursorDirectionAsc = "asc"

// generateCursor generates a cursor token from a timestamp, ID and sort order.
// The cursor format is base64-encoded "{RFC3339Nano_timestamp}__{id}" for
// descending order and "{RFC3339Nano_timestamp}__{id}__asc" for ascending.
func generateCursor(timestamp time.Time, id string, order SortDirection) string {
	cursor := fmt.Sprintf("%s__%s", timestamp.Format(time.RFC3339Nano), id)
	if order == SortAsc {
		cursor = fmt.Sprintf("%s__%s", cursor, CursorDirectionAsc)
	}

	return base64.URLEncoding.EncodeToString([]byte(cursor))
}

// minLimit is the smallest page size a request can ask for, mirroring the
// spec's `minimum: 1` on the shared paginationLimit parameter.
const minLimit int32 = 1

// SortDirection is the order in which keyset-paginated results are returned.
// The zero value is SortDesc, preserving the default newest-first behavior.
type SortDirection int

const (
	SortDesc SortDirection = iota
	SortAsc
)

// PaginationParams holds pagination parameters from the API request
type PaginationParams struct {
	Limit     *int32
	NextToken *string
}

// PaginationConfig holds configuration for pagination behavior
type PaginationConfig struct {
	DefaultLimit int32
	MaxLimit     int32
	DefaultID    string // Default cursor ID when no token is provided (e.g., max UUID or max sandbox ID)
	// Order controls the first-page cursor when no token is provided. For
	// SortDesc the first page starts at "now" (newest first); for SortAsc it
	// starts at the zero time (oldest first).
	Order SortDirection
}

// Cursor represents a parsed pagination cursor
type Cursor struct {
	Time time.Time
	ID   string
}

// Pagination handles pagination logic for list endpoints
type Pagination[T any] struct {
	config    PaginationConfig
	limit     int32
	cursor    Cursor
	nextToken *string
}

// NewPagination creates a new pagination instance from request parameters
func NewPagination[T any](params PaginationParams, config PaginationConfig) (*Pagination[T], error) {
	p := &Pagination[T]{
		config: config,
	}

	// Parse and validate limit. The spec declares minimum: 1 and the API server
	// does enforce it at the request validator, so a zero or negative limit is a
	// 400 before it reaches here. Clamp from below anyway, as defence in depth
	// for any caller that reaches this helper without that middleware: an
	// unclamped zero would index results[limit-1] out of range in processResults.
	p.limit = config.DefaultLimit
	if params.Limit != nil {
		p.limit = *params.Limit
	}
	if p.limit > config.MaxLimit {
		p.limit = config.MaxLimit
	}
	if p.limit < minLimit {
		p.limit = minLimit
	}

	// Parse cursor token
	var err error
	p.cursor, err = parseCursorToken(params.NextToken, config)
	if err != nil {
		return nil, fmt.Errorf("invalid next token: %w", err)
	}

	return p, nil
}

// QueryLimit returns the limit to use for database queries (limit + 1 to detect more results)
func (p *Pagination[T]) QueryLimit() int32 {
	return p.limit + 1
}

// CursorTime returns the cursor timestamp
func (p *Pagination[T]) CursorTime() time.Time {
	return p.cursor.Time
}

// CursorID returns the cursor ID
func (p *Pagination[T]) CursorID() string {
	return p.cursor.ID
}

// setNextToken sets the next token from the last item in the results
func (p *Pagination[T]) setNextToken(timestamp time.Time, id string) {
	cursor := generateCursor(timestamp, id, p.config.Order)
	p.nextToken = &cursor
}

// hasMore checks if there are more results based on the result count
func (p *Pagination[T]) hasMore(resultCount int) bool {
	return resultCount > int(p.limit)
}

// trimResults trims the results to the requested limit if there are more
func (p *Pagination[T]) trimResults(results []T) []T {
	if p.hasMore(len(results)) {
		return results[:p.limit]
	}

	return results
}

// processResults handles pagination: checks for more results, sets next token from last item, and trims results.
// The getTimestampAndID function extracts the timestamp and ID from each result item.
func (p *Pagination[T]) processResults(results []T, getTimestampAndID func(T) (time.Time, string)) []T {
	if p.hasMore(len(results)) {
		lastItem := results[p.limit-1]
		timestamp, id := getTimestampAndID(lastItem)
		p.setNextToken(timestamp, id)
	}

	return p.trimResults(results)
}

// ProcessResultsWithHeader handles pagination and sets the X-Next-Token header in one call.
// This is a convenience method that combines ProcessResults and SetHeader.
func (p *Pagination[T]) ProcessResultsWithHeader(c *gin.Context, results []T, getTimestampAndID func(T) (time.Time, string)) []T {
	trimmed := p.processResults(results, getTimestampAndID)
	p.setHeader(c)

	return trimmed
}

// ProcessResultsWithHeaderContinuing is ProcessResultsWithHeader for results whose source
// stopped early at continueAfter: when they do not fill the page, the next token still
// continues after that position, so the short page does not end pagination.
func (p *Pagination[T]) ProcessResultsWithHeaderContinuing(c *gin.Context, results []T, getTimestampAndID func(T) (time.Time, string), continueAfter *Cursor) []T {
	trimmed := p.processResults(results, getTimestampAndID)
	if p.nextToken == nil && continueAfter != nil {
		p.setNextToken(continueAfter.Time, continueAfter.ID)
	}
	p.setHeader(c)

	return trimmed
}

// setHeader sets the X-Next-Token header if there are more results
func (p *Pagination[T]) setHeader(c *gin.Context) {
	if p.nextToken != nil {
		c.Header("X-Next-Token", *p.nextToken)
	}
}

// parseCursorToken parses a cursor token, returning default values if token is nil/empty
func parseCursorToken(token *string, config PaginationConfig) (Cursor, error) {
	if token != nil && *token != "" {
		cursorTime, cursorID, cursorOrder, err := ParseCursor(*token)
		if err != nil {
			return Cursor{}, err
		}

		// The direction is baked into the keyset comparison on both sides (the
		// in-memory filter and the SQL predicate), and the two directions are
		// exact inverses. Replaying a token under the other order would silently
		// re-serve rows the caller already saw instead of advancing, so refuse it.
		if cursorOrder != config.Order {
			return Cursor{}, errors.New("cursor was issued for a different sort order")
		}

		// Truncate for the same reason the startedAfter bound is truncated: the
		// keyset values this is compared against are microsecond-aligned on both
		// sides, so a nanosecond cursor time sorts strictly before the row that
		// minted it and re-serves that row. Tokens minted here already carry
		// PaginationTimestamp, but one minted before that change did not.
		return Cursor{Time: cursorTime.Truncate(time.Microsecond), ID: cursorID}, nil
	}

	// Default cursor for the first page. For descending order we start at "now"
	// (so everything older is included); for ascending order we start at the
	// zero time (so everything newer is included).
	defaultTime := time.Now()
	if config.Order == SortAsc {
		defaultTime = time.Time{}
	}

	return Cursor{Time: defaultTime, ID: config.DefaultID}, nil
}
