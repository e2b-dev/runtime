package queries

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnapshotCursorQueriesShareOneProjection guards the four snapshot cursor queries
// against silent divergence.
//
// sqlc has no include mechanism, so each of the ascending, descending, filtered and
// unfiltered variants carries its own copy of the projection, the page subquery's joins
// and the alias lookup; only the page's WHERE and ORDER BY clauses are meant to differ.
// The generated row structs are convertible in Go, so a change to the selected columns is
// a compile error at the call site -- but changing the build assignment's tag or status
// filter, or the alias aggregation, in one copy and not the others would silently make
// the two orders (or the filtered and unfiltered paths) return different rows for the
// same sandbox, with nothing failing to tell us.
//
// Each query also orders twice: the page subquery picks its rows in one order and the
// outer query must return them in that same order.
//
// If this test fails, the fix is to apply the edit to all four queries in
// get_snapshots_with_cursor.sql and regenerate, not to relax the assertion.
func TestSnapshotCursorQueriesShareOneProjection(t *testing.T) {
	t.Parallel()

	// split returns the text every variant must share -- the column list, the page
	// subquery's joins, and the outer joins -- plus the page's ORDER BY and the outer
	// ORDER BY. The `-- name:` header and the page's WHERE clause are per-query by
	// construction.
	split := func(t *testing.T, query string) (shared, pageOrder, outerOrder string) {
		t.Helper()

		start := strings.Index(query, "SELECT ")
		require.NotEqual(t, -1, start, "query should have a SELECT")

		head, rest, found := strings.Cut(query[start:], "\n    WHERE\n")
		require.True(t, found, "page subquery should have a WHERE clause")

		_, rest, found = strings.Cut(rest, "\n    ORDER BY ")
		require.True(t, found, "page subquery should have an ORDER BY")

		pageOrder, rest, found = strings.Cut(rest, "\n    LIMIT $1\n) page\n")
		require.True(t, found, "page subquery should end with LIMIT $1")

		tail, outerOrder, found := strings.Cut(rest, "\nORDER BY ")
		require.True(t, found, "query should order the page")

		return head + tail, pageOrder, strings.TrimSpace(outerOrder)
	}

	queries := map[string]string{
		"GetSnapshotsWithCursor":              getSnapshotsWithCursor,
		"GetSnapshotsWithCursorAsc":           getSnapshotsWithCursorAsc,
		"GetSnapshotsByTemplateWithCursor":    getSnapshotsByTemplateWithCursor,
		"GetSnapshotsByTemplateWithCursorAsc": getSnapshotsByTemplateWithCursorAsc,
	}

	reference, _, _ := split(t, getSnapshotsWithCursor)

	// Sanity-check that the shared body really is the part worth pinning, so this test
	// cannot pass by comparing two empty strings.
	require.Contains(t, reference, "eba.tag = 'default'")
	require.Contains(t, reference, "eb.status_group = 'ready'")
	require.Contains(t, reference, "ARRAY_AGG(alias ORDER BY alias)")

	for name, query := range queries {
		shared, pageOrder, outerOrder := split(t, query)

		assert.Equal(t, reference, shared,
			"%s must select and join exactly as GetSnapshotsWithCursor does", name)
		assert.Equal(t, strings.ReplaceAll(pageOrder, "s.", "page."), outerOrder,
			"%s must return the page in the order the page subquery picked it", name)
	}
}
