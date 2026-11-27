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
// unfiltered variants carries its own copy of the projection and the two lateral
// subqueries; only the WHERE and ORDER BY clauses are meant to differ. The generated row
// structs are convertible in Go, so a change to the selected columns is a compile error
// at the call site -- but changing the build assignment's tag or status filter, or the
// alias aggregation, in one copy and not the others would silently make the two orders
// (or the filtered and unfiltered paths) return different rows for the same sandbox,
// with nothing failing to tell us.
//
// If this test fails, the fix is to apply the edit to all four queries in
// get_snapshots_with_cursor.sql and regenerate, not to relax the assertion.
func TestSnapshotCursorQueriesShareOneProjection(t *testing.T) {
	t.Parallel()

	// projection returns everything from SELECT up to the WHERE clause: the column
	// list and the joins, which every variant must share. The `-- name:` header and
	// everything from WHERE onwards are per-query by construction.
	projection := func(t *testing.T, query string) string {
		t.Helper()

		start := strings.Index(query, "SELECT ")
		require.NotEqual(t, -1, start, "query should have a SELECT")

		body, _, found := strings.Cut(query[start:], "\nWHERE\n")
		require.True(t, found, "query should have a WHERE clause")

		return body
	}

	reference := projection(t, getSnapshotsWithCursor)

	// Sanity-check that the shared body really is the part worth pinning, so this test
	// cannot pass by comparing two empty strings.
	require.Contains(t, reference, "eba.tag = 'default'")
	require.Contains(t, reference, "eb.status_group = 'ready'")
	require.Contains(t, reference, "ARRAY_AGG(alias ORDER BY alias)")

	for name, query := range map[string]string{
		"GetSnapshotsWithCursorAsc":           getSnapshotsWithCursorAsc,
		"GetSnapshotsByTemplateWithCursor":    getSnapshotsByTemplateWithCursor,
		"GetSnapshotsByTemplateWithCursorAsc": getSnapshotsByTemplateWithCursorAsc,
	} {
		assert.Equal(t, reference, projection(t, query),
			"%s must select and join exactly as GetSnapshotsWithCursor does", name)
	}
}
