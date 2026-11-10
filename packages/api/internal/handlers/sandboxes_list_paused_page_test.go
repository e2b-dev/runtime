package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/queries"
	sharedutils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// compareKeyset orders two snapshots the way the cursor query for the order does:
// descending is started_at DESC, sandbox_id ASC, and ascending is its exact reverse.
func compareKeyset(order utils.SortDirection, aTime time.Time, aID string, bTime time.Time, bID string) int {
	c := aTime.Compare(bTime)
	if c == 0 {
		c = strings.Compare(bID, aID)
	}

	if order == utils.SortAsc {
		return c
	}

	return -c
}

// fakeSnapshotQuery serves the snapshot cursor query from memory with the SQL's
// ordering and strictly-after keyset predicate, and records every request. beforeFetch,
// when set, runs before each request is served, numbered from 1, so a test can change
// the rows between two statements the way a concurrent pause does.
type fakeSnapshotQuery struct {
	order       utils.SortDirection
	rows        []queries.GetSnapshotsWithCursorRow
	calls       []queries.GetSnapshotsWithCursorParams
	beforeFetch func(call int, params queries.GetSnapshotsWithCursorParams)
}

func newFakeSnapshotQuery(order utils.SortDirection, rows []queries.GetSnapshotsWithCursorRow) *fakeSnapshotQuery {
	f := &fakeSnapshotQuery{order: order, rows: slices.Clone(rows)}
	f.sort()

	return f
}

func (f *fakeSnapshotQuery) sort() {
	slices.SortFunc(f.rows, func(a, b queries.GetSnapshotsWithCursorRow) int {
		return compareKeyset(f.order, a.Snapshot.SandboxStartedAt.Time, a.Snapshot.SandboxID, b.Snapshot.SandboxStartedAt.Time, b.Snapshot.SandboxID)
	})
}

// move rewrites one row's start time, as a pause does, and keeps the rows in walk order.
func (f *fakeSnapshotQuery) move(id string, startedAt time.Time) {
	for i := range f.rows {
		if f.rows[i].Snapshot.SandboxID == id {
			f.rows[i].Snapshot.SandboxStartedAt.Time = startedAt
		}
	}

	f.sort()
}

// timeBetween returns a start time that falls strictly between two rows in walk order.
// Their start times must differ.
func timeBetween(a, b queries.GetSnapshotsWithCursorRow) time.Time {
	from, to := a.Snapshot.SandboxStartedAt.Time, b.Snapshot.SandboxStartedAt.Time

	return from.Add(to.Sub(from) / 2)
}

func (f *fakeSnapshotQuery) fetch(_ context.Context, params queries.GetSnapshotsWithCursorParams) ([]queries.GetSnapshotsWithCursorRow, error) {
	f.calls = append(f.calls, params)
	if f.beforeFetch != nil {
		f.beforeFetch(len(f.calls), params)
	}

	var out []queries.GetSnapshotsWithCursorRow
	for _, row := range f.rows {
		if len(out) >= int(params.Limit) {
			break
		}

		if compareKeyset(f.order, row.Snapshot.SandboxStartedAt.Time, row.Snapshot.SandboxID, params.CursorTime.Time, params.CursorID) > 0 {
			out = append(out, row)
		}
	}

	return out, nil
}

// firstPage returns the request for the first page in the order, mirroring the cursor
// the pagination layer starts from when no token is given.
func firstPage(order utils.SortDirection, queryLimit int32) queries.GetSnapshotsWithCursorParams {
	cursorTime := time.Now().Add(time.Hour)
	if order == utils.SortAsc {
		cursorTime = time.Time{}
	}

	return queries.GetSnapshotsWithCursorParams{
		Limit:      queryLimit,
		CursorTime: pgtype.Timestamptz{Time: cursorTime, Valid: true},
		CursorID:   utils.MaxSandboxID,
	}
}

// snapshotRows builds n rows whose start times come in pairs, so the sandbox ID
// tie-breaker decides the order inside each pair.
func snapshotRows(n int) []queries.GetSnapshotsWithCursorRow {
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	rows := make([]queries.GetSnapshotsWithCursorRow, 0, n)
	for i := range n {
		rows = append(rows, queries.GetSnapshotsWithCursorRow{Snapshot: queries.Snapshot{
			SandboxID:        fmt.Sprintf("sbx-%03d", i),
			SandboxStartedAt: pgtype.Timestamptz{Time: base.Add(time.Duration(i/2) * time.Second), Valid: true},
		}})
	}

	return rows
}

func rowIDs(rows []queries.GetSnapshotsWithCursorRow) []string {
	return sharedutils.Map(rows, func(row queries.GetSnapshotsWithCursorRow) string {
		return row.Snapshot.SandboxID
	})
}

func excludeSetOf(ids ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}

	return set
}

// absentIDs names live sandboxes that have no snapshot row in the walked range.
func absentIDs(n int) []string {
	ids := make([]string, 0, n)
	for i := range n {
		ids = append(ids, fmt.Sprintf("live-only-%03d", i))
	}

	return ids
}

var sortDirections = []struct {
	name  string
	order utils.SortDirection
}{
	{"desc", utils.SortDesc},
	{"asc", utils.SortAsc},
}

func TestFetchPausedPage_NoExclusionsAsksForThePageOnly(t *testing.T) {
	t.Parallel()

	for _, dir := range sortDirections {
		t.Run(dir.name, func(t *testing.T) {
			t.Parallel()

			fake := newFakeSnapshotQuery(dir.order, snapshotRows(10))
			start := firstPage(dir.order, 4)

			page, walkedTo, err := fetchPausedPage(t.Context(), fake.fetch, start, nil, true, 0)
			require.NoError(t, err)
			assert.Nil(t, walkedTo)

			assert.Equal(t, rowIDs(fake.rows[:4]), rowIDs(page))
			require.Len(t, fake.calls, 1)
			assert.Equal(t, start, fake.calls[0])
		})
	}
}

// TestFetchPausedPage_LiveSandboxesOutsideThePageCostNothing is the reason for
// chunking: live sandboxes the walk never meets no longer widen the request.
func TestFetchPausedPage_LiveSandboxesOutsideThePageCostNothing(t *testing.T) {
	t.Parallel()

	fake := newFakeSnapshotQuery(utils.SortDesc, snapshotRows(10))

	page, walkedTo, err := fetchPausedPage(t.Context(), fake.fetch, firstPage(utils.SortDesc, 4), excludeSetOf(absentIDs(500)...), true, 0)
	require.NoError(t, err)
	assert.Nil(t, walkedTo)

	assert.Equal(t, rowIDs(fake.rows[:4]), rowIDs(page))
	require.Len(t, fake.calls, 1)
	assert.Equal(t, int32(4), fake.calls[0].Limit)
}

// TestFetchPausedPage_ContinuesFromTheLastReturnedRow excludes the last row of the
// first chunk, so continuing from the last kept row instead would read it again.
func TestFetchPausedPage_ContinuesFromTheLastReturnedRow(t *testing.T) {
	t.Parallel()

	for _, dir := range sortDirections {
		t.Run(dir.name, func(t *testing.T) {
			t.Parallel()

			fake := newFakeSnapshotQuery(dir.order, snapshotRows(10))
			sorted := fake.rows
			start := firstPage(dir.order, 4)

			page, walkedTo, err := fetchPausedPage(t.Context(), fake.fetch, start,
				excludeSetOf(sorted[1].Snapshot.SandboxID, sorted[3].Snapshot.SandboxID), true, 0)
			require.NoError(t, err)
			assert.Nil(t, walkedTo)

			assert.Equal(t, rowIDs([]queries.GetSnapshotsWithCursorRow{sorted[0], sorted[2], sorted[4], sorted[5]}), rowIDs(page))

			require.Len(t, fake.calls, 2)
			assert.Equal(t, start, fake.calls[0])

			second := fake.calls[1]
			assert.Equal(t, int32(2), second.Limit, "every excluded ID was met, so the rows still missing finish the page")
			assert.Equal(t, sorted[3].Snapshot.SandboxID, second.CursorID)
			assert.Equal(t, sorted[3].Snapshot.SandboxStartedAt, second.CursorTime)
		})
	}
}

// TestFetchPausedPage_AsksForTheBoundAfterTheEstimatedRequests starts on rows that are
// almost all kept and then meets a long run of live sandboxes, so the estimates stay
// small and fall short. After pausedPageEstimatedFetches of them the walk asks once for
// the missing rows plus every excluded ID it has not met yet, which finishes the page.
func TestFetchPausedPage_AsksForTheBoundAfterTheEstimatedRequests(t *testing.T) {
	t.Parallel()

	const need = 10
	const runEnd = 200

	for _, dir := range sortDirections {
		t.Run(dir.name, func(t *testing.T) {
			t.Parallel()

			fake := newFakeSnapshotQuery(dir.order, snapshotRows(runEnd+100))
			sorted := slices.Clone(fake.rows)

			excluded := append(rowIDs(sorted[need-1:runEnd]), absentIDs(2)...)

			page, walkedTo, err := fetchPausedPage(t.Context(), fake.fetch, firstPage(dir.order, need), excludeSetOf(excluded...), true, 0)
			require.NoError(t, err)
			assert.Nil(t, walkedTo)

			wantRows := append(slices.Clone(sorted[:need-1]), sorted[runEnd])
			assert.Equal(t, rowIDs(wantRows), rowIDs(page))

			require.Len(t, fake.calls, pausedPageEstimatedFetches+1)

			readBefore := 0
			for _, call := range fake.calls[:pausedPageEstimatedFetches] {
				readBefore += int(call.Limit)
			}
			require.Less(t, readBefore, runEnd, "the estimated requests stop inside the run")

			final := fake.calls[pausedPageEstimatedFetches]
			metBefore := readBefore - (need - 1)
			assert.Equal(t, int32(1+len(excluded)-metBefore), final.Limit)

			lastRead := sorted[readBefore-1].Snapshot
			assert.Equal(t, lastRead.SandboxID, final.CursorID)
			assert.Equal(t, lastRead.SandboxStartedAt, final.CursorTime)
		})
	}
}

// TestFetchPausedPage_DenseExclusionsReadCloseToThePage interleaves live sandboxes with
// every other row. Asking each time for only the missing rows crosses them in shrinking
// steps and ends in the full bound, as many rows as one over-fetched request; sizing
// each request from the share kept so far reads about the rows the page needs.
func TestFetchPausedPage_DenseExclusionsReadCloseToThePage(t *testing.T) {
	t.Parallel()

	const need = 101
	const live = 400

	fake := newFakeSnapshotQuery(utils.SortDesc, snapshotRows(2*live+need))
	sorted := slices.Clone(fake.rows)

	var excluded []string
	var want []string
	for i, row := range sorted {
		if i%2 == 0 && len(excluded) < live {
			excluded = append(excluded, row.Snapshot.SandboxID)
		} else if len(want) < need {
			want = append(want, row.Snapshot.SandboxID)
		}
	}

	page, walkedTo, err := fetchPausedPage(t.Context(), fake.fetch, firstPage(utils.SortDesc, need), excludeSetOf(excluded...), true, 0)
	require.NoError(t, err)
	assert.Nil(t, walkedTo)
	assert.Equal(t, want, rowIDs(page))

	requested := 0
	for _, call := range fake.calls {
		requested += int(call.Limit)
	}
	assert.Less(t, requested, (need+live)/2, "well under the one over-fetched request of %d rows", need+live)
	assert.LessOrEqual(t, len(fake.calls), 3)
}

// TestFetchPausedPage_ExcludedRowMetAgainStillFillsThePage moves an excluded row the
// first request met to inside the second request's range, as a pause between the two
// statements does in ascending order, or a replica further behind does in either. The
// second request asks for the full bound, meets that row twice and comes back full but
// one row short, so the walk must continue instead of returning a short page.
func TestFetchPausedPage_ExcludedRowMetAgainStillFillsThePage(t *testing.T) {
	t.Parallel()

	const need = 3
	const run = 12

	for _, dir := range sortDirections {
		t.Run(dir.name, func(t *testing.T) {
			t.Parallel()

			fake := newFakeSnapshotQuery(dir.order, snapshotRows(30))
			sorted := slices.Clone(fake.rows)
			excluded := rowIDs(sorted[:run])

			fake.beforeFetch = func(call int, _ queries.GetSnapshotsWithCursorParams) {
				if call == 2 {
					fake.move(sorted[0].Snapshot.SandboxID, timeBetween(sorted[run-1], sorted[run]))
				}
			}

			page, walkedTo, err := fetchPausedPage(t.Context(), fake.fetch, firstPage(dir.order, need), excludeSetOf(excluded...), true, 0)
			require.NoError(t, err)
			assert.Nil(t, walkedTo)
			assert.Equal(t, rowIDs(sorted[run:run+need]), rowIDs(page))

			require.Len(t, fake.calls, 3)
			assert.Equal(t, int32(need+(run-need)), fake.calls[1].Limit, "the second request asks for the missing rows plus the excluded IDs not met yet")
		})
	}
}

// TestFetchPausedPage_KeptRowMetAgainIsListedOnce moves a row the first request kept to
// inside the second request's range, as a resume and pause between the two statements
// does in ascending order. The page lists it once and still fills.
func TestFetchPausedPage_KeptRowMetAgainIsListedOnce(t *testing.T) {
	t.Parallel()

	const need = 4

	for _, dir := range sortDirections {
		t.Run(dir.name, func(t *testing.T) {
			t.Parallel()

			fake := newFakeSnapshotQuery(dir.order, snapshotRows(20))
			sorted := slices.Clone(fake.rows)

			fake.beforeFetch = func(call int, _ queries.GetSnapshotsWithCursorParams) {
				if call == 2 {
					fake.move(sorted[0].Snapshot.SandboxID, timeBetween(sorted[need-1], sorted[need]))
				}
			}

			page, walkedTo, err := fetchPausedPage(t.Context(), fake.fetch, firstPage(dir.order, need), excludeSetOf(sorted[1].Snapshot.SandboxID), true, 0)
			require.NoError(t, err)
			assert.Nil(t, walkedTo)

			wantRows := []queries.GetSnapshotsWithCursorRow{sorted[0], sorted[2], sorted[3], sorted[4]}
			assert.Equal(t, rowIDs(wantRows), rowIDs(page))
			assert.Len(t, fake.calls, 3)
		})
	}
}

// moveJustPastTheCursor moves the row with the given ID to a millisecond past the cursor
// in walk order before every request from the second through the until-th, so each of
// those requests meets it again first.
func moveJustPastTheCursor(fake *fakeSnapshotQuery, id string, until int) func(int, queries.GetSnapshotsWithCursorParams) {
	step := -time.Millisecond
	if fake.order == utils.SortAsc {
		step = time.Millisecond
	}

	return func(call int, params queries.GetSnapshotsWithCursorParams) {
		if call > 1 && call <= until {
			fake.move(id, params.CursorTime.Time.Add(step))
		}
	}
}

// TestFetchPausedPage_ReturnsWhereItStoppedWhenRowsKeepMoving keeps an excluded row
// moving to just past the cursor, so every request after the first meets it again and
// comes back full without completing the page. When the requests run out, the fetch
// returns the short page and the last row it read instead of failing, and continuing
// after that row lists every remaining row once.
func TestFetchPausedPage_ReturnsWhereItStoppedWhenRowsKeepMoving(t *testing.T) {
	t.Parallel()

	for _, dir := range sortDirections {
		t.Run(dir.name, func(t *testing.T) {
			t.Parallel()

			fake := newFakeSnapshotQuery(dir.order, snapshotRows(20))
			moving := fake.rows[0].Snapshot.SandboxID
			fake.beforeFetch = moveJustPastTheCursor(fake, moving, pausedPageMaxFetches)

			page, walkedTo, err := fetchPausedPage(t.Context(), fake.fetch, firstPage(dir.order, 2), excludeSetOf(moving), true, 0)
			require.NoError(t, err)
			require.NotNil(t, walkedTo)
			assert.Less(t, len(page), 2)
			assert.Len(t, fake.calls, pausedPageMaxFetches)

			fake.beforeFetch = nil
			next := firstPage(dir.order, 100)
			next.CursorTime = pgtype.Timestamptz{Time: walkedTo.Time, Valid: true}
			next.CursorID = walkedTo.ID
			rest, more, err := fetchPausedPage(t.Context(), fake.fetch, next, excludeSetOf(moving), true, 0)
			require.NoError(t, err)
			assert.Nil(t, more)

			var want []string
			for _, row := range fake.rows {
				if row.Snapshot.SandboxID != moving {
					want = append(want, row.Snapshot.SandboxID)
				}
			}
			assert.Equal(t, want, append(rowIDs(page), rowIDs(rest)...))
		})
	}
}

// TestFetchPausedPage_LastRequestRunningOutEndsThePage moves an excluded row past the
// cursor before every request but the last one allowed, and no other row is left to
// read. That request comes back short, so the rows have run out and the short page is
// the end of the list, not an error.
func TestFetchPausedPage_LastRequestRunningOutEndsThePage(t *testing.T) {
	t.Parallel()

	for _, dir := range sortDirections {
		t.Run(dir.name, func(t *testing.T) {
			t.Parallel()

			fake := newFakeSnapshotQuery(dir.order, snapshotRows(2))
			sorted := slices.Clone(fake.rows)
			moving := sorted[0].Snapshot.SandboxID
			fake.beforeFetch = moveJustPastTheCursor(fake, moving, pausedPageMaxFetches-1)

			page, walkedTo, err := fetchPausedPage(t.Context(), fake.fetch, firstPage(dir.order, 2), excludeSetOf(moving), true, 0)
			require.NoError(t, err)
			assert.Nil(t, walkedTo)
			assert.Equal(t, rowIDs(sorted[1:]), rowIDs(page))
			assert.Len(t, fake.calls, pausedPageMaxFetches)
		})
	}
}

func TestFetchPausedPage_StopsWhenTheQueryRunsOut(t *testing.T) {
	t.Parallel()

	t.Run("a short first chunk", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSnapshotQuery(utils.SortDesc, snapshotRows(5))
		sorted := fake.rows

		page, walkedTo, err := fetchPausedPage(t.Context(), fake.fetch, firstPage(utils.SortDesc, 10), excludeSetOf(sorted[1].Snapshot.SandboxID), true, 0)
		require.NoError(t, err)
		assert.Nil(t, walkedTo)

		assert.Equal(t, rowIDs([]queries.GetSnapshotsWithCursorRow{sorted[0], sorted[2], sorted[3], sorted[4]}), rowIDs(page))
		assert.Len(t, fake.calls, 1)
	})

	t.Run("a full chunk then an empty one", func(t *testing.T) {
		t.Parallel()

		fake := newFakeSnapshotQuery(utils.SortAsc, snapshotRows(5))
		sorted := fake.rows

		page, walkedTo, err := fetchPausedPage(t.Context(), fake.fetch, firstPage(utils.SortAsc, 5), excludeSetOf(sorted[0].Snapshot.SandboxID), true, 0)
		require.NoError(t, err)
		assert.Nil(t, walkedTo)

		assert.Equal(t, rowIDs(sorted[1:]), rowIDs(page))
		require.Len(t, fake.calls, 2)
		assert.Equal(t, int32(1), fake.calls[1].Limit)
		assert.Equal(t, sorted[4].Snapshot.SandboxID, fake.calls[1].CursorID)
	})
}

// TestFetchPausedPage_UnchunkedKeepsOneOverFetchedRequest pins the metadata-filtered
// path: one request for the page plus one row per excluded ID, whatever it returns.
func TestFetchPausedPage_UnchunkedKeepsOneOverFetchedRequest(t *testing.T) {
	t.Parallel()

	fake := newFakeSnapshotQuery(utils.SortDesc, snapshotRows(30))
	sorted := fake.rows
	excluded := append(rowIDs(sorted[:10]), absentIDs(3)...)
	start := firstPage(utils.SortDesc, 4)

	page, walkedTo, err := fetchPausedPage(t.Context(), fake.fetch, start, excludeSetOf(excluded...), false, 0)
	require.NoError(t, err)
	assert.Nil(t, walkedTo)

	assert.Equal(t, rowIDs(sorted[10:14]), rowIDs(page))
	require.Len(t, fake.calls, 1)

	want := start
	want.Limit = 4 + int32(len(excluded))
	assert.Equal(t, want, fake.calls[0])
}

// TestFetchPausedPage_MaxRequestCapsEveryRequest puts a run of live sandboxes longer
// than the page's requests can cross under the cap. No request asks for more than the
// cap, so the page ends early with where it stopped, and continuing from there lists
// every remaining row once.
func TestFetchPausedPage_MaxRequestCapsEveryRequest(t *testing.T) {
	t.Parallel()

	const need = 3
	const run = 100
	const maxRequest = 5

	for _, dir := range sortDirections {
		for _, chunked := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/chunked %t", dir.name, chunked), func(t *testing.T) {
				t.Parallel()

				fake := newFakeSnapshotQuery(dir.order, snapshotRows(run+need))
				exclude := excludeSetOf(rowIDs(fake.rows[:run])...)
				params := firstPage(dir.order, need)

				var listed []string
				for pages := 1; ; pages++ {
					require.LessOrEqual(t, pages, run, "pagination did not end")
					fake.calls = nil

					page, walkedTo, err := fetchPausedPage(t.Context(), fake.fetch, params, exclude, chunked, maxRequest)
					require.NoError(t, err)
					for _, call := range fake.calls {
						require.LessOrEqual(t, call.Limit, int32(maxRequest))
					}
					listed = append(listed, rowIDs(page)...)

					if walkedTo == nil {
						assert.Greater(t, pages, 1, "the cap ends the first page early")

						break
					}

					params.CursorTime = pgtype.Timestamptz{Time: walkedTo.Time, Valid: true}
					params.CursorID = walkedTo.ID
				}

				assert.Equal(t, rowIDs(fake.rows[run:]), listed)
			})
		}
	}
}

func TestFetchPausedPage_ReturnsTheFetchError(t *testing.T) {
	t.Parallel()

	fake := newFakeSnapshotQuery(utils.SortDesc, snapshotRows(10))
	errRead := errors.New("read failed")
	calls := 0
	fetch := func(ctx context.Context, params queries.GetSnapshotsWithCursorParams) ([]queries.GetSnapshotsWithCursorRow, error) {
		calls++
		if calls == 2 {
			return nil, errRead
		}

		return fake.fetch(ctx, params)
	}

	page, walkedTo, err := fetchPausedPage(t.Context(), fetch, firstPage(utils.SortDesc, 4), excludeSetOf(fake.rows[0].Snapshot.SandboxID), true, 0)
	require.ErrorIs(t, err, errRead)
	assert.Nil(t, walkedTo)
	assert.Nil(t, page)
}

// TestFetchPausedPage_PagesEveryRowOnce pages through a fixture the way the pagination
// layer does -- a page of queryLimit rows means another page exists, and the next one
// starts after the last row shown -- and checks every kept row comes back exactly once,
// in order, for each exclusion pattern and page size.
func TestFetchPausedPage_PagesEveryRowOnce(t *testing.T) {
	t.Parallel()

	const total = 40
	all := rowIDs(snapshotRows(total))

	var everyThird []string
	for i := 0; i < total; i += 3 {
		everyThird = append(everyThird, all[i])
	}

	patterns := map[string][]string{
		"none":          nil,
		"every third":   everyThird,
		"long run":      append(slices.Clone(all[8:32]), absentIDs(5)...),
		"all but one":   append(slices.Clone(all[:17]), all[18:]...),
		"all":           all,
		"absent only":   absentIDs(50),
		"leading run":   slices.Clone(all[:25]),
		"trailing run":  slices.Clone(all[15:]),
		"pairs at edge": {all[0], all[1], all[38], all[39]},
	}

	for _, dir := range sortDirections {
		for name, excluded := range patterns {
			for _, pageSize := range []int{1, 2, 3, 5, 10, 100} {
				t.Run(fmt.Sprintf("%s/%s/page %d", dir.name, name, pageSize), func(t *testing.T) {
					t.Parallel()

					fake := newFakeSnapshotQuery(dir.order, snapshotRows(total))
					exclude := excludeSetOf(excluded...)

					var want []string
					for _, row := range fake.rows {
						if _, skip := exclude[row.Snapshot.SandboxID]; !skip {
							want = append(want, row.Snapshot.SandboxID)
						}
					}

					queryLimit := pageSize + 1
					params := firstPage(dir.order, int32(queryLimit))

					var listed []string
					for range total + 2 {
						fake.calls = nil

						page, walkedTo, err := fetchPausedPage(t.Context(), fake.fetch, params, exclude, true, 0)
						require.NoError(t, err)
						assert.Nil(t, walkedTo)

						for _, call := range fake.calls {
							require.Positive(t, call.Limit)
							require.LessOrEqual(t, int(call.Limit), queryLimit+len(exclude), "never more than one over-fetched request")
						}

						shown := page[:min(len(page), pageSize)]
						listed = append(listed, rowIDs(shown)...)

						if len(page) < queryLimit {
							assert.Equal(t, want, listed)

							return
						}

						last := shown[len(shown)-1].Snapshot
						params.CursorTime = last.SandboxStartedAt
						params.CursorID = last.SandboxID
					}

					t.Fatal("pagination did not end")
				})
			}
		}
	}
}

func TestPausedPageFetchesInChunks(t *testing.T) {
	t.Parallel()

	assert.True(t, pausedPageFetchesInChunks(nil))
	assert.True(t, pausedPageFetchesInChunks(&map[string]string{}), "an empty filter matches every row like no filter")
	assert.False(t, pausedPageFetchesInChunks(&map[string]string{"key": "value"}))
}

// limitRecorder records the LIMIT of every statement run through it. All four snapshot
// cursor queries bind the limit as their first argument.
type limitRecorder struct {
	queries.DBTX

	limits []any
}

func (r *limitRecorder) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	var limit any
	if len(args) > 0 {
		limit = args[0]
	}
	r.limits = append(r.limits, limit)

	return r.DBTX.Query(ctx, sql, args...)
}

// take returns the limits recorded since the last call.
func (r *limitRecorder) take() []any {
	limits := r.limits
	r.limits = nil

	return limits
}

// TestGetPausedSandboxes_PagesEverySandboxOnce pages a database fixture with many live
// sandboxes through the real query variants and the pagination layer, as the paused-only
// list does, and checks every paused sandbox is listed exactly once and no live one is.
// It also checks which requests each page makes: one over-fetched request with a
// metadata filter, and a first request for the page alone without one.
func TestGetPausedSandboxes_PagesEverySandboxOnce(t *testing.T) {
	t.Parallel()

	const total = 36

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, db)
	templateA := testutils.CreateTestTemplate(t, db, teamID)
	templateB := testutils.CreateTestTemplate(t, db, teamID)
	base := time.Now().Add(-time.Hour).Truncate(time.Second)

	type fixture struct {
		id        string
		startedAt time.Time
		template  string
		group     string
	}

	fixtures := make([]fixture, 0, total)
	for i := range total {
		f := fixture{
			id: fmt.Sprintf("sbx-%02d", i),
			// Start times come in threes so the sandbox ID tie-breaker is exercised,
			// including across request and page boundaries.
			startedAt: base.Add(time.Duration(i/3) * time.Second),
			template:  templateA,
			group:     "even",
		}
		if i%4 == 3 {
			f.template = templateB
		}
		if i%2 == 1 {
			f.group = "odd"
		}
		fixtures = append(fixtures, f)

		testutils.UpsertTestSnapshot(t, ctx, db, fmt.Sprintf("snap-%02d", i), f.id, teamID, f.template)
		require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
			"UPDATE public.snapshots SET sandbox_started_at = $1, metadata = jsonb_build_object('group', $2::text) WHERE sandbox_id = $3",
			f.startedAt, f.group, f.id))
	}

	// A run of live sandboxes longer than the first requests cross at small page
	// sizes, two scattered ones, and one with no snapshot row.
	live := []string{"sbx-02", "sbx-31", "live-only"}
	for i := 9; i <= 26; i++ {
		live = append(live, fmt.Sprintf("sbx-%02d", i))
	}
	liveSet := excludeSetOf(live...)

	sem, err := sharedutils.NewAdjustableSemaphore(1)
	require.NoError(t, err)

	templateFilters := map[string]*string{"any template": nil, "one template": &templateA}
	metadataFilters := map[string]*map[string]string{"no metadata": nil, "metadata": {"group": "even"}}

	for _, dir := range sortDirections {
		for templateName, templateID := range templateFilters {
			for metadataName, metadata := range metadataFilters {
				for _, pageSize := range []int32{1, 2, 3, 5, 100} {
					t.Run(fmt.Sprintf("%s/%s/%s/page %d", dir.name, templateName, metadataName, pageSize), func(t *testing.T) {
						t.Parallel()

						var expected []fixture
						for _, f := range fixtures {
							if _, isLive := liveSet[f.id]; isLive {
								continue
							}
							if templateID != nil && f.template != *templateID {
								continue
							}
							if metadata != nil && f.group != (*metadata)["group"] {
								continue
							}
							expected = append(expected, f)
						}
						slices.SortFunc(expected, func(a, b fixture) int {
							return compareKeyset(dir.order, a.startedAt, a.id, b.startedAt, b.id)
						})
						want := sharedutils.Map(expected, func(f fixture) string { return f.id })

						recorder := &limitRecorder{DBTX: db.SqlcClient.Pool()}
						store := &APIStore{sqlcReadDB: &sqlcdb.Client{Queries: queries.New(recorder)}, sandboxListSem: sem}

						listed, pageLimits := pagePausedSandboxes(t, store, recorder, teamID, live, metadata, dir.order, templateID, pageSize)
						assert.Equal(t, want, listed)

						queryLimit := pageSize + 1
						for i, limits := range pageLimits {
							if metadata != nil {
								assert.Equal(t, []any{queryLimit + int32(len(liveSet))}, limits, "page %d makes one over-fetched request", i)

								continue
							}

							if assert.NotEmpty(t, limits, "page %d", i) {
								assert.Equal(t, queryLimit, limits[0], "page %d first asks for the page alone", i)
							}
						}
					})
				}
			}
		}
	}
}

// pagePausedSandboxes follows X-Next-Token through the paused list the way a client of
// a paused-only request would, returning every sandbox ID in the order listed and the
// limits of the requests each page made.
func pagePausedSandboxes(
	t *testing.T,
	store *APIStore,
	recorder *limitRecorder,
	teamID uuid.UUID,
	live []string,
	metadata *map[string]string,
	order utils.SortDirection,
	templateID *string,
	pageSize int32,
) ([]string, [][]any) {
	t.Helper()

	listed := make([]string, 0)
	var pageLimits [][]any
	var token *string
	for range 100 {
		pagination, err := utils.NewPagination[utils.PaginatedSandbox](
			utils.PaginationParams{Limit: &pageSize, NextToken: token},
			utils.PaginationConfig{
				DefaultLimit: sandboxesDefaultLimit,
				MaxLimit:     sandboxesMaxLimit,
				DefaultID:    utils.MaxSandboxID,
				Order:        order,
			},
		)
		require.NoError(t, err)

		page, walkedTo, err := store.getPausedSandboxes(t.Context(), teamID, live, metadata, pagination.QueryLimit(),
			pagination.CursorTime(), pagination.CursorID(), order, time.Time{}, templateID)
		require.NoError(t, err)
		assert.Nil(t, walkedTo)
		pageLimits = append(pageLimits, recorder.take())

		response := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(response)
		page = pageSandboxes(ginCtx, pagination, page, order, walkedTo)

		for _, sbx := range page {
			listed = append(listed, sbx.SandboxID)
		}

		next := response.Header().Get("X-Next-Token")
		if next == "" {
			return listed, pageLimits
		}
		token = &next
	}

	t.Fatal("pagination did not end")

	return nil, nil
}

// TestPageSandboxes_ContinuesAfterAnEarlyStop merges a short paused page with live
// sandboxes on both sides of where the paused walk stopped. The page lists only what is
// not past that point, and its next token continues there, so the live sandbox past it
// is listed on the next page and nothing is listed twice.
func TestPageSandboxes_ContinuesAfterAnEarlyStop(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, dir := range sortDirections {
		t.Run(dir.name, func(t *testing.T) {
			t.Parallel()

			step := -time.Second
			if dir.order == utils.SortAsc {
				step = time.Second
			}
			at := func(n int) time.Time { return base.Add(time.Duration(n) * step) }
			sandbox := func(id string, n int) utils.PaginatedSandbox {
				return utils.PaginatedSandbox{
					ListedSandbox:       api.ListedSandbox{SandboxID: id},
					PaginationTimestamp: at(n),
				}
			}
			walkedTo := &utils.Cursor{Time: at(3), ID: "walked"}
			merged := []utils.PaginatedSandbox{sandbox("live-past", 5), sandbox("paused", 1), sandbox("live-before", 2)}

			limit := int32(10)
			pagination, err := utils.NewPagination[utils.PaginatedSandbox](
				utils.PaginationParams{Limit: &limit},
				utils.PaginationConfig{DefaultLimit: sandboxesDefaultLimit, MaxLimit: sandboxesMaxLimit, DefaultID: utils.MaxSandboxID, Order: dir.order},
			)
			require.NoError(t, err)

			response := httptest.NewRecorder()
			ginCtx, _ := gin.CreateTestContext(response)
			page := pageSandboxes(ginCtx, pagination, merged, dir.order, walkedTo)

			assert.Equal(t, []string{"paused", "live-before"}, sharedutils.Map(page, func(s utils.PaginatedSandbox) string { return s.SandboxID }))

			cursorTime, cursorID, _, err := utils.ParseCursor(response.Header().Get("X-Next-Token"))
			require.NoError(t, err)
			assert.True(t, walkedTo.Time.Equal(cursorTime))
			assert.Equal(t, walkedTo.ID, cursorID)

			next := utils.FilterBasedOnCursor(merged, cursorTime, cursorID, dir.order)
			assert.Equal(t, []string{"live-past"}, sharedutils.Map(next, func(s utils.PaginatedSandbox) string { return s.SandboxID }))
		})
	}
}
