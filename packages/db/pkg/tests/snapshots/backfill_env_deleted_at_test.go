package snapshots

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

// backfill runs the operator script the way psql does: settings first, then
// the DO block on its own, outside a transaction. It returns the notices.
func backfill(t *testing.T, db *testutils.Database, settings map[string]string) ([]string, error) {
	t.Helper()
	ctx := t.Context()

	script, err := os.ReadFile(filepath.Join("..", "..", "..", "scripts", "backfill_snapshots_env_deleted_at.sql"))
	require.NoError(t, err)

	config, err := pgx.ParseConfig(db.ConnStr())
	require.NoError(t, err)
	var notices []string
	config.OnNotice = func(_ *pgconn.PgConn, notice *pgconn.Notice) {
		notices = append(notices, notice.Message)
	}
	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(ctx) })

	for name, value := range settings {
		_, err := conn.Exec(ctx, "SELECT set_config($1, $2, false)", name, value)
		require.NoError(t, err)
	}
	_, err = conn.PgConn().Exec(ctx, string(script)).ReadAll()

	return notices, err
}

// runBackfill runs the script to completion and returns its last notice.
func runBackfill(t *testing.T, db *testutils.Database, settings map[string]string) string {
	t.Helper()

	notices, err := backfill(t, db, settings)
	require.NoError(t, err)
	require.NotEmpty(t, notices)

	return notices[len(notices)-1]
}

// spreadOverPages leaves room for only a few snapshot rows per heap page, so
// a handful of sandboxes spans several pages.
func spreadOverPages(t *testing.T, db *testutils.Database) {
	t.Helper()

	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), "ALTER TABLE public.snapshots SET (fillfactor = 10)"))
}

// heapPages returns the heap page that holds each sandbox's snapshot row.
func heapPages(t *testing.T, db *testutils.Database, sandboxIDs []string) []int64 {
	t.Helper()

	pages := make([]int64, 0, len(sandboxIDs))
	for _, sandboxID := range sandboxIDs {
		err := db.SqlcClient.TestsRawSQLQuery(t.Context(),
			"SELECT (ctid::text::point)[0]::bigint FROM public.snapshots WHERE sandbox_id = $1",
			func(rows pgx.Rows) error {
				if !rows.Next() {
					return rows.Err()
				}
				var page int64
				if err := rows.Scan(&page); err != nil {
					return err
				}
				pages = append(pages, page)

				return nil
			}, sandboxID)
		require.NoError(t, err)
	}
	require.Len(t, pages, len(sandboxIDs))

	return pages
}

// unstampedSandboxes pauses n sandboxes of the team and deletes their envs the
// way releases before the stamping soft deletes did.
func unstampedSandboxes(t *testing.T, db *testutils.Database, teamID uuid.UUID, baseTemplateID string, n int) []string {
	t.Helper()

	sandboxIDs := make([]string, 0, n)
	for range n {
		sandboxID, envID := pausedSandbox(t, db, teamID, baseTemplateID)
		testutils.SoftDeleteEnvUnstamped(t, t.Context(), db, envID)
		sandboxIDs = append(sandboxIDs, sandboxID)
	}

	return sandboxIDs
}

func stamps(t *testing.T, db *testutils.Database, sandboxIDs []string) []string {
	t.Helper()

	got := make([]string, 0, len(sandboxIDs))
	for _, sandboxID := range sandboxIDs {
		got = append(got, testutils.SnapshotEnvStamp(t, t.Context(), db, sandboxID))
	}

	return got
}

// rowVersions identifies each snapshot row version, so a rerun that writes
// nothing leaves it unchanged.
func rowVersions(t *testing.T, db *testutils.Database) map[string]string {
	t.Helper()

	versions := map[string]string{}
	err := db.SqlcClient.TestsRawSQLQuery(t.Context(), "SELECT sandbox_id, xmin::text FROM public.snapshots",
		func(rows pgx.Rows) error {
			for rows.Next() {
				var sandboxID, xmin string
				if err := rows.Scan(&sandboxID, &xmin); err != nil {
					return err
				}
				versions[sandboxID] = xmin
			}

			return rows.Err()
		})
	require.NoError(t, err)

	return versions
}

func TestBackfillSnapshotsEnvDeletedAt(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	spreadOverPages(t, db)

	first := testutils.CreateTestTeam(t, db)
	firstBase := testutils.CreateTestTemplate(t, db, first)
	firstDeleted := unstampedSandboxes(t, db, first, firstBase, 3)
	firstLive, _ := pausedSandbox(t, db, first, firstBase)

	second := testutils.CreateTestTeam(t, db)
	secondBase := testutils.CreateTestTemplate(t, db, second)
	secondDeleted := unstampedSandboxes(t, db, second, secondBase, 2)
	secondLive, _ := pausedSandbox(t, db, second, secondBase)

	pages := heapPages(t, db, slices.Concat(firstDeleted, secondDeleted, []string{firstLive, secondLive}))
	require.Positive(t, slices.Max(pages), "the rows must span more than one page")
	notices, err := backfill(t, db, map[string]string{"backfill.batch_pages": "1", "backfill.pause": "0"})
	require.NoError(t, err)
	assert.Len(t, notices, int(slices.Max(pages))+2, "one notice per page, then the summary")
	assert.Equal(t, "done: stamped 5 snapshots", notices[len(notices)-1])
	assert.Equal(t, []string{"stamped", "stamped", "stamped"}, stamps(t, db, firstDeleted))
	assert.Equal(t, []string{"stamped", "stamped"}, stamps(t, db, secondDeleted))
	assert.Equal(t, []string{"live", "live"}, stamps(t, db, []string{firstLive, secondLive}))

	before := rowVersions(t, db)
	assert.Equal(t, "done: stamped 0 snapshots", runBackfill(t, db, nil))
	assert.Equal(t, before, rowVersions(t, db))
}

func TestBackfillSnapshotsEnvDeletedAt_ResumesFromAPage(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	spreadOverPages(t, db)

	teamID := testutils.CreateTestTeam(t, db)
	sandboxIDs := unstampedSandboxes(t, db, teamID, testutils.CreateTestTemplate(t, db, teamID), 12)
	pages := heapPages(t, db, sandboxIDs)
	resumeAt := slices.Max(pages)
	require.Positive(t, resumeAt, "the rows must span more than one page")

	want := make([]string, 0, len(sandboxIDs))
	stamped := 0
	for _, page := range pages {
		if page >= resumeAt {
			want = append(want, "stamped")
			stamped++
		} else {
			want = append(want, "unstamped")
		}
	}

	last := runBackfill(t, db, map[string]string{"backfill.start_page": strconv.FormatInt(resumeAt, 10), "backfill.pause": "0"})
	assert.Equal(t, fmt.Sprintf("done: stamped %d snapshots", stamped), last)
	assert.Equal(t, want, stamps(t, db, sandboxIDs))
}

// statement_timeout counts from the start of the DO block, through every
// batch's COMMIT, so the script refuses to start under one.
func TestBackfillSnapshotsEnvDeletedAt_RefusesAStatementTimeout(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)

	teamID := testutils.CreateTestTeam(t, db)
	sandboxIDs := unstampedSandboxes(t, db, teamID, testutils.CreateTestTemplate(t, db, teamID), 1)

	_, err := backfill(t, db, map[string]string{"statement_timeout": "1min"})
	require.ErrorContains(t, err, "statement_timeout is 1min")
	assert.Equal(t, []string{"unstamped"}, stamps(t, db, sandboxIDs))
}
