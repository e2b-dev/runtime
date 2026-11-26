package snapshots

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/postgres/dberrors"
)

// snapshotExpiringIn pauses a sandbox with a TTL and sets its expiry to in
// from now, in the past when negative, as a run's end would. It returns the
// sandbox and its snapshot env.
func snapshotExpiringIn(t *testing.T, db *testutils.Database, teamID uuid.UUID, baseTemplateID string, in time.Duration) (string, string) {
	t.Helper()

	ttl := int64(3600)
	sandboxID := "sandbox-" + uuid.NewString()
	upsertSnapshotWithTTL(t, db, teamID, baseTemplateID, sandboxID, &ttl)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		"UPDATE public.snapshots SET expires_at = now() + make_interval(secs => $2) WHERE sandbox_id = $1",
		sandboxID, in.Seconds()))

	var envID string
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT env_id FROM public.snapshots WHERE sandbox_id = $1",
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&envID)
		}, sandboxID))

	return sandboxID, envID
}

func listExpired(t *testing.T, db *testutils.Database, after queries.ListExpiredPausedSnapshotsRow, batch int32) []queries.ListExpiredPausedSnapshotsRow {
	t.Helper()

	rows, err := db.SqlcClient.ListExpiredPausedSnapshots(t.Context(), queries.ListExpiredPausedSnapshotsParams{
		AsOf:           time.Now(),
		AfterExpiresAt: after.ExpiresAt,
		AfterSandboxID: after.SandboxID,
		BatchSize:      batch,
	})
	require.NoError(t, err)

	return rows
}

func expiredIDs(rows []queries.ListExpiredPausedSnapshotsRow) []string {
	ids := make([]string, len(rows))
	for i, row := range rows {
		ids[i] = row.SandboxID
	}

	return ids
}

var sweepStart = queries.ListExpiredPausedSnapshotsRow{ExpiresAt: time.Unix(0, 0)}

func TestListExpiredPausedSnapshots(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)

	oldest, _ := snapshotExpiringIn(t, db, teamID, baseTemplateID, -3*time.Hour)
	middle, _ := snapshotExpiringIn(t, db, teamID, baseTemplateID, -2*time.Hour)
	newest, _ := snapshotExpiringIn(t, db, teamID, baseTemplateID, -time.Hour)
	deleted, deletedEnv := snapshotExpiringIn(t, db, teamID, baseTemplateID, -time.Hour)
	_, err := db.SqlcClient.SoftDeleteTemplate(t.Context(), queries.SoftDeleteTemplateParams{TemplateID: deletedEnv, TeamID: teamID})
	require.NoError(t, err)

	notDue, _ := snapshotExpiringIn(t, db, teamID, baseTemplateID, time.Hour)
	noTTL := "sandbox-" + uuid.NewString()
	upsertSnapshotWithTTL(t, db, teamID, baseTemplateID, noTTL, nil)

	all := listExpired(t, db, sweepStart, 100)
	assert.Equal(t, []string{oldest, middle, newest}, expiredIDs(all),
		"only live snapshots whose expiry passed, oldest first; not a deleted one, one not yet due or one without a TTL")
	assert.NotContains(t, expiredIDs(all), deleted)
	assert.NotContains(t, expiredIDs(all), notDue)
	assert.NotContains(t, expiredIDs(all), noTTL)

	first := listExpired(t, db, sweepStart, 2)
	require.Equal(t, []string{oldest, middle}, expiredIDs(first))
	rest := listExpired(t, db, first[len(first)-1], 2)
	assert.Equal(t, []string{newest}, expiredIDs(rest), "the cursor continues after the page, past rows the sweep left in place")
}

func TestListExpiredPausedSnapshots_BreaksExpiryTiesBySandboxID(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)

	a, _ := snapshotExpiringIn(t, db, teamID, baseTemplateID, -time.Hour)
	b, _ := snapshotExpiringIn(t, db, teamID, baseTemplateID, -time.Hour)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		"UPDATE public.snapshots SET expires_at = (SELECT expires_at FROM public.snapshots WHERE sandbox_id = $1) WHERE sandbox_id = $2", a, b))
	first, second := min(a, b), max(a, b)

	page := listExpired(t, db, sweepStart, 1)
	require.Equal(t, []string{first}, expiredIDs(page))
	assert.Equal(t, []string{second}, expiredIDs(listExpired(t, db, page[0], 1)), "a tie at the page boundary is not skipped")
}

// explainingTx records the plan of each query it runs before running it.
type explainingTx struct {
	pgx.Tx

	plan []string
}

func (e *explainingTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := e.Tx.Query(ctx, "EXPLAIN (COSTS OFF) "+sql, args...)
	if err != nil {
		return nil, err
	}
	plan, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	e.plan = append(e.plan, plan...)

	return e.Tx.Query(ctx, sql, args...)
}

// Seq scans are off because the test table is too small for the planner to
// prefer an index on its own.
func TestListExpiredPausedSnapshots_UsesTheExpiresAtIndex(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	_, tx, err := db.SqlcClient.WithTx(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(ctx) })
	_, err = tx.Exec(ctx, "SET LOCAL enable_seqscan = off")
	require.NoError(t, err)

	explaining := &explainingTx{Tx: tx}
	_, err = queries.New(explaining).ListExpiredPausedSnapshots(ctx, queries.ListExpiredPausedSnapshotsParams{
		AsOf:           time.Now(),
		AfterExpiresAt: sweepStart.ExpiresAt,
		BatchSize:      100,
	})
	require.NoError(t, err)

	assert.Contains(t, strings.Join(explaining.plan, "\n"), "Index Scan using idx_snapshots_expires_at on snapshots")
}

func TestSoftDeleteExpiredSnapshot(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)

	t.Run("an expired snapshot is soft-deleted and stamped", func(t *testing.T) {
		t.Parallel()

		sandboxID, envID := snapshotExpiringIn(t, db, teamID, baseTemplateID, -time.Minute)
		deletedID, err := db.SqlcClient.SoftDeleteExpiredSnapshot(t.Context(), queries.SoftDeleteExpiredSnapshotParams{TeamID: teamID, SandboxID: sandboxID})
		require.NoError(t, err)
		assert.Equal(t, envID, deletedID)
		assert.Equal(t, "stamped", testutils.SnapshotEnvStamp(t, t.Context(), db, sandboxID))
	})

	t.Run("a snapshot whose clock restarted is left alone", func(t *testing.T) {
		t.Parallel()

		sandboxID, _ := snapshotExpiringIn(t, db, teamID, baseTemplateID, time.Hour)
		_, err := db.SqlcClient.SoftDeleteExpiredSnapshot(t.Context(), queries.SoftDeleteExpiredSnapshotParams{TeamID: teamID, SandboxID: sandboxID})
		require.True(t, dberrors.IsNotFoundError(err), "got %v", err)
		assert.Equal(t, "live", testutils.SnapshotEnvStamp(t, t.Context(), db, sandboxID))
	})

	t.Run("a snapshot without a TTL is left alone", func(t *testing.T) {
		t.Parallel()

		sandboxID := "sandbox-" + uuid.NewString()
		upsertSnapshotWithTTL(t, db, teamID, baseTemplateID, sandboxID, nil)
		_, err := db.SqlcClient.SoftDeleteExpiredSnapshot(t.Context(), queries.SoftDeleteExpiredSnapshotParams{TeamID: teamID, SandboxID: sandboxID})
		require.True(t, dberrors.IsNotFoundError(err), "got %v", err)
		assert.Equal(t, "live", testutils.SnapshotEnvStamp(t, t.Context(), db, sandboxID))
	})

	t.Run("another team's snapshot is left alone", func(t *testing.T) {
		t.Parallel()

		sandboxID, _ := snapshotExpiringIn(t, db, teamID, baseTemplateID, -time.Minute)
		_, err := db.SqlcClient.SoftDeleteExpiredSnapshot(t.Context(), queries.SoftDeleteExpiredSnapshotParams{TeamID: testutils.CreateTestTeam(t, db), SandboxID: sandboxID})
		require.True(t, dberrors.IsNotFoundError(err), "got %v", err)
		assert.Equal(t, "live", testutils.SnapshotEnvStamp(t, t.Context(), db, sandboxID))
	})
}
