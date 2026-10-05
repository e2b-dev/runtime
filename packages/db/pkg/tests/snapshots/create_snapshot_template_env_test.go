package snapshots

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/retry"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
)

// snapshotTemplateFixture mirrors what CreateSnapshotTemplate passes once
// UpsertSnapshot has already created the build.
func snapshotTemplateFixture(t *testing.T, client *testutils.Database) queries.CreateSnapshotTemplateEnvParams {
	t.Helper()

	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, client)
	baseTemplateID := testutils.CreateTestTemplate(t, client, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, client, baseTemplateID, "uploaded")
	originNodeID := "node-1"

	return queries.CreateSnapshotTemplateEnvParams{
		SnapshotID:   "snapshot-tmpl-" + uuid.New().String(),
		TeamID:       teamID,
		SandboxID:    "sandbox-" + uuid.New().String(),
		OriginNodeID: &originNodeID,
		BuildID:      &buildID,
		Tag:          "default",
	}
}

func countRows(t *testing.T, ctx context.Context, client *testutils.Database, query, envID string) int {
	t.Helper()

	var count int

	err := client.SqlcClient.TestsRawSQLQuery(ctx, query,
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&count)
		},
		envID,
	)
	require.NoError(t, err)

	return count
}

func countEnvs(t *testing.T, ctx context.Context, client *testutils.Database, envID string) int {
	t.Helper()

	return countRows(t, ctx, client, "SELECT count(*) FROM public.envs WHERE id = $1", envID)
}

func countSnapshotTemplates(t *testing.T, ctx context.Context, client *testutils.Database, envID string) int {
	t.Helper()

	return countRows(t, ctx, client,
		"SELECT count(*) FROM public.snapshot_templates WHERE env_id = $1", envID)
}

func countBuildAssignments(t *testing.T, ctx context.Context, client *testutils.Database, envID string) int {
	t.Helper()

	return countRows(t, ctx, client,
		"SELECT count(*) FROM public.env_build_assignments WHERE env_id = $1", envID)
}

func TestCreateSnapshotTemplateEnv_CreatesTemplate(t *testing.T) {
	t.Parallel()

	client := testutils.SetupDatabase(t)
	ctx := t.Context()

	params := snapshotTemplateFixture(t, client)

	envID, err := client.SqlcClient.CreateSnapshotTemplateEnv(ctx, params)
	require.NoError(t, err)
	assert.Equal(t, params.SnapshotID, envID)

	var source string
	err = client.SqlcClient.TestsRawSQLQuery(ctx,
		"SELECT source FROM public.envs WHERE id = $1",
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&source)
		},
		envID,
	)
	require.NoError(t, err)
	assert.Equal(t, "snapshot_template", source)
}

// inDoubtOnce lets the first statement commit for real, then reports the
// connection loss the client would have seen. 57P01 is what a terminated
// backend actually sends, and retry.IsRetriable accepts it, so the pool
// replays the statement with the id the caller already used.
type inDoubtOnce struct {
	types.DBTX

	fired atomic.Bool
}

func (f *inDoubtOnce) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	row := f.DBTX.QueryRow(ctx, sql, args...)
	if f.fired.CompareAndSwap(false, true) {
		return inDoubtRow{inner: row}
	}

	return row
}

type inDoubtRow struct{ inner pgx.Row }

func (r inDoubtRow) Scan(dest ...any) error {
	_ = r.inner.Scan(dest...)

	return &pgconn.PgError{
		Severity: "FATAL",
		Code:     pgerrcode.AdminShutdown,
		Message:  "terminating connection due to administrator command",
	}
}

// A backend terminated after its commit but before the ack — a failover, or an
// operator ending the session — leaves the pool retrying a statement that has
// already taken effect.
func TestCreateSnapshotTemplateEnv_SurvivesInDoubtCommit(t *testing.T) {
	t.Parallel()

	client := testutils.SetupDatabase(t)
	ctx := t.Context()

	raw, err := pgxpool.New(ctx, client.ConnStr())
	require.NoError(t, err)
	defer raw.Close()

	replaying := queries.New(retry.Wrap(&inDoubtOnce{DBTX: raw}, retry.DefaultConfig()))

	params := snapshotTemplateFixture(t, client)

	envID, err := replaying.CreateSnapshotTemplateEnv(ctx, params)
	require.NoError(t, err, "a replayed statement must not fail on its own key")

	assert.Equal(t, params.SnapshotID, envID)
	assert.Equal(t, 1, countEnvs(t, ctx, client, params.SnapshotID), "replay must not create a second env")
	assert.Equal(t, 1, countSnapshotTemplates(t, ctx, client, params.SnapshotID))
	assert.Equal(t, 1, countBuildAssignments(t, ctx, client, params.SnapshotID))
}

func TestCreateSnapshotTemplateEnv_ConcurrentCallsCreateOneAssignment(t *testing.T) {
	t.Parallel()

	client := testutils.SetupDatabase(t)
	ctx := t.Context()

	params := snapshotTemplateFixture(t, client)

	const calls = 2

	start := make(chan struct{})
	errors := make(chan error, calls)

	for range calls {
		go func() {
			<-start
			_, err := client.SqlcClient.CreateSnapshotTemplateEnv(ctx, params)
			errors <- err
		}()
	}

	close(start)

	for range calls {
		require.NoError(t, <-errors)
	}

	assert.Equal(t, 1, countSnapshotTemplates(t, ctx, client, params.SnapshotID))
	assert.Equal(t, 1, countBuildAssignments(t, ctx, client, params.SnapshotID))
}

// The conflict path must never adopt an env this call did not write.
func TestCreateSnapshotTemplateEnv_RefusesAnotherTeamsEnv(t *testing.T) {
	t.Parallel()

	client := testutils.SetupDatabase(t)
	ctx := t.Context()

	params := snapshotTemplateFixture(t, client)

	// A different team already owns an env with the id we are about to use.
	otherTeamID := testutils.CreateTestTeam(t, client)
	err := client.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.envs (id, public, team_id, updated_at, source)
		 VALUES ($1, FALSE, $2, NOW(), 'snapshot_template')`,
		params.SnapshotID, otherTeamID,
	)
	require.NoError(t, err)

	_, err = client.SqlcClient.CreateSnapshotTemplateEnv(ctx, params)
	require.Error(t, err, "must not attach the build to another team's env")

	var owner uuid.UUID
	err = client.SqlcClient.TestsRawSQLQuery(ctx,
		"SELECT team_id FROM public.envs WHERE id = $1",
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&owner)
		},
		params.SnapshotID,
	)
	require.NoError(t, err)
	assert.Equal(t, otherTeamID, owner, "the other team's env must be untouched")
}

// A template deleted deliberately must not return as a side effect of a retry.
func TestCreateSnapshotTemplateEnv_RefusesSoftDeletedEnv(t *testing.T) {
	t.Parallel()

	client := testutils.SetupDatabase(t)
	ctx := t.Context()

	params := snapshotTemplateFixture(t, client)

	_, err := client.SqlcClient.CreateSnapshotTemplateEnv(ctx, params)
	require.NoError(t, err)

	err = client.SqlcClient.TestsRawSQL(ctx,
		"UPDATE public.envs SET deleted_at = NOW() WHERE id = $1", params.SnapshotID)
	require.NoError(t, err)

	_, err = client.SqlcClient.CreateSnapshotTemplateEnv(ctx, params)
	require.Error(t, err, "a soft-deleted template must not be revived by a replay")
}
