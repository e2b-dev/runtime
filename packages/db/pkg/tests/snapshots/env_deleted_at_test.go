package snapshots

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
)

// pausedSandbox upserts a snapshot, which creates the sandbox's own env, and
// returns the sandbox and env IDs.
func pausedSandbox(t *testing.T, db *testutils.Database, teamID uuid.UUID, baseTemplateID string) (string, string) {
	t.Helper()

	sandboxID := "sandbox-" + uuid.NewString()
	envID := "snapshot-env-" + uuid.NewString()
	testutils.UpsertTestSnapshot(t, t.Context(), db, envID, sandboxID, teamID, baseTemplateID)

	return sandboxID, envID
}

func TestSoftDeleteTemplate_StampsTheEnvsSnapshots(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	killed, killedEnv := pausedSandbox(t, db, teamID, baseTemplateID)
	kept, _ := pausedSandbox(t, db, teamID, baseTemplateID)

	// Killing a paused sandbox soft-deletes its snapshot env.
	deletedID, err := db.SqlcClient.SoftDeleteTemplate(ctx, queries.SoftDeleteTemplateParams{TemplateID: killedEnv, TeamID: teamID})
	require.NoError(t, err)
	assert.Equal(t, killedEnv, deletedID)
	assert.Equal(t, "stamped", testutils.SnapshotEnvStamp(t, ctx, db, killed))
	assert.Equal(t, "live", testutils.SnapshotEnvStamp(t, ctx, db, kept))

	// The column mirrors the snapshot's own env: deleting the template it was
	// started from leaves it live.
	_, err = db.SqlcClient.SoftDeleteTemplate(ctx, queries.SoftDeleteTemplateParams{TemplateID: baseTemplateID, TeamID: teamID})
	require.NoError(t, err)
	assert.Equal(t, "live", testutils.SnapshotEnvStamp(t, ctx, db, kept))
}

func TestSoftDeleteProjectEnvironments_StampsTheProjectsSnapshots(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	project := testutils.CreateTestTeam(t, db)
	projectBase := testutils.CreateTestTemplate(t, db, project)
	first, firstEnv := pausedSandbox(t, db, project, projectBase)
	second, secondEnv := pausedSandbox(t, db, project, projectBase)
	killedEarlier, killedEarlierEnv := pausedSandbox(t, db, project, projectBase)
	_, err := db.SqlcClient.SoftDeleteTemplate(ctx, queries.SoftDeleteTemplateParams{TemplateID: killedEarlierEnv, TeamID: project})
	require.NoError(t, err)

	other := testutils.CreateTestTeam(t, db)
	otherSandbox, _ := pausedSandbox(t, db, other, testutils.CreateTestTemplate(t, db, other))

	deletedIDs, err := db.SqlcClient.Dashboard.SoftDeleteProjectEnvironments(ctx, project)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{projectBase, firstEnv, secondEnv}, deletedIDs)

	assert.Equal(t, "stamped", testutils.SnapshotEnvStamp(t, ctx, db, first))
	assert.Equal(t, "stamped", testutils.SnapshotEnvStamp(t, ctx, db, second))
	// Deleted before, so not deleted again: it keeps its first stamp.
	assert.Equal(t, "stamped", testutils.SnapshotEnvStamp(t, ctx, db, killedEarlier))
	assert.Equal(t, "live", testutils.SnapshotEnvStamp(t, ctx, db, otherSandbox))
}

func TestSoftDeleteClusterEnvironments_StampsTheClustersSnapshots(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	createCluster := func() uuid.UUID {
		t.Helper()
		id := uuid.New()
		require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
			`INSERT INTO public.clusters (id, name, endpoint, endpoint_tls, token)
			VALUES ($1::uuid, $1::uuid::text, 'api.example.test:5008', true, 'token')`, id))

		return id
	}
	placeOn := func(envID string, clusterID uuid.UUID) {
		t.Helper()
		require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
			`UPDATE public.envs SET cluster_id = $2 WHERE id = $1`, envID, clusterID))
	}

	deletedCluster := createCluster()
	keptCluster := createCluster()
	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	onDeleted, onDeletedEnv := pausedSandbox(t, db, teamID, baseTemplateID)
	placeOn(onDeletedEnv, deletedCluster)
	onKept, onKeptEnv := pausedSandbox(t, db, teamID, baseTemplateID)
	placeOn(onKeptEnv, keptCluster)
	local, _ := pausedSandbox(t, db, teamID, baseTemplateID)

	deletedIDs, err := db.SqlcClient.Dashboard.SoftDeleteClusterEnvironments(ctx, deletedCluster)
	require.NoError(t, err)
	assert.Equal(t, []string{onDeletedEnv}, deletedIDs)

	assert.Equal(t, "stamped", testutils.SnapshotEnvStamp(t, ctx, db, onDeleted))
	assert.Equal(t, "live", testutils.SnapshotEnvStamp(t, ctx, db, onKept))
	assert.Equal(t, "live", testutils.SnapshotEnvStamp(t, ctx, db, local))
}

func TestUpsertSnapshot_EnvDeletedAt(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	upsertAgain := func(sandboxID string) {
		t.Helper()
		testutils.UpsertTestSnapshot(t, ctx, db, "unused-"+uuid.NewString(), sandboxID, teamID, baseTemplateID)
	}

	live, _ := pausedSandbox(t, db, teamID, baseTemplateID)
	assert.Equal(t, "live", testutils.SnapshotEnvStamp(t, ctx, db, live))
	upsertAgain(live)
	assert.Equal(t, "live", testutils.SnapshotEnvStamp(t, ctx, db, live))

	// The upsert never repoints a row, so a stamp from the soft delete stays.
	stamped, stampedEnv := pausedSandbox(t, db, teamID, baseTemplateID)
	_, err := db.SqlcClient.SoftDeleteTemplate(ctx, queries.SoftDeleteTemplateParams{TemplateID: stampedEnv, TeamID: teamID})
	require.NoError(t, err)
	upsertAgain(stamped)
	assert.Equal(t, "stamped", testutils.SnapshotEnvStamp(t, ctx, db, stamped))

	unstamped, unstampedEnv := pausedSandbox(t, db, teamID, baseTemplateID)
	testutils.SoftDeleteEnvUnstamped(t, ctx, db, unstampedEnv)
	require.Equal(t, "unstamped", testutils.SnapshotEnvStamp(t, ctx, db, unstamped))
	upsertAgain(unstamped)
	assert.Equal(t, "stamped", testutils.SnapshotEnvStamp(t, ctx, db, unstamped))
}

func repauseParams(teamID uuid.UUID, sandboxID, baseTemplateID string) queries.UpsertSnapshotParams {
	return queries.UpsertSnapshotParams{
		TemplateID:         "unused-" + uuid.NewString(),
		TeamID:             teamID,
		SandboxID:          sandboxID,
		BaseTemplateID:     baseTemplateID,
		StartedAt:          pgtype.Timestamptz{Time: time.Now(), Valid: true},
		Vcpu:               2,
		RamMb:              2048,
		Metadata:           types.JSONBStringMap{},
		KernelVersion:      "6.1.0",
		FirecrackerVersion: "1.4.0",
		OriginNodeID:       "test-node",
		Status:             types.BuildStatusSuccess,
	}
}

// waitForLockWait waits until the named query is blocked on a lock.
func waitForLockWait(t *testing.T, db *testutils.Database, queryName string) {
	t.Helper()

	require.Eventually(t, func() bool {
		var blocked bool
		err := db.SqlcClient.TestsRawSQLQuery(t.Context(), `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				WHERE datname = current_database()
				  AND state = 'active'
				  AND wait_event_type = 'Lock'
				  AND query LIKE '%' || $1 || '%'
			)`, func(rows pgx.Rows) error {
			if !rows.Next() {
				return rows.Err()
			}

			return rows.Scan(&blocked)
		}, "-- name: "+queryName+" ")

		return err == nil && blocked
	}, 5*time.Second, 10*time.Millisecond)
}

// A pause that waits on a soft delete reads envs from before the delete
// committed; it must keep the stamp the delete wrote rather than clear it.
func TestUpsertSnapshot_KeepsAStampCommittedWhileItWaited(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	sandboxID, envID := pausedSandbox(t, db, teamID, baseTemplateID)

	txDB, tx, err := db.SqlcClient.WithTx(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	_, err = txDB.SoftDeleteTemplate(ctx, queries.SoftDeleteTemplateParams{TemplateID: envID, TeamID: teamID})
	require.NoError(t, err)

	upserted := make(chan error, 1)
	go func() {
		_, err := db.SqlcClient.UpsertSnapshot(ctx, repauseParams(teamID, sandboxID, baseTemplateID))
		upserted <- err
	}()
	waitForLockWait(t, db, "UpsertSnapshot")
	require.NoError(t, tx.Commit(ctx))

	select {
	case err := <-upserted:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the upsert did not finish after the soft delete committed")
	}
	assert.Equal(t, "stamped", testutils.SnapshotEnvStamp(t, ctx, db, sandboxID))
}

// A soft delete that waits on a pause's row lock stamps the row once the
// pause commits; the pause holds nothing the delete has locked first.
func TestSoftDeleteTemplate_StampsARowAPauseHeld(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	sandboxID, envID := pausedSandbox(t, db, teamID, baseTemplateID)

	txDB, tx, err := db.SqlcClient.WithTx(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	_, err = txDB.UpsertSnapshot(ctx, repauseParams(teamID, sandboxID, baseTemplateID))
	require.NoError(t, err)

	deleted := make(chan error, 1)
	go func() {
		_, err := db.SqlcClient.SoftDeleteTemplate(ctx, queries.SoftDeleteTemplateParams{TemplateID: envID, TeamID: teamID})
		deleted <- err
	}()
	waitForLockWait(t, db, "SoftDeleteTemplate")
	require.NoError(t, tx.Commit(ctx))

	select {
	case err := <-deleted:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the soft delete did not finish after the pause committed")
	}
	assert.Equal(t, "stamped", testutils.SnapshotEnvStamp(t, ctx, db, sandboxID))
}
