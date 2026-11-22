package tests

import (
	"database/sql"
	"testing"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbmodule "github.com/e2b-dev/infra/packages/api/internal/db"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
)

const dropClusterReferencesVersion int64 = 20261114062635

// clusterReferences lists the constraints, trigger and function that refer to
// public.clusters.
func clusterReferences(t *testing.T, sqlDB *sql.DB) []string {
	t.Helper()

	rows, err := sqlDB.QueryContext(t.Context(), `
		SELECT 'constraint ' || conrelid::regclass || '.' || conname FROM pg_constraint WHERE confrelid = 'public.clusters'::regclass
		UNION ALL
		SELECT 'trigger ' || tgrelid::regclass || '.' || tgname FROM pg_trigger WHERE tgname = 'teams_cluster_not_deleting'
		UNION ALL
		SELECT 'function ' || proname FROM pg_proc WHERE proname = 'refuse_team_on_deleting_cluster'
		ORDER BY 1
	`)
	require.NoError(t, err)
	defer rows.Close()

	var references []string
	for rows.Next() {
		var reference string
		require.NoError(t, rows.Scan(&reference))
		references = append(references, reference)
	}
	require.NoError(t, rows.Err())

	return references
}

// The rollback runs without a transaction, so one the lock timeout stops, or
// one the migrator does not get to record, has to run again over what it
// already restored.
func TestTheClusterReferencesRollbackCanRunAgain(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, sqlDB.Close()) })
	store, err := database.NewStore(goose.DialectPostgres, testutils.TrackingTable)
	require.NoError(t, err)
	migrations, err := goose.NewProvider("", sqlDB, dbmodule.Migrations(), goose.WithStore(store))
	require.NoError(t, err)
	restored := []string{
		"constraint envs.envs_cluster_id_fkey",
		"constraint teams.teams_cluster_id_fkey",
		"function refuse_team_on_deleting_cluster",
		"trigger teams.teams_cluster_not_deleting",
	}

	_, err = migrations.DownTo(ctx, dropClusterReferencesVersion-1)
	require.NoError(t, err)
	require.Equal(t, restored, clusterReferences(t, sqlDB))

	_, err = sqlDB.ExecContext(ctx, `INSERT INTO `+testutils.TrackingTable+` (version_id, is_applied) VALUES ($1, true)`, dropClusterReferencesVersion)
	require.NoError(t, err)
	_, err = migrations.DownTo(ctx, dropClusterReferencesVersion-1)
	require.NoError(t, err)
	assert.Equal(t, restored, clusterReferences(t, sqlDB))
}
