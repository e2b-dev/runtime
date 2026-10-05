package tests

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

const teamProfilePictureDropVersion int64 = 20260915204518

// The infra set drops teams.profile_picture_url, a column only the separate
// dashboard migration set ever declared. A fresh database must end without it,
// and a database that carried the column before the drop must lose it.
func TestTeamProfilePictureColumnIsDropped(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	newProvider := func(dir, table string) *goose.Provider {
		t.Helper()
		store, err := database.NewStore(goose.DialectPostgres, table)
		require.NoError(t, err)
		provider, err := goose.NewProvider("", sqlDB, os.DirFS(dir), goose.WithStore(store))
		require.NoError(t, err)

		return provider
	}
	hasColumn := func() bool {
		t.Helper()
		var count int
		err := sqlDB.QueryRowContext(ctx, `
			SELECT count(*) FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'teams' AND column_name = 'profile_picture_url'
		`).Scan(&count)
		require.NoError(t, err)

		return count == 1
	}

	// Fresh database: the infra set has already run, the column is absent.
	require.False(t, hasColumn(), "a fresh database must not carry teams.profile_picture_url")

	// A database from before the drop: rewind one step, apply the legacy dashboard
	// set on top (the way production databases got the column), then upgrade.
	migrations := newProvider(filepath.Join("..", "..", "migrations"), testutils.TrackingTable)
	_, err = migrations.DownTo(ctx, teamProfilePictureDropVersion-1)
	require.NoError(t, err)
	require.True(t, hasColumn(), "Down must re-add the column")

	legacy := newProvider(filepath.Join("testdata", "legacy_dashboard"), "_dashboard_migrations")
	_, err = legacy.Up(ctx)
	require.NoError(t, err)
	require.True(t, hasColumn())

	results, err := migrations.UpTo(ctx, teamProfilePictureDropVersion)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, teamProfilePictureDropVersion, results[0].Source.Version)
	require.False(t, hasColumn(), "the drop must remove the column a legacy database carried")

	// Applying the drop again is a no-op, and the legacy ledger is untouched.
	results, err = migrations.UpTo(ctx, teamProfilePictureDropVersion)
	require.NoError(t, err)
	require.Empty(t, results)
	var legacyVersion int64
	err = sqlDB.QueryRowContext(ctx,
		`SELECT MAX(version_id) FROM public._dashboard_migrations WHERE is_applied`).Scan(&legacyVersion)
	require.NoError(t, err)
	require.Equal(t, int64(20260316130000), legacyVersion)
}
