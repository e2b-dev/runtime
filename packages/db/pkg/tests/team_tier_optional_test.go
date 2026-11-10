package tests

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

const teamTierOptionalVersion int64 = 20261028103239

// Down gives the base tier to every team without one, so NOT NULL can return,
// and leaves an assigned tier alone.
func TestTeamTierOptionalMigrationRoundTrip(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	_, err = sqlDB.ExecContext(ctx, `
		INSERT INTO public.tiers (id, name, disk_mb, concurrent_instances, max_length_hours,
			default_free_disk_size_mb, max_disk_size_mb)
		VALUES ('tier_round_trip', 'Round trip', 10240, 1, 24, 8000, 30000)`)
	require.NoError(t, err)
	untiered := testutils.CreateTestTeam(t, db)
	tiered := seedTeam(t, sqlDB, "tiered")
	_, err = sqlDB.ExecContext(ctx, `UPDATE public.teams SET tier = 'tier_round_trip' WHERE id = $1`, tiered)
	require.NoError(t, err)

	tierOf := func(id uuid.UUID) sql.NullString {
		t.Helper()

		var tier sql.NullString
		require.NoError(t, sqlDB.QueryRowContext(ctx, `SELECT tier FROM public.teams WHERE id = $1`, id).Scan(&tier))

		return tier
	}
	insertUntiered := func(slug string) error {
		_, err := sqlDB.ExecContext(ctx, `INSERT INTO public.teams (id, name, email, slug) VALUES ($1, $2, $3, $2)`,
			uuid.New(), slug, slug+"@example.com")

		return err
	}
	require.False(t, tierOf(untiered).Valid)

	store, err := database.NewStore(goose.DialectPostgres, testutils.TrackingTable)
	require.NoError(t, err)
	provider, err := goose.NewProvider("", sqlDB, os.DirFS(filepath.Join("..", "..", "migrations")), goose.WithStore(store))
	require.NoError(t, err)

	_, err = provider.DownTo(ctx, teamTierOptionalVersion-1)
	require.NoError(t, err)
	assert.Equal(t, sql.NullString{String: "base_v1", Valid: true}, tierOf(untiered))
	assert.Equal(t, sql.NullString{String: "tier_round_trip", Valid: true}, tierOf(tiered))
	require.Error(t, insertUntiered("untiered-before"), "the tier is required again after Down")

	_, err = provider.UpTo(ctx, teamTierOptionalVersion)
	require.NoError(t, err)
	require.NoError(t, insertUntiered("untiered-after"))
}
