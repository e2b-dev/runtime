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

	dbmodule "github.com/e2b-dev/infra/packages/db"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

const (
	apiListRateMigration              = 20260910094221
	projectLimitsAPIListRateMigration = 20260923090000
)

func TestAPIListRateResolvesDefaultsUntilPushed(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	teamID := seedTeam(t, sqlDB, "rate-limit")
	otherTeamID := seedTeam(t, sqlDB, "other-rate-limit")

	resolved := func(id uuid.UUID) int64 {
		t.Helper()

		row, err := db.AuthDB.GetTeamByTeamID(t.Context(), id)
		require.NoError(t, err)

		return row.ApiTeamRpsList
	}
	assert.Zero(t, resolved(teamID))
	const largeRPS int64 = 1 << 32
	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.default_limits SET api_team_rps_list = $1`, largeRPS)
	require.NoError(t, err)
	assert.Equal(t, largeRPS, resolved(teamID))

	_, err = sqlDB.ExecContext(t.Context(), `
		INSERT INTO public.project_limits (
			team_id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
			max_vcpu, max_ram_mb, disk_mb, events_ttl_days, default_free_disk_size_mb, max_disk_size_mb,
			api_team_rps_list, api_team_rps_delete, max_secrets
		) VALUES ($1, 1, 200, 20, 8, 8192, 10240, 7, 10240, 25600, 40, 0, 10)
	`, teamID)
	require.NoError(t, err)
	assert.Equal(t, int64(40), resolved(teamID), "a pushed rate replaces the default")
	assert.Equal(t, largeRPS, resolved(otherTeamID), "another team's push does not reach this one")

	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.default_limits SET api_team_rps_list = -1`)
	require.Error(t, err)
}

func TestAPIListRateMigrationRoundTrip(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	teamID := seedTeam(t, sqlDB, "rate-migration")
	seedAddon(t, sqlDB, teamID)

	store, err := database.NewStore(goose.DialectPostgres, testutils.TrackingTable)
	require.NoError(t, err)
	provider, err := goose.NewProvider("", sqlDB, os.DirFS(filepath.Join("..", "..", "migrations")), goose.WithStore(store))
	require.NoError(t, err)

	resourceLimits := func() string {
		t.Helper()

		var limits string
		err := sqlDB.QueryRowContext(t.Context(), `SELECT (to_jsonb(tl) - 'api_team_rps_list' - 'api_team_rps_delete' - 'max_secrets')::text FROM public.team_limits tl WHERE id = $1`, teamID).Scan(&limits)
		require.NoError(t, err)

		return limits
	}
	// Pinned to the migration under test, so later schema changes stay out of
	// the round trip.
	_, err = provider.DownTo(t.Context(), apiListRateMigration)
	require.NoError(t, err)
	before := resourceLimits()
	_, err = provider.DownTo(t.Context(), apiListRateMigration-1)
	require.NoError(t, err)
	assert.JSONEq(t, before, resourceLimits())
	_, err = provider.UpTo(t.Context(), apiListRateMigration)
	require.NoError(t, err)
	assert.JSONEq(t, before, resourceLimits())
	var rate int64
	require.NoError(t, sqlDB.QueryRowContext(t.Context(), `SELECT api_team_rps_list FROM public.team_limits WHERE id = $1`, teamID).Scan(&rate))
	assert.Zero(t, rate)
}

func TestProjectLimitsAPIListRateMigrationKeepsWhatAnEarlierRowResolvedTo(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	teamID := seedTeam(t, sqlDB, "rate-before-column")

	store, err := database.NewStore(goose.DialectPostgres, testutils.TrackingTable)
	require.NoError(t, err)
	provider, err := goose.NewProvider("", sqlDB, dbmodule.Migrations(), goose.WithStore(store))
	require.NoError(t, err)
	_, err = provider.DownTo(t.Context(), projectLimitsAPIListRateMigration-1)
	require.NoError(t, err)

	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.tiers SET api_team_rps_list = 7 WHERE id = 'base_v1'`)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(t.Context(), `
		INSERT INTO public.addons (team_id, name, extra_api_team_rps_list, valid_from, added_by)
		VALUES ($1, 'active', 3, now() - interval '1 hour', $2)
	`, teamID, seedUser(t, sqlDB))
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(t.Context(), `
		INSERT INTO public.project_limits (
			team_id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
			max_vcpu, max_ram_mb, disk_mb, events_ttl_days, default_free_disk_size_mb, max_disk_size_mb
		) VALUES ($1, 1, 200, 20, 8, 8192, 10240, 7, 10240, 25600)
	`, teamID)
	require.NoError(t, err)

	_, err = provider.UpTo(t.Context(), projectLimitsAPIListRateMigration)
	require.NoError(t, err)

	var stored int64
	require.NoError(t, sqlDB.QueryRowContext(t.Context(),
		`SELECT api_team_rps_list FROM public.project_limits WHERE team_id = $1`, teamID,
	).Scan(&stored))
	assert.Equal(t, int64(10), stored, "the tier and active add-ons are stored")

	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.project_limits SET api_team_rps_list = NULL WHERE team_id = $1`, teamID)
	require.Error(t, err, "the column is NOT NULL")
}
