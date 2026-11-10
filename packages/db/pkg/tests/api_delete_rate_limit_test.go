package tests

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbmodule "github.com/e2b-dev/infra/packages/db"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestAPIDeleteRateResolvesDefaultsUntilPushed(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	teamID := seedTeam(t, sqlDB, "delete-rate")
	otherTeamID := seedTeam(t, sqlDB, "other-delete-rate")
	resolved := func(id uuid.UUID) int64 {
		t.Helper()
		row, err := db.AuthDB.GetTeamByTeamID(t.Context(), id)
		require.NoError(t, err)

		return row.ApiTeamRpsDelete
	}
	assert.Zero(t, resolved(teamID))
	const largeRPS int64 = 1 << 32
	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.default_limits SET api_team_rps_delete = $1`, largeRPS)
	require.NoError(t, err)
	assert.Equal(t, largeRPS, resolved(teamID))
	_, err = sqlDB.ExecContext(t.Context(), `
		INSERT INTO public.project_limits (
			team_id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
			max_vcpu, max_ram_mb, disk_mb, events_ttl_days, default_free_disk_size_mb, max_disk_size_mb,
			api_team_rps_list, api_team_rps_delete, max_secrets
		) VALUES ($1, 1, 200, 20, 8, 8192, 10240, 7, 10240, 25600, 41, 43, 10)
	`, teamID)
	require.NoError(t, err)
	assert.Equal(t, int64(43), resolved(teamID), "a pushed rate replaces the default")
	assert.Equal(t, largeRPS, resolved(otherTeamID), "another team's push does not reach this one")
	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.project_limits SET api_team_rps_delete = 0 WHERE team_id = $1`, teamID)
	require.NoError(t, err)
	assert.Zero(t, resolved(teamID), "a pushed zero disables the limit rather than falling back")
	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.default_limits SET api_team_rps_delete = -1`)
	require.Error(t, err)
}

func TestAPIDeleteRateMigrationRoundTrip(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	teamID := seedTeam(t, sqlDB, "delete-rate-migration")
	seedAddon(t, sqlDB, teamID)
	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.tiers SET api_team_rps_list = 17 WHERE id = 'base_v1'`)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(t.Context(), `
		INSERT INTO public.project_limits (
			team_id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
			max_vcpu, max_ram_mb, disk_mb, events_ttl_days, default_free_disk_size_mb, max_disk_size_mb,
			api_team_rps_list, api_team_rps_delete, max_secrets
		) VALUES ($1, 1, 200, 20, 8, 8192, 10240, 7, 10240, 25600, 43, 0, 10)
	`, teamID)
	require.NoError(t, err)
	store, err := database.NewStore(goose.DialectPostgres, testutils.TrackingTable)
	require.NoError(t, err)
	provider, err := goose.NewProvider("", sqlDB, dbmodule.Migrations(), goose.WithStore(store))
	require.NoError(t, err)
	otherLimits := func() string {
		t.Helper()
		var limits string
		err := sqlDB.QueryRowContext(t.Context(), `SELECT (to_jsonb(pl) - 'api_team_rps_delete' - 'max_secrets' - 'updated_at')::text FROM public.project_limits pl WHERE team_id = $1`, teamID).Scan(&limits)
		require.NoError(t, err)

		return limits
	}
	before := otherLimits()
	_, err = provider.DownTo(t.Context(), 20261016090000)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.tiers SET api_team_rps_delete = 13 WHERE id = 'base_v1'`)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.addons SET extra_api_team_rps_delete = 4 WHERE team_id = $1`, teamID)
	require.NoError(t, err)
	_, err = provider.Up(t.Context())
	require.NoError(t, err)
	row, err := db.AuthDB.GetTeamByTeamID(t.Context(), teamID)
	require.NoError(t, err)
	assert.Equal(t, int64(17), row.ApiTeamRpsDelete, "a row pushed before the column keeps the tier's rate and add-ons")
	assert.JSONEq(t, before, otherLimits())
	_, err = provider.DownTo(t.Context(), 20261002234434)
	require.NoError(t, err)
	assert.JSONEq(t, before, otherLimits())
	_, err = provider.Up(t.Context())
	require.NoError(t, err)
	assert.JSONEq(t, before, otherLimits())
	row, err = db.AuthDB.GetTeamByTeamID(t.Context(), teamID)
	require.NoError(t, err)
	assert.Zero(t, row.ApiTeamRpsDelete)
	assert.Equal(t, int64(43), row.ApiTeamRpsList)
}
