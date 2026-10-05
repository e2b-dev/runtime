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

func TestAPIListRateResolvesTierAndActiveAddons(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	teamID := seedTeam(t, sqlDB, "rate-limit")
	otherTeamID := seedTeam(t, sqlDB, "other-rate-limit")

	resolved := func(id uuid.UUID) int64 {
		t.Helper()

		row, err := db.AuthDB.GetTeamWithTierByTeamID(t.Context(), id)
		require.NoError(t, err)

		return row.TeamLimit.ApiTeamRpsList
	}
	assert.Zero(t, resolved(teamID))
	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.tiers SET api_team_rps_list = 5 WHERE id = 'base_v1'`)
	require.NoError(t, err)
	assert.Equal(t, int64(5), resolved(teamID))

	_, err = sqlDB.ExecContext(t.Context(), `
		INSERT INTO public.addons (team_id, name, extra_api_team_rps_list, valid_from, valid_to, added_by)
		VALUES
		($1, 'active', 2, now() - interval '1 hour', NULL, $3),
		($1, 'active with end', 3, now() - interval '1 hour', now() + interval '1 hour', $3),
		($1, 'expired', 100, now() - interval '2 hours', now() - interval '1 hour', $3),
		($1, 'future', 100, now() + interval '1 hour', NULL, $3),
		($2, 'other team', 7, now() - interval '1 hour', NULL, $3)
	`, teamID, otherTeamID, uuid.Nil)
	require.NoError(t, err)
	assert.Equal(t, int64(10), resolved(teamID))
	assert.Equal(t, int64(12), resolved(otherTeamID))

	const largeRPS int64 = 1 << 32
	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.tiers SET api_team_rps_list = $1 WHERE id = 'base_v1'`, largeRPS)
	require.NoError(t, err)
	assert.Equal(t, largeRPS+5, resolved(teamID))
	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.addons SET extra_api_team_rps_list = $1 WHERE team_id = $2 AND name = 'active'`, largeRPS+1, teamID)
	require.NoError(t, err)
	assert.Equal(t, 2*largeRPS+4, resolved(teamID))

	_, err = sqlDB.ExecContext(t.Context(), `
		INSERT INTO public.project_limits (
			team_id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
			max_vcpu, max_ram_mb, disk_mb, events_ttl_days, default_free_disk_size_mb, max_disk_size_mb,
			api_team_rps_list
		) VALUES ($1, 1, 200, 20, 8, 8192, 10240, 7, 10240, 25600, 40)
	`, teamID)
	require.NoError(t, err)
	assert.Equal(t, int64(40), resolved(teamID), "a pushed rate replaces the tier and team add-on rate")
	assert.Equal(t, largeRPS+7, resolved(otherTeamID), "another team's push does not reach this one")

	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.tiers SET api_team_rps_list = -1 WHERE id = 'base_v1'`)
	require.Error(t, err)
	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.addons SET extra_api_team_rps_list = -1 WHERE team_id = $1`, teamID)
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
		err := sqlDB.QueryRowContext(t.Context(), `SELECT (to_jsonb(tl) - 'api_team_rps_list')::text FROM public.team_limits tl WHERE id = $1`, teamID).Scan(&limits)
		require.NoError(t, err)

		return limits
	}
	before := resourceLimits()
	_, err = provider.DownTo(t.Context(), 20260826075153)
	require.NoError(t, err)
	assert.JSONEq(t, before, resourceLimits())
	_, err = provider.Up(t.Context())
	require.NoError(t, err)
	assert.JSONEq(t, before, resourceLimits())
	var rate int64
	require.NoError(t, sqlDB.QueryRowContext(t.Context(), `SELECT api_team_rps_list FROM public.team_limits WHERE id = $1`, teamID).Scan(&rate))
	assert.Zero(t, rate)
}
