package tests

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
