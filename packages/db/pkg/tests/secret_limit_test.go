package tests

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestSecretLimitResolvesDefaultsUntilPushed(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	teamID := seedTeam(t, sqlDB, "secret-limit")
	otherTeamID := seedTeam(t, sqlDB, "other-secret-limit")
	resolved := func(id uuid.UUID) int64 {
		t.Helper()
		row, err := db.AuthDB.GetTeamByTeamID(t.Context(), id)
		require.NoError(t, err)

		return row.MaxSecrets
	}

	assert.Equal(t, int64(10), resolved(teamID), "the default keeps the limit the API used to hardcode")
	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.default_limits SET max_secrets = 30`)
	require.NoError(t, err)
	assert.Equal(t, int64(30), resolved(teamID))

	_, err = sqlDB.ExecContext(t.Context(), `
		INSERT INTO public.project_limits (
			team_id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
			max_vcpu, max_ram_mb, disk_mb, events_ttl_days, default_free_disk_size_mb, max_disk_size_mb,
			api_team_rps_list, api_team_rps_delete, max_secrets
		) VALUES ($1, 1, 200, 20, 8, 8192, 10240, 7, 10240, 25600, 41, 0, 40)
	`, teamID)
	require.NoError(t, err)
	assert.Equal(t, int64(40), resolved(teamID), "a pushed limit replaces the default")
	assert.Equal(t, int64(30), resolved(otherTeamID), "another team's push does not reach this one")

	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.default_limits SET max_secrets = 0`)
	require.Error(t, err)
	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.project_limits SET max_secrets = 0 WHERE team_id = $1`, teamID)
	require.Error(t, err)
}
