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

const requiredProjectLimitsMigration = 20261016165228

func TestProjectLimitsMigrationKeepsWhatAnUnpushedRowResolvedTo(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	unpushed := seedTeam(t, sqlDB, "unpushed-limits")
	pushed := seedTeam(t, sqlDB, "pushed-limits")

	store, err := database.NewStore(goose.DialectPostgres, testutils.TrackingTable)
	require.NoError(t, err)
	provider, err := goose.NewProvider("", sqlDB, dbmodule.Migrations(), goose.WithStore(store))
	require.NoError(t, err)
	_, err = provider.DownTo(t.Context(), requiredProjectLimitsMigration-1)
	require.NoError(t, err)

	_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.tiers SET api_team_rps_delete = 13 WHERE id = 'base_v1'`)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(t.Context(), `
		INSERT INTO public.addons (team_id, name, extra_api_team_rps_delete, extra_max_secrets, valid_from, added_by)
		VALUES ($1, 'active', 4, 5, now() - interval '1 hour', $2)
	`, unpushed, uuid.Nil)
	require.NoError(t, err)
	insert := `
		INSERT INTO public.project_limits (
			team_id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
			max_vcpu, max_ram_mb, disk_mb, events_ttl_days, default_free_disk_size_mb, max_disk_size_mb,
			api_team_rps_list, api_team_rps_delete, max_secrets
		) VALUES ($1, 1, 200, 20, 8, 8192, 10240, 7, 10240, 25600, 41, $2, $3)`
	_, err = sqlDB.ExecContext(t.Context(), insert, unpushed, nil, nil)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(t.Context(), insert, pushed, 0, 70)
	require.NoError(t, err)

	_, err = provider.Up(t.Context())
	require.NoError(t, err)

	stored := func(teamID uuid.UUID) [2]int64 {
		t.Helper()
		var limits [2]int64
		require.NoError(t, sqlDB.QueryRowContext(t.Context(),
			`SELECT api_team_rps_delete, max_secrets FROM public.project_limits WHERE team_id = $1`, teamID,
		).Scan(&limits[0], &limits[1]))

		return limits
	}
	assert.Equal(t, [2]int64{17, 15}, stored(unpushed), "the tier and active add-ons are stored in place of NULL")
	assert.Equal(t, [2]int64{0, 70}, stored(pushed), "pushed values are left as they are")

	for _, column := range []string{"api_team_rps_delete", "max_secrets"} {
		_, err = sqlDB.ExecContext(t.Context(), `UPDATE public.project_limits SET `+column+` = NULL WHERE team_id = $1`, pushed)
		require.Error(t, err, column)
	}

	_, err = provider.DownTo(t.Context(), requiredProjectLimitsMigration-1)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(t.Context(), insert, seedTeam(t, sqlDB, "unpushed-after-down"), nil, nil)
	require.NoError(t, err, "down admits NULL again")
}
