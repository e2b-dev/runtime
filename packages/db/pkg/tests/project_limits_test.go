package tests

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

// The team lookups serve both columns straight from a pushed row, so the
// constraint here is what keeps a team's free disk allowance at or below its
// ceiling.
func TestProjectLimitsRejectsFreeDiskAboveTheCeiling(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	teamID := seedTeam(t, sqlDB, "incoherent")

	insert := func(defaultFree, ceiling int64) error {
		_, err := sqlDB.ExecContext(ctx, `
			INSERT INTO public.project_limits (
				team_id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
				max_vcpu, max_ram_mb, disk_mb, events_ttl_days,
				default_free_disk_size_mb, max_disk_size_mb, api_team_rps_list, api_team_rps_delete, max_secrets
			) VALUES ($1, 1, 1, 1, 1, 1, 1, 1, $2, $3, 0, 0, 1)
			ON CONFLICT (team_id) DO UPDATE SET
				default_free_disk_size_mb = EXCLUDED.default_free_disk_size_mb,
				max_disk_size_mb = EXCLUDED.max_disk_size_mb
		`, teamID, defaultFree, ceiling)

		return err
	}

	require.Error(t, insert(2048, 1024), "a free allowance above the ceiling must be rejected")
	require.NoError(t, insert(1024, 1024), "equal values are the boundary and must be allowed")
	require.NoError(t, insert(512, 1024))
}

// Deleting a team takes its overrides with it. The FK is intra-schema and
// stays after billing is detached, so this is the one referential guarantee
// this table keeps.
func TestProjectLimitsIsRemovedWithItsTeam(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	teamID := seedTeam(t, sqlDB, "deleted")
	_, err = sqlDB.ExecContext(ctx, `
		INSERT INTO public.project_limits (
			team_id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
			max_vcpu, max_ram_mb, disk_mb, events_ttl_days,
			default_free_disk_size_mb, max_disk_size_mb, api_team_rps_list, api_team_rps_delete, max_secrets
		) VALUES ($1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1)
	`, teamID)
	require.NoError(t, err)

	_, err = sqlDB.ExecContext(ctx, `DELETE FROM public.teams WHERE id = $1`, teamID)
	require.NoError(t, err)

	var remaining int
	require.NoError(t, sqlDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM public.project_limits WHERE team_id = $1`, teamID).Scan(&remaining))
	require.Zero(t, remaining)
}
