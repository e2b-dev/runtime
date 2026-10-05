package tests

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

// team_limits gates sandbox creation, and this migration rewrote every one of
// its columns. The claim the migration rests on is that an empty
// project_limits changes nothing, so it is worth proving rather than reading.
//
// Comparing against the arithmetic the view used to perform, rather than
// against fixed numbers, means the assertion stays honest if tier defaults or
// addon columns move underneath it.
func TestTeamLimitsIsUnchangedWhileProjectLimitsIsEmpty(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	seedTeam(t, sqlDB, "no-addons")
	teamWithAddons := seedTeam(t, sqlDB, "with-addons")
	seedAddon(t, sqlDB, teamWithAddons)

	var mismatches int
	err = sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM public.team_limits tl
		JOIN public.teams t ON t.id = tl.id
		JOIN public.tiers tier ON tier.id = t.tier
		LEFT JOIN LATERAL (
			SELECT COALESCE(SUM(extra_concurrent_sandboxes), 0)::bigint AS extra_concurrent_sandboxes,
			       COALESCE(SUM(extra_concurrent_template_builds), 0)::bigint AS extra_concurrent_template_builds,
			       COALESCE(SUM(extra_max_vcpu), 0)::bigint AS extra_max_vcpu,
			       COALESCE(SUM(extra_max_ram_mb), 0)::bigint AS extra_max_ram_mb,
			       COALESCE(SUM(extra_disk_mb), 0)::bigint AS extra_disk_mb,
			       COALESCE(SUM(extra_events_ttl_days), 0)::bigint AS extra_events_ttl_days,
			       COALESCE(SUM(COALESCE(extra_max_disk_size_mb, extra_disk_mb)), 0)::bigint AS extra_max_disk_size_mb
			FROM public.addons addon
			WHERE addon.team_id = t.id
			  AND addon.valid_from <= now()
			  AND (addon.valid_to IS NULL OR addon.valid_to > now())
		) a ON true
		WHERE (tl.max_length_hours, tl.concurrent_sandboxes, tl.concurrent_template_builds,
		       tl.max_vcpu, tl.max_ram_mb, tl.disk_mb, tl.events_ttl_days,
		       tl.default_free_disk_size_mb, tl.max_disk_size_mb, tl.max_free_disk_size_mb)
		   IS DISTINCT FROM
		      (tier.max_length_hours,
		       tier.concurrent_instances + a.extra_concurrent_sandboxes,
		       tier.concurrent_template_builds + a.extra_concurrent_template_builds,
		       tier.max_vcpu + a.extra_max_vcpu,
		       tier.max_ram_mb + a.extra_max_ram_mb,
		       tier.disk_mb + a.extra_disk_mb,
		       tier.events_ttl_days + a.extra_events_ttl_days,
		       (tier.default_free_disk_size_mb + a.extra_disk_mb)::bigint,
		       (tier.max_disk_size_mb + a.extra_max_disk_size_mb)::bigint,
		       (tier.max_disk_size_mb + a.extra_max_disk_size_mb)::bigint)
	`).Scan(&mismatches)
	require.NoError(t, err)
	require.Zero(t, mismatches, "team_limits diverged from the tier+addon arithmetic it replaced")

	var rows int
	require.NoError(t, sqlDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM public.project_limits`).Scan(&rows))
	require.Zero(t, rows, "the comparison above only means anything while no team is overridden")
}

// A populated row must win outright: every column comes from project_limits,
// and the addon that would otherwise raise the tier is ignored. Without this,
// a COALESCE pointed at the wrong side would still pass the test above.
func TestProjectLimitsOverridesTierAndAddons(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	teamID := seedTeam(t, sqlDB, "overridden")
	seedAddon(t, sqlDB, teamID)

	_, err = sqlDB.ExecContext(ctx, `
		INSERT INTO public.project_limits (
			team_id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
			max_vcpu, max_ram_mb, disk_mb, events_ttl_days,
			default_free_disk_size_mb, max_disk_size_mb, api_team_rps_list
		) VALUES ($1, 111, 222, 333, 444, 555, 666, 777, 888, 999, 1010)
	`, teamID)
	require.NoError(t, err)

	var got [11]int64
	err = sqlDB.QueryRowContext(ctx, `
		SELECT max_length_hours, concurrent_sandboxes, concurrent_template_builds,
		       max_vcpu, max_ram_mb, disk_mb, events_ttl_days,
		       default_free_disk_size_mb, max_disk_size_mb, max_free_disk_size_mb, api_team_rps_list
		FROM public.team_limits WHERE id = $1
	`, teamID).Scan(&got[0], &got[1], &got[2], &got[3], &got[4], &got[5], &got[6], &got[7], &got[8], &got[9], &got[10])
	require.NoError(t, err)

	require.Equal(t, [11]int64{111, 222, 333, 444, 555, 666, 777, 888, 999, 999, 1010}, got)
}

// tiers has always guaranteed that a team's free disk allowance sits at or
// below its ceiling. The view reads both columns straight from an override, so
// without the same constraint here a pushed row could surface a pair tiers can
// never produce, to readers that have never had to handle one.
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
				default_free_disk_size_mb, max_disk_size_mb, api_team_rps_list
			) VALUES ($1, 1, 1, 1, 1, 1, 1, 1, $2, $3, 0)
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
			default_free_disk_size_mb, max_disk_size_mb, api_team_rps_list
		) VALUES ($1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0)
	`, teamID)
	require.NoError(t, err)

	_, err = sqlDB.ExecContext(ctx, `DELETE FROM public.teams WHERE id = $1`, teamID)
	require.NoError(t, err)

	var remaining int
	require.NoError(t, sqlDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM public.project_limits WHERE team_id = $1`, teamID).Scan(&remaining))
	require.Zero(t, remaining)
}

func seedTeam(t *testing.T, sqlDB *sql.DB, slug string) uuid.UUID {
	t.Helper()

	teamID := uuid.New()
	_, err := sqlDB.ExecContext(t.Context(), `
		INSERT INTO public.teams (id, name, tier, email, slug)
		VALUES ($1, $2, 'base_v1', $3, $4)
	`, teamID, slug, slug+"@example.com", slug+"-"+teamID.String()[:8])
	require.NoError(t, err)

	return teamID
}

func seedAddon(t *testing.T, sqlDB *sql.DB, teamID uuid.UUID) {
	t.Helper()

	_, err := sqlDB.ExecContext(t.Context(), `
		INSERT INTO public.addons (
			team_id, name, extra_concurrent_sandboxes, extra_concurrent_template_builds,
			extra_max_vcpu, extra_max_ram_mb, extra_disk_mb, extra_events_ttl_days,
			valid_from, added_by
		) VALUES ($1, 'test-addon', 5, 6, 7, 8, 9, 10, now() - interval '1 hour',
		          '00000000-0000-0000-0000-000000000000')
	`, teamID)
	require.NoError(t, err)
}
