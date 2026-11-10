package tests

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

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
