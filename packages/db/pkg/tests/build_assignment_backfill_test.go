package tests

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

// Inserting into env_build_assignments fills env_builds.env_id and team_id
// from the first assignment, only while they are NULL, in a single UPDATE.
func TestBuildAssignmentBackfillsBothColumnsInOneUpdate(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	team, otherTeam := testutils.CreateTestTeam(t, db), testutils.CreateTestTeam(t, db)
	template := testutils.CreateTestTemplate(t, db, team)
	otherTemplate := testutils.CreateTestTemplate(t, db, otherTeam)

	insertBuild := func(t *testing.T, envID *string, teamID *uuid.UUID) uuid.UUID {
		t.Helper()
		id := uuid.New()
		_, err := sqlDB.ExecContext(ctx, `INSERT INTO public.env_builds
			(id, env_id, team_id, status, vcpu, ram_mb, free_disk_size_mb, kernel_version, firecracker_version, cluster_node_id, created_at, updated_at)
			VALUES ($1, $2, $3, 'success', 2, 2048, 512, '6.1.0', '1.4.0', 'test-node', NOW(), NOW())`, id, envID, teamID)
		require.NoError(t, err)

		return id
	}
	// assign inserts one assignment and returns how many env_builds row
	// versions the insert wrote. The xact counters also carry this backend's
	// unflushed counts from earlier transactions, so take the difference around
	// the insert.
	assign := func(t *testing.T, envID string, buildID uuid.UUID, tag string) int64 {
		t.Helper()
		tx, err := sqlDB.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		updates := func() int64 {
			var n int64
			require.NoError(t, tx.QueryRowContext(ctx,
				`SELECT coalesce((SELECT n_tup_upd FROM pg_stat_xact_user_tables WHERE relid = 'public.env_builds'::regclass), 0)`).Scan(&n))

			return n
		}
		before := updates()
		_, err = tx.ExecContext(ctx, `INSERT INTO public.env_build_assignments (env_id, build_id, tag, source, created_at)
			VALUES ($1, $2, $3, 'app', NOW())`, envID, buildID, tag)
		require.NoError(t, err)
		written := updates() - before
		require.NoError(t, tx.Commit())

		return written
	}
	read := func(t *testing.T, buildID uuid.UUID) (*string, *uuid.UUID) {
		t.Helper()
		var envID *string
		var teamID *uuid.UUID
		require.NoError(t, sqlDB.QueryRowContext(ctx, `SELECT env_id, team_id FROM public.env_builds WHERE id = $1`, buildID).Scan(&envID, &teamID))

		return envID, teamID
	}

	preset := "preset-env"
	fresh := insertBuild(t, nil, nil)
	envOnly := insertBuild(t, &preset, nil)
	both := insertBuild(t, &preset, &otherTeam)

	for _, tc := range []struct {
		name        string
		envID       string
		buildID     uuid.UUID
		tag         string
		wantUpdates int64
		wantEnv     string
		wantTeam    uuid.UUID
	}{
		{"fills both columns", template, fresh, "default", 1, template, team},
		{"keeps a set env_id and fills team_id", template, envOnly, "default", 1, preset, team},
		{"leaves both set columns alone", template, both, "default", 0, preset, otherTeam},
		{"first assignment wins", otherTemplate, fresh, "other", 0, template, team},
	} {
		require.Equal(t, tc.wantUpdates, assign(t, tc.envID, tc.buildID, tc.tag), tc.name)
		envID, teamID := read(t, tc.buildID)
		require.NotNil(t, envID, tc.name)
		require.NotNil(t, teamID, tc.name)
		require.Equal(t, tc.wantEnv, *envID, tc.name)
		require.Equal(t, tc.wantTeam, *teamID, tc.name)
	}
}
