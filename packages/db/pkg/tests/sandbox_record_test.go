package tests

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func TestSandboxRecordIsTheLatestStartedExecution(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	// The migrations here do not create the table this query reads; its
	// columns come from schema/sqlc_overrides.sql.
	_, err = sqlDB.ExecContext(ctx, `
		CREATE SCHEMA billing;
		CREATE TABLE billing.sandbox_logs (
			sandbox_id TEXT NOT NULL,
			env_id TEXT NOT NULL,
			vcpu BIGINT NOT NULL,
			ram_mb BIGINT NOT NULL,
			total_disk_size_mb BIGINT NOT NULL,
			started_at TIMESTAMPTZ NOT NULL,
			stopped_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL,
			team_id UUID NOT NULL
		);
	`)
	require.NoError(t, err)

	teamID := seedTeam(t, sqlDB, "sandbox-record")
	sandboxID := "sbx-record"
	base := time.Date(2026, 3, 4, 5, 0, 0, 0, time.UTC)

	insert := func(startedAt, stoppedAt, createdAt time.Time, vcpu int64) {
		t.Helper()

		_, err := sqlDB.ExecContext(ctx, `
			INSERT INTO billing.sandbox_logs (sandbox_id, env_id, vcpu, ram_mb, total_disk_size_mb, started_at, stopped_at, created_at, team_id)
			VALUES ($1, 'tmpl', $2, 512, 1024, $3, $4, $5, $6)
		`, sandboxID, vcpu, startedAt, stoppedAt, createdAt, teamID)
		require.NoError(t, err)
	}

	// The first execution's stop is recorded after the second execution's.
	insert(base, base.Add(time.Minute), base.Add(10*time.Minute), 1)
	insert(base.Add(2*time.Minute), base.Add(3*time.Minute), base.Add(3*time.Minute), 2)

	record, err := db.SqlcClient.GetSandboxRecordByTeamAndSandboxID(ctx, queries.GetSandboxRecordByTeamAndSandboxIDParams{
		TeamID:    teamID,
		SandboxID: sandboxID,
	})
	require.NoError(t, err)

	assert.Equal(t, int64(2), record.Vcpu)
	assert.True(t, base.Add(2*time.Minute).Equal(record.StartedAt), "started_at %s", record.StartedAt)
}
