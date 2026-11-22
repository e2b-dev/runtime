package snapshots

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
)

type snapshotExpiry struct {
	// expiresIn is expires_at minus the database's now(), nil when expires_at is NULL.
	expiresIn     *time.Duration
	pausedTTL     *int64
	configVersion *string
}

func readSnapshotExpiry(t *testing.T, db *testutils.Database, sandboxID string) snapshotExpiry {
	t.Helper()

	var expiresInSeconds *float64
	var pausedTTL *int64
	var configVersion *string
	err := db.SqlcClient.TestsRawSQLQuery(t.Context(), `
		SELECT EXTRACT(EPOCH FROM expires_at - now())::float8, (config ->> 'pausedTtl')::bigint, config ->> 'version'
		FROM public.snapshots WHERE sandbox_id = $1`,
		func(rows pgx.Rows) error {
			if !rows.Next() {
				return errors.New("snapshot not found")
			}

			return rows.Scan(&expiresInSeconds, &pausedTTL, &configVersion)
		},
		sandboxID,
	)
	require.NoError(t, err)

	expiry := snapshotExpiry{pausedTTL: pausedTTL, configVersion: configVersion}
	if expiresInSeconds != nil {
		d := time.Duration(*expiresInSeconds * float64(time.Second))
		expiry.expiresIn = &d
	}

	return expiry
}

func upsertSnapshotWithTTL(t *testing.T, db *testutils.Database, teamID uuid.UUID, baseTemplateID, sandboxID string, ttl *int64) {
	t.Helper()

	envdVersion := "v1.0.0"
	_, err := db.SqlcClient.UpsertSnapshot(t.Context(), queries.UpsertSnapshotParams{
		TemplateID:         "template-" + uuid.NewString(),
		TeamID:             teamID,
		SandboxID:          sandboxID,
		BaseTemplateID:     baseTemplateID,
		StartedAt:          pgtype.Timestamptz{Time: time.Now(), Valid: true},
		Vcpu:               2,
		RamMb:              2048,
		Metadata:           types.JSONBStringMap{},
		KernelVersion:      "6.1.0",
		FirecrackerVersion: "1.4.0",
		EnvdVersion:        &envdVersion,
		AutoPause:          true,
		Config: &types.PausedSandboxConfig{
			Version:   types.PausedSandboxConfigVersion,
			PausedTTL: ttl,
		},
		OriginNodeID: "node-1",
		StatusGroup:  types.BuildStatusGroupInProgress,
	})
	require.NoError(t, err)
}

func stampSnapshotExpiry(t *testing.T, db *testutils.Database, teamID uuid.UUID, sandboxID string, ttl *int64) int64 {
	t.Helper()

	rows, err := db.SqlcClient.StampSnapshotExpiry(t.Context(), queries.StampSnapshotExpiryParams{
		SandboxID:        sandboxID,
		TeamID:           teamID,
		PausedTtlSeconds: ttl,
	})
	require.NoError(t, err)

	return rows
}

func boundSnapshotExpiry(t *testing.T, db *testutils.Database, teamID uuid.UUID, sandboxID string, expiresAt *time.Time) {
	t.Helper()

	require.NoError(t, db.SqlcClient.BoundSnapshotExpiry(t.Context(), queries.BoundSnapshotExpiryParams{
		ExpiresAt: expiresAt,
		SandboxID: sandboxID,
		TeamID:    teamID,
	}))
}

func assertExpiresIn(t *testing.T, want time.Duration, got snapshotExpiry) {
	t.Helper()

	require.NotNil(t, got.expiresIn, "expires_at is NULL")
	assert.InDelta(t, want.Seconds(), got.expiresIn.Seconds(), 5)
}

func TestUpsertSnapshot_PausedTTL(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	ttl := int64(3600)

	// The run's end stamps the expiry once the snapshot is taken.
	t.Run("a first snapshot stores the TTL and leaves expires_at NULL", func(t *testing.T) {
		t.Parallel()

		sandboxID := "sandbox-" + uuid.NewString()
		upsertSnapshotWithTTL(t, db, teamID, baseTemplateID, sandboxID, &ttl)

		got := readSnapshotExpiry(t, db, sandboxID)
		assert.Nil(t, got.expiresIn)
		assert.Equal(t, &ttl, got.pausedTTL)
	})

	// A checkpoint, a snapshot template or a refused pause upserts while the
	// run goes on; replacing its resume bound would undo the bound.
	t.Run("a later snapshot keeps expires_at", func(t *testing.T) {
		t.Parallel()

		sandboxID := "sandbox-" + uuid.NewString()
		upsertSnapshotWithTTL(t, db, teamID, baseTemplateID, sandboxID, &ttl)
		boundSnapshotExpiry(t, db, teamID, sandboxID, new(time.Now().Add(48*time.Hour)))

		upsertSnapshotWithTTL(t, db, teamID, baseTemplateID, sandboxID, &ttl)

		assertExpiresIn(t, 48*time.Hour, readSnapshotExpiry(t, db, sandboxID))
	})

	t.Run("a later snapshot without a TTL drops the stored TTL", func(t *testing.T) {
		t.Parallel()

		sandboxID := "sandbox-" + uuid.NewString()
		upsertSnapshotWithTTL(t, db, teamID, baseTemplateID, sandboxID, &ttl)
		upsertSnapshotWithTTL(t, db, teamID, baseTemplateID, sandboxID, nil)

		assert.Nil(t, readSnapshotExpiry(t, db, sandboxID).pausedTTL)
	})
}

func TestStampSnapshotExpiry(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	hour := int64(3600)
	day := int64(86400)

	t.Run("a run end restarts the clock with the run's TTL", func(t *testing.T) {
		t.Parallel()

		sandboxID := "sandbox-" + uuid.NewString()
		upsertSnapshotWithTTL(t, db, teamID, baseTemplateID, sandboxID, &hour)

		assert.Equal(t, int64(1), stampSnapshotExpiry(t, db, teamID, sandboxID, &day))

		got := readSnapshotExpiry(t, db, sandboxID)
		assertExpiresIn(t, 24*time.Hour, got)
		assert.Equal(t, &day, got.pausedTTL)
	})

	t.Run("a run end without a TTL clears both", func(t *testing.T) {
		t.Parallel()

		sandboxID := "sandbox-" + uuid.NewString()
		upsertSnapshotWithTTL(t, db, teamID, baseTemplateID, sandboxID, &hour)
		stampSnapshotExpiry(t, db, teamID, sandboxID, &hour)

		assert.Equal(t, int64(1), stampSnapshotExpiry(t, db, teamID, sandboxID, nil))

		got := readSnapshotExpiry(t, db, sandboxID)
		assert.Nil(t, got.expiresIn)
		assert.Nil(t, got.pausedTTL)
	})

	t.Run("keeps the rest of the config", func(t *testing.T) {
		t.Parallel()

		sandboxID := "sandbox-" + uuid.NewString()
		upsertSnapshotWithTTL(t, db, teamID, baseTemplateID, sandboxID, nil)

		stampSnapshotExpiry(t, db, teamID, sandboxID, &hour)

		got := readSnapshotExpiry(t, db, sandboxID)
		require.NotNil(t, got.configVersion)
		assert.Equal(t, types.PausedSandboxConfigVersion, *got.configVersion)
	})

	t.Run("a sandbox that was never paused has no row to stamp", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, int64(0), stampSnapshotExpiry(t, db, teamID, "sandbox-"+uuid.NewString(), &hour))
	})

	t.Run("another team's sandbox is left alone", func(t *testing.T) {
		t.Parallel()

		otherTeamID := testutils.CreateTestTeam(t, db)
		otherBaseTemplateID := testutils.CreateTestTemplate(t, db, otherTeamID)
		sandboxID := "sandbox-" + uuid.NewString()
		upsertSnapshotWithTTL(t, db, otherTeamID, otherBaseTemplateID, sandboxID, nil)

		assert.Equal(t, int64(0), stampSnapshotExpiry(t, db, teamID, sandboxID, &hour))
		assert.Nil(t, readSnapshotExpiry(t, db, sandboxID).expiresIn)
	})

	t.Run("a deleted sandbox is left alone", func(t *testing.T) {
		t.Parallel()

		sandboxID := "sandbox-" + uuid.NewString()
		upsertSnapshotWithTTL(t, db, teamID, baseTemplateID, sandboxID, nil)
		err := db.SqlcClient.TestsRawSQL(t.Context(),
			"UPDATE public.snapshots SET env_deleted_at = now() WHERE sandbox_id = $1", sandboxID)
		require.NoError(t, err)

		assert.Equal(t, int64(0), stampSnapshotExpiry(t, db, teamID, sandboxID, &hour))
	})
}

func TestBoundSnapshotExpiry(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	hour := int64(3600)

	t.Run("sets expires_at and keeps the stored TTL", func(t *testing.T) {
		t.Parallel()

		sandboxID := "sandbox-" + uuid.NewString()
		upsertSnapshotWithTTL(t, db, teamID, baseTemplateID, sandboxID, &hour)
		stampSnapshotExpiry(t, db, teamID, sandboxID, &hour)

		boundSnapshotExpiry(t, db, teamID, sandboxID, new(time.Now().Add(25*time.Hour)))

		got := readSnapshotExpiry(t, db, sandboxID)
		assertExpiresIn(t, 25*time.Hour, got)
		assert.Equal(t, &hour, got.pausedTTL)
	})

	t.Run("a NULL bound clears expires_at", func(t *testing.T) {
		t.Parallel()

		sandboxID := "sandbox-" + uuid.NewString()
		upsertSnapshotWithTTL(t, db, teamID, baseTemplateID, sandboxID, &hour)
		stampSnapshotExpiry(t, db, teamID, sandboxID, &hour)

		boundSnapshotExpiry(t, db, teamID, sandboxID, nil)

		assert.Nil(t, readSnapshotExpiry(t, db, sandboxID).expiresIn)
	})

	t.Run("another team's sandbox is left alone", func(t *testing.T) {
		t.Parallel()

		otherTeamID := testutils.CreateTestTeam(t, db)
		otherBaseTemplateID := testutils.CreateTestTemplate(t, db, otherTeamID)
		sandboxID := "sandbox-" + uuid.NewString()
		upsertSnapshotWithTTL(t, db, otherTeamID, otherBaseTemplateID, sandboxID, &hour)

		boundSnapshotExpiry(t, db, teamID, sandboxID, new(time.Now().Add(25*time.Hour)))

		assert.Nil(t, readSnapshotExpiry(t, db, sandboxID).expiresIn)
	})

	t.Run("a deleted sandbox is left alone", func(t *testing.T) {
		t.Parallel()

		sandboxID := "sandbox-" + uuid.NewString()
		upsertSnapshotWithTTL(t, db, teamID, baseTemplateID, sandboxID, &hour)
		err := db.SqlcClient.TestsRawSQL(t.Context(),
			"UPDATE public.snapshots SET env_deleted_at = now() WHERE sandbox_id = $1", sandboxID)
		require.NoError(t, err)

		boundSnapshotExpiry(t, db, teamID, sandboxID, new(time.Now().Add(25*time.Hour)))

		assert.Nil(t, readSnapshotExpiry(t, db, sandboxID).expiresIn)
	})
}
