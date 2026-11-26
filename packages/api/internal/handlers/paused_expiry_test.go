package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/outbox"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	redisreservations "github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/reservations/redis"
	sandboxredis "github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/storage/redis"
)

type expiryFixture struct {
	db     *testutils.Database
	store  *sandbox.Store
	api    *APIStore
	teamID uuid.UUID
	base   string
}

func newExpiryFixture(t *testing.T) expiryFixture {
	t.Helper()

	db := testutils.SetupDatabase(t)
	redisClient := redis_utils.SetupInstance(t)
	storage, err := sandboxredis.NewStorage(redisClient, noop.NewMeterProvider())
	require.NoError(t, err)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })
	store := sandbox.NewStore(storage, redisreservations.NewReservationStorage(redisClient, storage.Notifier()), sandbox.Callbacks{
		AsyncNewlyCreatedSandbox: func(context.Context, sandbox.Sandbox, sandbox.CreationMetadata) {},
	})

	teamID := testutils.CreateTestTeam(t, db)

	return expiryFixture{
		db:    db,
		store: store,
		api: &APIStore{
			redisClient:   redisClient,
			sqlcDB:        db.SqlcClient,
			orchestrator:  orchestrator.NewTestOrchestratorWithStore(store),
			templateCache: templatecache.NewTemplateCache(db.SqlcClient, redisClient),
			snapshotCache: snapshotcache.NewSnapshotCache(db.SqlcClient, redisClient),
		},
		teamID: teamID,
		base:   testutils.CreateTestTemplate(t, db, teamID),
	}
}

// pausedWithExpiry pauses a sandbox with a one-hour TTL and moves its expiry
// by shift from now.
func (f expiryFixture) pausedWithExpiry(t *testing.T, shift time.Duration) string {
	t.Helper()

	sandboxID := "i" + id.Generate()
	ttl := int64(3600)
	envdVersion := "v1.0.0"
	_, err := f.db.SqlcClient.UpsertSnapshot(t.Context(), queries.UpsertSnapshotParams{
		TemplateID:         id.Generate(),
		TeamID:             f.teamID,
		SandboxID:          sandboxID,
		BaseTemplateID:     f.base,
		StartedAt:          pgtype.Timestamptz{Time: time.Now(), Valid: true},
		Vcpu:               2,
		RamMb:              2048,
		Metadata:           dbtypes.JSONBStringMap{},
		KernelVersion:      "6.1.0",
		FirecrackerVersion: "1.4.0",
		EnvdVersion:        &envdVersion,
		OriginNodeID:       "test-node",
		StatusGroup:        dbtypes.BuildStatusGroupReady,
		Config:             &dbtypes.PausedSandboxConfig{PausedTTL: &ttl},
	})
	require.NoError(t, err)
	require.NoError(t, f.db.SqlcClient.TestsRawSQL(t.Context(),
		"UPDATE public.snapshots SET expires_at = now() + make_interval(secs => $2) WHERE sandbox_id = $1",
		sandboxID, shift.Seconds()))

	return sandboxID
}

func (f expiryFixture) run(t *testing.T, sandboxID string) {
	t.Helper()

	require.NoError(t, f.store.Add(t.Context(), sandbox.Sandbox{
		SandboxID:         sandboxID,
		TemplateID:        f.base,
		ExecutionID:       uuid.NewString(),
		TeamID:            f.teamID,
		BaseTemplateID:    f.base,
		MaxInstanceLength: time.Hour,
		StartTime:         time.Now(),
		EndTime:           time.Now().Add(time.Hour),
		VCpu:              2,
		RamMB:             512,
		NodeID:            "test-node",
		State:             sandbox.StateRunning,
	}, nil))
}

func (f expiryFixture) resumeBlocked(t *testing.T, sandboxID string) bool {
	t.Helper()

	_, _, err := f.store.Reserve(t.Context(), f.teamID, sandboxID, -1)
	if err != nil {
		require.ErrorIs(t, err, sandbox.ErrSandboxKilled)

		return true
	}

	return false
}

func TestExpirePausedSandbox(t *testing.T) {
	t.Parallel()

	t.Run("an expired paused sandbox is deleted and stays claimed", func(t *testing.T) {
		t.Parallel()

		f := newExpiryFixture(t)
		sandboxID := f.pausedWithExpiry(t, -time.Minute)

		outcome, err := f.api.ExpirePausedSandbox(t.Context(), f.teamID, sandboxID)
		require.NoError(t, err)
		assert.Equal(t, outbox.PausedExpiryDeleted, outcome)
		assert.Equal(t, "stamped", testutils.SnapshotEnvStamp(t, t.Context(), f.db, sandboxID))
		assert.True(t, f.resumeBlocked(t, sandboxID), "like DELETE, the claim holds resumes back while caches settle")
	})

	t.Run("a resumed sandbox past its expiry is left running", func(t *testing.T) {
		t.Parallel()

		f := newExpiryFixture(t)
		sandboxID := f.pausedWithExpiry(t, -time.Minute)
		f.run(t, sandboxID)

		outcome, err := f.api.ExpirePausedSandbox(t.Context(), f.teamID, sandboxID)
		require.NoError(t, err)
		assert.Equal(t, outbox.PausedExpirySkippedLive, outcome)
		assert.Equal(t, "live", testutils.SnapshotEnvStamp(t, t.Context(), f.db, sandboxID))

		stored, err := f.store.Get(t.Context(), f.teamID, sandboxID)
		require.NoError(t, err)
		assert.Equal(t, sandbox.StateRunning, stored.State)
	})

	t.Run("a sandbox whose clock restarted since it was listed is kept and its claim released", func(t *testing.T) {
		t.Parallel()

		f := newExpiryFixture(t)
		sandboxID := f.pausedWithExpiry(t, time.Hour)

		outcome, err := f.api.ExpirePausedSandbox(t.Context(), f.teamID, sandboxID)
		require.NoError(t, err)
		assert.Equal(t, outbox.PausedExpirySkippedRefreshed, outcome)
		assert.Equal(t, "live", testutils.SnapshotEnvStamp(t, t.Context(), f.db, sandboxID))
		assert.False(t, f.resumeBlocked(t, sandboxID))
	})
}

func TestPausedSandboxLive(t *testing.T) {
	t.Parallel()

	f := newExpiryFixture(t)
	paused := f.pausedWithExpiry(t, -time.Minute)
	running := f.pausedWithExpiry(t, -time.Minute)
	f.run(t, running)

	live, err := f.api.PausedSandboxLive(t.Context(), f.teamID, paused)
	require.NoError(t, err)
	assert.False(t, live)

	live, err = f.api.PausedSandboxLive(t.Context(), f.teamID, running)
	require.NoError(t, err)
	assert.True(t, live)
	assert.False(t, f.resumeBlocked(t, paused), "the dry-run probe claims nothing")
}

func TestPausedExpirySweepCursor(t *testing.T) {
	t.Parallel()

	api := &APIStore{redisClient: redis_utils.SetupInstance(t)}

	cursor, err := api.LoadSweepCursor(t.Context())
	require.NoError(t, err)
	assert.Nil(t, cursor, "no sweep has stopped early yet")

	stopped := queries.ListExpiredPausedSnapshotsRow{ExpiresAt: time.Date(2026, 11, 13, 10, 0, 0, 123456000, time.UTC), SandboxID: "isweepcursor"}
	require.NoError(t, api.SaveSweepCursor(t.Context(), &stopped))
	cursor, err = api.LoadSweepCursor(t.Context())
	require.NoError(t, err)
	require.NotNil(t, cursor)
	assert.True(t, stopped.ExpiresAt.Equal(cursor.ExpiresAt))
	assert.Equal(t, stopped.SandboxID, cursor.SandboxID)

	require.NoError(t, api.SaveSweepCursor(t.Context(), nil))
	cursor, err = api.LoadSweepCursor(t.Context())
	require.NoError(t, err)
	assert.Nil(t, cursor, "a sweep that reached the end clears it")
}
