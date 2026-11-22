package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/bsm/redislock"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	sqlcdb "github.com/e2b-dev/infra/packages/api/internal/db/client"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// readExpiresIn returns expires_at minus the database's now(), nil when NULL.
func readExpiresIn(t *testing.T, db *sqlcdb.Client, sandboxID string) *time.Duration {
	t.Helper()

	var seconds *float64
	err := db.TestsRawSQLQuery(t.Context(),
		`SELECT EXTRACT(EPOCH FROM expires_at - now())::float8 FROM public.snapshots WHERE sandbox_id = $1`,
		func(rows pgx.Rows) error {
			if !rows.Next() {
				return errors.New("snapshot not found")
			}

			return rows.Scan(&seconds)
		},
		sandboxID,
	)
	require.NoError(t, err)
	if seconds == nil {
		return nil
	}
	d := time.Duration(*seconds * float64(time.Second))

	return &d
}

func assertExpiresIn(t *testing.T, want time.Duration, got *time.Duration) {
	t.Helper()

	require.NotNil(t, got, "expires_at is NULL")
	assert.InDelta(t, want.Seconds(), got.Seconds(), 5)
}

// pausedWithTTL writes the sandbox's snapshot row as its pause would, with a
// ready build so the snapshot cache can read it and the expiry the pause's end
// stamps, and gives the running record the TTL its next run carries.
func (f refusalFixture) pausedWithTTL(t *testing.T, pausedTTL *time.Duration, runTTL *time.Duration) {
	t.Helper()

	paused := f.sbx
	paused.PausedTTL = pausedTTL
	params := buildUpsertSnapshotParams(paused, f.o.GetNode(f.sbx.ClusterID, f.sbx.NodeID), false, false)
	params.StatusGroup = types.BuildStatusGroupReady
	_, err := f.o.sqlcDB.UpsertSnapshot(t.Context(), params)
	require.NoError(t, err)
	_, err = f.o.sqlcDB.StampSnapshotExpiry(t.Context(), queries.StampSnapshotExpiryParams{
		SandboxID:        f.sbx.SandboxID,
		TeamID:           f.sbx.TeamID,
		PausedTtlSeconds: pausedTTLSeconds(pausedTTL),
	})
	require.NoError(t, err)

	_, err = f.o.sandboxStore.Update(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, func(s sandbox.Sandbox) (sandbox.Sandbox, error) {
		s.PausedTTL = runTTL
		s.PausedTTLStored = pausedTTL != nil

		return s, nil
	})
	require.NoError(t, err)
}

// resumedWithBound bounds the expiry as the record's resume would have, and
// returns the bound.
func (f refusalFixture) resumedWithBound(t *testing.T) time.Time {
	t.Helper()

	record, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.NoError(t, err)
	require.NoError(t, f.o.boundPausedExpiry(t.Context(), record))

	return record.StartTime.Add(record.MaxInstanceLength + *record.PausedTTL)
}

// lockSnapshotRow holds the snapshot row's lock past one expiry write's
// timeout, as a stalled database would, and returns when it was released.
func lockSnapshotRow(t *testing.T, db *sqlcdb.Client, sandboxID string) <-chan struct{} {
	t.Helper()

	holder, err := db.Pool().Begin(t.Context())
	require.NoError(t, err)
	_, err = holder.Exec(t.Context(), `SELECT 1 FROM public.snapshots WHERE sandbox_id = $1 FOR UPDATE`, sandboxID)
	require.NoError(t, err)
	released := make(chan struct{})
	go func() {
		defer close(released)
		time.Sleep(pausedExpiryWriteTimeout + time.Second)
		assert.NoError(t, holder.Rollback(context.WithoutCancel(t.Context())))
	}()

	return released
}

func (f refusalFixture) kill(t *testing.T) {
	t.Helper()

	require.NoError(t, f.o.RemoveSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{
		Action: sandbox.StateActionKill,
		Reason: sandbox.KillReasonTimeout,
	}))
}

func TestPausedExpiry(t *testing.T) {
	t.Parallel()

	hour := time.Hour
	day := 24 * time.Hour

	t.Run("a pause sets expires_at from the record's TTL", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		_, err := f.o.sandboxStore.Update(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, func(s sandbox.Sandbox) (sandbox.Sandbox, error) {
			s.PausedTTL = &hour

			return s, nil
		})
		require.NoError(t, err)

		require.NoError(t, f.removePause(t))

		assertExpiresIn(t, time.Hour, readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))
	})

	t.Run("a resume bounds the expiry past the run's latest end", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		f.pausedWithTTL(t, &hour, &day)

		bound := f.resumedWithBound(t)

		assertExpiresIn(t, time.Until(bound), readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))
		assert.Greater(t, time.Until(bound), 24*time.Hour)
	})

	// Without the bound the row would keep the previous pause's expiry,
	// already past for a run longer than its TTL, and the sweep would delete
	// the sandbox this run just ended.
	t.Run("a kill whose stamp fails keeps the resume bound", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		f.pausedWithTTL(t, &hour, &day)
		bound := f.resumedWithBound(t)

		released := lockSnapshotRow(t, f.o.sqlcDB, f.sbx.SandboxID)
		f.kill(t)
		<-released

		assertExpiresIn(t, time.Until(bound), readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))
	})

	// A replica that finds the record stuck past the stale cutoff removes a
	// record that may since have been resumed and paused again; its stamp
	// would replace the newer run's expiry with one from its own old record.
	t.Run("a stale record's cleanup leaves the expiry alone", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		f.pausedWithTTL(t, &hour, &day)
		bound := f.resumedWithBound(t)
		_, err := f.o.sandboxStore.Update(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, func(s sandbox.Sandbox) (sandbox.Sandbox, error) {
			s.State = sandbox.StateKilling
			s.EndTime = time.Now().Add(-sandbox.StaleCutoff - time.Minute)

			return s, nil
		})
		require.NoError(t, err)

		f.kill(t)

		_, err = f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.ErrorIs(t, err, sandbox.ErrNotFound, "the stale branch removed the record")
		assertExpiresIn(t, time.Until(bound), readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))
	})

	// The pause writes its snapshot row before the node refuses, and the run
	// goes on.
	t.Run("a refused pause keeps the resume bound", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, true, consts.LocalClusterID, refusedPauseErr())
		f.pausedWithTTL(t, &hour, &day)
		bound := f.resumedWithBound(t)

		require.ErrorIs(t, f.removePause(t), PauseQueueExhaustedError{})

		assertExpiresIn(t, time.Until(bound), readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))
	})

	t.Run("a kill restarts the clock with the run's TTL and refreshes the cached config", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		f.pausedWithTTL(t, &hour, &day)
		cached, err := snapshotcache.NewSnapshotCache(f.o.sqlcDB, f.client).Get(t.Context(), f.sbx.SandboxID)
		require.NoError(t, err)
		require.Equal(t, int64(3600), *cached.Snapshot.Config.PausedTTL)

		f.kill(t)

		assertExpiresIn(t, 24*time.Hour, readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))
		cached, err = snapshotcache.NewSnapshotCache(f.o.sqlcDB, f.client).Get(t.Context(), f.sbx.SandboxID)
		require.NoError(t, err)
		require.NotNil(t, cached.Snapshot.Config.PausedTTL)
		assert.Equal(t, int64(86400), *cached.Snapshot.Config.PausedTTL)
	})

	// A cache refresh that read the snapshot before the stamp holds the
	// entry's lock and writes the old config back once its read returns. The
	// invalidation must wait that write out, however long past the stamp's
	// own timeout it takes, rather than delete early and let the stale entry
	// return.
	t.Run("a kill outwaits a cache writer holding the stale config", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		f.pausedWithTTL(t, &hour, &day)
		cache := snapshotcache.NewSnapshotCache(f.o.sqlcDB, f.client)
		_, err := cache.Get(t.Context(), f.sbx.SandboxID)
		require.NoError(t, err)
		entryKey := "snapshot:last:v2:" + f.sbx.SandboxID
		stale, err := f.client.Get(t.Context(), entryKey).Result()
		require.NoError(t, err)

		lock, err := redislock.New(f.client).Obtain(t.Context(), redis_utils.GetLockKey(entryKey), time.Minute, nil)
		require.NoError(t, err)
		written := make(chan struct{})
		go func() {
			defer close(written)
			time.Sleep(pausedExpiryWriteTimeout + time.Second)
			assert.NoError(t, f.client.Set(context.WithoutCancel(t.Context()), entryKey, stale, 0).Err())
			assert.NoError(t, lock.Release(context.WithoutCancel(t.Context())))
		}()

		f.kill(t)
		<-written

		cached, err := cache.Get(t.Context(), f.sbx.SandboxID)
		require.NoError(t, err)
		require.NotNil(t, cached.Snapshot.Config.PausedTTL)
		assert.Equal(t, int64(86400), *cached.Snapshot.Config.PausedTTL)
	})

	t.Run("a kill after the TTL was removed clears the expiry", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		f.pausedWithTTL(t, &hour, nil)

		f.kill(t)

		assert.Nil(t, readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))
		cached, err := snapshotcache.NewSnapshotCache(f.o.sqlcDB, f.client).Get(t.Context(), f.sbx.SandboxID)
		require.NoError(t, err)
		assert.Nil(t, cached.Snapshot.Config.PausedTTL)
	})

	t.Run("a run that never had a TTL leaves the row alone", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		f.pausedWithTTL(t, &hour, nil)
		_, err := f.o.sandboxStore.Update(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, func(s sandbox.Sandbox) (sandbox.Sandbox, error) {
			s.PausedTTLStored = false

			return s, nil
		})
		require.NoError(t, err)

		f.kill(t)

		assertExpiresIn(t, time.Hour, readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))
	})
}

func TestCreateSandbox_ResumeBoundsPausedExpiry(t *testing.T) {
	t.Parallel()

	day := 24 * time.Hour

	// resume starts the paused sandbox again on a fresh orchestrator, from a
	// snapshot whose pause stamped an hour's expiry.
	type resumed struct {
		o         *Orchestrator
		db        *testutils.Database
		teamID    uuid.UUID
		sandboxID string
		apiErr    *api.APIError
	}
	resume := func(t *testing.T, beforeResume func(db *testutils.Database, sandboxID string)) resumed {
		t.Helper()

		o := newCreateSandboxTestOrchestrator(t)
		db := testutils.SetupDatabase(t)
		o.sqlcDB = db.SqlcClient
		team := testTeam()
		team.Team.ID = testutils.CreateTestTeam(t, db)
		baseTemplateID := testutils.CreateTestTemplate(t, db, team.Team.ID)
		sandboxID := "sbx-bound-" + uuid.NewString()[:8]
		testutils.UpsertTestSnapshot(t, t.Context(), db, "tpl-"+uuid.NewString()[:8], sandboxID, team.Team.ID, baseTemplateID)
		_, err := db.SqlcClient.StampSnapshotExpiry(t.Context(), queries.StampSnapshotExpiryParams{
			SandboxID:        sandboxID,
			TeamID:           team.Team.ID,
			PausedTtlSeconds: new(int64(3600)),
		})
		require.NoError(t, err)
		beforeResume(db, sandboxID)

		fetcher := func(context.Context) (SandboxMetadata, *api.APIError) {
			return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: baseTemplateID, Build: testBuild(), PausedTTL: &day, PausedTTLStored: true}, nil
		}
		now := time.Now()
		_, apiErr := o.CreateSandbox(t.Context(), sandboxID, uuid.NewString(), team, fetcher,
			now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{IsResume: true})

		return resumed{o: o, db: db, teamID: team.Team.ID, sandboxID: sandboxID, apiErr: apiErr}
	}

	t.Run("the bound covers the longest run plus its TTL", func(t *testing.T) {
		t.Parallel()

		r := resume(t, func(*testutils.Database, string) {})
		require.Nil(t, r.apiErr)

		assertExpiresIn(t, 24*time.Hour+day, readExpiresIn(t, r.db.SqlcClient, r.sandboxID))
	})

	t.Run("a bound that cannot be written refuses the resume", func(t *testing.T) {
		t.Parallel()

		var released <-chan struct{}
		r := resume(t, func(db *testutils.Database, sandboxID string) {
			released = lockSnapshotRow(t, db.SqlcClient, sandboxID)
		})
		<-released

		require.NotNil(t, r.apiErr)
		assert.Equal(t, http.StatusServiceUnavailable, r.apiErr.Code)
		_, err := r.o.sandboxStore.Get(t.Context(), r.teamID, r.sandboxID)
		require.ErrorIs(t, err, sandbox.ErrNotFound)
		expiresIn := readExpiresIn(t, r.db.SqlcClient, r.sandboxID)
		require.NotNil(t, expiresIn)
		assert.LessOrEqual(t, *expiresIn, time.Hour, "the pause's expiry stays, unbounded")
	})
}
