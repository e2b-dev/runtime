package orchestrator

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

// paused leaves the sandbox as its pause would: the snapshot row with the
// TTL's expiry, and no record.
func (f refusalFixture) paused(t *testing.T, ttl *time.Duration) {
	t.Helper()

	f.pausedWithTTL(t, ttl, ttl)
	f.o.sandboxStore.Remove(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, f.sbx.ExecutionID)
}

func (f refusalFixture) cachedPausedTTL(t *testing.T) *int64 {
	t.Helper()

	cached, err := snapshotcache.NewSnapshotCache(f.o.sqlcDB, f.client).Get(t.Context(), f.sbx.SandboxID)
	require.NoError(t, err)

	return cached.Snapshot.Config.PausedTTL
}

// waitingOnRowLock reports a query of the fixture's database blocked on a row
// lock.
func waitingOnRowLock(t *testing.T, f refusalFixture) bool {
	t.Helper()

	var waiting int
	err := f.o.sqlcDB.TestsRawSQLQuery(t.Context(),
		`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`,
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&waiting)
		},
	)
	require.NoError(t, err)

	return waiting > 0
}

func TestSetPausedTTL(t *testing.T) {
	t.Parallel()

	hour := time.Hour
	day := 24 * time.Hour

	// A run end whose stamp fails leaves the bound, which must not stay at the
	// old, shorter TTL.
	t.Run("a running sandbox keeps the TTL on its record and moves its bound", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		f.pausedWithTTL(t, &hour, &hour)
		f.resumedWithBound(t)

		require.Nil(t, f.o.SetPausedTTL(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, &day))

		record, err := f.o.GetSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.NoError(t, err)
		assert.Equal(t, &day, record.PausedTTL)
		assertExpiresIn(t, time.Until(record.StartTime.Add(record.MaxInstanceLength+day)), readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))
	})

	t.Run("a running sandbox can have its TTL removed", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		f.pausedWithTTL(t, &hour, &hour)

		f.resumedWithBound(t)

		require.Nil(t, f.o.SetPausedTTL(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, nil))

		record, err := f.o.GetSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.NoError(t, err)
		assert.Nil(t, record.PausedTTL)
		assert.True(t, record.PausedTTLStored, "the run must still clear the stored expiry when it ends")
		assert.Nil(t, readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))
	})

	t.Run("a running sandbox that was never paused takes the TTL on its record", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)

		require.Nil(t, f.o.SetPausedTTL(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, &day), "no snapshot to bound yet is not an error")

		record, err := f.o.GetSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.NoError(t, err)
		assert.Equal(t, &day, record.PausedTTL, "its first pause stores the TTL from the record")
	})

	// Its resume read no TTL, so nothing marked the snapshot as holding one;
	// the TTL set during the run puts a bound there that the clear and the
	// run's end must still remove.
	t.Run("a TTL set and removed during a run leaves no expiry", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		f.pausedWithTTL(t, nil, nil)

		require.Nil(t, f.o.SetPausedTTL(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, &hour))
		require.NotNil(t, readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))

		require.Nil(t, f.o.SetPausedTTL(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, nil))
		assert.Nil(t, readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))

		f.kill(t)
		assert.Nil(t, readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))
		assert.Nil(t, f.cachedPausedTTL(t))
	})

	// A pause that took the record between its update and the bound would
	// stamp its expiry and then have the later bound overwrite it.
	t.Run("the record stays locked until the bound is written", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		f.pausedWithTTL(t, &hour, &hour)

		holder, err := f.o.sqlcDB.Pool().Begin(t.Context())
		require.NoError(t, err)
		t.Cleanup(func() { _ = holder.Rollback(context.WithoutCancel(t.Context())) })
		_, err = holder.Exec(t.Context(), `SELECT 1 FROM public.snapshots WHERE sandbox_id = $1 FOR UPDATE`, f.sbx.SandboxID)
		require.NoError(t, err)

		done := make(chan *api.APIError, 1)
		go func() { done <- f.o.SetPausedTTL(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, &day) }()
		require.Eventually(t, func() bool { return waitingOnRowLock(t, f) }, 3*time.Second, 10*time.Millisecond)

		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		_, err = f.o.sandboxStore.Update(ctx, f.sbx.TeamID, f.sbx.SandboxID, func(s sandbox.Sandbox) (sandbox.Sandbox, error) { return s, nil })
		require.Error(t, err, "the record must stay locked while the bound waits on the database")

		require.NoError(t, holder.Rollback(t.Context()))
		require.Nil(t, <-done)
	})

	t.Run("a paused sandbox restarts its clock with the new TTL", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		f.paused(t, &hour)
		require.Equal(t, int64(3600), *f.cachedPausedTTL(t))

		require.Nil(t, f.o.SetPausedTTL(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, &day))

		assertExpiresIn(t, 24*time.Hour, readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))
		require.NotNil(t, f.cachedPausedTTL(t))
		assert.Equal(t, int64(86400), *f.cachedPausedTTL(t))
	})

	t.Run("a paused sandbox without a TTL gets an expiry", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		f.paused(t, nil)

		require.Nil(t, f.o.SetPausedTTL(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, &hour))

		assertExpiresIn(t, time.Hour, readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))
	})

	t.Run("removing a paused sandbox's TTL clears its expiry", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		f.paused(t, &hour)
		require.NotNil(t, f.cachedPausedTTL(t))

		require.Nil(t, f.o.SetPausedTTL(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, nil))

		assert.Nil(t, readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))
		assert.Nil(t, f.cachedPausedTTL(t))
	})

	t.Run("another team's sandbox is not found", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		f.paused(t, &hour)

		apiErr := f.o.SetPausedTTL(t.Context(), uuid.New(), f.sbx.SandboxID, &day)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusNotFound, apiErr.Code)
		assertExpiresIn(t, time.Hour, readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))
	})

	t.Run("an unknown sandbox is not found", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)

		apiErr := f.o.SetPausedTTL(t.Context(), f.sbx.TeamID, "sbx-unknown-"+uuid.NewString()[:8], &day)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusNotFound, apiErr.Code)
	})

	// The pause has already taken the TTL it stamps, so a change now would
	// be lost.
	t.Run("a pausing sandbox is refused", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		f.pausedWithTTL(t, &hour, &hour)
		_, err := f.o.sandboxStore.Update(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, func(s sandbox.Sandbox) (sandbox.Sandbox, error) {
			s.State = sandbox.StatePausing

			return s, nil
		})
		require.NoError(t, err)

		apiErr := f.o.SetPausedTTL(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, &day)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusConflict, apiErr.Code)

		record, err := f.o.GetSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.NoError(t, err)
		assert.Equal(t, &hour, record.PausedTTL)
	})

	// A resume that read the snapshot before the write would run with the old
	// TTL and stamp it back when the run ends.
	t.Run("a resume in flight gets the new TTL on its record and bound", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
		f.paused(t, &hour)

		finishStart, _, err := f.o.sandboxStore.Reserve(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, 10)
		require.NoError(t, err)
		require.NotNil(t, finishStart)

		done := make(chan *api.APIError, 1)
		go func() { done <- f.o.SetPausedTTL(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, &day) }()

		require.Eventually(t, func() bool {
			expiresIn := readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID)

			return expiresIn != nil && *expiresIn > 23*time.Hour
		}, 10*time.Second, 10*time.Millisecond)
		select {
		case <-done:
			require.Fail(t, "returned before the start in flight finished")
		default:
		}

		resumed := f.sbx
		resumed.ExecutionID = uuid.NewString()
		resumed.PausedTTL = &hour
		resumed.PausedTTLStored = true
		require.NoError(t, f.o.boundPausedExpiry(t.Context(), resumed))
		require.NoError(t, f.o.sandboxStore.Add(t.Context(), resumed, nil))
		finishStart(resumed, nil)

		select {
		case apiErr := <-done:
			require.Nil(t, apiErr)
		case <-time.After(10 * time.Second):
			require.Fail(t, "did not return after the start finished")
		}

		record, err := f.o.GetSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.NoError(t, err)
		assert.Equal(t, &day, record.PausedTTL)
		assertExpiresIn(t, time.Until(record.StartTime.Add(record.MaxInstanceLength+day)), readExpiresIn(t, f.o.sqlcDB, f.sbx.SandboxID))
	})
}
