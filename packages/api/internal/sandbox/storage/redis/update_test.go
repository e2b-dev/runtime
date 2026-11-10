package redis

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// Get reads a record through Add back as it was written; a sandbox without
// a record is not found.
func TestGetReadsTheRecord(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	teamID := uuid.New()

	sbx := newRecord(teamID, "sbx-stored")
	addRecord(t, storage, sbx)
	got, err := storage.Get(t.Context(), teamID, sbx.SandboxID)
	require.NoError(t, err)
	require.Equal(t, mustJSON(t, sbx), mustJSON(t, got))

	_, err = storage.Get(t.Context(), teamID, "sbx-none")
	require.ErrorIs(t, err, sandboxtypes.ErrNotFound)
}

// A record is updated under its lock, in place and with its member rescored.
func TestUpdateLocksTheRecord(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	teamID := uuid.New()
	sbx := newRecord(teamID, "sbx-updated")
	key := addRecord(t, storage, sbx)

	updated, err := storage.Update(t.Context(), teamID, sbx.SandboxID, func(cur sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
		require.True(t, lockHeld(t, storage.locker, redis_utils.GetLockKey(key)), "the record's lock is not held")
		cur.EndTime = cur.EndTime.Add(time.Hour)

		return cur, nil
	})
	require.NoError(t, err)

	requireRecord(t, client, key, mustJSON(t, updated))
	requireMemberScore(t, client, sandboxExpirationMember(updated), float64(updated.EndTime.UnixMilli()))
	require.False(t, lockHeld(t, storage.locker, redis_utils.GetLockKey(key)))
}
