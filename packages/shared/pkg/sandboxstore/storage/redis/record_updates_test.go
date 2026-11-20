package redis

import (
	"context"
	"testing"
	"time"

	"github.com/bsm/redislock"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

// startedStorage is a running Storage on client; two of them on one client
// share nothing but Redis, as two processes would.
func startedStorage(t *testing.T, client redis.UniversalClient) *Storage {
	t.Helper()

	storage := newTestStorage(t, client)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })

	return storage
}

// beginWithin is BeginUpdate bounded by testPubSubWakeupTimeout, below the
// lock's first fallback retry: it returns in time only if the lock is free
// or its release notification wakes it.
func beginWithin(ctx context.Context, storage *Storage, teamID uuid.UUID, sandboxID string) (sandboxtypes.UpdateToken, error) {
	ctx, cancel := context.WithTimeout(ctx, testPubSubWakeupTimeout)
	defer cancel()

	_, token, err := storage.BeginUpdate(ctx, teamID, sandboxID)

	return token, err
}

func TestBeginUpdate_RefusalHoldsNoLock(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	held := heldSandbox(t, storage, "sbx-begin-held")
	require.NoError(t, holdExecution(t, storage, held, uuid.NewString(), time.Now().Add(time.Minute)))

	for _, tc := range []struct {
		name      string
		teamID    uuid.UUID
		sandboxID string
		want      error
	}{
		{name: "absent", teamID: uuid.New(), sandboxID: "sbx-begin-absent", want: sandboxtypes.ErrNotFound},
		{name: "held", teamID: held.TeamID, sandboxID: held.SandboxID, want: sandboxtypes.ErrLifecycleHeld},
	} {
		lockKey := redis_utils.GetLockKey(recordKey(tc.sandboxID))
		raw := client.Get(t.Context(), recordKey(tc.sandboxID)).Val()

		_, token, err := storage.BeginUpdate(t.Context(), tc.teamID, tc.sandboxID)
		require.ErrorIs(t, err, tc.want, tc.name)
		assert.Empty(t, token, tc.name)
		assert.False(t, lockHeld(t, storage.locker, lockKey), "%s: the lock is not left held", tc.name)

		_, err = beginWithin(t.Context(), storage, tc.teamID, tc.sandboxID)
		require.ErrorIs(t, err, tc.want, "%s: a following begin does not wait", tc.name)
		assert.Equal(t, raw, client.Get(t.Context(), recordKey(tc.sandboxID)).Val(), "%s: nothing written", tc.name)
	}
}

func TestAbortUpdate_WritesNothingAndReleasesTheLock(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	sbx := newRecord(uuid.New(), "sbx-abort")
	key := addRecord(t, storage, sbx)
	raw := client.Get(t.Context(), key).Val()

	_, token, err := storage.BeginUpdate(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	require.True(t, lockHeld(t, storage.locker, redis_utils.GetLockKey(key)))

	require.NoError(t, storage.AbortUpdate(t.Context(), sbx.TeamID, sbx.SandboxID, token))
	assert.False(t, lockHeld(t, storage.locker, redis_utils.GetLockKey(key)))
	requireRecord(t, client, key, raw)

	require.ErrorIs(t, storage.AbortUpdate(t.Context(), sbx.TeamID, sbx.SandboxID, token), redislock.ErrLockNotHeld, "a second abort finds the lock released")
	require.ErrorIs(t, storage.AbortUpdate(t.Context(), sbx.TeamID, sbx.SandboxID, token), sandboxtypes.ErrLockLost, "a released lock is a lost lock")
}

func TestCommitUpdate_RescoresOnlyAChangedEndTime(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	sbx := newRecord(uuid.New(), "sbx-commit")
	key := addRecord(t, storage, sbx)

	cur, token, err := storage.BeginUpdate(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	cur.EndTime = cur.EndTime.Add(time.Hour)
	committed, err := storage.CommitUpdate(t.Context(), sbx.TeamID, sbx.SandboxID, token, cur)
	require.NoError(t, err)
	requireRecord(t, client, key, mustJSON(t, committed))
	requireMemberScore(t, client, sandboxExpirationMember(committed), float64(committed.EndTime.UnixMilli()))
	assert.False(t, lockHeld(t, storage.locker, redis_utils.GetLockKey(key)))

	// A score the commit would overwrite if it re-scored.
	const sentinel = 42
	require.NoError(t, client.ZAdd(t.Context(), globalExpirationSet, redis.Z{Score: sentinel, Member: sandboxExpirationMember(committed)}).Err())
	cur, token, err = storage.BeginUpdate(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	cur.TemplateID = "changed"
	committed, err = storage.CommitUpdate(t.Context(), sbx.TeamID, sbx.SandboxID, token, cur)
	require.NoError(t, err)
	requireRecord(t, client, key, mustJSON(t, committed))
	requireMemberScore(t, client, sandboxExpirationMember(committed), sentinel)
	assert.False(t, lockHeld(t, storage.locker, redis_utils.GetLockKey(key)))
}

func TestCommitUpdate_LostLockWritesNothing(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	sbx := newRecord(uuid.New(), "sbx-commit-lost")
	key := addRecord(t, storage, sbx)
	lockKey := redis_utils.GetLockKey(key)
	raw := client.Get(t.Context(), key).Val()

	cur, token, err := storage.BeginUpdate(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	require.NoError(t, client.Del(t.Context(), lockKey).Err())
	other, err := storage.locker.Obtain(t.Context(), lockKey, lockTimeout)
	require.NoError(t, err)
	otherValue := lockValueOf(other)

	cur.EndTime = cur.EndTime.Add(time.Hour)
	_, err = storage.CommitUpdate(t.Context(), sbx.TeamID, sbx.SandboxID, token, cur)
	require.ErrorIs(t, err, sandboxtypes.ErrLockLost)
	requireRecord(t, client, key, raw)
	requireMemberScore(t, client, sandboxExpirationMember(sbx), float64(sbx.EndTime.UnixMilli()))
	assert.Equal(t, otherValue, client.Get(t.Context(), lockKey).Val(), "the other holder's lock stands")
	require.NoError(t, other.Release(t.Context()))
}

func TestRecordUpdates_TokenWorksOnAnotherInstance(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	first, second := startedStorage(t, client), startedStorage(t, client)
	sbx := newRecord(uuid.New(), "sbx-cross-instance")
	key := addRecord(t, first, sbx)
	raw := client.Get(t.Context(), key).Val()

	_, token, err := first.BeginUpdate(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	require.NoError(t, second.AbortUpdate(t.Context(), sbx.TeamID, sbx.SandboxID, token))
	assert.False(t, lockHeld(t, first.locker, redis_utils.GetLockKey(key)))
	requireRecord(t, client, key, raw)

	cur, token, err := first.BeginUpdate(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	cur.EndTime = cur.EndTime.Add(time.Hour)
	committed, err := second.CommitUpdate(t.Context(), sbx.TeamID, sbx.SandboxID, token, cur)
	require.NoError(t, err)
	requireRecord(t, client, key, mustJSON(t, committed))
	requireMemberScore(t, client, sandboxExpirationMember(committed), float64(committed.EndTime.UnixMilli()))
	assert.False(t, lockHeld(t, first.locker, redis_utils.GetLockKey(key)))
}

func TestCommitUpdate_WakesAWaitingBegin(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)
	sbx := newRecord(uuid.New(), "sbx-commit-wakes")
	key := addRecord(t, storage, sbx)
	routingKey := getLockRoutingKey(redis_utils.GetLockKey(key))

	cur, token, err := storage.BeginUpdate(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)

	waiterDone := make(chan error, 1)
	go func() {
		waiterToken, beginErr := beginWithin(t.Context(), storage, sbx.TeamID, sbx.SandboxID)
		if beginErr != nil {
			waiterDone <- beginErr

			return
		}

		waiterDone <- storage.AbortUpdate(context.WithoutCancel(t.Context()), sbx.TeamID, sbx.SandboxID, waiterToken)
	}()
	waitForLockWaiter(t, storage.subManager, routingKey, waiterDone)

	_, err = storage.CommitUpdate(t.Context(), sbx.TeamID, sbx.SandboxID, token, cur)
	require.NoError(t, err)
	requireNoErrorFromChannel(t, waiterDone)
}

func TestRecordUpdates_InvalidTokenWritesNothing(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	sbx := newRecord(uuid.New(), "sbx-invalid-token")
	key := addRecord(t, storage, sbx)
	raw := client.Get(t.Context(), key).Val()

	cur, token, err := storage.BeginUpdate(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	cur.EndTime = cur.EndTime.Add(time.Hour)

	for _, invalid := range []sandboxtypes.UpdateToken{"", "not a token", `{"lock":""}`, `{"lock":1}`} {
		_, err := storage.CommitUpdate(t.Context(), sbx.TeamID, sbx.SandboxID, invalid, cur)
		require.ErrorIs(t, err, sandboxtypes.ErrInvalidUpdateToken, "commit %q", invalid)
		require.ErrorIs(t, storage.AbortUpdate(t.Context(), sbx.TeamID, sbx.SandboxID, invalid), sandboxtypes.ErrInvalidUpdateToken, "abort %q", invalid)
	}
	requireRecord(t, client, key, raw)
	requireMemberScore(t, client, sandboxExpirationMember(sbx), float64(sbx.EndTime.UnixMilli()))
	require.True(t, lockHeld(t, storage.locker, redis_utils.GetLockKey(key)), "an invalid token releases nothing")

	require.NoError(t, storage.AbortUpdate(t.Context(), sbx.TeamID, sbx.SandboxID, token))
}
