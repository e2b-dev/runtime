package redis

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// sandboxKeys returns every per-sandbox key: the record, its lock, the
// transition marker, a transition result and its refusal class.
func sandboxKeys(sandboxID, transitionID string) []string {
	record := recordKey(sandboxID)
	result := transitionResultKeyFor(sandboxID, transitionID)

	return []string{
		record,
		redis_utils.GetLockKey(record),
		transitionKeyFor(sandboxID),
		result,
		refusalClassKey(result),
	}
}

// Every key carries exactly one hash tag, and a sandbox's keys share a slot
// of their own, so the scripts and the MGET that span them stay single-slot
// while two sandboxes' records are spread over the cluster.
func TestSandboxKeysShareOneSlot(t *testing.T) {
	t.Parallel()

	const sandboxA, sandboxB, transition = "sbx-a", "sbx-b", "tx-1"

	keys := sandboxKeys(sandboxA, transition)
	for _, key := range keys {
		open, closing := strings.Index(key, "{"), strings.Index(key, "}")
		assert.Equal(t, 1, strings.Count(key, "{"), key)
		assert.Equal(t, 1, strings.Count(key, "}"), key)
		assert.Less(t, open+1, closing, "empty or reversed tag in %q", key)
	}
	assert.True(t, redis_utils.AllSameSlot(keys...), "a sandbox's keys span slots: %q", keys)
	assert.NotEqual(t,
		redis_utils.HashSlot(recordKey(sandboxA)),
		redis_utils.HashSlot(recordKey(sandboxB)),
		"two sandboxes' records share a slot")
}

// An empty ID would make Redis hash the whole key, and a brace would move or
// end the tag; either splits a sandbox's keys across slots, so the builders
// refuse such IDs instead of producing a key.
func TestBuildersRejectBadIDs(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{"", "a{b", "a}b"} {
		assert.Panics(t, func() { sandboxPrefix(bad) }, "sandboxPrefix(%q)", bad)
		assert.Panics(t, func() { recordKey(bad) }, "recordKey(%q)", bad)
		assert.Panics(t, func() { transitionKeyFor(bad) }, "transitionKeyFor(%q)", bad)
		assert.Panics(t, func() { transitionResultKeyFor(bad, "tx") }, "transitionResultKeyFor(%q)", bad)
	}
}

// A record of another team is not found by the pre-lock probe, so no caller
// takes the owner's lock for it: while the owner's lock is held, Get,
// Update, StartRemoving, Remove and WaitForStateChange by another team
// answer as for an absent sandbox instead of waiting for the lock or the
// transition, run no closure, start no transition, and touch neither the
// record, the owner's index entry and expiration member, nor the caller's
// index. The owner reads the record.
func TestOtherTeamIsRefused(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	owner, caller := uuid.New(), uuid.New()
	sbx := newRecord(owner, "sbx-held")
	key := addRecord(t, storage, sbx)

	// The held lock carries no team: a call that reached it would wait out
	// the context and fail with another error than the guard's. The owner's
	// transition marker is what a caller joining the wait would follow.
	held, err := storage.locker.Obtain(t.Context(), redis_utils.GetLockKey(key), time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, held.Release(context.WithoutCancel(t.Context()))) })
	require.NoError(t, client.Set(t.Context(), transitionKeyFor(sbx.SandboxID), uuid.NewString(), transitionKeyTTL).Err())
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	for _, tc := range []struct {
		name string
		call func() error
		want error
	}{
		{"Get", func() error {
			_, err := storage.Get(ctx, caller, sbx.SandboxID)

			return err
		}, sandboxtypes.ErrNotFound},
		{"Update", func() error {
			_, err := storage.Update(ctx, caller, sbx.SandboxID, func(cur sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
				require.FailNow(t, "Update ran the closure on another team's record")

				return cur, nil
			})

			return err
		}, sandboxtypes.ErrNotFound},
		{"StartRemoving", func() error {
			_, alreadyDone, finish, err := storage.StartRemoving(ctx, caller, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
			assert.False(t, alreadyDone)
			assert.Nil(t, finish)

			return err
		}, sandboxtypes.ErrNotFound},
		{"Remove", func() error { return storage.Remove(ctx, caller, sbx.SandboxID, "") }, nil},
		{"Remove of the execution", func() error { return storage.Remove(ctx, caller, sbx.SandboxID, sbx.ExecutionID) }, nil},
		{"WaitForStateChange", func() error { return storage.WaitForStateChange(ctx, caller, sbx.SandboxID) }, sandboxtypes.ErrNotFound},
	} {
		if tc.want == nil {
			require.NoError(t, tc.call(), tc.name)
		} else {
			require.ErrorIs(t, tc.call(), tc.want, tc.name)
		}
		requireRecord(t, client, key, mustJSON(t, sbx))
		requireIndexed(t, client, GetSandboxStorageTeamIndexKey(owner.String()), sbx.SandboxID, true)
		requireIndexed(t, client, GetSandboxStorageTeamIndexKey(caller.String()), sbx.SandboxID, false)
		requireMemberScore(t, client, sandboxExpirationMember(sbx), float64(sbx.EndTime.UnixMilli()))
	}

	got, err := storage.Get(t.Context(), owner, sbx.SandboxID)
	require.NoError(t, err)
	require.Equal(t, mustJSON(t, sbx), mustJSON(t, got))
}
