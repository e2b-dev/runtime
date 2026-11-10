package redis

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// addRecord stores sbx through Add and returns its record key.
func addRecord(t *testing.T, storage *Storage, sbx sandboxtypes.Sandbox) string {
	t.Helper()

	require.NoError(t, storage.Add(t.Context(), sbx))

	return recordKey(sbx.SandboxID)
}

// Remove deletes the record, its index entry and its expiration member, for
// the execution the caller names or for any when it names none, and is a
// success on a sandbox already gone.
func TestRemoveNamedOrAnyExecution(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())

	for _, named := range []bool{true, false} {
		sbx := newRecord(teamID, fmt.Sprintf("sbx-%t", named))
		key := addRecord(t, storage, sbx)
		expected := ""
		if named {
			expected = sbx.ExecutionID
		}

		require.NoError(t, storage.Remove(t.Context(), teamID, sbx.SandboxID, expected))

		requireNoRecord(t, client, key)
		requireIndexed(t, client, index, sbx.SandboxID, false)
		requireMemberAbsent(t, client, sandboxExpirationMember(sbx))
		require.False(t, lockHeld(t, storage.locker, redis_utils.GetLockKey(key)))

		require.NoError(t, storage.Remove(t.Context(), teamID, sbx.SandboxID, expected), "a sandbox already gone")
	}
}

// A record delete that fails after the entry is removed is retried; when it
// keeps failing the record is left stranded for the healer,
// the anomaly counted once and the error returned, the lock released.
func TestRemoveStrandedCounter(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())
	errDelete := errors.New("delete failed")

	sbx := newRecord(teamID, "sbx-retried")
	key := addRecord(t, storage, sbx)
	failures := 0
	storage.SetFaultHook(func(p faultPoint) error {
		if p == faultRemoveAfterSREM && failures < 3 {
			failures++

			return errDelete
		}

		return nil
	})
	require.NoError(t, storage.Remove(t.Context(), teamID, sbx.SandboxID, sbx.ExecutionID))
	require.Equal(t, 3, failures)
	requireNoRecord(t, client, key)
	requireMemberAbsent(t, client, sandboxExpirationMember(sbx))
	require.Zero(t, writeAnomalies(t, reader, writeAnomalyRemoveStranded))

	// Every attempt fails: the entry gone, the record and its member left,
	// the anomaly counted once.
	sbx = newRecord(teamID, "sbx-stranded")
	key = addRecord(t, storage, sbx)
	attempts := 0
	storage.SetFaultHook(func(p faultPoint) error {
		if p == faultRemoveAfterSREM {
			attempts++

			return errDelete
		}

		return nil
	})
	require.ErrorIs(t, storage.Remove(t.Context(), teamID, sbx.SandboxID, sbx.ExecutionID), errDelete)
	require.Equal(t, 4, attempts)
	requireIndexed(t, client, index, sbx.SandboxID, false)
	requireRecord(t, client, key, mustJSON(t, sbx))
	requireMemberScore(t, client, sandboxExpirationMember(sbx), float64(sbx.EndTime.UnixMilli()))
	require.Equal(t, int64(1), writeAnomalies(t, reader, writeAnomalyRemoveStranded))
	require.False(t, lockHeld(t, storage.locker, redis_utils.GetLockKey(key)))
}

// An Add of the same sandbox ID waits for the lock a Remove of the record
// holds: its record appears, and the index entry the Remove took out is
// back, only after the Remove ends.
func TestRemoveSuccessorAddWaitsForLock(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	startNotifications(t, storage)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())
	old := newRecord(teamID, "sbx-reused")
	key := addRecord(t, storage, old)
	successor := newRecord(teamID, old.SandboxID)
	sandboxLock := lockKeyOf(old.SandboxID)

	// Remove stops between the record delete and the member removal, lock
	// held, until released.
	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	storage.SetFaultHook(func(p faultPoint) error {
		if p == faultRemoveAfterDelete {
			once.Do(func() { close(reached) })
			<-release
		}

		return nil
	})
	removeDone := make(chan error, 1)
	go func() { removeDone <- storage.Remove(t.Context(), teamID, old.SandboxID, old.ExecutionID) }()
	select {
	case <-reached:
	case err := <-removeDone:
		require.FailNow(t, "Remove ended before the fault point", "err: %v", err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "Remove did not reach the fault point")
	}

	addDone := make(chan error, 1)
	go func() { addDone <- storage.Add(t.Context(), successor) }()

	waitForLockWaiter(t, storage.subManager, getLockRoutingKey(sandboxLock), addDone)
	requireIndexed(t, client, index, old.SandboxID, false)
	requireNoRecord(t, client, key)
	requireMemberScore(t, client, sandboxExpirationMember(old), float64(old.EndTime.UnixMilli()))

	close(release)
	requireNoErrorFromChannel(t, removeDone)
	requireNoErrorFromChannel(t, addDone)

	requireRecord(t, client, key, mustJSON(t, successor))
	requireIndexed(t, client, index, old.SandboxID, true)
	requireMemberAbsent(t, client, sandboxExpirationMember(old))
	requireMemberScore(t, client, sandboxExpirationMember(successor), float64(successor.EndTime.UnixMilli()))
	requireLocksFree(t, storage, old.SandboxID)
}
