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

// addRecord stores sbx through Add in the given form and returns its key.
func addRecord(t *testing.T, storage *Storage, form keyForm, sbx sandboxtypes.Sandbox) string {
	t.Helper()

	storage.setWriteForm(form)
	require.NoError(t, storage.Add(t.Context(), sbx))

	return recordKey(form, sbx.TeamID.String(), sbx.SandboxID)
}

// Remove deletes the record, its index entry and its expiration member in
// either form, for the execution the caller names or for any when it names
// none, and is a success on a sandbox already gone.
func TestRemoveBothForms(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())

	for _, form := range []keyForm{formTeam, formSandbox} {
		for _, named := range []bool{true, false} {
			sbx := newRecord(teamID, fmt.Sprintf("sbx-%d-%t", form, named))
			key := addRecord(t, storage, form, sbx)
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
}

// A sandbox-form record delete that fails after the entry is removed is
// retried; when it keeps failing the record is left stranded for the healer,
// the anomaly counted once and the error returned, the lock released.
func TestRemoveStrandedCounter(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())
	errDelete := errors.New("delete failed")

	sbx := newRecord(teamID, "sbx-retried")
	key := addRecord(t, storage, formSandbox, sbx)
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
	key = addRecord(t, storage, formSandbox, sbx)
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

// An Add of the same sandbox ID under the other write form waits for the
// lock a Remove holds: its record appears, and the index entry the Remove
// took out is back, only after the Remove ends.
func TestRemoveSuccessorAddWaitsForLock(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	startNotifications(t, storage)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())
	old := newRecord(teamID, "sbx-reused")
	oldKey := addRecord(t, storage, formSandbox, old)
	successor := newRecord(teamID, old.SandboxID)
	successorKey := recordKey(formTeam, teamID.String(), old.SandboxID)
	teamLock, sandboxLock := lockKeys(teamID, old.SandboxID)

	// Remove stops between the SREM and the record delete, lock held, until
	// released.
	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	storage.SetFaultHook(func(p faultPoint) error {
		if p == faultRemoveAfterSREM {
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

	storage.setWriteForm(formTeam)
	addDone := make(chan error, 1)
	go func() { addDone <- storage.Add(t.Context(), successor) }()

	waitForLockWaiter(t, storage.subManager, getLockRoutingKey(sandboxLock), addDone)
	require.True(t, lockHeld(t, storage.locker, teamLock))
	requireIndexed(t, client, index, old.SandboxID, false)
	requireRecord(t, client, oldKey, mustJSON(t, old))
	requireNoRecord(t, client, successorKey)

	close(release)
	requireNoErrorFromChannel(t, removeDone)
	requireNoErrorFromChannel(t, addDone)

	requireNoRecord(t, client, oldKey)
	requireRecord(t, client, successorKey, mustJSON(t, successor))
	requireIndexed(t, client, index, old.SandboxID, true)
	requireMemberAbsent(t, client, sandboxExpirationMember(old))
	requireMemberScore(t, client, sandboxExpirationMember(successor), float64(successor.EndTime.UnixMilli()))
	requireLocksFree(t, storage, teamID, old.SandboxID)
}

// A Remove that resolved its form, then lost the race for that form's lock
// to a resume of the same ID writing the other form, finds its form empty
// and checks the other one under the lock it holds: the successor is a
// mismatch, left whole with its index entry and member, so Store.Remove
// keeps its reservation, rather than a sandbox already gone. Both
// directions of the move, each triggered on Remove's probe of the form the
// record is in: the sandbox-form GET, or the team-form EXISTS after it.
func TestRemoveJudgesSuccessorMovedToOtherForm(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())

	for _, from := range []keyForm{formTeam, formSandbox} {
		to, probe := otherFormAndProbe(from)
		old := newRecord(teamID, fmt.Sprintf("sbx-moved-%d", from))
		oldKey := addRecord(t, storage, from, old)
		successor := newRecord(teamID, old.SandboxID)
		successorKey := recordKey(to, teamID.String(), old.SandboxID)

		fired := landSuccessorOnProbe(t, client, storage, oldKey, probe, to, successor)

		err := storage.Remove(t.Context(), teamID, old.SandboxID, old.ExecutionID)

		require.ErrorIs(t, err, sandboxtypes.ErrExecutionMismatch)
		require.True(t, fired.Load(), "the resume did not land on the probe")
		requireNoRecord(t, client, oldKey)
		requireRecord(t, client, successorKey, mustJSON(t, successor))
		requireIndexed(t, client, index, old.SandboxID, true)
		requireMemberScore(t, client, sandboxExpirationMember(successor), float64(successor.EndTime.UnixMilli()))
		requireLocksFree(t, storage, teamID, old.SandboxID)
	}
}
