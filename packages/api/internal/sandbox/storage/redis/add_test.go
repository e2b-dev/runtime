package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// newMeteredTestStorage returns a storage whose metrics reader collects.
func newMeteredTestStorage(t *testing.T, client redis.UniversalClient) (*Storage, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
	storage, err := NewStorage(client, provider)
	require.NoError(t, err)

	return storage, reader
}

// startNotifications runs the storage's publisher and subscription manager
// until the test ends, so lock waiters wake on release.
func startNotifications(t *testing.T, s *Storage) {
	t.Helper()

	go s.publisher.run(t.Context())
	t.Cleanup(func() { s.publisher.close(context.WithoutCancel(t.Context())) })
	go s.subManager.start(t.Context())
	t.Cleanup(s.subManager.close)
}

// counterByAttr returns the count a counter holds under one attribute.
func counterByAttr(t *testing.T, reader *sdkmetric.ManualReader, name string, attr attribute.KeyValue) int64 {
	t.Helper()

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))

	return counterPoints(t, collected, name)[attrs(attr)]
}

// writeAnomalies returns the count of write anomalies of one kind.
func writeAnomalies(t *testing.T, reader *sdkmetric.ManualReader, kind string) int64 {
	t.Helper()

	return counterByAttr(t, reader, string(telemetry.ApiRedisStorageWriteAnomalies), attribute.String(writeAnomalyKindAttr, kind))
}

func mustJSON(t *testing.T, sbx sandboxtypes.Sandbox) string {
	t.Helper()

	data, err := json.Marshal(sbx)
	require.NoError(t, err)

	return string(data)
}

var errInjected = errors.New("injected failure")

// failAt returns a hook that injects err at point p.
func failAt(p faultPoint, err error) func(faultPoint) error {
	return func(point faultPoint) error {
		if point == p {
			return err
		}

		return nil
	}
}

func lockKeys(teamID uuid.UUID, sandboxID string) (team, sandbox string) {
	return redis_utils.GetLockKey(recordKey(formTeam, teamID.String(), sandboxID)),
		redis_utils.GetLockKey(recordKey(formSandbox, teamID.String(), sandboxID))
}

// requireLocksFree asserts that neither form's lock of the sandbox is held.
func requireLocksFree(t *testing.T, s *Storage, teamID uuid.UUID, sandboxID string) {
	t.Helper()

	teamLock, sandboxLock := lockKeys(teamID, sandboxID)
	require.Falsef(t, lockHeld(t, s.locker, teamLock), "lock %s is held", teamLock)
	require.Falsef(t, lockHeld(t, s.locker, sandboxLock), "lock %s is held", sandboxLock)
}

// Add writes the record in the write form with its index entry, its
// expiration member and its team's global entry, and nothing in the other
// form: a record of the same sandbox left there, as a crashed predecessor
// leaves one, is gone. That the delete precedes the write is shown by
// TestAddRecordWriteFailureCleansUp.
func TestAddWritesSelectedForm(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())

	for _, form := range []keyForm{formTeam, formSandbox} {
		storage.setWriteForm(form)
		sbx := newRecord(teamID, fmt.Sprintf("sbx-%d", form))
		stale := recordKey(form.other(), teamID.String(), sbx.SandboxID)
		writeRecord(t, client, stale, "", newRecord(teamID, sbx.SandboxID))

		require.NoError(t, storage.Add(t.Context(), sbx))

		requireRecord(t, client, recordKey(form, teamID.String(), sbx.SandboxID), mustJSON(t, sbx))
		requireNoRecord(t, client, stale)
		requireIndexed(t, client, index, sbx.SandboxID, true)
		requireMemberScore(t, client, sandboxExpirationMember(sbx), float64(sbx.EndTime.UnixMilli()))
		require.NoError(t, client.ZScore(t.Context(), globalTeamsSet, teamID.String()).Err())
	}
}

// Add waits for the sandbox's lock in either form, and takes the team form's
// first, so a predecessor's operation under either lock finishes before it
// writes.
func TestAddTakesBothLocks(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	startNotifications(t, storage)
	teamID := uuid.New()

	for _, held := range []keyForm{formTeam, formSandbox} {
		sbx := newRecord(teamID, fmt.Sprintf("sbx-%d", held))
		teamLock, sandboxLock := lockKeys(teamID, sbx.SandboxID)
		heldLock := redis_utils.GetLockKey(recordKey(held, teamID.String(), sbx.SandboxID))

		holder, err := storage.locker.Obtain(t.Context(), heldLock, testLockTimeout)
		require.NoError(t, err)

		done := make(chan error, 1)
		go func() { done <- storage.Add(t.Context(), sbx) }()

		waitForLockWaiter(t, storage.subManager, getLockRoutingKey(heldLock), done)
		if held == formTeam {
			require.False(t, lockHeld(t, storage.locker, sandboxLock), "the sandbox-form lock was taken before the team-form one")
		} else {
			require.True(t, lockHeld(t, storage.locker, teamLock), "the team-form lock is not held while the sandbox-form one is awaited")
		}
		requireNoRecord(t, client, recordKey(formTeam, teamID.String(), sbx.SandboxID))

		require.NoError(t, holder.Release(context.WithoutCancel(t.Context())))
		requireNoErrorFromChannel(t, done)
		requireRecord(t, client, recordKey(formTeam, teamID.String(), sbx.SandboxID), mustJSON(t, sbx))
		requireLocksFree(t, storage, teamID, sbx.SandboxID)
	}
}

// An index entry that fails after the record is written is retried; when
// it keeps failing, Add returns success with the record unindexed and counts
// the anomaly: the sandbox is reachable by ID, and the healer indexes it.
func TestAddIndexFailureIsRepairableSuccess(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	storage.setWriteForm(formSandbox)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())
	errIndex := errors.New("SADD failed")

	sbx := newRecord(teamID, "sbx-retried")
	failures := 0
	storage.SetFaultHook(func(p faultPoint) error {
		if p == faultAddAfterRecordWrite && failures < addIndexRetries {
			failures++

			return errIndex
		}

		return nil
	})
	require.NoError(t, storage.Add(t.Context(), sbx))
	require.Equal(t, addIndexRetries, failures)
	requireIndexed(t, client, index, sbx.SandboxID, true)
	require.Zero(t, writeAnomalies(t, reader, writeAnomalyAddUnindexed))

	sbx = newRecord(teamID, "sbx-unindexed")
	attempts := 0
	storage.SetFaultHook(func(p faultPoint) error {
		if p == faultAddAfterRecordWrite {
			attempts++

			return errIndex
		}

		return nil
	})
	require.NoError(t, storage.Add(t.Context(), sbx))
	require.Equal(t, addIndexRetries+1, attempts)
	requireRecord(t, client, recordKey(formSandbox, teamID.String(), sbx.SandboxID), mustJSON(t, sbx))
	requireIndexed(t, client, index, sbx.SandboxID, false)
	require.Equal(t, int64(1), writeAnomalies(t, reader, writeAnomalyAddUnindexed))
}

// A record write that fails before it runs leaves nothing of this Add: no
// record in either form, the other form's stale record already deleted, no
// index entry, even one the healer had added for the deleted record; Add
// returns the error and releases the locks.
func TestAddRecordWriteFailureCleansUp(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())
	errWrite := errors.New("write failed")
	storage.SetFaultHook(failAt(faultAddAfterOtherDel, errWrite))

	for _, form := range []keyForm{formTeam, formSandbox} {
		storage.setWriteForm(form)
		sbx := newRecord(teamID, fmt.Sprintf("sbx-%d", form))
		stale := recordKey(form.other(), teamID.String(), sbx.SandboxID)
		writeRecord(t, client, stale, index, newRecord(teamID, sbx.SandboxID))

		require.ErrorIs(t, storage.Add(t.Context(), sbx), errWrite)

		requireNoRecord(t, client, recordKey(form, teamID.String(), sbx.SandboxID))
		requireNoRecord(t, client, stale)
		requireIndexed(t, client, index, sbx.SandboxID, false)
		requireLocksFree(t, storage, teamID, sbx.SandboxID)
	}
}

// A predecessor's record in the other form must survive a fault at
// faultAddBeforeOtherDel, which sits before Add's DEL of it. The fault
// matrix cannot tell: its predecessor was paused out before the Add.
func TestAddFaultBeforeOtherDelSparesOtherFormRecord(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	teamID := uuid.New()
	for _, form := range []keyForm{formTeam, formSandbox} {
		storage.setWriteForm(form)
		sbx := newRecord(teamID, fmt.Sprintf("sbx-adv-%d", form))
		otherKey := recordKey(form.other(), teamID.String(), sbx.SandboxID)
		data := writeRecord(t, client, otherKey, "", newRecord(teamID, sbx.SandboxID))
		storage.SetFaultHook(failAt(faultAddBeforeOtherDel, errInjected))

		require.ErrorIs(t, storage.Add(t.Context(), sbx), errInjected)

		requireRecord(t, client, otherKey, data)
	}
}

// A write whose reply is lost after it landed is undone in either form the
// way every removal is ordered, the index entry first, then the guarded
// record delete, which in the team form is the one script that wrote both.
// The entry a healer re-index of a crashed predecessor had made, whose
// other-form record this Add deleted before the write, goes with it:
// neither form holds a record any more.
func TestAddLostReplyCleansUp(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())
	errLost := errors.New("reply lost")

	for _, form := range []keyForm{formTeam, formSandbox} {
		storage.setWriteForm(form)
		sbx := newRecord(teamID, fmt.Sprintf("sbx-%d", form))
		key := recordKey(form, teamID.String(), sbx.SandboxID)
		predecessor := recordKey(form.other(), teamID.String(), sbx.SandboxID)
		writeRecord(t, client, predecessor, index, newRecord(teamID, sbx.SandboxID))
		storage.SetFaultHook(func(p faultPoint) error {
			if p != faultAddRecordWrite {
				return nil
			}
			// The write landed, the predecessor is gone and the entry is
			// there, the team form's script's or the predecessor's; the
			// reply is lost now.
			requireRecord(t, client, key, mustJSON(t, sbx))
			requireNoRecord(t, client, predecessor)
			requireIndexed(t, client, index, sbx.SandboxID, true)

			return errLost
		})

		require.ErrorIs(t, storage.Add(t.Context(), sbx), errLost)

		requireNoRecord(t, client, key)
		requireNoRecord(t, client, predecessor)
		requireIndexed(t, client, index, sbx.SandboxID, false)
	}
}

// A caller gone at the commit point, its context cancelled as the write's
// reply is read, does not spare the cleanup: the record and its index entry
// go in either form, nothing is counted, the locks are released and the
// error returned.
func TestAddCleanupRunsPastCancellation(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())

	for _, form := range []keyForm{formTeam, formSandbox} {
		storage.setWriteForm(form)
		sbx := newRecord(teamID, fmt.Sprintf("sbx-%d", form))
		key := recordKey(form, teamID.String(), sbx.SandboxID)
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		storage.SetFaultHook(func(p faultPoint) error {
			if p != faultAddRecordWrite {
				return nil
			}
			cancel()

			return ctx.Err()
		})

		require.ErrorIs(t, storage.Add(ctx, sbx), context.Canceled)

		requireNoRecord(t, client, key)
		requireIndexed(t, client, index, sbx.SandboxID, false)
		require.Zero(t, writeAnomalies(t, reader, writeAnomalyAddWriteUnconfirmed))
		requireLocksFree(t, storage, teamID, sbx.SandboxID)
	}
}

// A caller gone once the record is committed, its context cancelled before
// the steps that index the sandbox and list its team, does not leave them
// undone: the index entry and the team's global entry are still written,
// nothing is counted, the locks are released and Add succeeds. The team
// form writes the entry with the record, so there the caller goes before
// the global entry.
func TestAddFinishesPastCancellation(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)

	for _, form := range []keyForm{formTeam, formSandbox} {
		storage.setWriteForm(form)
		teamID := uuid.New()
		sbx := newRecord(teamID, fmt.Sprintf("sbx-%d", form))
		point := faultAddAfterIndex
		if form == formSandbox {
			point = faultAddAfterRecordWrite
		}
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		storage.SetFaultHook(func(p faultPoint) error {
			if p == point {
				cancel()
			}

			return nil
		})

		require.NoError(t, storage.Add(ctx, sbx))

		requireRecord(t, client, recordKey(form, teamID.String(), sbx.SandboxID), mustJSON(t, sbx))
		requireIndexed(t, client, GetSandboxStorageTeamIndexKey(teamID.String()), sbx.SandboxID, true)
		require.NoError(t, client.ZScore(t.Context(), globalTeamsSet, teamID.String()).Err())
		require.Zero(t, writeAnomalies(t, reader, writeAnomalyAddUnindexed))
		requireLocksFree(t, storage, teamID, sbx.SandboxID)
	}
}

// A write that never ran may leave a stranded predecessor's record under the
// key it would have written, re-indexed by the healer; the cleanup, guarded
// by this Add's execution, leaves that record and its entry whole.
func TestAddCleanupSparesStrandedPredecessor(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())
	errWrite := errors.New("write failed")
	storage.SetFaultHook(failAt(faultAddAfterOtherDel, errWrite))

	for _, form := range []keyForm{formTeam, formSandbox} {
		storage.setWriteForm(form)
		sbx := newRecord(teamID, fmt.Sprintf("sbx-%d", form))
		key := recordKey(form, teamID.String(), sbx.SandboxID)
		stranded := writeRecord(t, client, key, index, newRecord(teamID, sbx.SandboxID))

		require.ErrorIs(t, storage.Add(t.Context(), sbx), errWrite)

		requireRecord(t, client, key, stranded)
		requireIndexed(t, client, index, sbx.SandboxID, true)
	}
}

// When the cleanup of an unconfirmed write fails too, the write error is
// still returned and the anomaly counted once; what the cleanup did not
// reach, here the record behind an index key of the wrong type, stays for
// the healer and the evictor.
func TestAddCleanupFailureIsCounted(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	storage.setWriteForm(formSandbox)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())
	require.NoError(t, client.Set(t.Context(), index, "not a set", 0).Err())
	sbx := newRecord(teamID, "sbx-unconfirmed")
	errLost := errors.New("reply lost")
	storage.SetFaultHook(failAt(faultAddRecordWrite, errLost))

	require.ErrorIs(t, storage.Add(t.Context(), sbx), errLost)

	require.Equal(t, int64(1), writeAnomalies(t, reader, writeAnomalyAddWriteUnconfirmed))
	requireRecord(t, client, recordKey(formSandbox, teamID.String(), sbx.SandboxID), mustJSON(t, sbx))
}
