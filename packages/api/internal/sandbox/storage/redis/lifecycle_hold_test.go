package redis

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func heldSandbox(t *testing.T, storage *Storage, sandboxID string) sandboxtypes.Sandbox {
	t.Helper()

	sbx := createTestSandbox(sandboxID)
	sbx.NodeID = "node-a"
	// Old enough for the node sync to treat it as an orphan once it is gone.
	sbx.StartTime = time.Now().Add(-time.Hour)
	sbx.Metadata = map[string]string{"k": "v"}
	sbx.NetworkPlacement = &orchestrator.SandboxNetworkPlacement{ExecutionId: sbx.ExecutionID, NetworkMode: "full-router", RouterId: uuid.NewString()}
	require.NoError(t, storage.Add(t.Context(), sbx))

	return sbx
}

func holdExecution(t *testing.T, storage *Storage, sbx sandboxtypes.Sandbox, holdID string, until time.Time) error {
	t.Helper()

	return storage.HoldExecution(t.Context(), sandboxtypes.Hold{TeamID: sbx.TeamID, SandboxID: sbx.SandboxID, ExecutionID: sbx.ExecutionID, ID: holdID}, until)
}

func moveHeldExecution(t *testing.T, storage *Storage, sbx sandboxtypes.Sandbox, holdID, nodeID string) error {
	t.Helper()

	_, err := storage.MoveHeldExecution(t.Context(), sandboxtypes.Hold{TeamID: sbx.TeamID, SandboxID: sbx.SandboxID, ExecutionID: sbx.ExecutionID, ID: holdID}, nodeID)

	return err
}

// movedHeldExecution is moveHeldExecution with whether the record was written.
func movedHeldExecution(t *testing.T, storage *Storage, sbx sandboxtypes.Sandbox, holdID, nodeID string) bool {
	t.Helper()

	moved, err := storage.MoveHeldExecution(t.Context(), sandboxtypes.Hold{TeamID: sbx.TeamID, SandboxID: sbx.SandboxID, ExecutionID: sbx.ExecutionID, ID: holdID}, nodeID)
	require.NoError(t, err)

	return moved
}

func releaseHeldExecution(t *testing.T, storage *Storage, sbx sandboxtypes.Sandbox, holdID string) error {
	t.Helper()

	return storage.ReleaseHeldExecution(t.Context(), sandboxtypes.Hold{TeamID: sbx.TeamID, SandboxID: sbx.SandboxID, ExecutionID: sbx.ExecutionID, ID: holdID})
}

func removeHeldExecution(t *testing.T, storage *Storage, sbx sandboxtypes.Sandbox, holdID string) error {
	t.Helper()

	removed, err := storage.RemoveHeldExecution(t.Context(), sandboxtypes.Hold{TeamID: sbx.TeamID, SandboxID: sbx.SandboxID, ExecutionID: sbx.ExecutionID, ID: holdID})
	if err == nil {
		assert.Equal(t, sbx.ExecutionID, removed.ExecutionID, "the removed record is returned")
		assert.Equal(t, holdID, removed.LifecycleHold)
	}

	return err
}

// A hold refuses every ordinary transition, update and replacement until the
// holder moves it, and the move changes only the node and the hold.
func TestLifecycleHold(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	ctx := t.Context()
	sbx := heldSandbox(t, storage, "held")
	key := recordKey(sbx.SandboxID)
	holdID := uuid.NewString()
	until := time.Now().Add(time.Minute)

	unheldRaw := client.Get(ctx, key).Val()
	require.Error(t, holdExecution(t, storage, sbx, "", until), "an empty hold ID")
	require.Error(t, moveHeldExecution(t, storage, sbx, "", "node-b"), "an empty hold ID")
	assert.Equal(t, unheldRaw, client.Get(ctx, key).Val(), "an empty hold ID writes nothing")

	other := sbx
	other.ExecutionID = uuid.NewString()
	require.ErrorIs(t, holdExecution(t, storage, other, holdID, until), sandboxtypes.ErrHoldConflict, "another execution")
	require.NoError(t, holdExecution(t, storage, sbx, holdID, until))
	heldRaw := client.Get(ctx, key).Val()
	require.NoError(t, holdExecution(t, storage, sbx, holdID, until.Add(time.Hour)), "the same hold again")
	assert.Equal(t, heldRaw, client.Get(ctx, key).Val(), "the same hold again writes nothing")
	require.ErrorIs(t, holdExecution(t, storage, sbx, uuid.NewString(), until), sandboxtypes.ErrHoldConflict, "another hold")

	held, err := storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, holdID, held.LifecycleHold)
	assert.Equal(t, until.UnixMilli(), held.LifecycleHoldUntil)

	for _, opts := range []sandboxtypes.RemoveOpts{
		{Action: sandboxtypes.StateActionPause},
		{Action: sandboxtypes.StateActionKill},
		{Action: sandboxtypes.StateActionSnapshot},
		{Action: sandboxtypes.StateActionKill, Eviction: true},
	} {
		_, _, _, err := storage.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, opts)
		require.ErrorIs(t, err, sandboxtypes.ErrLifecycleHeld, opts.Action.Name)
	}
	called := false
	_, err = storage.Update(ctx, sbx.TeamID, sbx.SandboxID, func(s sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
		called = true

		return s, nil
	})
	require.ErrorIs(t, err, sandboxtypes.ErrLifecycleHeld)
	assert.False(t, called, "a refused update never reaches the node")
	replacement := sbx
	replacement.ExecutionID, replacement.NodeID = uuid.NewString(), "node-c"
	require.ErrorIs(t, storage.Add(ctx, replacement), sandboxtypes.ErrLifecycleHeld)
	require.ErrorIs(t, storage.Replace(ctx, replacement), sandboxtypes.ErrLifecycleHeld)
	require.ErrorIs(t, storage.Replace(ctx, sbx), sandboxtypes.ErrLifecycleHeld, "a re-add of the held execution")
	require.NoError(t, storage.Add(ctx, sbx), "a late duplicate of the create is kept out")
	require.ErrorIs(t, storage.Remove(ctx, sbx.TeamID, sbx.SandboxID, ""), sandboxtypes.ErrLifecycleHeld)
	// A transition whose lock expired before its write: the script refuses
	// the hold taken since, which the Go check before it could not see.
	transitionID := uuid.NewString()
	keys := transitionKeysFor(t, client, sbx, transitionID)
	written, err := RunScript(ctx, client, "start_transition", startTransitionScript, keys, `{"executionID":"stale"}`, transitionID, 60, 60, "", transitionLockValue).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(2), written)
	assert.Zero(t, client.Exists(ctx, keys[1]).Val(), "no transition started")
	require.NoError(t, client.Del(ctx, keys[3]).Err())
	assert.Equal(t, heldRaw, client.Get(ctx, key).Val(), "every refusal wrote nothing")

	require.ErrorIs(t, releaseHeldExecution(t, storage, sbx, uuid.NewString()), sandboxtypes.ErrNotHeld)
	require.True(t, movedHeldExecution(t, storage, sbx, holdID, "node-b"), "the move writes")
	movedRaw, err := client.Get(ctx, key).Result()
	require.NoError(t, err)
	moved, err := storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	want := held
	want.NodeID, want.LifecycleHold, want.LifecycleHoldUntil = "node-b", "", 0
	assert.Equal(t, want, moved, "only the node and the hold changed")
	// A repeat reads its own move from the record and writes nothing.
	require.False(t, movedHeldExecution(t, storage, sbx, holdID, "node-b"), "a repeat reports no write")
	assert.Equal(t, movedRaw, client.Get(ctx, key).Val())
	require.ErrorIs(t, moveHeldExecution(t, storage, sbx, holdID, "node-c"), sandboxtypes.ErrNotHeld,
		"a move to another node is not this one")
	// A later operation's hold on the moved record does not hide the move.
	require.NoError(t, holdExecution(t, storage, sbx, "next", until))
	require.NoError(t, moveHeldExecution(t, storage, sbx, holdID, "node-b"))
	require.NoError(t, releaseHeldExecution(t, storage, sbx, "next"))
	require.NoError(t, storage.Add(ctx, sbx))
	moved, err = storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, "node-b", moved.NodeID, "a late duplicate of the create never takes the record back to the source")

	// Ordinary operations reopen. Once they removed the record, a
	// move finds nothing.
	_, err = storage.Update(ctx, sbx.TeamID, sbx.SandboxID, func(s sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) { return s, nil })
	require.NoError(t, err)
	_, _, finish, err := storage.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionKill})
	require.NoError(t, err)
	finish(ctx, nil)
	require.NoError(t, storage.Remove(ctx, sbx.TeamID, sbx.SandboxID, ""))
	require.ErrorIs(t, moveHeldExecution(t, storage, sbx, holdID, "node-b"), sandboxtypes.ErrNotFound)
}

// A hold never starts under a transition in flight. A released hold reopens
// the record where it was. A removed held execution leaves no record, so
// the node's copy is an orphan and a late duplicate of the create stays out.
func TestLifecycleHoldReleaseAndRemove(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	ctx := t.Context()
	until := time.Now().Add(time.Minute)

	sbx := heldSandbox(t, storage, "released")
	_, _, finish, err := storage.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionSnapshot})
	require.NoError(t, err)
	require.ErrorIs(t, holdExecution(t, storage, sbx, "op", until), sandboxtypes.ErrHoldConflict)
	// Restored to running, but the transition has not finished yet.
	require.NoError(t, storage.restoreToRunning(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.StateSnapshotting))
	require.ErrorIs(t, holdExecution(t, storage, sbx, "op", until), sandboxtypes.ErrHoldConflict)
	finish(ctx, sandboxtypes.ErrSkipRestore)
	require.NoError(t, holdExecution(t, storage, sbx, "op", until))
	require.NoError(t, releaseHeldExecution(t, storage, sbx, "op"))
	require.ErrorIs(t, releaseHeldExecution(t, storage, sbx, "op"), sandboxtypes.ErrNotHeld)
	released, err := storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.False(t, released.Held(time.Now()))
	assert.Equal(t, "node-a", released.NodeID)
	_, _, finish, err = storage.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)
	finish(ctx, nil)

	removed := heldSandbox(t, storage, "removed")
	onNode := sandboxtypes.NodeSandbox{SandboxID: removed.SandboxID, ExecutionID: removed.ExecutionID, TeamID: removed.TeamID, NodeID: "node-a", StartTime: removed.StartTime}
	require.ErrorIs(t, removeHeldExecution(t, storage, removed, "op"), sandboxtypes.ErrNotHeld)
	require.NoError(t, holdExecution(t, storage, removed, "op", until))
	assert.Empty(t, storage.Reconcile(ctx, []sandboxtypes.NodeSandbox{onNode}, "node-a"), "a held record is no orphan")
	require.NoError(t, removeHeldExecution(t, storage, removed, "op"))

	_, err = storage.Get(ctx, removed.TeamID, removed.SandboxID)
	require.ErrorIs(t, err, sandboxtypes.ErrNotFound)
	_, err = client.ZScore(ctx, globalExpirationSet, sandboxExpirationMember(removed)).Result()
	require.ErrorIs(t, err, redis.Nil, "the expiration member goes with the record")
	requireIndexed(t, client, GetSandboxStorageTeamIndexKey(removed.TeamID.String()), removed.SandboxID, false)
	assert.Equal(t, []string{removed.SandboxID}, sandboxIDsOfNodes(storage.Reconcile(ctx, []sandboxtypes.NodeSandbox{onNode}, "node-a")),
		"the node's copy is an orphan")
	require.NoError(t, storage.Add(ctx, removed))
	_, err = storage.Get(ctx, removed.TeamID, removed.SandboxID)
	require.ErrorIs(t, err, sandboxtypes.ErrNotFound, "a late duplicate of the create stays out")
	require.ErrorIs(t, removeHeldExecution(t, storage, removed, "op"), sandboxtypes.ErrNotFound)
}

// A removal of a held record that fails keeps the hold, so the holder's
// retry still removes the record.
func TestLifecycleHoldRemoveRetry(t *testing.T) {
	t.Parallel()

	storage := newTestStorage(t, redis_utils.SetupInstance(t))
	ctx := t.Context()
	sbx := heldSandbox(t, storage, "remove-retry")
	require.NoError(t, holdExecution(t, storage, sbx, "op", time.Now().Add(time.Minute)))

	storage.SetFaultHook(failAt(faultRemoveAfterSREM, errors.New("record delete failed")))
	require.Error(t, removeHeldExecution(t, storage, sbx, "op"))
	storage.SetFaultHook(nil)
	held, err := storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, "op", held.LifecycleHold, "the failed removal kept the hold")

	require.NoError(t, removeHeldExecution(t, storage, sbx, "op"))
	_, err = storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
	require.ErrorIs(t, err, sandboxtypes.ErrNotFound)
	require.ErrorIs(t, removeHeldExecution(t, storage, sbx, "op"), sandboxtypes.ErrNotFound)
}

// A hold past its deadline is released: every ordinary operation proceeds
// and the holder can no longer move or release it. Redis's clock, not the
// writer's, decides it in the scripts.
func TestLifecycleHoldExpires(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	ctx := t.Context()

	sbx := heldSandbox(t, storage, "expired-hold")
	key := recordKey(sbx.SandboxID)
	unheld := client.Get(ctx, key).Val()
	require.Error(t, holdExecution(t, storage, sbx, "op", time.Now()), "a deadline already passed")
	assert.Equal(t, unheld, client.Get(ctx, key).Val(), "a refused hold writes nothing")
	expiry := time.Now().Add(100 * time.Millisecond)
	require.NoError(t, holdExecution(t, storage, sbx, "op", expiry))
	time.Sleep(time.Until(expiry) + 10*time.Millisecond)
	require.ErrorIs(t, moveHeldExecution(t, storage, sbx, "op", "node-b"), sandboxtypes.ErrNotHeld)
	require.ErrorIs(t, releaseHeldExecution(t, storage, sbx, "op"), sandboxtypes.ErrNotHeld)
	stored := client.Get(ctx, key).Val()
	require.NoError(t, storage.Add(ctx, sbx))
	assert.Equal(t, stored, client.Get(ctx, key).Val(), "a late duplicate of the create is kept out")

	_, _, finish, err := storage.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)
	require.NoError(t, storage.restoreToRunning(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.StatePausing))
	finish(ctx, sandboxtypes.ErrSkipRestore)
	_, err = storage.Update(ctx, sbx.TeamID, sbx.SandboxID, func(s sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) { return s, nil })
	require.NoError(t, err)
	got, err := storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	require.Equal(t, "op", got.LifecycleHold, "the expired hold is still on the record")
	_, _, finish, err = storage.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionKill})
	require.NoError(t, err)
	finish(ctx, nil)
	require.NoError(t, storage.Remove(ctx, sbx.TeamID, sbx.SandboxID, sbx.ExecutionID))
	_, err = storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
	require.ErrorIs(t, err, sandboxtypes.ErrNotFound)

	// The remove script refuses the hold until Redis's clock passes it.
	short := heldSandbox(t, storage, "short-hold")
	shortKey := recordKey(short.SandboxID)
	until := time.Now().Add(300 * time.Millisecond)
	require.NoError(t, holdExecution(t, storage, short, "op", until))
	lockRecord(t, client, shortKey)
	removeKeys := []string{shortKey, goneKeyFor(shortKey), redis_utils.GetLockKey(shortKey)}
	deleted, _, _, err := parseRemoveReply(RunScript(ctx, client, "remove_record", removeSandboxScript, removeKeys, short.SandboxID, short.ExecutionID, 60, "", testLockValue))
	require.NoError(t, err)
	assert.False(t, deleted, "refused while held")
	time.Sleep(time.Until(until) + 50*time.Millisecond)
	deleted, _, _, err = parseRemoveReply(RunScript(ctx, client, "remove_record", removeSandboxScript, removeKeys, short.SandboxID, short.ExecutionID, 60, "", testLockValue))
	require.NoError(t, err)
	assert.True(t, deleted, "removed once the hold passed")
}

// A held sandbox past its end time is not evicted and leaves the sweep's
// window for the recheck delay. Once the hold is released the ordinary expiry
// evicts it.
func TestLifecycleHoldDefersExpiry(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	ctx := t.Context()

	sbx := createTestSandbox("expired")
	sbx.NodeID = "node-a"
	sbx.StartTime = time.Now().Add(-2 * time.Hour)
	sbx.EndTime = time.Now().Add(-time.Minute)
	sbx.MaxInstanceLength = 2 * time.Hour
	require.NoError(t, storage.Add(ctx, sbx))
	require.NoError(t, holdExecution(t, storage, sbx, "op", time.Now().Add(time.Minute)))

	member := sandboxExpirationMember(sbx)
	before := time.Now()
	expired, err := storage.ExpiredItems(ctx)
	require.NoError(t, err)
	require.Empty(t, expired, "the sweep leaves a held sandbox alone")
	deferred, err := client.ZScore(ctx, globalExpirationSet, member).Result()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, deferred, float64(before.Add(transitionRecheckDelay).UnixMilli()), "the member leaves the expired window")
	assert.LessOrEqual(t, deferred, float64(time.Now().Add(transitionRecheckDelay).UnixMilli()))
	evict := sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionKill, Eviction: true, ExpectExecutionID: sbx.ExecutionID}
	_, alreadyDone, _, err := storage.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, evict)
	require.ErrorIs(t, err, sandboxtypes.ErrLifecycleHeld)
	assert.False(t, alreadyDone)

	require.NoError(t, releaseHeldExecution(t, storage, sbx, "op"))
	// The recheck delay passed: the member is back in the window.
	require.NoError(t, client.ZAdd(ctx, globalExpirationSet, redis.Z{Score: float64(sbx.EndTime.UnixMilli()), Member: member}).Err())
	expired, err = storage.ExpiredItems(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{sbx.SandboxID}, sandboxIDsOf(expired))
	_, _, finish, err := storage.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, evict)
	require.NoError(t, err)
	finish(ctx, nil)
}

// A move of a record that is gone, replaced by another execution or
// released is refused and writes nothing.
func TestLifecycleHoldMoveRefusesARecordItNoLongerHolds(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	ctx := t.Context()
	until := time.Now().Add(time.Minute)

	gone := heldSandbox(t, storage, "gone")
	goneKey := recordKey(gone.SandboxID)
	require.NoError(t, holdExecution(t, storage, gone, "op", until))
	require.NoError(t, client.Del(ctx, goneKey).Err())
	require.ErrorIs(t, moveHeldExecution(t, storage, gone, "op", "node-b"), sandboxtypes.ErrNotFound)

	replaced := heldSandbox(t, storage, "replaced")
	replacedKey := recordKey(replaced.SandboxID)
	require.NoError(t, holdExecution(t, storage, replaced, "op", until))
	successor := replaced
	successor.ExecutionID, successor.NodeID = uuid.NewString(), "node-b"
	data, err := json.Marshal(successor)
	require.NoError(t, err)
	require.NoError(t, client.Set(ctx, replacedKey, data, 0).Err())
	require.ErrorIs(t, moveHeldExecution(t, storage, replaced, "op", "node-b"), sandboxtypes.ErrExecutionMismatch)

	released := heldSandbox(t, storage, "released")
	require.NoError(t, holdExecution(t, storage, released, "op", until))
	require.NoError(t, releaseHeldExecution(t, storage, released, "op"))
	require.ErrorIs(t, moveHeldExecution(t, storage, released, "op", "node-b"), sandboxtypes.ErrNotHeld)

	_, err = client.Get(ctx, goneKey).Result()
	require.ErrorIs(t, err, redis.Nil, "a gone record stays gone")
	assert.Equal(t, string(data), client.Get(ctx, replacedKey).Val(), "the successor is untouched")
	record, err := storage.Get(ctx, released.TeamID, released.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, "node-a", record.NodeID, "the released record stays on its node")
	assert.Empty(t, record.LifecycleHold)
}

// A hold whose deadline passes while its request waits for the record's
// lock is refused, not written: a hold read as released the moment it was
// written would let every ordinary operation through while the holder
// proceeds as if held.
func TestLifecycleHoldRefusesADeadlineThatPassedWaitingForTheLock(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)
	ctx := t.Context()
	sbx := heldSandbox(t, storage, "late-hold")
	key := recordKey(sbx.SandboxID)
	held, err := storage.locker.Obtain(ctx, redis_utils.GetLockKey(key), time.Minute)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- holdExecution(t, storage, sbx, "op", time.Now().Add(200*time.Millisecond)) }()
	time.Sleep(400 * time.Millisecond)
	require.NoError(t, held.Release(ctx))
	select {
	case err := <-done:
		require.ErrorContains(t, err, "passed while waiting")
	case <-time.After(10 * time.Second):
		t.Fatal("the hold did not return")
	}
	record, err := storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Empty(t, record.LifecycleHold, "nothing was written")
}

// A holder whose hold lapsed cannot remove the record: a later hold may
// have moved the execution, which keeps its ID, and the record is that
// operation's now, or nobody's. The refusal is in the delete itself, so a
// removal delayed past the holder's own check is refused the same way.
func TestLifecycleHoldRemoveRefusesALapsedHold(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	ctx := t.Context()

	moved := heldSandbox(t, storage, "moved")
	require.NoError(t, holdExecution(t, storage, moved, "old", time.Now().Add(time.Minute)))
	// The old hold lapses and a later operation moves the execution.
	require.NoError(t, releaseHeldExecution(t, storage, moved, "old"))
	require.NoError(t, holdExecution(t, storage, moved, "new", time.Now().Add(time.Minute)))
	require.NoError(t, moveHeldExecution(t, storage, moved, "new", "node-b"))
	require.ErrorIs(t, removeHeldExecution(t, storage, moved, "old"), sandboxtypes.ErrNotHeld)
	record, err := storage.Get(ctx, moved.TeamID, moved.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, "node-b", record.NodeID, "the moved record stays")
	members, err := client.SMembers(ctx, GetSandboxStorageTeamIndexKey(moved.TeamID.String())).Result()
	require.NoError(t, err)
	assert.Contains(t, members, moved.SandboxID, "with its index entry")

	// The delete itself refuses once the hold lapsed between the holder's
	// check and the script.
	lapsed := heldSandbox(t, storage, "lapsed")
	require.NoError(t, holdExecution(t, storage, lapsed, "op", time.Now().Add(time.Minute)))
	key := recordKey(lapsed.SandboxID)
	lock, err := storage.locker.Obtain(ctx, redis_utils.GetLockKey(key), time.Minute)
	require.NoError(t, err)
	stored, err := storage.Get(ctx, lapsed.TeamID, lapsed.SandboxID)
	require.NoError(t, err)
	stored.LifecycleHoldUntil = time.Now().Add(-time.Second).UnixMilli()
	data, err := json.Marshal(stored)
	require.NoError(t, err)
	require.NoError(t, client.Set(ctx, key, data, 0).Err())
	err = storage.removeUnderLock(ctx, lock, lapsed.TeamID, lapsed.SandboxID, lapsed.ExecutionID, "op")
	require.ErrorIs(t, err, sandboxtypes.ErrNotHeld)
	require.NoError(t, lock.Release(ctx))
	assert.Equal(t, string(data), client.Get(ctx, key).Val(), "the record stays")
}
