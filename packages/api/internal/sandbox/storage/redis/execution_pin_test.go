package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
)

// A sandbox ID is reused across incarnations: kill or pause a sandbox, resume
// it, and the ID comes back attached to a new execution — potentially on a
// different node. A caller that decided from a snapshot taken earlier can
// therefore have its removal land on a sandbox that was never in scope.
//
// Eviction already guards its own version of this, by re-checking expiry under
// the storage lock: the evictor reads the expired set and then removes, so its
// decision can go stale in between. These tests pin the general form of that
// guard, for callers whose staleness is not expiry-shaped. Enforced under the
// same lock, because that is the only place the check cannot be raced.

func TestStartRemoving_ExecutionPinMismatchRefuses(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)

	sbx := createTestSandbox("sbx-replaced")
	require.NoError(t, storage.Add(t.Context(), sbx))

	_, _, finish, err := storage.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{
		Action:            sandboxtypes.StateActionKill,
		Reason:            sandboxtypes.KillReasonRequest,
		ExpectExecutionID: uuid.NewString(), // the incarnation the caller saw, now gone
	})
	require.ErrorIs(t, err, sandboxtypes.ErrExecutionMismatch)
	require.Nil(t, finish, "a refused removal must not hand back a transition to finish")

	// The stored sandbox must be untouched — no state transition, still usable.
	stored, err := storage.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, sandboxtypes.StateRunning, stored.State, "a refused removal must not transition the live sandbox")
	assert.Equal(t, sbx.ExecutionID, stored.ExecutionID)
}

func TestStartRemoving_ExecutionPinMatchProceeds(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)

	sbx := createTestSandbox("sbx-still-there")
	require.NoError(t, storage.Add(t.Context(), sbx))

	_, alreadyDone, finish, err := storage.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{
		Action:            sandboxtypes.StateActionKill,
		Reason:            sandboxtypes.KillReasonRequest,
		ExpectExecutionID: sbx.ExecutionID,
	})
	require.NoError(t, err)
	require.False(t, alreadyDone)
	require.NotNil(t, finish)
	finish(t.Context(), nil)

	stored, err := storage.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, sandboxtypes.StateKilling, stored.State)
}

// TestStartRemoving_ExecutionPinSurvivesWaitingOutATransition covers the retry
// path, which is where the pin matters most and is easiest to lose.
//
// When another transition is already in flight, the removal releases the lock,
// waits for it, and then retries itself. That wait is precisely the window in
// which the sandbox can finish pausing and be resumed under a new incarnation,
// so a retry that forgot the caller's pin would act on exactly the record the
// pin exists to protect.
func TestStartRemoving_ExecutionPinSurvivesWaitingOutATransition(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)
	ctx := t.Context()

	sbx := createTestSandbox("sbx-pinned-through-wait")
	require.NoError(t, storage.Add(ctx, sbx))

	// Occupy the transition slot with a pause, so a kill cannot proceed
	// directly and has to wait it out and retry.
	_, _, finishPause, err := storage.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{
		Action: sandboxtypes.StateActionPause,
	})
	require.NoError(t, err)
	require.NotNil(t, finishPause)

	type outcome struct {
		finish func(context.Context, error)
		err    error
	}
	done := make(chan outcome, 1)

	// A kill pinned to the incarnation that is stored right now, so it clears
	// the pin check on the way in and blocks in the wait. The retry afterwards
	// is what this test is about.
	go func() {
		_, _, finish, err := storage.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{
			Action:            sandboxtypes.StateActionKill,
			Reason:            sandboxtypes.KillReasonRequest,
			ExpectExecutionID: sbx.ExecutionID,
		})
		done <- outcome{finish: finish, err: err}
	}()

	// Give the kill time to reach the wait, then do to the record what a resume
	// does: same sandbox ID, new incarnation. Completing a pause only touches
	// the transition key, so this write survives it.
	time.Sleep(100 * time.Millisecond)

	resumed := sbx
	resumed.ExecutionID = uuid.NewString()
	resumed.State = sandboxtypes.StateRunning
	writeTeamSandbox(t, storage, resumed)

	finishPause(ctx, nil)

	select {
	case got := <-done:
		require.ErrorIs(t, got.err, sandboxtypes.ErrExecutionMismatch,
			"a removal that waited out a transition must still honour its pin on retry")
		require.Nil(t, got.finish)
	case <-time.After(30 * time.Second):
		t.Fatal("pinned kill never returned after the in-flight transition completed")
	}

	// The resumed incarnation was never in scope and must be untouched.
	stored, err := storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, resumed.ExecutionID, stored.ExecutionID)
	assert.Equal(t, sandboxtypes.StateRunning, stored.State,
		"the retry must not have killed the incarnation that reclaimed the ID")
}

// The tests above drive the pin through StartRemoving, which checks it in Go
// before doing any work. That check is a fast path, not the guarantee: the lock
// it holds excludes the other lock-taking operations but not Add, which is
// lockless, so a resume can install a new incarnation between the comparison
// and the write. Without an atomic guard the write would then overwrite the new
// live record with the stale one, and the removal's own cleanup would delete it.
//
// The guard therefore lives inside startTransitionScript, and these exercise it
// directly. There is no way to interleave an Add into the middle of a Lua
// script from a test — which is the property being relied on — so the script is
// driven with a record that has already moved on, standing in for the resume
// that would have landed in that window.

func transitionKeysFor(sbx sandboxtypes.Sandbox, transitionID string) []string {
	team := sbx.TeamID.String()

	return []string{
		getSandboxKey(team, sbx.SandboxID),
		getTransitionKey(team, sbx.SandboxID),
		getTransitionResultKey(team, sbx.SandboxID, transitionID),
	}
}

func TestStartTransitionScript_RefusesAStaleIncarnation(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	ctx := t.Context()

	sbx := createTestSandbox("sbx-cas-refuses")
	require.NoError(t, storage.Add(ctx, sbx))

	transitionID := uuid.NewString()
	keys := transitionKeysFor(sbx, transitionID)

	written, err := startTransitionScript.Run(ctx, client, keys,
		`{"executionID":"clobbered"}`, transitionID, 60, 60,
		uuid.NewString(), // the incarnation the caller pinned, no longer stored
	).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(0), written, "the script must refuse a stale pin")

	// Nothing may have been written: not the record, not the transition.
	stored, err := storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, sbx.ExecutionID, stored.ExecutionID, "the live record must be untouched")
	assert.Equal(t, sandboxtypes.StateRunning, stored.State)
	assert.Zero(t, client.Exists(ctx, keys[1]).Val(), "a refused transition must not be started")
}

func TestStartTransitionScript_ProceedsOnTheMatchingIncarnation(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	ctx := t.Context()

	sbx := createTestSandbox("sbx-cas-proceeds")
	require.NoError(t, storage.Add(ctx, sbx))

	transitionID := uuid.NewString()
	keys := transitionKeysFor(sbx, transitionID)

	updated := sbx
	updated.State = sandboxtypes.StateKilling
	data, err := json.Marshal(updated)
	require.NoError(t, err)

	written, err := startTransitionScript.Run(ctx, client, keys,
		data, transitionID, 60, 60, sbx.ExecutionID,
	).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(1), written)

	stored, err := storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, sandboxtypes.StateKilling, stored.State)
}

// TestStartTransitionScript_RefusesADeletedRecord covers the other way the pin
// can stop holding: the record is gone entirely rather than replaced. Writing
// then would resurrect a removed sandbox.
func TestStartTransitionScript_RefusesADeletedRecord(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	ctx := t.Context()

	sbx := createTestSandbox("sbx-cas-deleted")
	require.NoError(t, storage.Add(ctx, sbx))
	require.NoError(t, storage.Remove(ctx, sbx.TeamID, sbx.SandboxID, sbx.ExecutionID))

	transitionID := uuid.NewString()
	keys := transitionKeysFor(sbx, transitionID)

	written, err := startTransitionScript.Run(ctx, client, keys,
		`{"executionID":"resurrected"}`, transitionID, 60, 60, sbx.ExecutionID,
	).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(0), written)
	assert.Zero(t, client.Exists(ctx, keys[0]).Val(), "a refused transition must not resurrect the record")
}

// TestStartTransitionScript_UnpinnedWritesUnconditionally keeps the guard
// opt-in at the script level too: every existing caller passes no pin and must
// be unaffected.
func TestStartTransitionScript_UnpinnedWritesUnconditionally(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	ctx := t.Context()

	sbx := createTestSandbox("sbx-cas-unpinned")
	require.NoError(t, storage.Add(ctx, sbx))

	transitionID := uuid.NewString()
	keys := transitionKeysFor(sbx, transitionID)

	updated := sbx
	updated.State = sandboxtypes.StateKilling
	data, err := json.Marshal(updated)
	require.NoError(t, err)

	written, err := startTransitionScript.Run(ctx, client, keys,
		data, transitionID, 60, 60, "",
	).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(1), written)
}

func TestRemove_StaleCleanupCannotDeleteReplacementExecution(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	ctx := t.Context()

	old := createTestSandbox("sbx-final-remove-race")
	replacement := old
	replacement.ExecutionID = uuid.NewString()
	replacement.EndTime = replacement.EndTime.Add(time.Hour)

	require.NoError(t, storage.Add(ctx, old))
	// Add is intentionally lockless. This models E2 landing after E1's node
	// cleanup began but before E1's deferred Redis removal runs.
	require.NoError(t, storage.Add(ctx, replacement))

	err := storage.Remove(ctx, old.TeamID, old.SandboxID, old.ExecutionID)
	require.ErrorIs(t, err, sandboxtypes.ErrExecutionMismatch)

	stored, err := storage.Get(ctx, replacement.TeamID, replacement.SandboxID)
	require.NoError(t, err)
	require.Equal(t, replacement.ExecutionID, stored.ExecutionID)
	require.True(t, client.SIsMember(ctx, GetSandboxStorageTeamIndexKey(replacement.TeamID.String()), replacement.SandboxID).Val())
}

// TestStartRemoving_NoExecutionPinRemovesWhateverIsStored keeps the guard
// opt-in: callers acting on user intent or a fresh read must not be forced to
// supply an execution ID.
func TestStartRemoving_NoExecutionPinRemovesWhateverIsStored(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)

	sbx := createTestSandbox("sbx-unpinned")
	require.NoError(t, storage.Add(t.Context(), sbx))

	_, _, finish, err := storage.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{
		Action: sandboxtypes.StateActionKill,
		Reason: sandboxtypes.KillReasonRequest,
	})
	require.NoError(t, err)
	require.NotNil(t, finish)
	finish(t.Context(), nil)

	stored, err := storage.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, sandboxtypes.StateKilling, stored.State)
}
