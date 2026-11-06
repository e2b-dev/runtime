package redis

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// pendingTransitionID reads the ID the transition marker of one form holds.
func pendingTransitionID(t *testing.T, client redis.UniversalClient, form keyForm, teamID uuid.UUID, sandboxID string) string {
	t.Helper()

	id, err := client.Get(t.Context(), transitionKeyFor(form, teamID.String(), sandboxID)).Result()
	require.NoError(t, err)
	require.NotEmpty(t, id)

	return id
}

// requireTransitionFailure waits for a waiter to return the failure a
// transition finished with.
func requireTransitionFailure(t *testing.T, waiter <-chan error, cause string) {
	t.Helper()

	select {
	case err := <-waiter:
		require.ErrorContains(t, err, cause)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the waiter never returned")
	}
}

// StartRemoving marks the transition and its result under the form the
// record is in and nothing under the other form, and the callback publishes
// the routing string of that result key: a waiter subscribed to it, whatever
// the write form is by then, is woken well before its fallback poll and
// returns the transition's result.
func TestStartRemovingUsesRecordForm(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	startNotifications(t, storage)
	teamID := uuid.New()
	team := teamID.String()

	for _, form := range []keyForm{formTeam, formSandbox} {
		sbx := newRecord(teamID, fmt.Sprintf("sbx-%d", form))
		key := addRecord(t, storage, form, sbx)

		transition, alreadyDone, finish, err := storage.StartRemoving(t.Context(), teamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
		require.NoError(t, err)
		require.False(t, alreadyDone)

		id := pendingTransitionID(t, client, form, teamID, sbx.SandboxID)
		resultKey := transitionResultKeyFor(form, team, sbx.SandboxID, id)
		requireRecord(t, client, resultKey, "")
		requireRecord(t, client, key, mustJSON(t, transition.Sandbox))
		requireNoRecord(t, client, transitionKeyFor(form.other(), team, sbx.SandboxID))
		requireNoRecord(t, client, recordKey(form.other(), team, sbx.SandboxID))
		require.False(t, lockHeld(t, storage.locker, redis_utils.GetLockKey(key)))

		// The write form flips after the transition started; the waiter
		// finds it in the record's form.
		storage.setWriteForm(form.other())
		waiter := make(chan error, 1)
		go func() { waiter <- storage.WaitForStateChange(t.Context(), teamID, sbx.SandboxID) }()
		waitForRegisteredWaiters(t, storage.subManager, transitionRoutingKey(resultKey), 1)

		finished := time.Now()
		finish(t.Context(), errors.New("boom"))
		requireTransitionFailure(t, waiter, "boom")
		require.Less(t, time.Since(finished), pollInterval/2, "the waiter was not woken by the callback's notification")
		requireNoRecord(t, client, transitionKeyFor(form, team, sbx.SandboxID))
		requireRecord(t, client, resultKey, "boom")
	}
}

// A wait carries the form and ID of the transition it found. The first
// transition, in the team form, fails after a waiter joined it; the write
// form flips and a successor's transition for the same sandbox ID appears in
// the sandbox form before the waiter looks again. The waiter returns the
// first transition's failure, not the successor's empty result.
func TestWaitCarriesForm(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	// No notifications: the fallback poll is the waiter's only wake-up, so the
	// successor is in place before the waiter looks again.
	storage := newTestStorage(t, client)
	teamID := uuid.New()
	team := teamID.String()
	sbx := newRecord(teamID, "sbx-carried")
	addRecord(t, storage, formTeam, sbx)
	_, _, finish, err := storage.StartRemoving(t.Context(), teamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)
	first := pendingTransitionID(t, client, formTeam, teamID, sbx.SandboxID)

	waiter := make(chan error, 1)
	go func() { waiter <- storage.WaitForStateChange(t.Context(), teamID, sbx.SandboxID) }()
	waitForRegisteredWaiters(t, storage.subManager, transitionRoutingKey(transitionResultKeyFor(formTeam, team, sbx.SandboxID, first)), 1)
	// Past the waiter's first look at the marker, well before its first poll.
	time.Sleep(50 * time.Millisecond)

	finish(t.Context(), errors.New("boom"))
	successor := newRecord(teamID, sbx.SandboxID)
	addRecord(t, storage, formSandbox, successor)
	_, _, finishSuccessor, err := storage.StartRemoving(t.Context(), teamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)
	second := pendingTransitionID(t, client, formSandbox, teamID, sbx.SandboxID)
	requireNoRecord(t, client, transitionKeyFor(formTeam, team, sbx.SandboxID))

	requireTransitionFailure(t, waiter, "boom")

	require.Equal(t, second, pendingTransitionID(t, client, formSandbox, teamID, sbx.SandboxID), "the successor's transition is still pending")
	finishSuccessor(t.Context(), nil)
}

// A stale sandbox-form marker, as a transition that crashed before it
// finished leaves one until its TTL, beside a live team-form record whose
// own transition is in progress: the wait follows the live record's form
// and returns that transition's result instead of sitting out the stale
// marker.
func TestWaitFollowsLiveRecordForm(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	startNotifications(t, storage)
	teamID := uuid.New()
	team := teamID.String()
	sbx := newRecord(teamID, "sbx-live")
	addRecord(t, storage, formTeam, sbx)
	require.NoError(t, client.Set(t.Context(), transitionKeyFor(formSandbox, team, sbx.SandboxID), uuid.NewString(), transitionKeyTTL).Err())
	_, _, finish, err := storage.StartRemoving(t.Context(), teamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)
	id := pendingTransitionID(t, client, formTeam, teamID, sbx.SandboxID)

	waiter := make(chan error, 1)
	go func() { waiter <- storage.WaitForStateChange(t.Context(), teamID, sbx.SandboxID) }()
	waitForRegisteredWaiters(t, storage.subManager, transitionRoutingKey(transitionResultKeyFor(formTeam, team, sbx.SandboxID, id)), 1)

	finish(t.Context(), errors.New("boom"))
	requireTransitionFailure(t, waiter, "boom")
}

// Without a record, as on the kill path, where the record goes before the
// transition finishes, the wait still finds the marker in either form.
func TestWaitFindsMarkerWithoutRecord(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	startNotifications(t, storage)
	teamID := uuid.New()
	team := teamID.String()

	for _, form := range []keyForm{formTeam, formSandbox} {
		sbx := newRecord(teamID, fmt.Sprintf("sbx-%d", form))
		key := addRecord(t, storage, form, sbx)
		_, _, finish, err := storage.StartRemoving(t.Context(), teamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionKill})
		require.NoError(t, err)
		id := pendingTransitionID(t, client, form, teamID, sbx.SandboxID)
		require.NoError(t, client.Del(t.Context(), key).Err())

		waiter := make(chan error, 1)
		go func() { waiter <- storage.WaitForStateChange(t.Context(), teamID, sbx.SandboxID) }()
		waitForRegisteredWaiters(t, storage.subManager, transitionRoutingKey(transitionResultKeyFor(form, team, sbx.SandboxID, id)), 1)

		finish(t.Context(), nil)
		requireNoErrorFromChannel(t, waiter)
	}
}
