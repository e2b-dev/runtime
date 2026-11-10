package redis

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
)

// A pause that requires a healthy guest cannot inherit a pause in flight,
// whose snapshot may be taken over an unresponsive one: it is answered as a
// conflicting transition, never as a join that reports its strict pause
// done. A plain pause still joins.
func TestStartRemoving_StrictPauseDoesNotJoinAPauseInFlight(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)
	sbx := createTestSandbox("strict-joiner")
	require.NoError(t, storage.Add(t.Context(), sbx))
	_, alreadyDone, finish, err := storage.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)
	require.False(t, alreadyDone)

	_, alreadyDone, _, err = storage.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause, RequireEnvdHealthy: true})
	var transErr *sandboxtypes.InvalidStateTransitionError
	require.ErrorAs(t, err, &transErr, "a strict pause must not ride the pause in flight")
	assert.Equal(t, sandboxtypes.StatePausing, transErr.CurrentState)
	assert.Equal(t, sandboxtypes.StatePausing, transErr.TargetState)
	assert.False(t, alreadyDone)

	joiner := make(chan error, 1)
	go func() {
		_, joined, _, err := storage.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
		if err == nil && !joined {
			err = assert.AnError
		}
		joiner <- err
	}()
	id := pendingTransitionID(t, storage.redisClient, sbx.SandboxID)
	routingKey := transitionRoutingKey(transitionResultKeyFor(sbx.SandboxID, id))
	require.Eventually(t, func() bool { return waitersOn(storage, routingKey) == 1 },
		5*time.Second, 5*time.Millisecond, "the plain pause must be waiting on the transition")
	finish(context.WithoutCancel(t.Context()), nil)

	select {
	case err := <-joiner:
		require.NoError(t, err, "a plain pause joins and inherits the result")
	case <-time.After(5 * time.Second):
		t.Fatal("the joiner never returned")
	}
}
