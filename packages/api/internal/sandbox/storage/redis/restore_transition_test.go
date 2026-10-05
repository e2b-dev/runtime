package redis

import (
	"testing"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
)

func TestPauseCallbackPreservesSuccessorTransition(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	ctx := t.Context()
	sbx := createTestSandbox("late-pause-callback")
	require.NoError(t, storage.Add(ctx, sbx))
	_, _, finishOld, err := storage.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)

	key := getTransitionKey(sbx.TeamID.String(), sbx.SandboxID)
	oldID, err := client.Get(ctx, key).Result()
	require.NoError(t, err)
	// Simulate the transition TTL expiring while its caller is still in flight.
	require.NoError(t, client.Del(ctx, key).Err())
	resumed := sbx
	resumed.ExecutionID = uuid.NewString()
	require.NoError(t, storage.Add(ctx, resumed))
	_, _, finishNew, err := storage.StartRemoving(ctx, resumed.TeamID, resumed.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)
	newID, err := client.Get(ctx, key).Result()
	require.NoError(t, err)

	finishOld(ctx, sandboxtypes.ErrTransitionRestored)
	result, err := client.Get(ctx, getTransitionResultKey(sbx.TeamID.String(), sbx.SandboxID, oldID)).Result()
	require.NoError(t, err)
	require.Equal(t, sandboxtypes.ErrTransitionRestored.Error(), result)
	currentID, err := client.Get(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, newID, currentID)
	stored, err := storage.Get(ctx, resumed.TeamID, resumed.SandboxID)
	require.NoError(t, err)
	require.Equal(t, resumed.ExecutionID, stored.ExecutionID)
	require.Equal(t, sandboxtypes.StatePausing, stored.State)

	finishNew(ctx, nil)
	require.ErrorIs(t, client.Get(ctx, key).Err(), redis.Nil)
}
