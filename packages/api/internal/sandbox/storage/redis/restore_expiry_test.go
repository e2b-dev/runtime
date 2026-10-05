package redis

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
)

func TestRestoreRunning_UsesTransitionExpiryWithoutIndex(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	ctx := t.Context()
	sbx := createTestSandbox("restore-captured-expiry")
	require.NoError(t, storage.Add(ctx, sbx))
	expiresAt := sbx.EndTime.Add(time.Hour)
	_, err := storage.Update(ctx, sbx.TeamID, sbx.SandboxID, func(s sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
		s.EndTime = expiresAt

		return s, nil
	})
	require.NoError(t, err)

	transition, alreadyDone, finish, err := storage.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)
	require.False(t, alreadyDone)
	defer finish(ctx, sandboxtypes.ErrTransitionRestored)
	require.NotNil(t, transition.OriginalEndTime)
	assert.True(t, expiresAt.Equal(*transition.OriginalEndTime))
	assert.True(t, transition.Sandbox.EndTime.Before(expiresAt))
	require.NoError(t, client.ZRem(ctx, globalExpirationSet, sandboxExpirationMember(sbx)).Err())

	restored, err := storage.RestoreRunning(ctx, transition, 10*time.Second)
	require.NoError(t, err)
	assert.True(t, expiresAt.Equal(restored.EndTime))
	stored, err := storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.True(t, expiresAt.Equal(stored.EndTime))
	assert.Equal(t, sandboxtypes.StateRunning, stored.State)
	assert.Equal(t, sbx.ExecutionID, stored.ExecutionID)
}

func TestRestoreRunning_RepeatedRefusalsPreserveOriginalExpiry(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	ctx := t.Context()
	sbx := createTestSandbox("restore-repeated-refusals")
	sbx.EndTime = time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	require.NoError(t, storage.Add(ctx, sbx))

	for range 3 {
		transition, alreadyDone, finish, err := storage.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
		require.NoError(t, err)
		require.False(t, alreadyDone)
		restored, err := storage.RestoreRunning(ctx, transition, 10*time.Second)
		require.NoError(t, err)
		finish(ctx, sandboxtypes.ErrTransitionRestored)

		assert.True(t, sbx.EndTime.Equal(restored.EndTime))
		assert.True(t, restored.IsExpired(time.Now()))
		assert.True(t, time.Now().Before(restored.RefusedUntil))
		score, err := client.ZScore(ctx, globalExpirationSet, sandboxExpirationMember(sbx)).Result()
		require.NoError(t, err)
		assert.InDelta(t, float64(restored.RefusedUntil.UnixMilli()), score, 0.5)
		stored, err := storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
		require.NoError(t, err)
		assert.True(t, sbx.EndTime.Equal(stored.EndTime))
	}
}

func TestRestoreRunning_RejectsUnownedTransition(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)
	ctx := t.Context()
	sbx := createTestSandbox("restore-unowned-transition")
	require.NoError(t, storage.Add(ctx, sbx))
	_, _, finish, err := storage.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)
	finish(ctx, nil)

	transition, alreadyDone, finish, err := storage.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)
	require.True(t, alreadyDone)
	defer finish(ctx, nil)
	require.Nil(t, transition.OriginalEndTime)
	_, err = storage.RestoreRunning(ctx, transition, 0)
	require.Error(t, err)
	stored, err := storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, sandboxtypes.StatePausing, stored.State)
}
