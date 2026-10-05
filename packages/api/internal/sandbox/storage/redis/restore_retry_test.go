package redis

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

type restoreCommandHook func(context.Context, redis.Cmder, redis.ProcessHook) error

func (h restoreCommandHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h restoreCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h restoreCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error { return h(ctx, cmd, next) }
}

func TestRestoreRunning_ReplayedWriteSucceeds(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	var replayed atomic.Bool
	client.AddHook(restoreCommandHook(func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		args := cmd.Args()
		if cmd.Name() == "evalsha" && args[1] == restoreSandboxScript.Hash() && replayed.CompareAndSwap(false, true) {
			// Apply the write without delivering its reply, then replay the identical command.
			if err := restoreSandboxScript.Eval(ctx, client, []string{args[3].(string)}, args[4:]...).Err(); err != nil {
				return err
			}
		}

		return next(ctx, cmd)
	}))
	storage := newTestStorage(t, client)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })

	sbx := createTestSandbox("restore-replayed-write")
	require.NoError(t, storage.Add(t.Context(), sbx))
	transition, _, finish, err := storage.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)
	defer finish(t.Context(), sandboxtypes.ErrTransitionRestored)

	restored, err := storage.RestoreRunning(t.Context(), transition, 10*time.Second)
	require.NoError(t, err)
	require.True(t, replayed.Load())
	assert.Equal(t, sandboxtypes.StateRunning, restored.State)
	assert.Equal(t, sbx.ExecutionID, restored.ExecutionID)
	stored, err := storage.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, restored.State, stored.State)
	assert.Equal(t, restored.ExecutionID, stored.ExecutionID)
	assert.True(t, restored.RefusedUntil.Equal(stored.RefusedUntil))
}

func TestRestoreRunning_RetryIndexFailureKeepsRestoredSandbox(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	var failIndexWrite atomic.Bool
	client.AddHook(restoreCommandHook(func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if cmd.Name() == "zadd" && cmd.Args()[1] == globalExpirationSet && failIndexWrite.CompareAndSwap(true, false) {
			return errors.New("retry index unavailable")
		}

		return next(ctx, cmd)
	}))
	storage := newTestStorage(t, client)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })

	sbx := createTestSandbox("restore-index-failure")
	sbx.EndTime = time.Now().Add(-time.Minute)
	require.NoError(t, storage.Add(t.Context(), sbx))
	transition, _, finish, err := storage.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)
	defer finish(t.Context(), sandboxtypes.ErrTransitionRestored)

	failIndexWrite.Store(true)
	restored, err := storage.RestoreRunning(t.Context(), transition, 10*time.Second)
	require.NoError(t, err)
	require.False(t, failIndexWrite.Load())
	stored, err := storage.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, sandboxtypes.StateRunning, stored.State)
	assert.Equal(t, sbx.ExecutionID, stored.ExecutionID)
	assert.WithinDuration(t, sbx.EndTime, stored.EndTime, time.Millisecond)
	assert.True(t, time.Now().Before(stored.RefusedUntil))
	assert.True(t, restored.RefusedUntil.Equal(stored.RefusedUntil))
	score, err := client.ZScore(t.Context(), globalExpirationSet, sandboxExpirationMember(sbx)).Result()
	require.NoError(t, err)
	assert.InDelta(t, float64(sbx.EndTime.UnixMilli()), score, 0.5)
}
