package redis

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sandboxtypes "github.com/e2b-dev/infra/packages/api/internal/sandbox"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// failingWriteHook fails the next `failures` fenced writes of the record key.
type failingWriteHook struct {
	key      string
	failures atomic.Int32
	writes   atomic.Int32
}

func (h *failingWriteHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *failingWriteHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *failingWriteHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if isFencedWriteOf(cmd, h.key) {
			h.writes.Add(1)
			if h.failures.Add(-1) >= 0 {
				err := errors.New("injected write failure")
				cmd.SetErr(err)

				return err
			}
		}

		return next(ctx, cmd)
	}
}

// isFencedWriteOf reports whether cmd runs a two-key script whose second key ends with key (the test client prefixes keys).
func isFencedWriteOf(cmd redis.Cmder, key string) bool {
	name := strings.ToLower(cmd.Name())
	args := cmd.Args()
	if (name != "evalsha" && name != "eval") || len(args) < 5 {
		return false
	}
	if n, ok := args[2].(int); !ok || n != 2 {
		return false
	}
	k, ok := args[4].(string)

	return ok && strings.HasSuffix(k, key)
}

func updateTemplateID(templateID string) func(sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
	return func(s sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
		s.TemplateID = templateID

		return s, nil
	}
}

// An update's write that fails transiently is retried, so the record ends up
// written and the update succeeds.
func TestUpdateRetriesAFailedWrite(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	sbx := createTestSandbox("sbx-write-retry")
	require.NoError(t, storage.Add(t.Context(), sbx))

	hook := &failingWriteHook{key: getSandboxKey(sbx.TeamID.String(), sbx.SandboxID)}
	hook.failures.Store(2)
	// Loaded first, so a NOSCRIPT resend does not count one write twice.
	require.NoError(t, fencedSetScript.Load(t.Context(), client).Err())
	client.AddHook(hook)

	updated, err := storage.Update(t.Context(), sbx.TeamID, sbx.SandboxID, updateTemplateID("after"))
	require.NoError(t, err)
	assert.Equal(t, "after", updated.TemplateID)
	assert.Equal(t, int32(3), hook.writes.Load(), "two failed writes and the one that landed")

	stored, err := storage.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, "after", stored.TemplateID, "the record is consistent with the update")
}

// A write that keeps failing gives up after the bounded attempts and returns
// the error; the record keeps its previous value.
func TestUpdateGivesUpOnAPersistentWriteFailure(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	sbx := createTestSandbox("sbx-write-fails")
	require.NoError(t, storage.Add(t.Context(), sbx))

	hook := &failingWriteHook{key: getSandboxKey(sbx.TeamID.String(), sbx.SandboxID)}
	hook.failures.Store(1 << 20)
	// Loaded first, so a NOSCRIPT resend does not count one write twice.
	require.NoError(t, fencedSetScript.Load(t.Context(), client).Err())
	client.AddHook(hook)

	_, err := storage.Update(t.Context(), sbx.TeamID, sbx.SandboxID, updateTemplateID("after"))
	require.ErrorContains(t, err, "injected write failure")
	assert.Equal(t, int32(updateWriteAttempts), hook.writes.Load())

	stored, err := storage.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, sbx.TemplateID, stored.TemplateID)
}

// A lock that changed hands between the update's side effect and its write
// refuses the write: the later holder's record stands.
func TestUpdateRefusesTheWriteWhenTheLockChangedHands(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	sbx := createTestSandbox("sbx-lock-lost")
	require.NoError(t, storage.Add(t.Context(), sbx))
	key := getSandboxKey(sbx.TeamID.String(), sbx.SandboxID)

	_, err := storage.Update(t.Context(), sbx.TeamID, sbx.SandboxID, func(s sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
		// The lock expires during the update, and another caller takes it.
		require.NoError(t, client.Set(t.Context(), redis_utils.GetLockKey(key), "another-holder", time.Minute).Err())
		s.TemplateID = "late"

		return s, nil
	})
	require.ErrorIs(t, err, ErrLockLost)

	stored, err := storage.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, sbx.TemplateID, stored.TemplateID, "the late write did not land")
	require.NoError(t, client.Del(t.Context(), redis_utils.GetLockKey(key)).Err())
}

// The fenced write keeps the record's TTL, writes only on a matching lock
// value, and leaves the record alone otherwise.
func TestFencedSetScript(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	ctx := t.Context()
	key := "{fenced-test}:record"
	lockKey := redis_utils.GetLockKey(key)
	require.NoError(t, client.Set(ctx, key, "v1", time.Hour).Err())
	require.NoError(t, client.Set(ctx, lockKey, "token-a", time.Minute).Err())

	written, err := fencedSetScript.Run(ctx, client, []string{lockKey, key}, "token-a", "v2").Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(1), written)
	value, err := client.Get(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, "v2", value)
	ttl, err := client.TTL(ctx, key).Result()
	require.NoError(t, err)
	assert.Greater(t, ttl, 50*time.Minute, "KEEPTTL keeps the record's expiry")

	written, err = fencedSetScript.Run(ctx, client, []string{lockKey, key}, "token-b", "v3").Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(0), written, "another holder's token writes nothing")
	value, err = client.Get(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, "v2", value)

	require.NoError(t, client.Del(ctx, lockKey).Err())
	written, err = fencedSetScript.Run(ctx, client, []string{lockKey, key}, "token-a", "v4").Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(0), written, "an expired lock writes nothing")
}
