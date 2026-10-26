package routing

import (
	"testing"

	"github.com/stretchr/testify/require"

	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

func TestPublisher_RedisPreservesNewLifecycleUntilItsOwnStop(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	store := catalog.NewRedisSandboxCatalog(client)
	p := newPublisher(t, store)
	ctx := t.Context()
	oldSandbox := testSandbox(t, "sbx-1", "lc-old", sandboxtypes.SandboxTypeSandbox)
	newSandbox := testSandbox(t, "sbx-1", "lc-new", sandboxtypes.SandboxTypeSandbox)

	// A resume-fresh checkpoint publishes a new VM under the same execution.
	p.OnInsert(ctx, oldSandbox)
	p.OnInsert(ctx, newSandbox)
	got, err := store.GetSandbox(ctx, "sbx-1")
	require.NoError(t, err)
	require.Equal(t, "exec-1", got.ExecutionID)
	require.Equal(t, "lc-new", got.LifecycleID)
	require.Equal(t, "orch-1", got.OrchestratorID)

	key := "sandbox:routing:sbx-1"
	value, err := client.Get(ctx, key).Result()
	require.NoError(t, err)
	expiresAt, err := client.PExpireTime(ctx, key).Result()
	require.NoError(t, err)

	p.OnStopping(ctx, oldSandbox)

	retainedValue, err := client.Get(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, value, retainedValue, "the old lifecycle must preserve the replacement route")
	retainedExpiry, err := client.PExpireTime(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, expiresAt, retainedExpiry, "a stale stop must not refresh or shorten the replacement TTL")

	p.OnStopping(ctx, newSandbox)
	_, err = store.GetSandbox(ctx, "sbx-1")
	require.ErrorIs(t, err, catalog.ErrSandboxNotFound)
}
