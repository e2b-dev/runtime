package factories

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterClientRediscoversRetiredShard(t *testing.T) {
	t.Parallel()

	const before, after = "shard.test:8500", "shard.test:8501"

	var moved atomic.Bool
	client := newRedisClusterClient(&redis.ClusterOptions{
		ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) {
			addr := before
			if moved.Load() {
				addr = after
			}

			return []redis.ClusterSlot{{Start: 0, End: 16383, Nodes: []redis.ClusterNode{{Addr: addr}}}}, nil
		},
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
		},
		MaxRedirects:           -1,
		DialerRetries:          1,
		DisableRoutingPolicies: true,
	})
	t.Cleanup(func() { assert.NoError(t, client.Close()) })
	ctx := t.Context()

	node, err := client.MasterForKey(ctx, "sandbox")
	require.NoError(t, err)
	require.Equal(t, before, node.Options().Addr)
	moved.Store(true)
	require.ErrorIs(t, client.Get(ctx, "sandbox").Err(), syscall.ECONNREFUSED)

	require.Eventually(t, func() bool {
		node, err := client.MasterForKey(ctx, "sandbox")

		return err == nil && node.Options().Addr == after
	}, time.Second, time.Millisecond)
}

func TestShardRefreshCoalescesConcurrentFailures(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var reloads, attempts atomic.Int32
		hook := shardRefreshHook{reload: func(context.Context) { reloads.Add(1) }}
		cmd := redis.NewCmd(t.Context(), "incr", "counter")
		fail := hook.ProcessHook(func(context.Context, redis.Cmder) error {
			attempts.Add(1)

			return io.EOF
		})

		var wg sync.WaitGroup
		for range 32 {
			wg.Go(func() { assert.ErrorIs(t, fail(t.Context(), cmd), io.EOF) })
		}
		wg.Wait()
		require.EqualValues(t, 32, attempts.Load())
		require.EqualValues(t, 1, reloads.Load())

		time.Sleep(5 * time.Second)
		require.ErrorIs(t, fail(t.Context(), cmd), io.EOF)
		require.EqualValues(t, 2, reloads.Load())
	})
}

func TestShardRefreshPipelineWithCacheMiss(t *testing.T) {
	t.Parallel()

	var reloads int
	hook := shardRefreshHook{reload: func(context.Context) { reloads++ }}
	ctx := t.Context()
	missing := redis.NewCmd(ctx, "get", "absent")
	missing.SetErr(redis.Nil)
	failed := redis.NewCmd(ctx, "get", "present")
	failed.SetErr(io.ErrUnexpectedEOF)
	run := hook.ProcessPipelineHook(func(_ context.Context, cmds []redis.Cmder) error { return cmds[0].Err() })

	require.ErrorIs(t, run(ctx, []redis.Cmder{missing}), redis.Nil)
	require.Zero(t, reloads)
	require.ErrorIs(t, run(ctx, []redis.Cmder{missing, failed}), redis.Nil)
	require.Equal(t, 1, reloads)
}
