package factories

import (
	"context"
	"errors"
	"io"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

func newRedisClusterClient(opts *redis.ClusterOptions) *redis.ClusterClient {
	client := redis.NewClusterClient(opts)
	client.AddHook(&shardRefreshHook{reload: client.ReloadState})

	return client
}

// A retired shard cannot send a MOVED redirect.
type shardRefreshHook struct {
	reload func(context.Context)
	mu     sync.Mutex
	next   time.Time
}

func (h *shardRefreshHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *shardRefreshHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if shardConnectionFailed(err) {
			h.refresh(ctx)
		}

		return err
	}
}

func (h *shardRefreshHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		if err == nil {
			return nil
		}

		// A cache miss can precede a connection failure on another shard in the same pipeline.
		if shardConnectionFailed(err) || slices.ContainsFunc(cmds, func(cmd redis.Cmder) bool {
			return shardConnectionFailed(cmd.Err())
		}) {
			h.refresh(ctx)
		}

		return err
	}
}

func shardConnectionFailed(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}

	_, networkError := errors.AsType[*net.OpError](err)

	return networkError || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func (h *shardRefreshHook) refresh(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	if now.Before(h.next) {
		return
	}

	h.next = now.Add(5 * time.Second)
	h.reload(ctx)
}
