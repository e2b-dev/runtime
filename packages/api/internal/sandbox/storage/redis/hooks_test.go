package redis

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// cmdHook is a go-redis hook built from the function a test needs for
// commands, for pipelines, or both; a nil one passes through. A cluster
// client runs the hooks once per pipeline, before the fan-out to its nodes,
// and once per command.
type cmdHook struct {
	process  func(context.Context, redis.Cmder, redis.ProcessHook) error
	pipeline func(context.Context, []redis.Cmder, redis.ProcessPipelineHook) error
}

func (h cmdHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h cmdHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	if h.process == nil {
		return next
	}

	return func(ctx context.Context, cmd redis.Cmder) error { return h.process(ctx, cmd, next) }
}

func (h cmdHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	if h.pipeline == nil {
		return next
	}

	return func(ctx context.Context, cmds []redis.Cmder) error { return h.pipeline(ctx, cmds, next) }
}
