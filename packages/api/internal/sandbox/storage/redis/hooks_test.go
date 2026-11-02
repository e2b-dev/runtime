package redis

import (
	"context"
	"slices"
	"sync"

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

// recordGets returns a hook that records the GET count of every pipeline the
// client sends that carries one and the key count of every MGET, and the
// function that reads both.
func recordGets() (cmdHook, func() (gets, mgets []int)) {
	var mu sync.Mutex
	var gets, mgets []int
	hook := cmdHook{
		process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
			if cmd.Name() == "mget" {
				mu.Lock()
				mgets = append(mgets, len(cmd.Args())-1)
				mu.Unlock()
			}

			return next(ctx, cmd)
		},
		pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
			n := 0
			for _, cmd := range cmds {
				if cmd.Name() == "get" {
					n++
				}
			}
			if n > 0 {
				mu.Lock()
				gets = append(gets, n)
				mu.Unlock()
			}

			return next(ctx, cmds)
		},
	}
	snapshot := func() ([]int, []int) {
		mu.Lock()
		defer mu.Unlock()

		return slices.Clone(gets), slices.Clone(mgets)
	}

	return hook, snapshot
}
