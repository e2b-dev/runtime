//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

type Cleanup struct {
	cleanup         []func(ctx context.Context) error
	priorityCleanup []func(ctx context.Context) error
	error           error
	once            sync.Once

	hasRun atomic.Bool
	mu     sync.Mutex
}

const lateCleanupLogMessage = "cleanup callback ran after cleanup completed"

func runLateCleanup(ctx context.Context, f func(context.Context) error, priority bool) {
	runCtx := context.WithoutCancel(ctx)
	start := time.Now()
	err := f(runCtx)
	fields := []zap.Field{
		zap.Bool("priority", priority),
		zap.Duration("duration", time.Since(start)),
	}
	if err != nil {
		logger.L().Error(runCtx, lateCleanupLogMessage, append(fields, zap.Error(err))...)

		return
	}

	logger.L().Info(runCtx, lateCleanupLogMessage, fields...)
}

func NewCleanup() *Cleanup {
	return &Cleanup{}
}

func (c *Cleanup) AddNoContext(ctx context.Context, f func() error) {
	c.Add(ctx, func(_ context.Context) error { return f() })
}

func (c *Cleanup) Add(ctx context.Context, f func(ctx context.Context) error) {
	// Keep the state check and registration under the same lock as Run. Without
	// this, Add can observe false, lose the lock to Run, then append after Run has
	// already drained the list.
	c.mu.Lock()
	if !c.hasRun.Load() {
		c.cleanup = append(c.cleanup, f)
		c.mu.Unlock()

		return
	}
	c.mu.Unlock()

	runLateCleanup(ctx, f, false)
}

func (c *Cleanup) AddPriority(ctx context.Context, f func(ctx context.Context) error) {
	c.mu.Lock()
	if !c.hasRun.Load() {
		c.priorityCleanup = append(c.priorityCleanup, f)
		c.mu.Unlock()

		return
	}
	c.mu.Unlock()

	runLateCleanup(ctx, f, true)
}

func (c *Cleanup) Run(ctx context.Context) error {
	c.once.Do(func() {
		c.run(context.WithoutCancel(ctx))
	})

	return c.error
}

// cleanupIfNotRegistered covers errors before a resource's normal cleanup callback is registered.
func cleanupIfNotRegistered(ctx context.Context, result *error, registered *bool, f func(context.Context) error) {
	if *result != nil && !*registered {
		*result = errors.Join(*result, f(context.WithoutCancel(ctx)))
	}
}

func (c *Cleanup) run(ctx context.Context) {
	c.hasRun.Store(true)

	c.mu.Lock()
	priorityCleanup := c.priorityCleanup
	normalCleanup := c.cleanup
	c.mu.Unlock()

	var errs []error

	for _, f := range slices.Backward(priorityCleanup) {
		err := f(ctx)
		if err != nil {
			errs = append(errs, err)
		}
	}

	for _, f := range slices.Backward(normalCleanup) {
		err := f(ctx)
		if err != nil {
			errs = append(errs, err)
		}
	}

	c.error = errors.Join(errs...)
}

func cleanupFiles(config cfg.BuilderConfig, files *storage.SandboxFiles) func(context.Context) error {
	return func(context.Context) error {
		var errs []error

		for _, p := range []string{
			files.SandboxFirecrackerSocketPath(),
			files.SandboxUffdSocketPath(),
			files.SandboxCacheRootfsLinkPath(config.StorageConfig),
		} {
			err := os.RemoveAll(p)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to delete '%s': %w", p, err))
			}
		}

		if len(errs) == 0 {
			return nil
		}

		return fmt.Errorf("failed to cleanup files: %w", errors.Join(errs...))
	}
}
