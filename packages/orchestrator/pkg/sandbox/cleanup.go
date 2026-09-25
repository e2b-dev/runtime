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

func NewCleanup() *Cleanup {
	return &Cleanup{}
}

func (c *Cleanup) AddNoContext(ctx context.Context, f func() error) {
	c.Add(ctx, func(_ context.Context) error { return f() })
}

func (c *Cleanup) Add(ctx context.Context, f func(ctx context.Context) error) {
	if c.hasRun.Load() {
		err := f(context.WithoutCancel(ctx))
		if err != nil {
			logger.L().Error(ctx, "failed to run function after cleanup has run", zap.Error(err))
		}

		return
	}

	c.mu.Lock()
	// Double-check under lock: run() may have completed between the Load above
	// and acquiring mu, silently dropping f into an already-drained slice.
	if c.hasRun.Load() {
		c.mu.Unlock()

		err := f(context.WithoutCancel(ctx))
		if err != nil {
			logger.L().Error(ctx, "failed to run function after cleanup has run", zap.Error(err))
		}

		return
	}
	defer c.mu.Unlock()

	c.cleanup = append(c.cleanup, f)
}

func (c *Cleanup) AddPriority(ctx context.Context, f func(ctx context.Context) error) {
	if c.hasRun.Load() {
		err := f(context.WithoutCancel(ctx))
		if err != nil {
			logger.L().Error(ctx, "failed to run priority function after cleanup has run", zap.Error(err))
		}

		return
	}

	c.mu.Lock()
	// Double-check under lock: run() may have completed between the Load above
	// and acquiring mu, silently dropping f into an already-drained slice.
	if c.hasRun.Load() {
		c.mu.Unlock()

		err := f(context.WithoutCancel(ctx))
		if err != nil {
			logger.L().Error(ctx, "failed to run priority function after cleanup has run", zap.Error(err))
		}

		return
	}
	defer c.mu.Unlock()

	c.priorityCleanup = append(c.priorityCleanup, f)
}

func (c *Cleanup) Run(ctx context.Context) error {
	c.once.Do(func() {
		c.run(context.WithoutCancel(ctx))
	})

	return c.error
}

func (c *Cleanup) run(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Set hasRun inside the lock so Add/AddPriority's double-check (also under
	// mu) sees the updated value before the slice is drained. Without this,
	// Add can observe hasRun==false, lose the race to run(), and append f to an
	// already-drained slice where it will never be executed.
	c.hasRun.Store(true)

	var errs []error

	for _, cleanup := range slices.Backward(c.priorityCleanup) {
		err := cleanup(ctx)
		if err != nil {
			errs = append(errs, err)
		}
	}

	for _, cleanup := range slices.Backward(c.cleanup) {
		err := cleanup(ctx)
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
