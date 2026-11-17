package clusters

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	startupAttempts     = 3
	startupProbeTimeout = 30 * time.Second
)

// startupCoordinator freezes the initial remote-cluster inventory and completes
// the startup gate once each target has either applied a discovery snapshot or
// exhausted its bounded startup attempts.
type startupCoordinator struct {
	inventory   func(context.Context) ([]Record, error)
	startTarget func(context.Context, Record) error
	onReady     func(context.Context)

	inventoryTimeout time.Duration
	probeTimeout     time.Duration
	waitRetry        func(context.Context) bool
}

func newStartupCoordinator(
	inventory func(context.Context) ([]Record, error),
	startTarget func(context.Context, Record) error,
	onReady func(context.Context),
) *startupCoordinator {
	return &startupCoordinator{
		inventory:        inventory,
		startTarget:      startTarget,
		onReady:          onReady,
		inventoryTimeout: clusterSyncTimeout,
		probeTimeout:     startupProbeTimeout,
		waitRetry: func(ctx context.Context) bool {
			timer := time.NewTimer(clustersSyncInterval)
			defer timer.Stop()

			select {
			case <-ctx.Done():
				return false
			case <-timer.C:
				return true
			}
		},
	}
}

func (s *startupCoordinator) Start(ctx context.Context) {
	targets, ok := s.firstSuccessfulInventory(ctx)
	if !ok {
		return
	}

	var targetsWG sync.WaitGroup
	for _, target := range targets {
		targetsWG.Go(func() {
			s.startWithRetries(ctx, target)
		})
	}
	targetsWG.Wait()

	if ctx.Err() == nil {
		s.onReady(ctx)
	}
}

func (s *startupCoordinator) firstSuccessfulInventory(ctx context.Context) ([]Record, bool) {
	for {
		if ctx.Err() != nil {
			return nil, false
		}

		inventoryCtx, cancel := context.WithTimeout(ctx, s.inventoryTimeout)
		targets, err := s.inventory(inventoryCtx)
		cancel()
		if err == nil {
			return targets, true
		}

		logger.L().Error(ctx, "Startup cluster inventory failed, retrying", zap.Error(err))

		if !s.waitRetry(ctx) {
			return nil, false
		}
	}
}

// startupSource lists the clusters for the startup gate. A cluster service that
// fails startupAttempts lists does not hold startup any longer: the local
// cluster starts alone, and the background sync adds the remote clusters once
// the service answers, as it does for a remote cluster that does not answer.
// Any other failure, such as the database's, keeps startup waiting. Only the
// startup coordinator calls it, from one goroutine.
type startupSource struct {
	list  func(context.Context) ([]Record, error)
	local *Record

	clusterListFailures int
}

func (s *startupSource) inventory(ctx context.Context) ([]Record, error) {
	clusters, err := s.list(ctx)
	if !errors.Is(err, errClusterList) {
		return clusters, err
	}

	s.clusterListFailures++
	if s.clusterListFailures < startupAttempts {
		return nil, err
	}

	logger.L().Error(ctx, "Starting without the remote clusters: the cluster service did not list them", zap.Error(err))
	if s.local == nil {
		return nil, nil
	}

	return []Record{*s.local}, nil
}

func (s *startupCoordinator) startWithRetries(ctx context.Context, target Record) {
	var lastErr error
	for range startupAttempts {
		if ctx.Err() != nil {
			return
		}

		attemptCtx, cancel := context.WithTimeout(ctx, s.probeTimeout)
		err := s.startTarget(attemptCtx, target)
		cancel()
		if err == nil || ctx.Err() != nil {
			return
		}
		lastErr = err
	}

	// Startup fails open: the cluster stays out of the pool for now, and the
	// ordinary background synchronization keeps trying to pick it up.
	logger.L().Error(
		ctx,
		"Cluster did not respond during startup, continuing without it",
		zap.Error(lastErr),
		logger.WithClusterID(target.ID),
	)
}
