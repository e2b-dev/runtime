//go:build linux

package nfsproxy

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/chroot"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/mountcache"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type mountRetirement struct {
	done chan struct{}
	err  error // published by closing done
}

type mountLifecycleSubscriber struct {
	gate    *mountcache.LifecycleGate
	chroots *chroot.NFSHandler
	cache   *mountcache.Handler
	mu      sync.Mutex
	owners  map[mountcache.Owner]*mountRetirement
}

func newMountLifecycleSubscriber(gate *mountcache.LifecycleGate, chroots *chroot.NFSHandler, cache *mountcache.Handler) (*mountLifecycleSubscriber, error) {
	s := &mountLifecycleSubscriber{
		gate: gate, chroots: chroots, cache: cache,
		mu: sync.Mutex{}, owners: make(map[mountcache.Owner]*mountRetirement),
	}
	_, err := otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy").Int64ObservableGauge(
		"nfs.retirements.gauge",
		metric.WithDescription("NFS root descriptor cleanups still pending or failed"),
		metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
			observer.Observe(int64(s.count()))

			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create NFS retirement gauge: %w", err)
	}

	return s, nil
}

func (s *mountLifecycleSubscriber) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.owners)
}

func (*mountLifecycleSubscriber) OnInsert(context.Context, *sandbox.Sandbox) {}

func (*mountLifecycleSubscriber) OnStopping(context.Context, *sandbox.Sandbox) {}

func (s *mountLifecycleSubscriber) OnNetworkRelease(ctx context.Context, sbx *sandbox.Sandbox) error {
	owner := mountcache.Owner{SandboxID: sbx.Runtime.SandboxID, LifecycleID: sbx.LifecycleID}
	// The captured network route is already gone. Closing admission and
	// invalidating its handles is sufficient for slot reuse: old operations
	// retain their old lifecycle's descriptor, never the replacement mount.
	// A blocked backing filesystem must not hold a reusable network slot.
	// The indexed worker still owns and observes physical descriptor close.
	s.retire(context.WithoutCancel(ctx), owner)

	return nil
}

func (s *mountLifecycleSubscriber) retire(ctx context.Context, owner mountcache.Owner) *mountRetirement {
	s.mu.Lock()
	defer s.mu.Unlock()
	// A lifecycle has one network assignment, so an existing retirement needs no new invalidation.
	if existing := s.owners[owner]; existing != nil {
		return existing
	}

	// Close admission before invalidation so no admitted MOUNT can publish a
	// new handle after this removal. The map has already removed the route.
	s.gate.Close(owner)
	s.cache.RemoveOwner(owner)
	retirement := &mountRetirement{done: make(chan struct{}), err: nil}
	s.owners[owner] = retirement
	go func() {
		s.gate.CloseAndWait(owner) //nolint:contextcheck // retirement owns the drain independently of the caller deadline
		err := s.chroots.RemoveOwner(ctx, owner)
		if err != nil {
			err = fmt.Errorf("close NFS owner roots: %w", err)
			logger.L().Error(ctx, "NFS root descriptor cleanup failed",
				logger.WithSandboxID(owner.SandboxID), logger.WithLifecycleID(owner.LifecycleID), zap.Error(err))
		}
		s.mu.Lock()
		retirement.err = err
		if err == nil {
			delete(s.owners, owner)
		}
		close(retirement.done)
		s.mu.Unlock()
	}()

	return retirement
}
