package synchronization

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/shared/pkg/synchronization")

type Store[SourceItem any, PoolItem any] interface {
	SourceList(ctx context.Context) ([]SourceItem, error)
	SourceExists(ctx context.Context, s []SourceItem, p PoolItem) bool

	PoolList(ctx context.Context) []PoolItem
	PoolExists(ctx context.Context, s SourceItem) bool
	PoolInsert(ctx context.Context, s SourceItem)
	PoolUpdate(ctx context.Context, s PoolItem)
	PoolRemove(ctx context.Context, s PoolItem)
}

// Synchronize is a generic type that provides methods for synchronizing a pool of items with a source.
// It uses a Store interface to interact with the source and pool, allowing for flexible synchronization logic.
// Replacer is an optional Store extension for items whose identity outlives
// their contents. When an outdated pool item still has a source item with the
// same identity, PoolReplace swaps in its replacement before retiring the old
// item, so a lookup never misses it. It reports false when the item has no
// replacement, and the item is then removed.
type Replacer[SourceItem any, PoolItem any] interface {
	PoolReplace(ctx context.Context, source []SourceItem, old PoolItem) bool
}

type Synchronize[SourceItem any, PoolItem any] struct {
	store Store[SourceItem, PoolItem]

	tracerSpanPrefix string
	logsPrefix       string

	cancel     chan struct{} // channel for cancellation of synchronization
	cancelOnce sync.Once

	// syncSem prevents concurrent PoolInsert calls
	syncSem *semaphore.Weighted
}

func NewSynchronize[SourceItem any, PoolItem any](spanPrefix string, logsPrefix string, store Store[SourceItem, PoolItem]) *Synchronize[SourceItem, PoolItem] {
	s := &Synchronize[SourceItem, PoolItem]{
		tracerSpanPrefix: spanPrefix,
		logsPrefix:       logsPrefix,
		store:            store,
		cancel:           make(chan struct{}),
		syncSem:          semaphore.NewWeighted(1),
	}

	return s
}

func (s *Synchronize[SourceItem, PoolItem]) Start(ctx context.Context, syncInterval time.Duration, syncRoundTimeout time.Duration, runInitialSync bool) {
	if runInitialSync {
		initialSyncTimeout, initialSyncCancel := context.WithTimeout(context.WithoutCancel(ctx), syncRoundTimeout)
		err := s.Sync(initialSyncTimeout)
		initialSyncCancel()
		if err != nil {
			logger.L().Error(ctx, s.getLog("Initial sync failed"), zap.Error(err))
		}
	}

	timer := time.NewTicker(syncInterval)
	defer timer.Stop()

	for {
		select {
		case <-s.cancel:
			logger.L().Info(ctx, s.getLog("Background synchronization ended"))

			return
		case <-timer.C:
			syncTimeout, syncCancel := context.WithTimeout(context.WithoutCancel(ctx), syncRoundTimeout)
			err := s.Sync(syncTimeout)
			syncCancel()
			if err != nil {
				logger.L().Error(ctx, s.getLog("Failed to synchronize"), zap.Error(err))
			}
		}
	}
}

func (s *Synchronize[SourceItem, PoolItem]) Close() {
	s.cancelOnce.Do(
		func() { close(s.cancel) },
	)
}

// Sync performs periodic sync or it can be done as an on-demand synchronization round against the source.
func (s *Synchronize[SourceItem, PoolItem]) Sync(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, s.getSpanName("sync-items"))
	defer span.End()

	if err := s.syncSem.Acquire(ctx, 1); err != nil {
		return fmt.Errorf("failed to acquire sync lock: %w", err)
	}
	defer s.syncSem.Release(1)

	sourceItems, err := s.store.SourceList(ctx)
	if err != nil {
		return err
	}

	// Remove outdated items before inserting discovered ones, so an item that
	// keeps its ID but changes (a moved endpoint, a restarted process) is
	// replaced in this round instead of missing until the next one.
	s.syncOutdated(ctx, sourceItems)
	s.syncDiscovered(ctx, sourceItems)

	return nil
}

func (s *Synchronize[SourceItem, PoolItem]) syncDiscovered(ctx context.Context, sourceItems []SourceItem) {
	ctx, span := tracer.Start(ctx, s.getSpanName("sync-discovered-items"))
	defer span.End()

	var wg sync.WaitGroup
	defer wg.Wait()

	for _, item := range sourceItems {
		// initialize newly discovered item
		wg.Add(1)
		go func(item SourceItem) {
			defer wg.Done()

			// item already exists in the pool, skip it
			if ok := s.store.PoolExists(ctx, item); ok {
				return
			}

			s.store.PoolInsert(ctx, item)
		}(item)
	}
}

func (s *Synchronize[SourceItem, PoolItem]) syncOutdated(ctx context.Context, sourceItems []SourceItem) {
	spanCtx, span := tracer.Start(ctx, s.getSpanName("sync-outdated-items"))
	defer span.End()

	var wg sync.WaitGroup
	defer wg.Wait()

	for _, poolItem := range s.store.PoolList(ctx) {
		// remove the item that is no longer present in the source
		wg.Add(1)
		go func(poolItem PoolItem) {
			defer wg.Done()

			found := s.store.SourceExists(ctx, sourceItems, poolItem)
			if found {
				s.store.PoolUpdate(ctx, poolItem)

				return
			}

			if replacer, ok := s.store.(Replacer[SourceItem, PoolItem]); ok && replacer.PoolReplace(spanCtx, sourceItems, poolItem) {
				return
			}

			s.store.PoolRemove(spanCtx, poolItem)
		}(poolItem)
	}
}

func (s *Synchronize[SourceItem, PoolItem]) getSpanName(name string) string {
	return fmt.Sprintf("%s-%s", s.tracerSpanPrefix, name)
}

func (s *Synchronize[SourceItem, PoolItem]) getLog(message string) string {
	return fmt.Sprintf("%s: %s", s.logsPrefix, message)
}
