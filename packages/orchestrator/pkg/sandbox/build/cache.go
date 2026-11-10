//go:build linux

package build

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	fallbackDiffSize = units.MBToBytes(100)

	meter                   = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build")
	residenceDurationMetric = utils.Must(meter.Int64Histogram("orchestrator.build.cache.residence_duration",
		metric.WithDescription("How long a diff was kept in the local build cache before eviction"),
		metric.WithUnit("s")))
)

type deleteDiff struct {
	size      int64
	cancel    chan struct{}
	closeOnce sync.Once
}

type DiffStore struct {
	cachePath string
	cache     *ttlcache.Cache[DiffStoreKey, Diff]
	initGroup singleflight.Group
	cancel    func()
	config    cfg.Config
	flags     *featureflags.Client

	// pdSizes is used to keep track of the diff sizes
	// that are scheduled for deletion, as this won't show up in the disk usage.
	pdSizes map[DiffStoreKey]*deleteDiff
	pdMu    sync.RWMutex
	pdDelay time.Duration

	insertionTimes sync.Map // map[DiffStoreKey]time.Time — tracks when each diff was cached

	// pinned entries are skipped by disk-pressure eviction (TTL eviction still
	// applies). Used to protect a diff whose Close would tear down state another
	// live entry depends on — e.g. the memfile diff whose DedupedMemfdCache is
	// also serving an in-flight provisional resume.
	pinned sync.Map // map[DiffStoreKey]struct{}
}

func NewDiffStore(
	config cfg.Config,
	flags *featureflags.Client,
	cachePath string,
	ttl, delay time.Duration,
) (*DiffStore, error) {
	err := os.MkdirAll(cachePath, 0o755)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	cache := ttlcache.New(
		ttlcache.WithTTL[DiffStoreKey, Diff](ttl),
	)

	ds := &DiffStore{
		cachePath: cachePath,
		cache:     cache,
		cancel:    func() {},
		config:    config,
		flags:     flags,
		pdSizes:   make(map[DiffStoreKey]*deleteDiff),
		pdDelay:   delay,
	}

	cache.OnEviction(func(ctx context.Context, _ ttlcache.EvictionReason, item *ttlcache.Item[DiffStoreKey, Diff]) {
		if insertedAt, ok := ds.insertionTimes.LoadAndDelete(item.Key()); ok {
			duration := time.Since(insertedAt.(time.Time))
			residenceDurationMetric.Record(ctx, int64(duration.Seconds()))
		}

		buildData := item.Value()

		// buildData will be deleted by calling buildData.Close()
		defer ds.resetDelete(item.Key())

		// A deferred diff whose background seal hasn't resolved would block Close()
		// on the reflink; close it off the eviction goroutine so this callback
		// isn't stalled. Bounded (the seal always resolves) and only reachable if
		// the TTL is shortened below the seal time — the disk-pressure eviction
		// path already skips unsealed diffs (see deferredDiff.sealed()).
		if dd, ok := buildData.(*deferredDiff); ok && !dd.sealed() {
			logCtx := context.WithoutCancel(ctx)
			go func() {
				if closeErr := dd.Close(); closeErr != nil {
					logger.L().Warn(logCtx, "failed to close unsealed deferred diff", zap.Any("item_key", item.Key()), zap.Error(closeErr))
				}
			}()

			return
		}

		if closeErr := buildData.Close(); closeErr != nil {
			logger.L().Warn(ctx, "failed to cleanup build data cache for item", zap.Any("item_key", item.Key()), zap.Error(closeErr))
		}
	})

	return ds, nil
}

type DiffStoreKey string

func GetDiffStoreKey(buildID string, diffType DiffType) DiffStoreKey {
	return DiffStoreKey(buildID + "/" + string(diffType))
}

func (s *DiffStore) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	go s.cache.Start()
	go s.startDiskSpaceEviction(ctx, s.config, s.flags)
}

func (s *DiffStore) Close() {
	s.cancel()
	s.cache.Stop()
}

// Get returns the cached Diff for key, refreshing TTL and cancelling any
// pending eviction. Returns (nil, false) if the key isn't present.
func (s *DiffStore) Get(key DiffStoreKey) (Diff, bool) {
	s.resetDelete(key)
	item := s.cache.Get(key)
	if item == nil {
		return nil, false
	}

	return item.Value(), true
}

// GetOrCreate returns the cached Diff for key, or calls create inside a
// singleflight to construct + cache a new one. The create closure is invoked
// at most once per key across concurrent callers; on success the returned Diff
// is cached and its insertion time recorded.
func (s *DiffStore) GetOrCreate(ctx context.Context, key DiffStoreKey, create func(context.Context) (Diff, error)) (Diff, error) {
	s.resetDelete(key)

	if item := s.cache.Get(key); item != nil {
		return item.Value(), nil
	}

	v, err, _ := s.initGroup.Do(string(key), func() (any, error) {
		// Double-check: another goroutine may have cached it while we waited.
		if item := s.cache.Get(key); item != nil {
			return item.Value(), nil
		}

		insertTime := time.Now()

		diff, err := create(ctx)
		if err != nil {
			return nil, err
		}

		s.cache.Set(key, diff, ttlcache.DefaultTTL)
		s.insertionTimes.Store(diff.CacheKey(), insertTime)

		return diff, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create diff: %w", err)
	}

	return v.(Diff), nil
}

func (s *DiffStore) Add(d Diff) {
	s.resetDelete(d.CacheKey())
	s.cache.Set(d.CacheKey(), d, ttlcache.DefaultTTL)
	s.insertionTimes.LoadOrStore(d.CacheKey(), time.Now())
}

func (s *DiffStore) Has(d Diff) bool {
	return s.cache.Has(d.CacheKey())
}

// Lookup returns the cached Diff for the given key without initialising a new one.
// Returns (nil, false) if the key is not present in the cache.
func (s *DiffStore) Lookup(key DiffStoreKey) (Diff, bool) {
	item := s.cache.Get(key)
	if item == nil {
		return nil, false
	}

	return item.Value(), true
}

func (s *DiffStore) startDiskSpaceEviction(
	ctx context.Context,
	config cfg.Config,
	flags *featureflags.Client,
) {
	services := cfg.GetServices(config)

	getDelay := func(fast bool) time.Duration {
		if fast {
			return time.Microsecond
		}

		return time.Second
	}

	timer := time.NewTimer(getDelay(false))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			dUsed, dTotal, err := diskUsage(s.cachePath)
			if err != nil {
				logger.L().Error(ctx, "failed to get disk usage", zap.Error(err))
				timer.Reset(getDelay(false))

				continue
			}

			pUsed := s.getPendingDeletesSize()
			used := int64(dUsed) - pUsed
			percentage := float64(used) / float64(dTotal) * 100

			threshold := evictionThreshold(ctx, flags, services)

			if percentage <= float64(threshold) {
				timer.Reset(getDelay(false))

				continue
			}

			succ, err := s.deleteOldestFromCache(ctx)
			if err != nil {
				logger.L().Error(ctx, "failed to delete oldest item from cache", zap.Error(err))
				timer.Reset(getDelay(false))

				continue
			}

			// Item evicted, reset timer to fast check
			timer.Reset(getDelay(succ))
		}
	}
}

// evictionThreshold returns the maximum allowed disk usage percentage for the
// build cache. When multiple services (template manager, orchestrator) are
// defined, the lowest of their configured thresholds wins to ensure none of
// the set limits is exceeded. Flag evaluation already falls back per service
// inside IntFlag, so the flag fallback is only used directly when no services
// are configured; a flag value above the fallback is honored.
func evictionThreshold(ctx context.Context, flags *featureflags.Client, services cfg.Services) int {
	if len(services) == 0 {
		return featureflags.BuildCacheMaxUsagePercentage.Fallback()
	}

	threshold := math.MaxInt
	for _, svc := range services {
		st := flags.IntFlag(ctx, featureflags.BuildCacheMaxUsagePercentage, featureflags.ServiceContext(string(svc)))
		if st < threshold {
			threshold = st
		}
	}

	return threshold
}

func (s *DiffStore) getPendingDeletesSize() int64 {
	s.pdMu.RLock()
	defer s.pdMu.RUnlock()

	var pendingSize int64
	for _, value := range s.pdSizes {
		pendingSize += value.size
	}

	return pendingSize
}

// deleteOldestFromCache deletes the oldest item (smallest TTL) from the cache.
// ttlcache has items in order by TTL
func (s *DiffStore) deleteOldestFromCache(ctx context.Context) (suc bool, e error) {
	defer func() {
		// Because of bug in ttlcache RangeBackwards method, we need to handle potential panic until it gets fixed
		if r := recover(); r != nil {
			e = fmt.Errorf("recovered from panic in deleteOldestFromCache: %v", r)
			suc = false

			logger.L().Error(ctx, "recovered from panic in deleteOldestFromCache", zap.Error(e))
		}
	}()

	success := false
	s.cache.RangeBackwards(func(item *ttlcache.Item[DiffStoreKey, Diff]) bool {
		isDeleted := s.isBeingDeleted(item.Key())
		if isDeleted {
			return true
		}

		// Skip pinned entries (e.g. a memfile diff still backing an in-flight
		// provisional resume); closing them would tear down shared state.
		if s.isPinned(item.Key()) {
			return true
		}

		// Skip a deferred diff whose background rootfs seal hasn't resolved yet:
		// FileSize below would block on the seal (stalling the sole eviction
		// goroutine), and a fresh, still-sealing snapshot is exactly what a
		// just-resumed peer needs. It becomes evictable once the seal resolves.
		if dd, ok := item.Value().(*deferredDiff); ok && !dd.sealed() {
			return true
		}

		sfSize, err := item.Value().FileSize(ctx)
		if err != nil {
			logger.L().Warn(ctx, "failed to get size of deleted item from cache", zap.Error(err))
			sfSize = fallbackDiffSize
		}

		s.scheduleDelete(ctx, item.Key(), sfSize)

		success = true

		return false
	})

	return success, e
}

func (s *DiffStore) resetDelete(key DiffStoreKey) {
	s.pdMu.Lock()
	defer s.pdMu.Unlock()

	dDiff, f := s.pdSizes[key]
	if !f {
		return
	}

	dDiff.closeOnce.Do(func() {
		close(dDiff.cancel)
	})
	delete(s.pdSizes, key)
}

func (s *DiffStore) isBeingDeleted(key DiffStoreKey) bool {
	s.pdMu.RLock()
	defer s.pdMu.RUnlock()

	_, f := s.pdSizes[key]

	return f
}

// Pin protects a cached entry from disk-pressure eviction (TTL eviction still
// applies). Idempotent; pair every Pin with an Unpin.
func (s *DiffStore) Pin(key DiffStoreKey) { s.pinned.Store(key, struct{}{}) }

// Unpin lifts a Pin, making the entry eligible for disk-pressure eviction again.
func (s *DiffStore) Unpin(key DiffStoreKey) { s.pinned.Delete(key) }

func (s *DiffStore) isPinned(key DiffStoreKey) bool {
	_, ok := s.pinned.Load(key)

	return ok
}

func (s *DiffStore) scheduleDelete(ctx context.Context, key DiffStoreKey, dSize int64) {
	s.pdMu.Lock()
	defer s.pdMu.Unlock()

	cancelCh := make(chan struct{})
	s.pdSizes[key] = &deleteDiff{
		size:   dSize,
		cancel: cancelCh,
	}

	// Delay cache (file close/removal) deletion,
	// this is to prevent race conditions with exposed slices,
	// pending data fetching, or data upload
	go (func() {
		select {
		case <-ctx.Done():
		case <-cancelCh:
		case <-time.After(s.pdDelay):
			// The entry may have been pinned after this delete was scheduled: a
			// Pin can race the eviction scan (its isPinned check in
			// deleteOldestFromCache runs just before scheduleDelete). Re-check at
			// fire time — the last point before eviction — since deleting a
			// pinned diff would tear down state an in-flight provisional resume
			// still needs. Clear the pending-delete record so the entry becomes
			// eligible for eviction again once it is unpinned.
			if s.isPinned(key) {
				s.resetDelete(key)

				return
			}
			s.cache.Delete(key)
		}
	})()
}

func diskUsage(path string) (uint64, uint64, error) {
	var stat unix.Statfs_t
	err := unix.Statfs(path, &stat)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get disk stats for path %s: %w", path, err)
	}

	// Available blocks * size per block = available space in bytes
	free := stat.Bavail * uint64(stat.Bsize)
	total := stat.Blocks * uint64(stat.Bsize)
	used := total - free

	return used, total, nil
}

func (s *DiffStore) RemoveCache() {
	s.cache.DeleteAll()
}
