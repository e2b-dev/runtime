//go:build linux

package build

// Race Condition Tests:
// To reproduce the data race condition reported in the cache eviction callbacks,
// run the following tests with the race detector enabled:
//
// Run all race tests:    go test -race -v -run "TestDiffStore.*Race"
// Run first race test:   go test -race -v -run TestDiffStoreConcurrentEvictionRace
// Run second race test:  go test -race -v -run TestDiffStoreResetDeleteRace
//
// These tests simulate the race condition where multiple OnEviction callbacks
// run concurrently and both try to access the same key in the resetDelete method,
// causing a race when closing the cancel channel.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	blockSize = int64(1024)
)

func newRootFSDiff(t *testing.T, cachePath, buildId string) Diff {
	t.Helper()

	localDiff, err := NewLocalDiffFile(cachePath, buildId, Rootfs)
	require.NoError(t, err)

	// Write 100 bytes to the file
	n, err := localDiff.WriteAt(make([]byte, 100), 0)
	require.NoError(t, err)
	assert.Equal(t, 100, n)

	diff, err := localDiff.CloseToDiff(blockSize)
	require.NoError(t, err)

	return diff
}

func newDiffWithAsserts(t *testing.T, cachePath, buildId string, diffType DiffType, blockSize int64) (Diff, error) {
	t.Helper()

	localDiff, err := NewLocalDiffFile(cachePath, buildId, diffType)
	if err != nil {
		return nil, err
	}

	// Write 100 bytes to the file
	n, err := localDiff.WriteAt(make([]byte, 100), 0)
	if err != nil {
		return nil, err
	}
	assert.Equal(t, 100, n)

	diff, err := localDiff.CloseToDiff(blockSize)
	if err != nil {
		return nil, err
	}

	return diff, nil
}

func TestNewDiffStore(t *testing.T) {
	t.Parallel()
	cachePath := t.TempDir()

	c, err := cfg.Parse()
	require.NoError(t, err)

	flags := flagsWithMaxBuildCachePercentage(t, 90)

	store, err := NewDiffStore(
		c,
		flags,
		cachePath,
		25*time.Hour,
		60*time.Second,
	)
	require.NoError(t, err)
	assert.NotNil(t, store)
}

func TestDiffStoreTTLEviction(t *testing.T) {
	t.Parallel()
	cachePath := t.TempDir()

	c, err := cfg.Parse()
	require.NoError(t, err)

	// Created outside the bubble: the client's background goroutines must not
	// join it, and flag evaluation from inside is synchronous.
	flags := flagsWithMaxBuildCachePercentage(t, 100)

	synctest.Test(t, func(t *testing.T) {
		ttl := 1 * time.Second
		delay := 60 * time.Second
		store, err := NewDiffStore(
			c,
			flags,
			cachePath,
			ttl,
			delay,
		)
		require.NoError(t, err)

		store.Start(t.Context())
		t.Cleanup(store.Close)

		// Add an item to the cache
		diff := newRootFSDiff(t, cachePath, "build-test-id")

		// Add an item to the cache
		store.Add(diff)

		// Expire diff
		time.Sleep(ttl + time.Second)

		found := store.Has(diff)
		assert.False(t, found)
	})
}

func TestDiffStoreRefreshTTLEviction(t *testing.T) {
	t.Parallel()
	cachePath := t.TempDir()

	c, err := cfg.Parse()
	require.NoError(t, err)

	flags := flagsWithMaxBuildCachePercentage(t, 100)

	synctest.Test(t, func(t *testing.T) {
		ttl := 1 * time.Second
		delay := 60 * time.Second
		store, err := NewDiffStore(
			c,
			flags,
			cachePath,
			ttl,
			delay,
		)
		require.NoError(t, err)

		// Add an item to the cache
		diff := newRootFSDiff(t, cachePath, "build-test-id")

		// Add an item to the cache
		store.Add(diff)

		// Refresh diff expiration
		time.Sleep(ttl / 2)
		_, ok := store.Get(diff.CacheKey())
		require.True(t, ok)

		// Try to expire diff
		time.Sleep(ttl/2 + time.Microsecond)

		// Is still in cache
		found2 := store.Has(diff)
		assert.True(t, found2)
	})
}

func TestDiffStoreDelayEviction(t *testing.T) {
	t.Parallel()
	cachePath := t.TempDir()

	c, err := cfg.Parse()
	require.NoError(t, err)

	flags := flagsWithMaxBuildCachePercentage(t, 0)

	synctest.Test(t, func(t *testing.T) {
		ttl := 60 * time.Second
		delay := 4 * time.Second
		store, err := NewDiffStore(
			c,
			flags,
			cachePath,
			ttl,
			delay,
		)
		require.NoError(t, err)

		store.Start(t.Context())
		t.Cleanup(store.Close)

		// Add an item to the cache
		diff := newRootFSDiff(t, cachePath, "build-test-id")

		// Add an item to the cache
		store.Add(diff)

		// Wait for removal trigger of diff
		time.Sleep(2 * time.Second)

		// Verify still in cache
		found := store.Has(diff)
		assert.True(t, found)
		dFound := store.isBeingDeleted(diff.CacheKey())
		assert.True(t, dFound)

		// Wait for complete removal of diff
		time.Sleep(delay)

		found = store.Has(diff)
		assert.False(t, found)
		dFound = store.isBeingDeleted(diff.CacheKey())
		assert.False(t, dFound)
	})
}

func TestDiffStoreDelayEvictionAbort(t *testing.T) {
	t.Parallel()
	cachePath := t.TempDir()

	c, err := cfg.Parse()
	require.NoError(t, err)

	flags := flagsWithMaxBuildCachePercentage(t, 100)

	synctest.Test(t, func(t *testing.T) {
		ttl := 60 * time.Second
		delay := 4 * time.Second
		store, err := NewDiffStore(
			c,
			flags,
			cachePath,
			ttl,
			delay,
		)
		require.NoError(t, err)

		// The store is deliberately not Start()ed: the disk-space eviction loop
		// would otherwise re-schedule a deletion of the diff (depending on the
		// host's actual disk usage) and race with the abort below. The delayed
		// deletion is instead scheduled explicitly via deleteOldestFromCache,
		// the same code path the eviction loop uses.

		// Add an item to the cache
		diff := newRootFSDiff(t, cachePath, "build-test-id")

		// Add an item to the cache
		store.Add(diff)

		// Schedule delayed deletion of the diff
		scheduled, err := store.deleteOldestFromCache(t.Context())
		require.NoError(t, err)
		require.True(t, scheduled)

		// Wait a part of the delay period before aborting the removal
		time.Sleep(delay / 2)

		// Verify still in cache
		found := store.Has(diff)
		assert.True(t, found)
		dFound := store.isBeingDeleted(diff.CacheKey())
		assert.True(t, dFound)

		// Abort removal of diff
		_, ok := store.Get(diff.CacheKey())
		require.True(t, ok)

		found = store.Has(diff)
		assert.True(t, found)
		dFound = store.isBeingDeleted(diff.CacheKey())
		assert.False(t, dFound)

		// Check insufficient delay cancellation of diff and verify it's still in the cache
		// after the delay period
		time.Sleep(delay/2 + time.Second)
		found = store.Has(diff)
		assert.True(t, found)
	})
}

// A pinned entry must be skipped by disk-pressure eviction (the next-oldest is
// chosen instead), and become eligible again once unpinned.
func TestDiffStorePinnedSkippedByEviction(t *testing.T) {
	t.Parallel()
	cachePath := t.TempDir()

	c, err := cfg.Parse()
	require.NoError(t, err)
	flags := flagsWithMaxBuildCachePercentage(t, 100)
	store, err := NewDiffStore(c, flags, cachePath, 60*time.Second, 4*time.Second)
	require.NoError(t, err)

	oldest := newRootFSDiff(t, cachePath, "pin-oldest")
	store.Add(oldest)
	newer := newRootFSDiff(t, cachePath, "pin-newer")
	store.Add(newer)

	// Pin the oldest → eviction skips it and schedules the next-oldest.
	store.Pin(oldest.CacheKey())
	_, err = store.deleteOldestFromCache(t.Context())
	require.NoError(t, err)
	assert.False(t, store.isBeingDeleted(oldest.CacheKey()))
	assert.True(t, store.isBeingDeleted(newer.CacheKey()))

	// Unpin → the oldest is eligible again.
	store.Unpin(oldest.CacheKey())
	_, err = store.deleteOldestFromCache(t.Context())
	require.NoError(t, err)
	assert.True(t, store.isBeingDeleted(oldest.CacheKey()))
}

// A Pin that lands after a delete was already scheduled (the Add→Pin window, or
// a Pin racing the eviction scan) must still protect the entry: the scheduled
// delete re-checks isPinned when it fires and skips the eviction, so the entry
// survives and is eligible again only once unpinned.
func TestDiffStorePinnedSurvivesScheduledDelete(t *testing.T) {
	t.Parallel()
	cachePath := t.TempDir()

	c, err := cfg.Parse()
	require.NoError(t, err)
	flags := flagsWithMaxBuildCachePercentage(t, 100)

	synctest.Test(t, func(t *testing.T) {
		delay := 150 * time.Millisecond
		store, err := NewDiffStore(c, flags, cachePath, 60*time.Second, delay)
		require.NoError(t, err)

		diff := newRootFSDiff(t, cachePath, "pin-after-schedule")
		store.Add(diff)

		// Delete scheduled first (as disk pressure would), then the Pin lands.
		store.scheduleDelete(t.Context(), diff.CacheKey(), 1024)
		require.True(t, store.isBeingDeleted(diff.CacheKey()))
		store.Pin(diff.CacheKey())

		// After the delay elapses the fire-time isPinned re-check must have skipped
		// the eviction: the entry is still cached and no longer marked for deletion.
		time.Sleep(delay + time.Millisecond)
		require.False(t, store.isBeingDeleted(diff.CacheKey()))
		_, found := store.Lookup(diff.CacheKey())
		assert.True(t, found, "pinned entry must survive the scheduled delete")

		// Unpin → a freshly scheduled delete now evicts it.
		store.Unpin(diff.CacheKey())
		store.scheduleDelete(t.Context(), diff.CacheKey(), 1024)
		time.Sleep(delay + time.Millisecond)
		_, found = store.Lookup(diff.CacheKey())
		require.False(t, found)
	})
}

func TestDiffStoreOldestFromCache(t *testing.T) {
	t.Parallel()
	cachePath := t.TempDir()

	c, err := cfg.Parse()
	require.NoError(t, err)

	flags := flagsWithMaxBuildCachePercentage(t, 100)

	synctest.Test(t, func(t *testing.T) {
		ttl := 60 * time.Second
		delay := 4 * time.Second
		store, err := NewDiffStore(
			c,
			flags,
			cachePath,
			ttl,
			delay,
		)
		require.NoError(t, err)

		// Add items to the cache
		diff := newRootFSDiff(t, cachePath, "build-test-id")
		store.Add(diff)
		diff2 := newRootFSDiff(t, cachePath, "build-test-id-2")
		store.Add(diff2)

		found := store.Has(diff)
		assert.True(t, found)

		// Delete oldest item
		_, err = store.deleteOldestFromCache(t.Context())
		require.NoError(t, err)

		assert.True(t, store.isBeingDeleted(diff.CacheKey()))
		// Wait for removal trigger of diff
		time.Sleep(delay + time.Second)

		// Verify oldest item is deleted
		found = store.Has(diff)
		assert.False(t, found)

		found = store.Has(diff2)
		assert.True(t, found)

		// Add another item to the cache
		diff3 := newRootFSDiff(t, cachePath, "build-test-id-3")
		store.Add(diff3)

		// Delete oldest item
		_, err = store.deleteOldestFromCache(t.Context())
		require.NoError(t, err)

		assert.True(t, store.isBeingDeleted(diff2.CacheKey()))
		// Wait for removal trigger of diff
		time.Sleep(delay + time.Second)

		// Verify oldest item is deleted
		found = store.Has(diff2)
		assert.False(t, found)

		found = store.Has(diff3)
		assert.True(t, found)
	})
}

// TestDiffStoreConcurrentEvictionRace simulates the data race condition where
// multiple eviction callbacks run concurrently and both try to close the same
// cancel channel in resetDelete method. This test should be run with the race
// detector enabled: go test -race
func TestDiffStoreConcurrentEvictionRace(t *testing.T) {
	t.Parallel()

	cachePath := t.TempDir()

	c, err := cfg.Parse()
	require.NoError(t, err)

	// Set to 0% to trigger disk space evictions
	flags := flagsWithMaxBuildCachePercentage(t, 0)

	// Use very short TTL and delay to trigger rapid evictions
	ttl := 10 * time.Millisecond
	delay := 50 * time.Millisecond
	store, err := NewDiffStore(
		c,
		flags,
		cachePath,
		ttl,
		delay,
	)
	require.NoError(t, err)

	store.Start(t.Context())
	t.Cleanup(store.Close)

	// Number of concurrent operations to create race conditions
	numGoroutines := 50
	numIterations := 100

	var wg sync.WaitGroup

	// Create multiple goroutines that add and remove items rapidly
	for i := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := range numIterations {
				// Create diffs with same buildID but different iterations
				// This increases chances of race conditions
				buildID := fmt.Sprintf("build-%d", goroutineID%10) // Limit to 10 different build IDs
				diff, err := newDiffWithAsserts(t, cachePath, buildID, Rootfs, blockSize)
				if !assert.NoError(t, err) {
					continue
				}

				// Add to store
				store.Add(diff)

				// Small delay to allow TTL expiration and concurrent access
				time.Sleep(time.Microsecond * 100)

				// Try to trigger manual deletion which can race with TTL eviction
				if j%10 == 0 {
					_, err := store.deleteOldestFromCache(t.Context())
					assert.NoError(t, err)
				}

				// Occasionally try to access the item, which calls resetDelete
				if j%5 == 0 {
					store.Get(diff.CacheKey())
				}
			}
		}(i)
	}

	// Additional goroutine that continuously tries to delete oldest items
	// to increase race condition probability
	wg.Go(func() {
		for range numIterations * 2 {
			_, err = store.deleteOldestFromCache(t.Context())
			assert.NoError(t, err) //nolint:testifylint
			time.Sleep(time.Microsecond * 50)
		}
	})

	// Wait for all goroutines to complete
	wg.Wait()

	// Allow some time for pending deletions to complete
	time.Sleep(delay * 2)

	// Test passes if no race condition panic occurs
	// The race detector will catch the race if it occurs
}

// TestDiffStoreResetDeleteRace specifically targets the resetDelete method
// race condition by simulating the exact scenario from the race report
func TestDiffStoreResetDeleteRace(t *testing.T) {
	t.Parallel()

	cachePath := t.TempDir()

	c, err := cfg.Parse()
	require.NoError(t, err)

	flags := flagsWithMaxBuildCachePercentage(t, 100)

	// Very short TTL to trigger evictions quickly
	ttl := 5 * time.Millisecond
	delay := 100 * time.Millisecond
	store, err := NewDiffStore(
		c,
		flags,
		cachePath,
		ttl,
		delay,
	)
	require.NoError(t, err)

	store.Start(t.Context())
	t.Cleanup(store.Close)

	// Create a base build ID for generating test diffs
	buildID := "race-test-build"

	var wg sync.WaitGroup
	const numConcurrentOps = 100

	// Simulate the exact race condition:
	// 1. Add item to cache
	// 2. Schedule it for deletion (creates entry in pdSizes)
	// 3. Multiple goroutines try to reset the deletion simultaneously

	for i := range numConcurrentOps {
		wg.Add(1)
		go func(iteration int) {
			defer wg.Done()

			// Create a unique diff for this iteration to increase concurrency
			iterDiff, err := newDiffWithAsserts(t, cachePath, fmt.Sprintf("%s-%d", buildID, iteration), Rootfs, blockSize)
			if !assert.NoError(t, err) {
				return
			}

			// Add to store
			store.Add(iterDiff)

			// Immediately schedule for deletion to populate pdSizes
			store.scheduleDelete(t.Context(), iterDiff.CacheKey(), 1024)

			// Small random delay to desynchronize goroutines slightly
			time.Sleep(time.Duration(iteration%10) * time.Microsecond)

			// This call will trigger resetDelete, which is where the race occurs.
			// Multiple goroutines calling resetDelete on the same key can race.
			store.Get(iterDiff.CacheKey())

			// Also try direct resetDelete calls to increase race probability
			store.resetDelete(iterDiff.CacheKey())
		}(i)
	}

	// Wait for all goroutines
	wg.Wait()

	// Allow cleanup to complete
	time.Sleep(delay * 2)
}

func TestEvictionThreshold(t *testing.T) {
	t.Parallel()

	t.Run("no services falls back", func(t *testing.T) {
		t.Parallel()

		flags := flagsWithMaxBuildCachePercentage(t, 95)

		got := evictionThreshold(t.Context(), flags, nil)
		assert.Equal(t, featureflags.BuildCacheMaxUsagePercentage.Fallback(), got)
	})

	t.Run("flag can raise threshold above fallback", func(t *testing.T) {
		t.Parallel()

		flags := flagsWithMaxBuildCachePercentage(t, 95)

		got := evictionThreshold(t.Context(), flags, cfg.Services{cfg.Orchestrator})
		assert.Equal(t, 95, got)
	})

	t.Run("flag can lower threshold below fallback", func(t *testing.T) {
		t.Parallel()

		flags := flagsWithMaxBuildCachePercentage(t, 10)

		got := evictionThreshold(t.Context(), flags, cfg.Services{cfg.Orchestrator})
		assert.Equal(t, 10, got)
	})

	t.Run("lowest service threshold wins", func(t *testing.T) {
		t.Parallel()

		datastore := ldtestdata.DataSource()
		datastore.Update(
			datastore.Flag(featureflags.BuildCacheMaxUsagePercentage.String()).
				Variations(ldvalue.Int(95), ldvalue.Int(40)).
				VariationIndexForKey(featureflags.ServiceKind, string(cfg.Orchestrator), 0).
				VariationIndexForKey(featureflags.ServiceKind, string(cfg.TemplateManager), 1).
				FallthroughVariationIndex(0),
		)

		flags, err := featureflags.NewClientWithDatasource(datastore)
		require.NoError(t, err)
		t.Cleanup(func() {
			assert.NoError(t, flags.Close(context.WithoutCancel(t.Context())))
		})

		got := evictionThreshold(t.Context(), flags, cfg.Services{cfg.Orchestrator, cfg.TemplateManager})
		assert.Equal(t, 40, got)
	})
}

func flagsWithMaxBuildCachePercentage(tb testing.TB, maxBuildCachePercentage int) *featureflags.Client {
	tb.Helper()

	datastore := ldtestdata.DataSource()

	datastore.Update(
		datastore.Flag(featureflags.BuildCacheMaxUsagePercentage.String()).
			ValueForAll(ldvalue.Int(maxBuildCachePercentage)),
	)

	flags, err := featureflags.NewClientWithDatasource(datastore)
	require.NoError(tb, err)

	tb.Cleanup(func() {
		err := flags.Close(context.WithoutCancel(tb.Context()))
		assert.NoError(tb, err)
	})

	return flags
}

// uuid.Nil mapping reports cached without touching the diff store.
func TestFileIsCached_UUIDNilMappingReportsCached(t *testing.T) {
	t.Parallel()

	store, err := NewDiffStore(
		mustParseCfg(t),
		flagsWithMaxBuildCachePercentage(t, 90),
		t.TempDir(),
		time.Hour,
		time.Minute,
	)
	require.NoError(t, err)

	const size = 4096
	hdr, err := header.NewHeader(
		header.NewTemplateMetadata(uuid.Nil, size, size),
		[]header.BuildMap{{Offset: 0, Length: size, BuildId: uuid.Nil}},
	)
	require.NoError(t, err)

	m, err := blockmetrics.NewMetrics(noop.NewMeterProvider())
	require.NoError(t, err)
	f := NewFile(hdr, store, Memfile, nil, m)

	require.True(t, f.IsCached(t.Context(), 0, size))
}

// Uninitialized StorageDiff reports uncached without panicking or Init'ing.
func TestFileIsCached_UninitializedChunkerReportsUncached(t *testing.T) {
	t.Parallel()

	store, err := NewDiffStore(
		mustParseCfg(t),
		flagsWithMaxBuildCachePercentage(t, 90),
		t.TempDir(),
		time.Hour,
		time.Minute,
	)
	require.NoError(t, err)

	parentBuildID := uuid.New()
	const size = 4096
	hdr, err := header.NewHeader(
		header.NewTemplateMetadata(parentBuildID, size, size),
		[]header.BuildMap{{Offset: 0, Length: size, BuildId: parentBuildID}},
	)
	require.NoError(t, err)

	m, err := blockmetrics.NewMetrics(noop.NewMeterProvider())
	require.NoError(t, err)
	f := NewFile(hdr, store, Memfile, nil, m)

	require.False(t, f.IsCached(t.Context(), 0, size))
}

func mustParseCfg(t *testing.T) cfg.Config {
	t.Helper()
	c, err := cfg.Parse()
	require.NoError(t, err)

	return c
}

// A deferred rootfs diff whose background seal hasn't resolved must be skipped
// by disk-pressure eviction: its data methods (FileSize) block on the seal, so
// evicting it would stall the sole eviction goroutine, and a fresh, still-sealing
// snapshot is exactly what a just-resumed peer needs. Once the seal resolves the
// entry becomes evictable like any other.
func TestDiffStoreEvictionSkipsUnsealedDeferredDiff(t *testing.T) {
	t.Parallel()
	cachePath := t.TempDir()

	c, err := cfg.Parse()
	require.NoError(t, err)
	flags := flagsWithMaxBuildCachePercentage(t, 100)
	store, err := NewDiffStore(c, flags, cachePath, 60*time.Second, 4*time.Second)
	require.NoError(t, err)

	// Oldest entry: a deferred rootfs diff whose background seal hasn't resolved.
	promise := utils.NewSetOnce[Diff]()
	deferred := NewDeferredDiff(GetDiffStoreKey("deferred-unsealed", Rootfs), blockSize, promise)
	store.Add(deferred)

	// A newer, ordinary diff behind it.
	newer := newRootFSDiff(t, cachePath, "seal-newer")
	store.Add(newer)

	// Eviction must skip the unsealed deferred diff and fall through to the
	// next-oldest instead of blocking on the seal.
	ok, err := store.deleteOldestFromCache(t.Context())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.False(t, store.isBeingDeleted(deferred.CacheKey()), "unsealed deferred diff must be skipped")
	assert.True(t, store.isBeingDeleted(newer.CacheKey()), "eviction must fall through to the next-oldest")

	// Once the seal resolves, the deferred diff is evictable like any other entry.
	require.NoError(t, promise.SetValue(newRootFSDiff(t, cachePath, "seal-resolved")))
	ok, err = store.deleteOldestFromCache(t.Context())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.True(t, store.isBeingDeleted(deferred.CacheKey()), "sealed deferred diff must be evictable")
}
