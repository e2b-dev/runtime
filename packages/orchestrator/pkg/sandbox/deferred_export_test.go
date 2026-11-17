//go:build linux

package sandbox

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// failingRootfs embeds rootfs.Provider (nil) so it satisfies the interface while
// only overriding PrepareExportDiff — the single method the error branch under
// test exercises. Any other call would nil-panic, which is the intended guard.
type failingRootfs struct {
	rootfs.Provider

	err error
}

func (f failingRootfs) PrepareExportDiff(context.Context, func(context.Context) error) (*block.Cache, error) {
	return nil, f.err
}

// stubRootfs hands back a pre-built ejected cache from PrepareExportDiff, standing
// in for the NBD provider once it has frozen and ejected the writable COW cache.
type stubRootfs struct {
	rootfs.Provider

	cache *block.Cache
}

func (s stubRootfs) PrepareExportDiff(context.Context, func(context.Context) error) (*block.Cache, error) {
	return s.cache, nil
}

// TestRunDeferredRootfsExport verifies the background lifecycle of the deferred
// rootfs export: the frozen (ejected) cache is reflinked into the deferred diff,
// the diff resolves with the sealed bytes, and the cache is closed — with no
// overlay/provider interaction (the sandbox is already stopped).
func TestRunDeferredRootfsExport(t *testing.T) {
	t.Parallel()

	blockSize := int64(header.PageSize)
	numBlocks := int64(3)
	size := blockSize * numBlocks

	// A standalone frozen cache with block 1 dirtied (stands in for the ejected
	// COW cache after the sandbox is stopped).
	sealCache, err := block.NewCache(size, blockSize, t.TempDir()+"/ejected", false)
	require.NoError(t, err)
	blockData := make([]byte, blockSize)
	for i := range blockData {
		blockData[i] = 0x5C
	}
	_, err = sealCache.WriteAt(blockData, blockSize)
	require.NoError(t, err)

	s := &Sandbox{
		Metadata:  &Metadata{},
		Resources: &Resources{},
		config:    cfg.BuilderConfig{DefaultCacheDir: t.TempDir()},
	}

	buildID := uuid.New()
	diffPromise := utils.NewSetOnce[build.Diff]()

	// The setup path captures the diff metadata up front and hands it to the
	// background seal (so the exported bytes match the header from the same read).
	meta, err := sealCache.DiffMetadata()
	require.NoError(t, err)

	s.runDeferredRootfsExport(t.Context(), sealCache, buildID, blockSize, meta, diffPromise)

	diff, err := diffPromise.Result()
	require.NoError(t, err)
	path, err := diff.CachePath(t.Context())
	require.NoError(t, err)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, blockData, got, "deferred diff must contain the dirtied block")
	require.NoError(t, diff.Close())
}

// TestSetupDeferredRootfsExport_PrepareError verifies the earliest failure branch:
// when ejecting the writable cache fails, setup propagates the error and returns
// no diff/header/seal — so Pause falls back to the synchronous export rather than
// registering a deferred diff whose seal would never run.
func TestSetupDeferredRootfsExport_PrepareError(t *testing.T) {
	t.Parallel()

	s := &Sandbox{
		Metadata:  &Metadata{},
		Resources: &Resources{rootfs: failingRootfs{err: errors.New("eject failed")}},
		config:    cfg.BuilderConfig{DefaultCacheDir: t.TempDir()},
	}

	cleanup := NewCleanup()
	diff, hdr, startSeal, err := s.setupDeferredRootfsExport(t.Context(), uuid.New(), nil, cleanup)
	require.Error(t, err)
	require.ErrorContains(t, err, "eject failed")
	require.Nil(t, diff)
	require.Nil(t, hdr)
	require.Nil(t, startSeal)
}

// TestSetupDeferredRootfsExport_AbortCleanupOrder pins the cleanup ordering on the
// abort path (Pause fails before startSeal runs, so `started` stays false and the
// registered abort cleanups fire). The promise-poisoning cleanup MUST run before
// deferredDiff.Close — Close waits on the seal promise with no context, so if the
// order regressed it would block forever on a seal that will never run. The test
// therefore asserts the whole cleanup stack completes promptly (a deadlock is the
// failure it guards against) and that the deferred diff resolves to the abort
// error rather than dangling.
func TestSetupDeferredRootfsExport_AbortCleanupOrder(t *testing.T) {
	t.Parallel()

	blockSize := int64(header.PageSize)
	numBlocks := int64(3)
	size := blockSize * numBlocks

	// A frozen ejected cache with one dirtied block, so setup takes the deferred
	// (non-empty diff) branch that registers the abort cleanups.
	ejected, err := block.NewCache(size, blockSize, t.TempDir()+"/ejected", false)
	require.NoError(t, err)
	dirty := make([]byte, blockSize)
	for i := range dirty {
		dirty[i] = 0xAB
	}
	_, err = ejected.WriteAt(dirty, blockSize)
	require.NoError(t, err)

	originalHeader, err := header.NewHeader(
		header.NewTemplateMetadata(uuid.New(), uint64(blockSize), uint64(size)),
		nil,
	)
	require.NoError(t, err)

	s := &Sandbox{
		Metadata:  &Metadata{},
		Resources: &Resources{rootfs: stubRootfs{cache: ejected}},
		config:    cfg.BuilderConfig{DefaultCacheDir: t.TempDir()},
	}

	cleanup := NewCleanup()
	rootfsDiff, hdr, startSeal, err := s.setupDeferredRootfsExport(t.Context(), uuid.New(), originalHeader, cleanup)
	require.NoError(t, err)
	require.NotNil(t, rootfsDiff)
	require.NotNil(t, hdr)
	require.NotNil(t, startSeal)

	// Do NOT call startSeal: simulate Pause aborting before the seal is handed off.
	// Running the cleanups must not deadlock — guard with a generous timeout so a
	// wrong ordering surfaces as a clear failure instead of a hung test.
	done := make(chan error, 1)
	go func() { done <- cleanup.Run(t.Context()) }()
	select {
	case runErr := <-done:
		require.NoError(t, runErr)
	case <-time.After(10 * time.Second):
		require.Fail(t, "cleanup deadlocked: deferred diff Close ran before the promise was poisoned")
	}

	// The promise was poisoned, so the diff's data methods resolve (to the abort
	// error) without blocking instead of waiting on a seal that never runs. The
	// specific message also confirms setup took the deferred branch (a NoDiff
	// fallback would register no abort cleanup and never poison anything).
	_, err = rootfsDiff.FileSize(t.Context())
	require.ErrorContains(t, err, "pause aborted before deferred rootfs export ran")
}
