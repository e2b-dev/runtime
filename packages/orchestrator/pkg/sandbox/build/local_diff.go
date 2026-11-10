//go:build linux

package build

import (
	"context"
	"fmt"
	"os"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

type LocalDiffFile struct {
	*os.File

	cachePath string
	cacheKey  DiffStoreKey
}

func NewLocalDiffFile(
	basePath string,
	buildId string,
	diffType DiffType,
) (*LocalDiffFile, error) {
	cachePath := GenerateDiffCachePath(basePath, buildId, diffType)

	f, err := os.OpenFile(cachePath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	return &LocalDiffFile{
		File:      f,
		cachePath: cachePath,
		cacheKey:  GetDiffStoreKey(buildId, diffType),
	}, nil
}

func (f *LocalDiffFile) Close() error {
	err := f.File.Close()
	if err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	err = os.Remove(f.cachePath)
	if err != nil {
		return fmt.Errorf("failed to remove file: %w", err)
	}

	return nil
}

func (f *LocalDiffFile) CloseToDiff(
	blockSize int64,
) (d Diff, e error) {
	defer f.File.Close()

	// On any failure to produce a usable Diff (e.g. a stat error after the bytes
	// were written), remove the partial cache file. Nothing registers it in
	// the DiffStore, so otherwise it orphans in the cache dir until process restart
	// — disk-pressure eviction can't reclaim a file it doesn't know about.
	defer func() {
		if e != nil {
			if rmErr := os.Remove(f.cachePath); rmErr != nil && !os.IsNotExist(rmErr) {
				e = fmt.Errorf("%w; remove partial diff file: %w", e, rmErr)
			}
		}
	}()

	// No sync: cache files are never read after a crash, and every reader goes through the page cache.
	size, err := f.File.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file size: %w", err)
	}

	if size.Size() == 0 {
		// NoDiff carries no path and nothing registers this file in the DiffStore,
		// so this is the last chance to reclaim it.
		if rmErr := os.Remove(f.cachePath); rmErr != nil && !os.IsNotExist(rmErr) {
			return nil, fmt.Errorf("failed to remove empty diff file: %w", rmErr)
		}

		return &NoDiff{}, nil
	}

	return newLocalDiff(
		f.cacheKey,
		f.cachePath,
		size.Size(),
		blockSize,
	)
}

type localDiff struct {
	cacheKey DiffStoreKey
	cache    block.DiffSource
}

var _ Diff = (*localDiff)(nil)

func NewLocalDiffFromCache(
	cacheKey DiffStoreKey,
	cache block.DiffSource,
) (Diff, error) {
	return &localDiff{
		cache:    cache,
		cacheKey: cacheKey,
	}, nil
}

func newLocalDiff(
	cacheKey DiffStoreKey,
	cachePath string,
	size,
	blockSize int64,
) (Diff, error) {
	cache, err := block.NewCache(size, blockSize, cachePath, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}

	return NewLocalDiffFromCache(cacheKey, cache)
}

func (b *localDiff) CachePath(ctx context.Context) (string, error) {
	return b.cache.Path(ctx)
}

func (b *localDiff) Close() error {
	return b.cache.Close()
}

// A local diff's bytes are on this node, in its cache file or its pause's
// memfd, so a read is reported to the caller as served from the local cache.
func (b *localDiff) ReadAt(ctx context.Context, p []byte, off int64, _ *storage.FrameTable) (int, error) {
	n, err := b.cache.ReadAt(p, off)
	if err == nil {
		storage.ReportSource(ctx, storage.SourceMmap)
	}

	return n, err
}

func (b *localDiff) Slice(ctx context.Context, off, length int64, _ *storage.FrameTable) ([]byte, error) {
	s, err := b.cache.Slice(off, length)
	if err == nil {
		storage.ReportSource(ctx, storage.SourceMmap)
	}

	return s, err
}

func (b *localDiff) Size(_ context.Context) (int64, error) {
	return b.cache.Size()
}

func (b *localDiff) FileSize(ctx context.Context) (int64, error) {
	return b.cache.FileSize(ctx)
}

func (b *localDiff) CacheKey() DiffStoreKey {
	return b.cacheKey
}

func (b *localDiff) BlockSize() int64 {
	return b.cache.BlockSize()
}
