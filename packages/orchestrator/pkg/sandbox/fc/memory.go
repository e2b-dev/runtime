//go:build linux

package fc

import (
	"context"
	"errors"
	"fmt"

	"github.com/RoaringBitmap/roaring/v2"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// MemoryInfo returns the memory info for the sandbox.
// The dirty field represents mincore resident pages—essentially pages that were faulted in.
// The empty field represents pages that are *resident*, but also completely empty.
func (p *Process) MemoryInfo(ctx context.Context, blockSize int64) (*header.DiffMetadata, error) {
	return p.client.memoryInfo(ctx, blockSize)
}

func (p *Process) DirtyMemory(ctx context.Context, blockSize int64) (*header.DiffMetadata, error) {
	return p.client.dirtyMemory(ctx, blockSize)
}

func (p *Process) exportMemoryFromFc(
	ctx context.Context,
	include *roaring.Bitmap,
	cachePath string,
	blockSize int64,
) (*block.Cache, error) {
	ctx, span := tracer.Start(ctx, "export-memory-from-fc")
	defer span.End()

	m, err := p.client.memoryMapping(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get memory mappings: %w", err)
	}

	var remoteRanges []block.Range

	for r := range block.BitsetRanges(include, blockSize) {
		hostVirtRanges, err := m.GetHostVirtRanges(r.Start, r.Size)
		if err != nil {
			return nil, fmt.Errorf("failed to get host virt ranges: %w", err)
		}

		remoteRanges = append(remoteRanges, hostVirtRanges...)
	}

	pid, err := p.Pid()
	if err != nil {
		return nil, fmt.Errorf("failed to get pid: %w", err)
	}

	cache, err := block.NewCacheFromProcessMemory(ctx, blockSize, cachePath, pid, remoteRanges)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}

	return cache, nil
}

// ExportMemory writes dirty guest memory to cachePath and resolves metaOut
// with the diff metadata once it is known. metaOut resolves asynchronously
// only for the memfd-dedup path; all other paths resolve it before returning.
// For the FC-dedup path, input.Empty is merged into the page-granular dedup
// Empty before metaOut is resolved.
func (p *Process) ExportMemory(
	ctx context.Context,
	include *roaring.Bitmap,
	cachePath string,
	blockSize int64,
	memfd *block.Memfd,
	bgCopy bool,
	originalMemfile block.ReadonlyDevice,
	dedupBestEffort bool,
	dedupDirectIO bool,
	dedupBudget block.DedupBudget,
	inputEmpty *roaring.Bitmap,
	metaOut *utils.SetOnce[*header.DiffMetadata],
	dedupInflightServe bool,
	dedupFreeIndex bool,
	keepMemfdOpen bool,
) (_ block.DiffSource, e error) {
	// Resolve metaOut on every sync error so Wait-ers don't hang. Success paths
	// resolve it inline; the memfd-dedup goroutine owns metaOut after this returns.
	defer func() {
		if e == nil {
			return
		}
		if setErr := metaOut.SetError(e); setErr != nil {
			logger.L().Warn(ctx, "set metaOut error", zap.Error(setErr))
		}
	}()

	inputMeta := &header.DiffMetadata{Dirty: include, Empty: inputEmpty, BlockSize: blockSize}
	// In-place snapshot: the running VM still owns the memfd, so copy the dirty
	// pages without consuming it and skip dedup/async/provisional (which would
	// close or double-manage the fd the live guest is faulting on).
	if memfd != nil && keepMemfdOpen {
		src, err := block.NewCacheFromMemfdKeepOpen(ctx, blockSize, cachePath, memfd, include)
		if err != nil {
			return nil, err
		}
		if setErr := metaOut.SetValue(inputMeta); setErr != nil {
			logger.L().Warn(ctx, "set metaOut", zap.Error(setErr))
		}

		return src, nil
	}
	if memfd != nil {
		if originalMemfile != nil {
			return block.NewCacheFromMemfdDeduped(ctx, originalMemfile, blockSize, cachePath, memfd, include,
				dedupBestEffort, dedupDirectIO, dedupBudget, inputEmpty, metaOut, dedupInflightServe, dedupFreeIndex)
		}
		var (
			src block.DiffSource
			err error
		)
		if bgCopy {
			src, err = block.NewCacheFromMemfdAsync(ctx, blockSize, cachePath, memfd, include)
		} else {
			src, err = block.NewCacheFromMemfd(ctx, blockSize, cachePath, memfd, include)
		}
		if err != nil {
			return nil, err
		}
		if setErr := metaOut.SetValue(inputMeta); setErr != nil {
			logger.L().Warn(ctx, "set metaOut", zap.Error(setErr))
		}

		return src, nil
	}

	cache, err := p.exportMemoryFromFc(ctx, include, cachePath, blockSize)
	if err != nil {
		return nil, err
	}
	if originalMemfile == nil {
		if setErr := metaOut.SetValue(inputMeta); setErr != nil {
			logger.L().Warn(ctx, "set metaOut", zap.Error(setErr))
		}

		return cache, nil
	}
	// .dedup suffix avoids clobbering the source mmap during truncate.
	dedupCache, meta, err := cache.Dedup(ctx, originalMemfile, include, blockSize, cachePath+".dedup", dedupBestEffort, dedupDirectIO, dedupBudget)
	if err != nil {
		return nil, fmt.Errorf("dedup memfile diff: %w", errors.Join(err, cache.Close()))
	}
	if err := cache.Close(); err != nil {
		return nil, fmt.Errorf("close pre-dedup cache: %w", errors.Join(err, dedupCache.Close()))
	}
	if blockSize%meta.BlockSize != 0 {
		return nil, errors.Join(
			fmt.Errorf("diff block size %d not a multiple of dedup block size %d", blockSize, meta.BlockSize),
			dedupCache.Close(),
		)
	}
	ratio := uint64(blockSize / meta.BlockSize)
	for start, end := range inputEmpty.Ranges() {
		meta.Empty.AddRange(uint64(start)*ratio, end*ratio)
	}
	if setErr := metaOut.SetValue(meta); setErr != nil {
		logger.L().Warn(ctx, "set metaOut", zap.Error(setErr))
	}

	return dedupCache, nil
}
