package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/lock"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	ErrOffsetUnaligned = errors.New("offset must be a multiple of chunk size")
	ErrBufferTooSmall  = errors.New("buffer is too small")
	ErrMultipleChunks  = errors.New("cannot read multiple chunks")
	ErrBufferTooLarge  = errors.New("buffer is too large")
)

type featureFlagsClient interface {
	BoolFlag(ctx context.Context, flag featureflags.BoolFlag, ldctx ...ldcontext.Context) bool
	IntFlag(ctx context.Context, flag featureflags.IntFlag, ldctx ...ldcontext.Context) int
}

type cachedSeekable struct {
	path      string
	chunkSize int64
	inner     Seekable
	flags     featureFlagsClient
	tracer    trace.Tracer
	objType   SeekableObjectType
	index     *indexToucher

	wg sync.WaitGroup
}

var (
	_ Seekable    = (*cachedSeekable)(nil)
	_ RangeOpener = (*cachedSeekable)(nil)
)

func (c *cachedSeekable) OpenRangeReader(ctx context.Context, off int64, length int64, frameTable *FrameTable) (RangeReader, Source, error) {
	compressed := frameTable.IsCompressed()

	ctx, span := c.tracer.Start(ctx, "read", trace.WithAttributes(
		attribute.Int64("offset", off),
		attribute.Int64("length", length),
		attribute.Bool("compressed", compressed),
	))

	var rc RangeReader
	var source Source
	var err error
	switch {
	case compressed:
		rc, source, err = c.openReaderCompressed(ctx, off, frameTable)
	default:
		if err = c.validateReadParams(length, off); err == nil {
			rc, source, err = c.openReaderUncompressed(ctx, off, length)
		}
	}

	if err != nil {
		recordError(span, err)
		span.End()

		return nil, source, err
	}

	return newSpanReader(rc, span), source, nil
}

func (c *cachedSeekable) openReaderUncompressed(ctx context.Context, off, length int64) (RangeReader, Source, error) {
	chunkPath := c.makeChunkFilename(off)

	start := time.Now()
	fp, err := os.Open(chunkPath)
	RecordReadOpen(ctx, time.Since(start), c.objType, SourceNFS, CompressionNone, err)
	if err == nil {
		c.index.touch()

		return newSectionReader(fp, 0, length), SourceNFS, nil
	}

	rc, innerSource, err := c.inner.OpenRangeReader(ctx, off, length, nil)
	if err != nil {
		return nil, innerSource, fmt.Errorf("failed to open inner range reader: %w", err)
	}

	if !skipCacheWriteback(ctx) {
		rc = newCaptureReader(rc, int(length), false,
			c.uncompressedChunkWriteback(chunkPath, off, length, innerSource))
	}

	return rc, innerSource, nil
}

// uncompressedChunkWriteback returns a captureReader callback that persists
// the captured chunk to the NFS cache in a detached goroutine. Best-effort:
// a short capture (e.g. upstream truncation) is dropped silently — a streaming
// reader always ends in EOF, so byte count is the only reliable signal.
func (c *cachedSeekable) uncompressedChunkWriteback(chunkPath string, off, expectedLen int64, src Source) func(context.Context, []byte) {
	return func(ctx context.Context, captured []byte) {
		if !isCompleteRead(len(captured), int(expectedLen), nil) {
			return
		}

		c.goCtx(ctx, func(ctx context.Context) {
			ctx, span := c.tracer.Start(ctx, "write range reader chunk back to cache")
			defer span.End()

			start := time.Now()
			err := c.writeToCache(ctx, off, chunkPath, captured)
			recordWriteback(ctx, time.Since(start), int64(len(captured)), c.objType, src, CompressionNone, TriggerRead, err)

			if err != nil && !errors.Is(err, lock.ErrLockAlreadyHeld) {
				recordError(span, err)
				logger.L().Warn(ctx, "failed to write chunk back to cache", zap.Error(err))
			}
		})
	}
}

func (c *cachedSeekable) Size(ctx context.Context) (n int64, e error) {
	ctx, span := c.tracer.Start(ctx, "get size of object")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	sizeStart := time.Now()
	size, err := c.readLocalSize(ctx)
	// Records the NFS attempt (hit, or not_found/err); a miss falls through to
	// inner.Size, which records its own source.
	RecordReadSize(ctx, time.Since(sizeStart), c.objType, SourceNFS, err)
	if err == nil {
		return size, nil
	}

	size, err = c.inner.Size(ctx)
	if err != nil {
		return size, err
	}

	if !skipCacheWriteback(ctx) {
		c.goCtx(ctx, func(ctx context.Context) {
			ctx, span := c.tracer.Start(ctx, "write size of object to cache")
			defer span.End()

			if err := c.writeLocalSize(ctx, size); err != nil {
				recordError(span, err)
				logger.L().Warn(ctx, "failed to write object size to cache", zap.Error(err))
			}
		})
	}

	return size, nil
}

func (c *cachedSeekable) StoreFile(ctx context.Context, path string, opts ...PutOption) (_ *FullFrameTable, _ [32]byte, e error) {
	ctx, span := c.tracer.Start(ctx, "write object from file system",
		trace.WithAttributes(attribute.String("path", path)),
	)
	defer func() {
		recordError(span, e)
		span.End()
	}()

	cfg := CompressConfigFromOpts(ApplyPutOptions(opts))
	writeThrough := c.flags.BoolFlag(ctx, featureflags.EnableWriteThroughCacheFlag)

	if cfg.IsCompressionEnabled() && writeThrough {
		opts = append(opts, WithFrameSink(c.frameSink(ctx, cfg.CompressionType())))
	}

	if !cfg.IsCompressionEnabled() && writeThrough {
		c.goCtx(ctx, func(ctx context.Context) {
			ctx, span := c.tracer.Start(ctx, "write cache object from file system",
				trace.WithAttributes(attribute.String("path", path)))
			defer span.End()

			size, err := c.createCacheBlocksFromFile(ctx, path)
			if err != nil {
				recordError(span, err)
				logger.L().Warn(ctx, "failed to create cache blocks from file system", zap.Error(err))

				return
			}

			if err := c.writeLocalSize(ctx, size); err != nil {
				recordError(span, err)
				logger.L().Warn(ctx, "failed to write object size to cache", zap.Error(err))
			}
		})
	}

	return c.inner.StoreFile(ctx, path, opts...)
}

// frameSink writes each compressed frame to a .frm file at its C-space offset,
// the layout openReaderCompressed expects. Writes are async (goCtx) and capped
// by MaxCacheWriterConcurrencyFlag.
func (c *cachedSeekable) frameSink(ctx context.Context, ct CompressionType) FrameSink {
	maxConcurrency := c.flags.IntFlag(ctx, featureflags.MaxCacheWriterConcurrencyFlag)
	if maxConcurrency <= 0 {
		logger.L().Warn(ctx, "max cache writer concurrency is too low, falling back to 1",
			zap.Int("max_concurrency", maxConcurrency))
		maxConcurrency = 1
	}
	sem := make(chan struct{}, maxConcurrency)

	return func(ctx context.Context, cOffset int64, data []byte) {
		// Acquire before spawning so goroutine count stays bounded; this also
		// backpressures the upload loop in compressStream.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}

		c.goCtx(ctx, func(ctx context.Context) {
			defer func() { <-sem }()

			framePath := makeFrameFilename(c.path, Range{Offset: cOffset, Length: len(data)})
			start := time.Now()
			err := c.writeToCache(ctx, cOffset, framePath, data)
			recordWriteback(ctx, time.Since(start), int64(len(data)), c.objType, SourceFS, ct, TriggerWrite, err)
			// ErrLockAlreadyHeld is normal dedup, not a write failure.
			if err != nil && !errors.Is(err, lock.ErrLockAlreadyHeld) {
				logger.L().Warn(ctx, "failed to write frame back to cache", zap.Error(err))
			}
		})
	}
}

// goCtx runs fn on c.wg with WithoutCancel so an in-flight cache write isn't
// aborted when the upload's context is cancelled.
func (c *cachedSeekable) goCtx(ctx context.Context, fn func(context.Context)) {
	c.wg.Go(func() {
		fn(context.WithoutCancel(ctx))
	})
}

func (c *cachedSeekable) makeChunkFilename(offset int64) string {
	return fmt.Sprintf("%s/%012d-%d.bin", c.path, offset/c.chunkSize, c.chunkSize)
}

func (c *cachedSeekable) makeTempFilename(path string) string {
	return path + ".tmp." + uuid.NewString()
}

func (c *cachedSeekable) sizeFilename() string {
	return filepath.Join(c.path, "size.txt")
}

func (c *cachedSeekable) readLocalSize(context.Context) (int64, error) {
	filename := c.sizeFilename()
	content, err := os.ReadFile(filename)
	if err != nil {
		return 0, fmt.Errorf("failed to read cached size: %w", err)
	}

	size, err := strconv.ParseInt(string(content), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse cached size: %w", err)
	}

	return size, nil
}

func (c *cachedSeekable) validateReadParams(buffSize, offset int64) error {
	if buffSize == 0 {
		return ErrBufferTooSmall
	}
	if buffSize > c.chunkSize {
		return ErrBufferTooLarge
	}
	if offset%c.chunkSize != 0 {
		return ErrOffsetUnaligned
	}
	if (offset%c.chunkSize)+buffSize > c.chunkSize {
		return ErrMultipleChunks
	}

	return nil
}

func (c *cachedSeekable) writeToCache(ctx context.Context, offset int64, finalPath string, bytes []byte) error {
	// Lock contention surfaces as ErrLockAlreadyHeld; callers skip it as dedup.
	lockFile, err := lock.TryAcquireLock(ctx, finalPath)
	if err != nil {
		return err
	}

	// Release lock after write completes
	defer func() {
		err := lock.ReleaseLock(ctx, lockFile)
		if err != nil {
			logger.L().Warn(ctx, "failed to release lock after writing to cache",
				zap.Int64("offset", offset),
				zap.String("path", finalPath),
				zap.Error(err))
		}
	}()

	tempPath := c.makeTempFilename(finalPath)

	if err := os.WriteFile(tempPath, bytes, cacheFilePermissions); err != nil {
		go safelyRemoveFile(ctx, tempPath)

		return fmt.Errorf("failed to write temp cache file: %w", err)
	}

	if err := utils.RenameOrDeleteFile(ctx, tempPath, finalPath); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}
	c.index.touch()

	return nil
}

func (c *cachedSeekable) writeLocalSize(ctx context.Context, size int64) error {
	finalFilename := c.sizeFilename()

	// Try to acquire lock for this chunk write to NFS cache
	lockFile, err := lock.TryAcquireLock(ctx, finalFilename)
	if err != nil {
		return fmt.Errorf("failed to acquire lock for local size: %w", err)
	}

	// Release lock after write completes
	defer func() {
		err := lock.ReleaseLock(ctx, lockFile)
		if err != nil {
			logger.L().Warn(ctx, "failed to release lock after writing chunk to cache",
				zap.Int64("size", size),
				zap.String("path", finalFilename),
				zap.Error(err))
		}
	}()

	tempFilename := filepath.Join(c.path, fmt.Sprintf(".size.bin.%s", uuid.NewString()))

	if err := os.WriteFile(tempFilename, fmt.Appendf(nil, "%d", size), cacheFilePermissions); err != nil {
		go safelyRemoveFile(ctx, tempFilename)

		return fmt.Errorf("failed to write temp local size file: %w", err)
	}

	if err := utils.RenameOrDeleteFile(ctx, tempFilename, finalFilename); err != nil {
		return fmt.Errorf("failed to rename local size temp file: %w", err)
	}

	return nil
}

func (c *cachedSeekable) createCacheBlocksFromFile(ctx context.Context, inputPath string) (count int64, err error) {
	ctx, span := c.tracer.Start(ctx, "create cache blocks from filesystem")
	defer func() {
		recordError(span, err)
		span.End()
	}()

	input, err := os.Open(inputPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open input file: %w", err)
	}
	defer utils.Cleanup(ctx, "failed to close file", input.Close)

	stat, err := input.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat input file: %w", err)
	}

	totalSize := stat.Size()

	maxConcurrency := c.flags.IntFlag(ctx, featureflags.MaxCacheWriterConcurrencyFlag)
	if maxConcurrency <= 0 {
		logger.L().Warn(ctx, "max cache writer concurrency is too low, falling back to 1",
			zap.Int("max_concurrency", maxConcurrency))
		maxConcurrency = 1
	}

	ec := utils.NewErrorCollector(maxConcurrency)
	for offset := int64(0); offset < totalSize; offset += c.chunkSize {
		ec.Go(ctx, func() error {
			if err := c.writeChunkFromFile(ctx, offset, input); err != nil {
				return fmt.Errorf("failed to write chunk file at offset %d: %w", offset, err)
			}

			return nil
		})
	}

	err = ec.Wait()

	return totalSize, err
}

// writeChunkFromFile writes a piece of a local file. It does not need to worry about race conditions, as it will only
// be called in the build layer, which cannot be built on multiple machines at the same time, or multiple times on the
// same machine..
func (c *cachedSeekable) writeChunkFromFile(ctx context.Context, offset int64, input *os.File) (err error) {
	_, span := c.tracer.Start(ctx, "write chunk from file at offset", trace.WithAttributes(
		attribute.Int64("offset", offset),
	))
	start := time.Now()
	var count int64
	defer func() {
		recordError(span, err)
		span.End()
		recordWriteback(ctx, time.Since(start), count, c.objType, SourceFS, CompressionNone, TriggerWrite, err)
	}()

	chunkPath := c.makeChunkFilename(offset)
	span.SetAttributes(attribute.String("chunk_path", chunkPath))

	output, err := os.OpenFile(chunkPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, cacheFilePermissions)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", chunkPath, err)
	}
	defer utils.Cleanup(ctx, "failed to close file", output.Close)

	count, err = io.Copy(output, io.NewSectionReader(input, offset, c.chunkSize))
	if err != nil {
		safelyRemoveFile(ctx, chunkPath)

		return fmt.Errorf("failed to copy chunk: %w", err)
	}
	c.index.touch()

	return nil
}

func safelyRemoveFile(ctx context.Context, path string) {
	if err := os.Remove(path); ignoreFileMissingError(err) != nil {
		logger.L().Warn(ctx, "failed to remove file",
			zap.String("path", path),
			zap.Error(err))
	}
}

func ignoreFileMissingError(err error) error {
	if os.IsNotExist(err) {
		return nil
	}

	return err
}
