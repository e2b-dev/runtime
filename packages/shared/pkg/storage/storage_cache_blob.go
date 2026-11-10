package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/lock"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	kilobyte = 1024
	megabyte = 1024 * kilobyte
)

type cachedBlob struct {
	path      string
	chunkSize int64
	inner     Blob
	flags     featureFlagsClient
	tracer    trace.Tracer

	wg sync.WaitGroup
}

var _ Blob = (*cachedBlob)(nil)

func (b *cachedBlob) Exists(ctx context.Context) (bool, error) {
	return b.inner.Exists(ctx)
}

// Metadata delegates to the inner (backend) blob so custom metadata is read
// from the authoritative backend, never the local byte cache.
func (b *cachedBlob) Metadata(ctx context.Context) (ObjectMetadata, error) {
	return BlobCustomMetadata(ctx, b.inner)
}

func (b *cachedBlob) WriteTo(ctx context.Context, dst io.Writer) (n int64, e error) {
	ctx, span := b.tracer.Start(ctx, "read object into writer")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	blobStart := time.Now()
	bytesRead, err := b.copyFullFileFromCache(ctx, dst)
	// Records the NFS attempt (hit, or not_found/err); a miss falls through to
	// inner.WriteTo, which records its own source.
	RecordReadBlob(ctx, time.Since(blobStart), bytesRead, b.path, SourceNFS, err)
	if err == nil {
		return bytesRead, nil
	}
	// The backend's copy would land after the bytes the cache already wrote.
	if bytesRead > 0 {
		return bytesRead, err
	}

	// This is semi-arbitrary. this code path is called for files that tend to be less than 1 MB (headers, metadata, etc),
	// so 2 MB allows us to read the file without needing to allocate more memory, with some room for growth. If the
	// file is larger than 2 MB, the buffer will grow, it just won't be as efficient WRT memory allocations.
	const writeToInitialBufferSize = 2 * megabyte

	buffer := bytes.NewBuffer(make([]byte, 0, writeToInitialBufferSize))

	if count, err := b.inner.WriteTo(ctx, buffer); ignoreEOF(err) != nil {
		return count, err
	}

	// store the byte slice before calling `buffer.Read`, which moves the offset.
	data := buffer.Bytes()

	if !skipCacheWriteback(ctx) {
		b.goCtxWithoutCancel(ctx, func(ctx context.Context) {
			ctx, span := b.tracer.Start(ctx, "write file back to cache")
			defer span.End()

			if _, err := b.writeFileToCache(ctx, buffer); err != nil {
				recordError(span, err)
				logger.L().Warn(ctx, "failed to write object back to cache", zap.Error(err))
			}
		})
	}

	written, err := dst.Write(data)
	if ignoreEOF(err) != nil {
		return int64(written), fmt.Errorf("failed to write object: %w", err)
	}

	return int64(written), err // in case  err == EOF
}

// Write pushes data to the wrapped object provider, and optionally pushes the data to a fast ephemeral cache as well.
// `p` is considered immutable, and won't change if we access it after the function returns.
func (b *cachedBlob) Put(ctx context.Context, data []byte, opts ...PutOption) (e error) {
	ctx, span := b.tracer.Start(ctx, "write data to object storage")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	if b.flags.BoolFlag(ctx, featureflags.EnableWriteThroughCacheFlag) {
		b.goCtxWithoutCancel(ctx, func(ctx context.Context) {
			ctx, span := b.tracer.Start(ctx, "write data to cache")
			defer span.End()

			if _, err := b.writeFileToCache(ctx, bytes.NewReader(data)); err != nil {
				recordError(span, err)
				logger.L().Warn(ctx, "failed to write object to cache", zap.Error(err))
			}
		})
	}

	return b.inner.Put(ctx, data, opts...)
}

func (b *cachedBlob) goCtxWithoutCancel(ctx context.Context, fn func(context.Context)) {
	b.wg.Go(func() {
		fn(context.WithoutCancel(ctx))
	})
}

func (b *cachedBlob) fullFilename() string {
	return fmt.Sprintf("%s/content.bin", b.path)
}

func (b *cachedBlob) copyFullFileFromCache(ctx context.Context, dst io.Writer) (n int64, e error) {
	ctx, span := b.tracer.Start(ctx, "read cached object into writer")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	path := b.fullFilename()

	var fp *os.File
	fp, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("failed to open cached file %s: %w", path, err)
	}

	defer utils.Cleanup(ctx, "failed to close full cached file", fp.Close)

	count, err := io.Copy(dst, fp)
	if ignoreEOF(err) != nil {
		return count, fmt.Errorf("failed to copy cached file %s: %w", path, err)
	}

	return count, nil
}

func (b *cachedBlob) writeFileToCache(ctx context.Context, input io.Reader) (int64, error) {
	path := b.fullFilename()

	output, err := lock.OpenFile(ctx, path)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire lock on file %s: %w", path, err)
	}
	defer utils.CleanupCtx(ctx, "failed to unlock file", output.Close)

	count, err := io.Copy(output, input)
	if ignoreEOF(err) != nil {
		return 0, fmt.Errorf("failed to write to cache file %s: %w", path, err)
	}

	if err := output.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit cache file %s: %w", path, err)
	}

	return count, nil
}
