package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	cacheFilePermissions = 0o600
	cacheDirPermissions  = 0o700
)

// skipCacheWritebackKeyType is the context key type for skipping NFS cache writeback.
type skipCacheWritebackKeyType struct{}

// WithSkipCacheWriteback returns a context that signals the NFS cache layer to
// skip writing fetched data back to the local cache. This is used by the
// prefetcher to avoid polluting the shared NFS cache with prefetch-specific reads.
func WithSkipCacheWriteback(ctx context.Context) context.Context {
	return context.WithValue(ctx, skipCacheWritebackKeyType{}, true)
}

// skipCacheWriteback reports whether the context has the skip-cache-writeback flag set.
func skipCacheWriteback(ctx context.Context) bool {
	v, _ := ctx.Value(skipCacheWritebackKeyType{}).(bool)

	return v
}

type cache struct {
	rootPath  string
	chunkSize int64
	inner     StorageProvider
	flags     *featureflags.Client

	tracer trace.Tracer
}

var _ StorageProvider = (*cache)(nil)

func WrapInNFSCache(
	ctx context.Context,
	rootPath string,
	inner StorageProvider,
	flags *featureflags.Client,
) StorageProvider {
	cacheTracer := tracer

	createCacheSpans := flags.BoolFlag(ctx, featureflags.CreateStorageCacheSpansFlag)
	if !createCacheSpans {
		cacheTracer = noop.NewTracerProvider().Tracer("github.com/e2b-dev/infra/packages/shared/pkg/storage")
	}

	return &cache{
		rootPath:  rootPath,
		inner:     inner,
		chunkSize: MemoryChunkSize,
		flags:     flags,
		tracer:    cacheTracer,
	}
}

func (c cache) DeleteObjectsWithPrefix(ctx context.Context, prefix string) error {
	if err := validateObjectPath(prefix); err != nil {
		return err
	}

	// no need to wait for cache deletion before returning
	go func(ctx context.Context) {
		c.deleteCachedObjectsWithPrefix(ctx, prefix)
	}(context.WithoutCancel(ctx))

	return c.inner.DeleteObjectsWithPrefix(ctx, prefix)
}

func (c cache) UploadSignedURL(ctx context.Context, path string, ttl time.Duration) (UploadURL, error) {
	if err := validateObjectPath(path); err != nil {
		return UploadURL{}, err
	}

	return c.inner.UploadSignedURL(ctx, path, ttl)
}

func (c cache) OpenBlob(ctx context.Context, path string) (Blob, error) {
	if err := validateObjectPath(path); err != nil {
		return nil, err
	}

	innerObject, err := c.inner.OpenBlob(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to open object: %w", err)
	}

	localPath := filepath.Join(c.rootPath, path)
	if err = os.MkdirAll(localPath, cacheDirPermissions); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	return &cachedBlob{
		path:      localPath,
		chunkSize: c.chunkSize,
		inner:     innerObject,
		flags:     c.flags,
		tracer:    c.tracer,
	}, nil
}

func (c cache) OpenSeekable(ctx context.Context, path string) (Seekable, error) {
	if err := validateObjectPath(path); err != nil {
		return nil, err
	}

	innerObject, err := c.inner.OpenSeekable(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to open object: %w", err)
	}

	localPath := filepath.Join(c.rootPath, path)
	if err = os.MkdirAll(localPath, cacheDirPermissions); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	objType, _ := seekableObjectType(path)

	return &cachedSeekable{
		path:      localPath,
		chunkSize: c.chunkSize,
		inner:     innerObject,
		flags:     c.flags,
		tracer:    c.tracer,
		objType:   objType,
	}, nil
}

func (c cache) GetDetails() string {
	return fmt.Sprintf("[Caching file storage, base path set to %s, which wraps %s]",
		c.rootPath, c.inner.GetDetails())
}

func (c cache) deleteCachedObjectsWithPrefix(ctx context.Context, prefix string) {
	fullPrefix := filepath.Join(c.rootPath, prefix)
	if err := os.RemoveAll(fullPrefix); err != nil {
		logger.L().Error(ctx, "failed to remove object with prefix",
			zap.String("prefix", prefix),
			zap.String("path", fullPrefix),
			zap.Error(err))
	}
}

func ignoreEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}

	return err
}

// isCompleteRead reports whether a read of n bytes into a buffer of expected
// size represents a valid, cacheable result. A read is complete when either
// the full buffer was filled or io.EOF explains a non-empty short read (last chunk).
//
// Writeback callers pass err=nil: a streaming reader always ends in io.EOF
// regardless of whether the upstream was truncated, so the byte count is the
// only reliable signal that the captured bytes are safe to cache.
func isCompleteRead(n, expected int, err error) bool {
	return n == expected || (n > 0 && errors.Is(err, io.EOF))
}
