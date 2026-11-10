//go:build linux

package block

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// fileSeekable makes the copy into the cache mapping a kernel read(2), which
// returns EFAULT where a Go copy would raise SIGBUS.
type fileSeekable struct{ f *os.File }

var _ storage.Seekable = (*fileSeekable)(nil)

func (s *fileSeekable) Size(context.Context) (int64, error) {
	st, err := s.f.Stat()
	if err != nil {
		return 0, err
	}

	return st.Size(), nil
}

func (s *fileSeekable) StoreFile(context.Context, string, ...storage.PutOption) (*storage.FullFrameTable, [32]byte, error) {
	panic("not used")
}

func (s *fileSeekable) OpenRangeReader(_ context.Context, off, length int64, _ *storage.FrameTable) (storage.RangeReader, storage.Source, error) {
	return &fileRangeReader{r: io.NewSectionReader(s.f, off, length)}, storage.SourceFS, nil
}

type fileRangeReader struct{ r *io.SectionReader }

func (r *fileRangeReader) Read(p []byte) (int, error)                        { return r.r.Read(p) }
func (r *fileRangeReader) Close(context.Context) (*storage.ReadStats, error) { return nil, nil }

// Truncating the cache under its mapping leaves the pages past the end
// unbackable, as a full filesystem does for a sparse cache.
func TestFetchReadEFAULTIsACacheBackingError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	size := int64(storage.MemoryChunkSize) * 2

	src, err := os.CreateTemp(dir, "src")
	require.NoError(t, err)
	defer src.Close()
	require.NoError(t, src.Truncate(size))

	cachePath := filepath.Join(dir, "cache")
	c, err := NewChunker(&featureflags.Client{}, size, testBlockSize, cachePath, newTestMetrics(t), storage.MemfileObjectType, "test-build")
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, os.Truncate(cachePath, int64(storage.MemoryChunkSize)))

	_, err = c.Slice(t.Context(), int64(storage.MemoryChunkSize), testBlockSize, &fileSeekable{f: src}, nil)
	require.Error(t, err)

	be, ok := errors.AsType[*CacheBackingError](err)
	require.True(t, ok, "expected a CacheBackingError, got %v", err)
	assert.Equal(t, cachePath, be.Path)
	assert.Positive(t, be.TotalBytes, "the filesystem size is reported")
	require.ErrorIs(t, err, unix.EFAULT, "the errno stays reachable")
	assert.Contains(t, err.Error(), "free of", "the message names the free space")
}

func TestClassifyBackingErrorPassesOtherErrorsThrough(t *testing.T) {
	t.Parallel()

	in := errors.New("boom")
	assert.Same(t, in, classifyBackingError("/nonexistent/cache", in))

	be, ok := errors.AsType[*CacheBackingError](classifyBackingError("/nonexistent/cache", unix.EFAULT))
	require.True(t, ok)
	assert.Zero(t, be.TotalBytes, "an unreadable filesystem leaves the sizes out")
	assert.Equal(t, "cache file /nonexistent/cache could not be populated: the read faulted on its mapping (bad address)", be.Error())
}

// fakeSeekable is copied into the mapping by Go code, so the unbackable page
// raises SIGBUS instead of EFAULT.
func TestFetchGoCopyFaultIsACacheBackingError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	size := int64(storage.MemoryChunkSize) * 2

	cachePath := filepath.Join(dir, "cache")
	c, err := NewChunker(&featureflags.Client{}, size, testBlockSize, cachePath, newTestMetrics(t), storage.MemfileObjectType, "test-build")
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, os.Truncate(cachePath, int64(storage.MemoryChunkSize)))

	source := &fakeSeekable{data: makeTestData(int(size))}
	_, err = c.Slice(t.Context(), int64(storage.MemoryChunkSize), testBlockSize, source, nil)
	require.Error(t, err)

	be, ok := errors.AsType[*CacheBackingError](err)
	require.True(t, ok, "expected a CacheBackingError, got %v", err)
	assert.Equal(t, cachePath, be.Path)
	_, faulted := errors.AsType[*MemoryFaultError](err)
	assert.True(t, faulted, "the recovered fault stays reachable")
}
