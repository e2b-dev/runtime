package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

var noopTracer = noop.TracerProvider{}.Tracer("github.com/e2b-dev/infra/packages/shared/pkg/storage")

func TestCachedObjectProvider_Put(t *testing.T) {
	t.Parallel()

	t.Run("can be cached successfully", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		cacheDir := filepath.Join(tempDir, "cache")
		data := []byte("hello world")

		err := os.MkdirAll(cacheDir, os.ModePerm)
		require.NoError(t, err)

		inner := NewMockBlob(t)
		inner.EXPECT().
			Put(mock.Anything, mock.Anything).
			Return(nil)

		featureFlags := NewMockFeatureFlagsClient(t)
		featureFlags.EXPECT().BoolFlag(mock.Anything, mock.Anything).Return(true)

		c := cachedBlob{path: cacheDir, inner: inner, chunkSize: 1024, flags: featureFlags, tracer: noopTracer}

		// write temp file
		err = c.Put(t.Context(), data)
		require.NoError(t, err)

		// file is written asynchronously, wait for it to finish
		c.wg.Wait()

		// prevent the provider from falling back to cache
		c.inner = nil

		gotData, err := GetBlob(t.Context(), &c)
		require.NoError(t, err)
		assert.Equal(t, data, gotData)
	})

	t.Run("uncached reads will be cached the second time", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		cacheDir := filepath.Join(tempDir, "cache")
		err := os.MkdirAll(cacheDir, 0o777)
		require.NoError(t, err)

		const dataSize = 10 * megabyte
		actualData := generateData(t, dataSize)

		inner := NewMockBlob(t)
		inner.EXPECT().
			WriteTo(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, dst io.Writer) (int64, error) {
				n, err := dst.Write(actualData)

				return int64(n), err
			})

		c := cachedBlob{path: cacheDir, inner: inner, chunkSize: 1024, tracer: noopTracer}

		read, err := GetBlob(t.Context(), &c)
		require.NoError(t, err)
		assert.Equal(t, actualData, read)

		c.wg.Wait()

		c.inner = nil

		read, err = GetBlob(t.Context(), &c)
		require.NoError(t, err)
		assert.Equal(t, actualData, read)
	})
}

// cachedCopy reads the blob with the backend detached, so only the cache can answer.
func cachedCopy(t *testing.T, c *cachedBlob) []byte {
	t.Helper()

	c.wg.Wait()
	c.inner = nil

	got, err := GetBlob(t.Context(), c)
	require.NoError(t, err)

	return got
}

func TestCachedObjectProvider_PutOverwrite(t *testing.T) {
	t.Parallel()

	oldData, newData := []byte("old"), []byte("new")

	newBlob := func(t *testing.T, writeThrough bool) (*cachedBlob, *MockBlob) {
		t.Helper()

		inner := NewMockBlob(t)
		featureFlags := NewMockFeatureFlagsClient(t)
		featureFlags.EXPECT().BoolFlag(mock.Anything, mock.Anything).Return(writeThrough)

		return &cachedBlob{path: t.TempDir(), inner: inner, chunkSize: 1024, flags: featureFlags, tracer: noopTracer}, inner
	}

	t.Run("replaces a copy cached by an earlier put", func(t *testing.T) {
		t.Parallel()

		c, inner := newBlob(t, true)
		inner.EXPECT().Put(mock.Anything, oldData).Return(nil)
		inner.EXPECT().Put(mock.Anything, newData, mock.Anything).Return(nil)

		require.NoError(t, c.Put(t.Context(), oldData))
		c.wg.Wait()
		require.NoError(t, c.Put(t.Context(), newData, WithOverwrite()))

		assert.Equal(t, newData, cachedCopy(t, c))
	})

	t.Run("replaces a copy cached by an earlier read when write-through is off", func(t *testing.T) {
		t.Parallel()

		c, inner := newBlob(t, false)
		inner.EXPECT().
			WriteTo(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, dst io.Writer) (int64, error) {
				n, err := dst.Write(oldData)

				return int64(n), err
			})
		inner.EXPECT().Put(mock.Anything, newData, mock.Anything).Return(nil)

		read, err := GetBlob(t.Context(), c)
		require.NoError(t, err)
		require.Equal(t, oldData, read)
		c.wg.Wait()
		require.NoError(t, c.Put(t.Context(), newData, WithOverwrite()))

		assert.Equal(t, newData, cachedCopy(t, c))
	})

	t.Run("caches nothing when write-through is off and nothing is cached", func(t *testing.T) {
		t.Parallel()

		c, inner := newBlob(t, false)
		inner.EXPECT().Put(mock.Anything, newData, mock.Anything).Return(nil)

		require.NoError(t, c.Put(t.Context(), newData, WithOverwrite()))
		c.wg.Wait()

		entries, err := os.ReadDir(c.path)
		require.NoError(t, err)
		assert.Empty(t, entries)
	})

	t.Run("a failed backend put leaves the cached copy as it was", func(t *testing.T) {
		t.Parallel()

		errTarget := errors.New("backend down")
		c, inner := newBlob(t, true)
		inner.EXPECT().Put(mock.Anything, oldData).Return(nil)
		inner.EXPECT().Put(mock.Anything, newData, mock.Anything).Return(errTarget)

		require.NoError(t, c.Put(t.Context(), oldData))
		c.wg.Wait()
		require.ErrorIs(t, c.Put(t.Context(), newData, WithOverwrite()), errTarget)

		assert.Equal(t, oldData, cachedCopy(t, c))
	})

	t.Run("a failed replace does not fail the put or leave a temp file", func(t *testing.T) {
		t.Parallel()

		c, inner := newBlob(t, true)
		inner.EXPECT().Put(mock.Anything, newData, mock.Anything).Return(nil)
		// A file cannot be renamed over a directory.
		require.NoError(t, os.Mkdir(c.fullFilename(), 0o700))

		require.NoError(t, c.Put(t.Context(), newData, WithOverwrite()))
		c.wg.Wait()

		entries, err := os.ReadDir(c.path)
		require.NoError(t, err)
		assert.Len(t, entries, 1)
	})

	t.Run("a cache write of the old content that lands after the replace is dropped", func(t *testing.T) {
		t.Parallel()

		c, inner := newBlob(t, true)
		inner.EXPECT().Put(mock.Anything, newData, mock.Anything).Return(nil)

		require.NoError(t, c.Put(t.Context(), newData, WithOverwrite()))
		_, err := c.writeFileToCache(t.Context(), bytes.NewReader(oldData))
		require.NoError(t, err)

		assert.Equal(t, newData, cachedCopy(t, c))
	})

	t.Run("an ordinary second put keeps the cached copy", func(t *testing.T) {
		t.Parallel()

		c, inner := newBlob(t, true)
		inner.EXPECT().Put(mock.Anything, mock.Anything).Return(nil)

		require.NoError(t, c.Put(t.Context(), oldData))
		c.wg.Wait()
		require.NoError(t, c.Put(t.Context(), newData))

		assert.Equal(t, oldData, cachedCopy(t, c))
	})
}

func TestCachedObjectProvider_WriteFileToCache(t *testing.T) {
	t.Parallel()

	c := cachedBlob{
		path:   t.TempDir(),
		tracer: noopTracer,
	}
	errTarget := errors.New("find me")
	reader := NewMockReader(t)
	reader.EXPECT().Read(mock.Anything).Return(4, nil).Once()
	reader.EXPECT().Read(mock.Anything).Return(0, errTarget).Once()

	count, err := c.writeFileToCache(t.Context(), reader)
	require.ErrorIs(t, err, errTarget)
	assert.Equal(t, int64(0), count)

	path := c.fullFilename()
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func generateData(t *testing.T, count int) []byte {
	t.Helper()

	data := make([]byte, count)
	for i := range count {
		data[i] = byte(rand.Intn(256))
	}

	return data
}

// failSecondWrite accepts every write but the second, which it fails, so a
// copy into it stops after some bytes have already arrived.
type failSecondWrite struct {
	writes int
	got    []byte
}

var errSecondWrite = errors.New("second write failed")

func (w *failSecondWrite) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == 2 {
		return 0, errSecondWrite
	}
	w.got = append(w.got, p...)

	return len(p), nil
}

func TestCachedObjectProvider_WriteToDoesNotRestartIntoAWrittenDestination(t *testing.T) {
	t.Parallel()

	c := cachedBlob{path: t.TempDir(), tracer: noopTracer}
	// Larger than io.Copy's buffer, so the cache copy writes more than once.
	data := generateData(t, 256*1024)
	require.NoError(t, os.WriteFile(c.fullFilename(), data, 0o600))

	inner := NewMockBlob(t)
	inner.EXPECT().
		WriteTo(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, dst io.Writer) (int64, error) {
			n, err := dst.Write(data)

			return int64(n), err
		}).
		Maybe()
	c.inner = inner

	dst := &failSecondWrite{}
	n, err := c.WriteTo(t.Context(), dst)
	require.ErrorIs(t, err, errSecondWrite)
	assert.Equal(t, int64(len(dst.got)), n)
	assert.Equal(t, data[:len(dst.got)], dst.got, "the backend's copy must not follow the bytes the cache already wrote")
	inner.AssertNotCalled(t, "WriteTo", mock.Anything, mock.Anything)
}
