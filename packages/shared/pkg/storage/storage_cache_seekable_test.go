package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// mustClose closes a RangeReader and asserts no error.
func mustClose(t *testing.T, rc RangeReader) {
	t.Helper()
	_, err := rc.Close(t.Context())
	require.NoError(t, err)
}

// bytesRangeReader wraps an in-memory byte slice as a RangeReader for tests.
func bytesRangeReader(b []byte) RangeReader {
	return NewRangeReader(io.NopCloser(bytes.NewReader(b)))
}

// testReadAt emulates the removed cachedSeekable.ReadAt via OpenRangeReader.
// This preserves the base test structure after ReadAt was removed from the Seekable interface.
func testReadAt(ctx context.Context, c *cachedSeekable, buff []byte, off int64) (int, error) {
	rc, _, err := c.OpenRangeReader(ctx, off, int64(len(buff)), nil)
	if err != nil {
		return 0, err
	}

	n, err := io.ReadFull(rc, buff)

	_, closeErr := rc.Close(ctx)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = io.EOF
	}

	if err == nil {
		err = closeErr
	}

	return n, err
}

func TestCachedFileObjectProvider_MakeChunkFilename(t *testing.T) {
	t.Parallel()

	c := cachedSeekable{path: "/a/b/c", chunkSize: 1024, tracer: noopTracer}
	filename := c.makeChunkFilename(1024 * 4)
	assert.Equal(t, "/a/b/c/000000000004-1024.bin", filename)
}

func TestCachedFileObjectProvider_Size(t *testing.T) {
	t.Parallel()

	t.Run("can be cached successfully", func(t *testing.T) {
		t.Parallel()

		const expectedSize int64 = 1024

		inner := NewMockSeekable(t)
		inner.EXPECT().Size(mock.Anything).Return(expectedSize, nil)

		c := cachedSeekable{path: t.TempDir(), inner: inner, tracer: noopTracer}

		// first call will write to cache
		size, err := c.Size(t.Context())
		require.NoError(t, err)
		assert.Equal(t, expectedSize, size)

		// sleep, cache writing is async
		c.wg.Wait()

		// second call must come from cache
		c.inner = nil

		size, err = c.Size(t.Context())
		require.NoError(t, err)
		assert.Equal(t, expectedSize, size)
	})
}

func TestCachedFileObjectProvider_WriteFromFileSystem(t *testing.T) {
	t.Parallel()

	t.Run("can be cached successfully", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		cacheDir := filepath.Join(tempDir, "cache")
		tempFilename := filepath.Join(tempDir, "temp.bin")
		data := []byte("hello world")

		err := os.MkdirAll(cacheDir, os.ModePerm)
		require.NoError(t, err)

		err = os.WriteFile(tempFilename, data, 0o644)
		require.NoError(t, err)

		inner := NewMockSeekable(t)
		inner.EXPECT().
			StoreFile(mock.Anything, mock.Anything).
			Return(nil, [32]byte{}, nil)

		featureFlags := NewMockFeatureFlagsClient(t)
		featureFlags.EXPECT().BoolFlag(mock.Anything, mock.Anything).Return(true)
		featureFlags.EXPECT().IntFlag(mock.Anything, mock.Anything).Return(10)

		c := cachedSeekable{path: cacheDir, inner: inner, chunkSize: 1024, flags: featureFlags, tracer: noopTracer}

		// write temp file
		_, _, err = c.StoreFile(t.Context(), tempFilename)
		require.NoError(t, err)

		// file is written asynchronously, wait for it to finish
		c.wg.Wait()

		c.inner = nil

		// size should be cached
		size, err := c.Size(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int64(len(data)), size)

		// verify that the size has been cached
		buff := make([]byte, len(data))
		bytesRead, err := testReadAt(t.Context(), &c, buff, 0)
		require.NoError(t, err)
		assert.Equal(t, data, buff)
		assert.Equal(t, len(data), bytesRead)
	})
}

func TestCachedFileObjectProvider_WriteTo(t *testing.T) {
	t.Parallel()

	t.Run("read from cache when the file exists", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()

		tempPath := filepath.Join(tempDir, "a", "b", "c")
		c := cachedSeekable{path: tempPath, chunkSize: 3, tracer: noopTracer}

		// create cache file
		cacheFilename := c.makeChunkFilename(0)
		dirName := filepath.Dir(cacheFilename)
		err := os.MkdirAll(dirName, 0o755)
		require.NoError(t, err)
		err = os.WriteFile(cacheFilename, []byte{1, 2, 3}, 0o600)
		require.NoError(t, err)

		buffer := make([]byte, 3)
		read, err := testReadAt(t.Context(), &c, buffer, 0)
		require.NoError(t, err)
		assert.Equal(t, []byte{1, 2, 3}, buffer)
		assert.Equal(t, 3, read)
	})

	t.Run("short cache file returns EOF via ReadAt", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()

		c := cachedSeekable{path: tempDir, chunkSize: 10, tracer: noopTracer}

		// Plant a 3-byte cache file (valid last chunk).
		chunkPath := c.makeChunkFilename(0)
		require.NoError(t, os.MkdirAll(filepath.Dir(chunkPath), 0o755))
		require.NoError(t, os.WriteFile(chunkPath, []byte{1, 2, 3}, 0o600))

		// ReadAt on a file shorter than the buffer returns (n, io.EOF)
		// per the io.ReaderAt contract. This is a cache hit — the caller
		// sees the data with EOF indicating end of file.
		buffer := make([]byte, 10)
		read, err := testReadAt(t.Context(), &c, buffer, 0)
		require.ErrorIs(t, err, io.EOF)
		assert.Equal(t, 3, read)
		assert.Equal(t, []byte{1, 2, 3}, buffer[:read])
	})

	t.Run("consecutive ReadAt calls should cache", func(t *testing.T) {
		t.Parallel()

		fakeData := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		inner := NewMockSeekable(t)

		inner.EXPECT().
			OpenRangeReader(mock.Anything, mock.Anything, mock.Anything, (*FrameTable)(nil)).
			RunAndReturn(func(_ context.Context, off int64, length int64, _ *FrameTable) (RangeReader, Source, error) {
				end := min(int(off)+int(length), len(fakeData))

				return NewRangeReader(io.NopCloser(bytes.NewReader(fakeData[off:end]))), UnknownSource, nil
			})

		tempDir := t.TempDir()
		c := cachedSeekable{
			path:      tempDir,
			chunkSize: 3,
			inner:     inner,
			tracer:    noopTracer,
		}

		// first read goes to source
		buffer := make([]byte, 3)
		read, err := testReadAt(t.Context(), &c, buffer, 3)
		require.NoError(t, err)
		assert.Equal(t, []byte{4, 5, 6}, buffer)
		assert.Equal(t, 3, read)

		// we write asynchronously, so let's wait until we're done
		c.wg.Wait()

		// second read pulls from cache
		c.inner = nil // prevent remote reads, force cache read
		buffer = make([]byte, 3)
		read, err = testReadAt(t.Context(), &c, buffer, 3)
		require.NoError(t, err)
		assert.Equal(t, []byte{4, 5, 6}, buffer)
		assert.Equal(t, 3, read)
	})

	t.Run("WriteTo calls should read from cache", func(t *testing.T) {
		t.Parallel()

		fakeData := []byte{1, 2, 3}

		fakeStorageObjectProvider := NewMockBlob(t)
		fakeStorageObjectProvider.EXPECT().
			WriteTo(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, dst io.Writer) (int64, error) {
				n, err := dst.Write(fakeData)

				return int64(n), err
			})

		tempDir := t.TempDir()
		c := cachedBlob{
			path:      tempDir,
			chunkSize: 3,
			inner:     fakeStorageObjectProvider,
			tracer:    noopTracer,
		}

		// write to both local and remote storage
		data, err := GetBlob(t.Context(), &c)
		require.NoError(t, err)
		assert.Equal(t, fakeData, data)

		// WriteTo is async, wait for the write to finish
		c.wg.Wait()

		// second read should go straight to local
		c.inner = nil
		data, err = GetBlob(t.Context(), &c)
		require.NoError(t, err)
		assert.Equal(t, fakeData, data)
	})
}

func TestCachedFileObjectProvider_validateReadAtParams(t *testing.T) {
	t.Parallel()

	testcases := map[string]struct {
		chunkSize, bufferSize, offset int64
		expected                      error
	}{
		"buffer is empty": {
			chunkSize:  1,
			bufferSize: 0,
			offset:     0,
			expected:   ErrBufferTooSmall,
		},
		"buffer is smaller than chunk size": {
			chunkSize:  10,
			bufferSize: 5,
			offset:     0,
		},
		"offset is unaligned": {
			chunkSize:  10,
			bufferSize: 10,
			offset:     3,
			expected:   ErrOffsetUnaligned,
		},
		"buffer is too large (unaligned)": {
			chunkSize:  10,
			bufferSize: 11,
			expected:   ErrBufferTooLarge,
		},
		"buffer is too large (aligned)": {
			chunkSize:  10,
			bufferSize: 20,
			expected:   ErrBufferTooLarge,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			c := cachedSeekable{
				chunkSize: tc.chunkSize,
				tracer:    noopTracer,
			}
			err := c.validateReadParams(tc.bufferSize, tc.offset)
			if tc.expected == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.expected)
			}
		})
	}
}

func TestCachedSeekableObjectProvider_ReadAt(t *testing.T) {
	t.Parallel()

	t.Run("zero byte read with EOF is not cached", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		inner := NewMockSeekable(t)
		inner.EXPECT().
			OpenRangeReader(mock.Anything, mock.Anything, mock.Anything, (*FrameTable)(nil)).
			Return(NewRangeReader(io.NopCloser(bytes.NewReader(nil))), UnknownSource, nil)

		c := cachedSeekable{
			path:      tempDir,
			chunkSize: 10,
			inner:     inner,
			tracer:    noopTracer,
		}

		buff := make([]byte, 10)
		count, err := testReadAt(t.Context(), &c, buff, 0)
		require.ErrorIs(t, err, io.EOF)
		assert.Equal(t, 0, count)

		c.wg.Wait()

		chunkPath := c.makeChunkFilename(0)
		_, err = os.Stat(chunkPath)
		assert.True(t, os.IsNotExist(err), "zero-byte read should not be cached")
	})

	t.Run("full read without EOF is cached", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		data := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		inner := NewMockSeekable(t)
		inner.EXPECT().
			OpenRangeReader(mock.Anything, mock.Anything, mock.Anything, (*FrameTable)(nil)).
			Return(NewRangeReader(io.NopCloser(bytes.NewReader(data))), UnknownSource, nil)

		c := cachedSeekable{
			path:      tempDir,
			chunkSize: 10,
			inner:     inner,
			tracer:    noopTracer,
		}

		buff := make([]byte, 10)
		count, err := testReadAt(t.Context(), &c, buff, 0)
		require.NoError(t, err)
		assert.Equal(t, 10, count)

		c.wg.Wait()

		// Verify the data was cached.
		chunkPath := c.makeChunkFilename(0)
		cached, err := os.ReadFile(chunkPath)
		require.NoError(t, err)
		assert.Equal(t, data, cached)
	})
}

func TestIsCompleteRead(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		n, expected int
		err         error
		want        bool
	}{
		"full read, no error":      {n: 10, expected: 10, err: nil, want: true},
		"full read, with EOF":      {n: 10, expected: 10, err: io.EOF, want: true},
		"short read, with EOF":     {n: 3, expected: 10, err: io.EOF, want: true},
		"short read, no error":     {n: 3, expected: 10, err: nil, want: false},
		"short read, other error":  {n: 3, expected: 10, err: errors.New("fail"), want: false},
		"zero bytes, with EOF":     {n: 0, expected: 10, err: io.EOF, want: false},
		"zero bytes, no error":     {n: 0, expected: 10, err: nil, want: false},
		"zero expected, zero read": {n: 0, expected: 0, err: nil, want: true},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := isCompleteRead(tc.n, tc.expected, tc.err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCachedSeekable_ReadAt_PreservesEOF(t *testing.T) {
	t.Parallel()

	t.Run("EOF from inner is returned to caller unchanged", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		inner := NewMockSeekable(t)
		inner.EXPECT().
			OpenRangeReader(mock.Anything, mock.Anything, mock.Anything, (*FrameTable)(nil)).
			Return(NewRangeReader(io.NopCloser(bytes.NewReader([]byte{1, 2, 3}))), UnknownSource, nil)

		c := cachedSeekable{
			path:      tempDir,
			chunkSize: 10,
			inner:     inner,
			tracer:    noopTracer,
		}

		buff := make([]byte, 10)
		n, err := testReadAt(t.Context(), &c, buff, 0)
		assert.Equal(t, 3, n)
		require.ErrorIs(t, err, io.EOF, "cachedSeekable must not swallow io.EOF")

		c.wg.Wait()
	})

	t.Run("nil error from inner is returned to caller unchanged", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		inner := NewMockSeekable(t)
		inner.EXPECT().
			OpenRangeReader(mock.Anything, mock.Anything, mock.Anything, (*FrameTable)(nil)).
			Return(NewRangeReader(io.NopCloser(bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))), UnknownSource, nil)

		c := cachedSeekable{
			path:      tempDir,
			chunkSize: 10,
			inner:     inner,
			tracer:    noopTracer,
		}

		buff := make([]byte, 10)
		n, err := testReadAt(t.Context(), &c, buff, 0)
		assert.Equal(t, 10, n)
		require.NoError(t, err, "cachedSeekable must not inject errors on full read")

		c.wg.Wait()
	})
}

func TestCachedSeekable_ReadAt_SkipCacheWriteback(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	data := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	inner := NewMockSeekable(t)
	inner.EXPECT().
		OpenRangeReader(mock.Anything, mock.Anything, mock.Anything, (*FrameTable)(nil)).
		RunAndReturn(func(_ context.Context, _ int64, _ int64, _ *FrameTable) (RangeReader, Source, error) {
			return NewRangeReader(io.NopCloser(bytes.NewReader(data))), UnknownSource, nil
		})

	c := cachedSeekable{
		path:      tempDir,
		chunkSize: 10,
		inner:     inner,
		tracer:    noopTracer,
	}

	ctx := WithSkipCacheWriteback(t.Context())
	buff := make([]byte, 10)
	n, err := testReadAt(ctx, &c, buff, 0)
	require.NoError(t, err)
	assert.Equal(t, 10, n)

	c.wg.Wait()

	chunkPath := c.makeChunkFilename(0)
	_, err = os.Stat(chunkPath)
	assert.True(t, os.IsNotExist(err), "cache writeback should be skipped")
}

func TestCachedSeekable_OpenRangeReader(t *testing.T) {
	t.Parallel()

	t.Run("cache miss then full read populates cache for next call", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		data := []byte("hello")

		inner := NewMockSeekable(t)
		inner.EXPECT().
			OpenRangeReader(mock.Anything, int64(0), int64(len(data)), (*FrameTable)(nil)).
			Return(NewRangeReader(io.NopCloser(bytes.NewReader(data))), UnknownSource, nil).
			Once()

		c := cachedSeekable{
			path:      tempDir,
			chunkSize: 10,
			inner:     inner,
			tracer:    noopTracer,
		}

		// First call: cache miss, reads from inner.
		rc, _, err := c.OpenRangeReader(t.Context(), 0, int64(len(data)), nil)
		require.NoError(t, err)

		got, err := io.ReadAll(rc)
		require.NoError(t, err)
		assert.Equal(t, data, got)
		mustClose(t, rc)

		c.wg.Wait()

		// Second call: should serve from NFS cache, inner not called again.
		c.inner = nil
		rc2, _, err := c.OpenRangeReader(t.Context(), 0, int64(len(data)), nil)
		require.NoError(t, err)

		got2, err := io.ReadAll(rc2)
		require.NoError(t, err)
		assert.Equal(t, data, got2)
		mustClose(t, rc2)
	})

	t.Run("skip cache writeback returns inner directly", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		data := []byte("hello")

		inner := NewMockSeekable(t)
		inner.EXPECT().
			OpenRangeReader(mock.Anything, int64(0), int64(len(data)), (*FrameTable)(nil)).
			RunAndReturn(func(_ context.Context, _ int64, _ int64, _ *FrameTable) (RangeReader, Source, error) {
				return NewRangeReader(io.NopCloser(bytes.NewReader(data))), UnknownSource, nil
			}).
			Times(2)

		c := cachedSeekable{
			path:      tempDir,
			chunkSize: 10,
			inner:     inner,
			tracer:    noopTracer,
		}

		ctx := WithSkipCacheWriteback(t.Context())

		rc, _, err := c.OpenRangeReader(ctx, 0, int64(len(data)), nil)
		require.NoError(t, err)

		got, err := io.ReadAll(rc)
		require.NoError(t, err)
		assert.Equal(t, data, got)
		mustClose(t, rc)

		c.wg.Wait()

		// Cache should still be empty — second call hits inner again.
		chunkPath := c.makeChunkFilename(0)
		_, err = os.Stat(chunkPath)
		assert.True(t, os.IsNotExist(err), "skip writeback should not populate cache")

		rc2, _, err := c.OpenRangeReader(ctx, 0, int64(len(data)), nil)
		require.NoError(t, err)

		got2, err := io.ReadAll(rc2)
		require.NoError(t, err)
		assert.Equal(t, data, got2)
		mustClose(t, rc2)
	})

	t.Run("truncated inner read does not populate cache", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()

		inner := NewMockSeekable(t)
		inner.EXPECT().
			OpenRangeReader(mock.Anything, int64(0), int64(5), (*FrameTable)(nil)).
			Return(NewRangeReader(io.NopCloser(bytes.NewReader([]byte{0xAA, 0xBB}))), UnknownSource, nil)

		c := cachedSeekable{
			path:      tempDir,
			chunkSize: 10,
			inner:     inner,
			tracer:    noopTracer,
		}

		rc, _, err := c.OpenRangeReader(t.Context(), 0, 5, nil)
		require.NoError(t, err)

		got, err := io.ReadAll(rc)
		require.NoError(t, err)
		assert.Equal(t, []byte{0xAA, 0xBB}, got)
		mustClose(t, rc)

		c.wg.Wait()

		chunkPath := c.makeChunkFilename(0)
		_, err = os.Stat(chunkPath)
		assert.True(t, os.IsNotExist(err), "truncated data should not be cached")
	})
}

func TestCachedSeekable_StoreFile_Compressed_WriteThrough(t *testing.T) {
	t.Parallel()

	const (
		frameSize = 64 * 1024
		numFrames = 8
	)

	tests := []struct {
		name string
		// deferSink holds every frame back until compressStream has returned,
		// like a cache writer that falls behind the upload. The frames must
		// still hold their own bytes by then.
		deferSink bool
	}{
		{"inline sink", false},
		{"sink after upload", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := defaultCfg(CompressionZstd, 2, frameSize)
			data := generateSemiRandomData(numFrames * frameSize)

			tempDir := t.TempDir()
			cacheDir := filepath.Join(tempDir, "cache")
			require.NoError(t, os.MkdirAll(cacheDir, os.ModePerm))

			srcPath := filepath.Join(tempDir, "src.bin")
			require.NoError(t, os.WriteFile(srcPath, data, 0o644))

			// Stub inner.StoreFile: open the file and run compressStream with the sink
			// pulled from opts — mirrors what fs/GCS backends do for compressed puts.
			up := &memPartUploader{}
			var capturedFT *FullFrameTable
			inner := NewMockSeekable(t)
			inner.EXPECT().
				StoreFile(mock.Anything, mock.Anything, mock.Anything).
				RunAndReturn(func(ctx context.Context, path string, opts ...PutOption) (*FullFrameTable, [32]byte, error) {
					po := ApplyPutOptions(opts)
					require.NotNil(t, po.FrameSink, "cachedSeekable must attach a FrameSink for compressed+writeThrough")

					f, err := os.Open(path)
					require.NoError(t, err)
					defer f.Close()

					sink := po.FrameSink
					var deferred []func()
					if tc.deferSink {
						// compressStream cancels its own context on return, so the
						// late writes run under the caller's.
						sink = func(_ context.Context, cOffset int64, compressed []byte) {
							deferred = append(deferred, func() { po.FrameSink(ctx, cOffset, compressed) })
						}
					}

					ft, sum, err := compressStream(ctx, f, cfg, up, 4, sink)
					capturedFT = ft
					for _, write := range deferred {
						write()
					}

					return ft, sum, err
				})

			flags := NewMockFeatureFlagsClient(t)
			flags.EXPECT().BoolFlag(mock.Anything, mock.Anything).Return(true)
			flags.EXPECT().IntFlag(mock.Anything, mock.Anything).Return(4)

			c := cachedSeekable{path: cacheDir, inner: inner, chunkSize: frameSize, flags: flags, tracer: noopTracer}

			_, _, err := c.StoreFile(t.Context(), srcPath, WithCompressConfig(cfg))
			require.NoError(t, err)

			c.wg.Wait()

			ft := capturedFT.Table()
			require.Equal(t, numFrames, ft.NumFrames())
			assembled := up.Assemble()
			decompressed, err := decompressAll(ft, assembled)
			require.NoError(t, err)
			require.Equal(t, data, decompressed)

			for i := range ft.NumFrames() {
				_, _, startC, endC := ft.FrameAt(i)
				framePath := makeFrameFilename(c.path, Range{Offset: startC, Length: int(endC - startC)})
				onDisk, err := os.ReadFile(framePath)
				require.NoError(t, err)
				assert.Equal(t, assembled[startC:endC], onDisk, "frame %d", i)
			}
		})
	}
}

func TestCacheWriteThroughReader(t *testing.T) {
	t.Parallel()

	newTestCache := func(t *testing.T) cachedSeekable {
		t.Helper()

		return cachedSeekable{
			path:      t.TempDir(),
			chunkSize: 10,
			tracer:    noopTracer,
		}
	}

	t.Run("complete read is cached", func(t *testing.T) {
		t.Parallel()

		c := newTestCache(t)
		data := []byte("hello")
		inner := NewRangeReader(io.NopCloser(bytes.NewReader(data)))

		r := newCaptureReader(inner, len(data), false,
			c.uncompressedChunkWriteback(c.makeChunkFilename(0), 0, int64(len(data)), UnknownSource))

		got, err := io.ReadAll(r)
		require.NoError(t, err)
		assert.Equal(t, data, got)

		mustClose(t, r)
		c.wg.Wait()

		cached, err := os.ReadFile(c.makeChunkFilename(0))
		require.NoError(t, err)
		assert.Equal(t, data, cached)
	})

	t.Run("truncated upstream fully consumed is not cached", func(t *testing.T) {
		t.Parallel()

		c := newTestCache(t)
		// Inner has only 2 bytes but expectedLen is 5. The reader is
		// fully consumed (EOF is reached), yet the total doesn't match
		// the expected length so it must not be cached.
		inner := NewRangeReader(io.NopCloser(bytes.NewReader([]byte{0xAA, 0xBB})))

		r := newCaptureReader(inner, 5, false,
			c.uncompressedChunkWriteback(c.makeChunkFilename(0), 0, 5, UnknownSource))

		got, err := io.ReadAll(r)
		require.NoError(t, err)
		assert.Equal(t, []byte{0xAA, 0xBB}, got)

		mustClose(t, r)
		c.wg.Wait()

		_, err = os.Stat(c.makeChunkFilename(0))
		assert.True(t, os.IsNotExist(err), "truncated data should not be cached")
	})

	t.Run("partially consumed reader closed early is not cached", func(t *testing.T) {
		t.Parallel()

		c := newTestCache(t)
		data := []byte("hello")
		inner := NewRangeReader(io.NopCloser(bytes.NewReader(data)))

		r := newCaptureReader(inner, len(data), false,
			c.uncompressedChunkWriteback(c.makeChunkFilename(0), 0, int64(len(data)), UnknownSource))

		// Read only 2 of 5 bytes, then close without reaching EOF.
		buf := make([]byte, 2)
		n, err := r.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, 2, n)

		mustClose(t, r)
		c.wg.Wait()

		_, err = os.Stat(c.makeChunkFilename(0))
		assert.True(t, os.IsNotExist(err), "partially read data should not be cached")
	})
}
