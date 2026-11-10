package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// generateSparseData produces frames that start with randomPrefix random bytes
// and are zero after that, the shape of a mostly empty rootfs. A 96 KiB prefix
// in a 2 MiB frame compresses to about 5% of the input.
func generateSparseData(size, frameSize, randomPrefix int) []byte {
	data := make([]byte, size)
	rng := rand.New(rand.NewPCG(3, 4)) //nolint:gosec // deterministic
	for off := 0; off < size; off += frameSize {
		end := min(off+randomPrefix, size)
		for i := off; i < end; i++ {
			data[i] = byte(rng.Uint32())
		}
	}

	return data
}

func TestCompressStreamLowRatioMultipart(t *testing.T) {
	t.Parallel()

	const (
		dataSize      = 64 * megabyte
		frameSize     = 2 * megabyte
		minPartSizeMB = 1
	)

	inputs := []struct {
		name      string
		data      []byte
		multipart bool
	}{
		{"zeros", make([]byte, dataSize), false},
		{"sparse", generateSparseData(dataSize, frameSize, 96*1024), true},
	}

	for _, codec := range []CompressionType{CompressionZstd, CompressionLZ4} {
		for _, in := range inputs {
			t.Run(codec.String()+"_"+in.name, func(t *testing.T) {
				t.Parallel()

				up := &memPartUploader{}
				cfg := defaultCfg(codec, 4, frameSize)
				cfg.MinPartSizeMB = minPartSizeMB

				fullFT, checksum, err := compressStream(t.Context(), bytes.NewReader(in.data), cfg, up, 4, nil)
				require.NoError(t, err)
				require.Equal(t, sha256.Sum256(in.data), checksum)

				ft := fullFT.Table()
				require.Equal(t, dataSize/frameSize, ft.NumFrames())

				decompressed, err := decompressAll(ft, up.Assemble())
				require.NoError(t, err)
				require.Equal(t, in.data, decompressed)

				keys := make([]int, 0, len(up.parts))
				for k := range up.parts {
					keys = append(keys, k)
				}
				slices.Sort(keys)
				if in.multipart {
					require.Greater(t, len(keys), 1, "input should span several parts")
				}
				for _, k := range keys[:len(keys)-1] {
					require.GreaterOrEqual(t, len(up.parts[k]), minPartSizeMB*megabyte,
						"non-final part %d is under the minimum (%d bytes)", k, len(up.parts[k]))
				}
			})
		}
	}
}

func TestCompressorOutputIsRightSizedAndOwned(t *testing.T) {
	t.Parallel()

	for _, codec := range []CompressionType{CompressionZstd, CompressionLZ4} {
		t.Run(codec.String(), func(t *testing.T) {
			t.Parallel()

			pool, err := newCompressorPool(defaultCfg(codec, 1, DefaultCompressFrameSize))
			require.NoError(t, err)
			c := pool.Get().(compressor)

			zeros := make([]byte, DefaultCompressFrameSize)
			a, err := c.compress(zeros)
			require.NoError(t, err)
			aCopy := bytes.Clone(a)

			b, err := c.compress(generateSemiRandomData(DefaultCompressFrameSize))
			require.NoError(t, err)

			require.Equal(t, len(a), cap(a), "output keeps the encoder's working capacity")
			require.Equal(t, len(b), cap(b), "output keeps the encoder's working capacity")
			require.NotSame(t, unsafe.SliceData(a), unsafe.SliceData(b), "outputs share a buffer")
			require.Equal(t, aCopy, a, "a later compress overwrote an earlier output")

			ft := NewFullFrameTable(codec, []FrameSize{{U: int32(len(zeros)), C: int32(len(a))}}).Table()
			decoded, err := decompressAll(ft, a)
			require.NoError(t, err)
			require.Equal(t, zeros, decoded)
		})
	}
}

// recordingPartUploader keeps the frame slices passed to UploadPart.
type recordingPartUploader struct {
	memPartUploader

	mu     sync.Mutex
	frames [][]byte
}

func (r *recordingPartUploader) UploadPart(ctx context.Context, partIndex int, data ...[]byte) error {
	r.mu.Lock()
	r.frames = append(r.frames, data...)
	r.mu.Unlock()

	return r.memPartUploader.UploadPart(ctx, partIndex, data...)
}

// TestCompressStreamHandsOffRightSizedFrames checks the slices compressStream
// gives the upload and the cache sink, which keep them until the part is
// uploaded and the cache write is done. A frame whose capacity exceeds its
// length keeps that whole allocation alive for its compressed bytes.
func TestCompressStreamHandsOffRightSizedFrames(t *testing.T) {
	t.Parallel()

	const (
		dataSize  = 16 * megabyte
		frameSize = 2 * megabyte
	)
	data := generateSparseData(dataSize, frameSize, 96*1024)

	for _, codec := range []CompressionType{CompressionZstd, CompressionLZ4} {
		t.Run(codec.String(), func(t *testing.T) {
			t.Parallel()

			up := &recordingPartUploader{}
			var sinkFrames [][]byte
			sink := func(_ context.Context, _ int64, compressed []byte) {
				sinkFrames = append(sinkFrames, compressed)
			}

			_, _, err := compressStream(t.Context(), bytes.NewReader(data), defaultCfg(codec, 4, frameSize), up, 4, sink)
			require.NoError(t, err)

			require.Len(t, up.frames, dataSize/frameSize)
			require.Len(t, sinkFrames, dataSize/frameSize)
			for i, f := range up.frames {
				require.Equal(t, len(f), cap(f), "uploaded frame %d", i)
			}
			for i, f := range sinkFrames {
				require.Equal(t, len(f), cap(f), "sink frame %d", i)
			}
		})
	}
}

func TestSharedCompressorPoolKey(t *testing.T) {
	t.Parallel()

	base := defaultCfg(CompressionZstd, 4, DefaultCompressFrameSize)
	basePool, err := sharedCompressorPool(base)
	require.NoError(t, err)

	tests := []struct {
		name   string
		change func(*CompressConfig)
		shared bool
	}{
		{"same config", func(*CompressConfig) {}, true},
		{"more encode workers", func(c *CompressConfig) { c.FrameEncodeWorkers = 8 }, true},
		{"larger parts", func(c *CompressConfig) { c.MinPartSizeMB = 100 }, true},
		{"compression type", func(c *CompressConfig) { c.Type = CompressionLZ4.String() }, false},
		{"level", func(c *CompressConfig) { c.Level = 3 }, false},
		{"frame size", func(c *CompressConfig) { c.FrameSizeKB = 1024 }, false},
		{"encoder concurrency", func(c *CompressConfig) { c.EncoderConcurrency = 2 }, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := base
			tc.change(&cfg)
			pool, err := sharedCompressorPool(cfg)
			require.NoError(t, err)

			if tc.shared {
				require.Same(t, basePool, pool)
			} else {
				require.NotSame(t, basePool, pool)
			}
		})
	}
}

// TestCompressStreamReusesCompressorPool checks that compressStream takes its
// compressors from the shared pool for its config, so the encoders one upload
// warms up are there for the next.
func TestCompressStreamReusesCompressorPool(t *testing.T) {
	t.Parallel()

	// A config no other test uses, so no other test creates its pool.
	cfg := defaultCfg(CompressionZstd, 4, 512*1024)
	cfg.Level = 1
	key := newCompressorPoolKey(cfg)
	_, existed := compressorPools.Load(key)
	require.False(t, existed, "another test uses this config")
	t.Cleanup(func() { compressorPools.Delete(key) })

	data := generateSparseData(4*megabyte, 512*1024, 32*1024)
	upload := func() {
		_, _, err := compressStream(t.Context(), bytes.NewReader(data), cfg, &memPartUploader{}, 4, nil)
		require.NoError(t, err)
	}

	upload()
	first, ok := compressorPools.Load(key)
	require.True(t, ok, "compressStream did not use the shared pool")

	upload()
	second, _ := compressorPools.Load(key)
	require.Same(t, first, second)
}
