package storage

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// generateSemiRandomData produces deterministic, compressible data.
// Random byte repeated 1-16 times — gives ~0.5-0.7 compression ratio.
func generateSemiRandomData(size int) []byte {
	data := make([]byte, size)
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // deterministic
	i := 0
	for i < size {
		runLen := rng.IntN(16) + 1
		if i+runLen > size {
			runLen = size - i
		}
		b := byte(rng.IntN(256))
		for j := range runLen {
			data[i+j] = b
		}
		i += runLen
	}

	return data
}

// ThrottledPartUploader wraps memPartUploader with simulated upload bandwidth.
type ThrottledPartUploader struct {
	memPartUploader

	bandwidth int64 // bytes/sec; 0 = unlimited
}

func (t *ThrottledPartUploader) UploadPart(ctx context.Context, partIndex int, data ...[]byte) error {
	if t.bandwidth > 0 {
		total := 0
		for _, d := range data {
			total += len(d)
		}
		time.Sleep(time.Duration(float64(total) / float64(t.bandwidth) * float64(time.Second)))
	}

	return t.memPartUploader.UploadPart(ctx, partIndex, data...)
}

// decompressAll walks the FrameTable and decompresses each frame from the
// concatenated compressed blob, returning the original uncompressed data.
func decompressAll(ft *FrameTable, compressed []byte) ([]byte, error) {
	var result []byte

	for i := range ft.NumFrames() {
		_, _, startC, endC := ft.FrameAt(i)
		cLen := endC - startC

		if startC+cLen > int64(len(compressed)) {
			return nil, fmt.Errorf("frame %d: compressed data truncated (need %d, have %d)", i, startC+cLen, len(compressed))
		}

		frameData := compressed[startC:endC]

		var frame []byte
		var err error

		switch ft.CompressionType() {
		case CompressionLZ4:
			dec := getLZ4Decoder(bytes.NewReader(frameData))
			frame, err = io.ReadAll(dec)
			putLZ4Decoder(dec)
		case CompressionZstd:
			var dec *zstd.Decoder
			dec, err = getZstdDecoder(bytes.NewReader(frameData))
			if err == nil {
				frame, err = io.ReadAll(dec)
				putZstdDecoder(dec)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("frame %d: %w", i, err)
		}
		result = append(result, frame...)
	}

	return result, nil
}

// defaultCfg returns a CompressConfig with the given overrides applied.
func defaultCfg(ct CompressionType, workers, frameSize int) CompressConfig {
	level := 2 // zstd default
	if ct == CompressionLZ4 {
		level = 0
	}

	return CompressConfig{
		Enabled:            true,
		Type:               ct.String(),
		Level:              level,
		EncoderConcurrency: 1,
		FrameEncodeWorkers: workers,
		FrameSizeKB:        frameSize / 1024,
		MinPartSizeMB:      50,
	}
}

func TestCompressStreamRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		dataSize       int
		frameSize      int
		workers        int
		codec          CompressionType
		incompressible bool // use crypto/rand data that cannot be compressed
	}{
		{"basic", 10 * megabyte, 2 * megabyte, 4, CompressionZstd, false},
		{"workers_1", 10 * megabyte, 2 * megabyte, 1, CompressionZstd, false},
		{"workers_2", 10 * megabyte, 2 * megabyte, 2, CompressionZstd, false},
		{"not_frame_aligned", 10*megabyte + 1, 2 * megabyte, 4, CompressionZstd, false},
		{"smaller_than_frame", 100 * 1024, 2 * megabyte, 4, CompressionZstd, false},
		{"smaller_than_part", 5 * megabyte, 2 * megabyte, 4, CompressionZstd, false},
		{"empty", 0, 2 * megabyte, 4, CompressionZstd, false},
		{"single_byte", 1, 2 * megabyte, 1, CompressionZstd, false},
		{"lz4", 10 * megabyte, 2 * megabyte, 4, CompressionLZ4, false},
		{"lz4_incompressible", 10 * megabyte, 2 * megabyte, 4, CompressionLZ4, true},
		{"zstd_incompressible", 10 * megabyte, 2 * megabyte, 4, CompressionZstd, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var original []byte
			if tc.dataSize > 0 {
				if tc.incompressible {
					original = make([]byte, tc.dataSize)
					_, err := crand.Read(original)
					require.NoError(t, err)
				} else {
					original = generateSemiRandomData(tc.dataSize)
				}
			}

			up := &memPartUploader{}
			cfg := defaultCfg(tc.codec, tc.workers, tc.frameSize)

			fullFT, checksum, err := compressStream(
				t.Context(),
				bytes.NewReader(original),
				cfg,
				up,
				4,
				nil,
			)
			require.NoError(t, err)
			ft := fullFT.Table()

			if tc.dataSize == 0 {
				require.Equal(t, 0, ft.NumFrames())
				require.Equal(t, sha256.Sum256(nil), checksum)

				return
			}

			// Verify frame count.
			expectedFrames := (tc.dataSize + tc.frameSize - 1) / tc.frameSize
			require.Equal(t, expectedFrames, ft.NumFrames())

			// Verify checksum.
			require.Equal(t, sha256.Sum256(original), checksum)

			// Round-trip: decompress and compare.
			compressed := up.Assemble()
			decompressed, err := decompressAll(ft, compressed)
			require.NoError(t, err)
			require.Equal(t, original, decompressed)
		})
	}
}

func TestCompressStreamContextCancel(t *testing.T) {
	t.Parallel()

	data := generateSemiRandomData(10 * megabyte)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	up := &memPartUploader{}
	cfg := defaultCfg(CompressionZstd, 4, 2*megabyte)

	_, _, err := compressStream(ctx, bytes.NewReader(data), cfg, up, 4, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCompressStreamNonPositiveUploadConcurrency(t *testing.T) {
	t.Parallel()

	for _, maxConcurrency := range []int{-1, 0} {
		t.Run(fmt.Sprintf("max_%d", maxConcurrency), func(t *testing.T) {
			t.Parallel()

			up := new(memPartUploader)
			cfg := defaultCfg(CompressionZstd, 1, 128*1024)
			_, _, err := compressStream(t.Context(), bytes.NewReader([]byte("payload")), cfg, up, maxConcurrency, nil)
			require.NoError(t, err)
			require.NotEmpty(t, up.parts)
		})
	}
}

func TestClampCloudMinPartSize(t *testing.T) {
	t.Parallel()

	// Below S3/GCS XML API's 5 MiB non-final part minimum → clamped.
	require.Equal(t, 5, clampCloudMinPartSize(CompressConfig{MinPartSizeMB: 1}, 0).MinPartSizeMB)
	// Unset resolves to the 50 MB default from MinPartSize().
	require.Equal(t, 50, clampCloudMinPartSize(CompressConfig{}, 0).MinPartSizeMB)
	require.Equal(t, 50, clampCloudMinPartSize(CompressConfig{MinPartSizeMB: 50}, 0).MinPartSizeMB)

	// Large files raise the part size to stay under the 10,000-part cap:
	// 900 GiB at 50 MB parts would need ~18,400 parts.
	clamped := clampCloudMinPartSize(CompressConfig{}, 900<<30)
	require.Equal(t, 103, clamped.MinPartSizeMB)
	require.LessOrEqual(t, int64(900<<30)/clamped.MinPartSize(), int64(cloudMaxParts))
}

func TestCompressStreamPartSizeMinimum(t *testing.T) {
	t.Parallel()

	// Generate once; subtests slice to their needed size.
	sharedData := generateSemiRandomData(100 * megabyte)

	tests := []struct {
		name          string
		dataSize      int
		frameSize     int
		minPartSizeMB int
	}{
		{"large_file", 100 * megabyte, 2 * megabyte, 50},
		{"small_file_one_part", 5 * megabyte, 2 * megabyte, 50},
		{"small_target", 100 * megabyte, 2 * megabyte, 10},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data := sharedData[:tc.dataSize]
			up := &memPartUploader{}
			cfg := defaultCfg(CompressionZstd, 4, tc.frameSize)
			cfg.MinPartSizeMB = tc.minPartSizeMB

			_, _, err := compressStream(t.Context(), bytes.NewReader(data), cfg, up, 4, nil)
			require.NoError(t, err)

			// Verify: no non-final part is under 5 MiB.
			keys := make([]int, 0, len(up.parts))
			for k := range up.parts {
				keys = append(keys, k)
			}
			slices.Sort(keys)

			for i, k := range keys {
				isFinal := i == len(keys)-1
				if !isFinal {
					require.GreaterOrEqual(t, len(up.parts[k]), 5*1024*1024,
						"non-final part %d is under 5 MiB (%d bytes)", k, len(up.parts[k]))
				}
			}

			require.NotEmpty(t, up.parts, "should have at least one part")
		})
	}
}

// TestCompressStreamRace runs many concurrent CompressStream calls with high
// worker counts to shake out data races in the compressor pool, memPartUploader,
// and errgroup coordination. Run with -race.
func TestCompressStreamRace(t *testing.T) {
	t.Parallel()

	const (
		streams       = 8            // concurrent CompressStream calls
		dataSize      = 4 * megabyte // small enough to be fast, big enough to exercise batching
		frameSize     = 128 * 1024   // 128 KB — many frames per part
		workers       = 8            // high worker count to maximise contention
		minPartSizeMB = 1            // small parts → many parts per stream
	)

	data := generateSemiRandomData(dataSize)
	wantChecksum := sha256.Sum256(data)

	// Use an errgroup to run all streams concurrently.
	eg, ctx := errgroup.WithContext(t.Context())
	for i := range streams {
		codec := CompressionZstd
		if i%2 == 1 {
			codec = CompressionLZ4 // mix codecs for more coverage
		}

		eg.Go(func() error {
			up := &memPartUploader{}
			cfg := defaultCfg(codec, workers, frameSize)
			cfg.MinPartSizeMB = minPartSizeMB
			if codec == CompressionZstd {
				cfg.EncoderConcurrency = 4 // multi-threaded zstd encoders for more contention
			}

			ft, checksum, err := compressStream(ctx, bytes.NewReader(data), cfg, up, 4, nil)
			if err != nil {
				return fmt.Errorf("stream %d: compress: %w", i, err)
			}

			if checksum != wantChecksum {
				return fmt.Errorf("stream %d: checksum mismatch", i)
			}

			decompressed, err := decompressAll(ft.Table(), up.Assemble())
			if err != nil {
				return fmt.Errorf("stream %d: decompress: %w", i, err)
			}

			if !bytes.Equal(data, decompressed) {
				return fmt.Errorf("stream %d: round-trip data mismatch", i)
			}

			return nil
		})
	}

	require.NoError(t, eg.Wait())
}

func BenchmarkCompress(b *testing.B) {
	const dataSize = 256 * megabyte
	data := generateSemiRandomData(dataSize)

	configs := []struct {
		name      string
		workers   int
		bandwidth int64 // bytes/sec; 0 = unlimited
	}{
		{"w1_unlimited", 1, 0},
		{"w2_unlimited", 2, 0},
		{"w4_unlimited", 4, 0},
		{"w1_200MBs", 1, 200 * megabyte},
		{"w4_200MBs", 4, 200 * megabyte},
		{"w4_100MBs", 4, 100 * megabyte},
	}

	for _, bcfg := range configs {
		b.Run(bcfg.name, func(b *testing.B) {
			compCfg := CompressConfig{
				Enabled:            true,
				Type:               CompressionZstd.String(),
				Level:              2,
				EncoderConcurrency: 1,
				FrameEncodeWorkers: bcfg.workers,
				FrameSizeKB:        2 * 1024,
				MinPartSizeMB:      50,
			}

			var lastParts atomic.Int32

			b.ResetTimer()
			b.SetBytes(int64(dataSize))

			for range b.N {
				up := &ThrottledPartUploader{bandwidth: bcfg.bandwidth}

				_, _, err := compressStream(
					b.Context(),
					bytes.NewReader(data),
					compCfg,
					up, 4,
					nil,
				)
				if err != nil {
					b.Fatal(err)
				}

				lastParts.Store(int32(len(up.parts)))
			}

			// Report after all iterations using last run's values.
			// b.SetBytes already reports MB/s (uncompressed throughput).
			b.ReportMetric(float64(lastParts.Load()), "parts")
		})
	}
}

func BenchmarkStoreFile(b *testing.B) {
	const dataSize = 1024 * megabyte // 1 GB

	data := generateSemiRandomData(dataSize)
	inputDir := b.TempDir()
	inputPath := filepath.Join(inputDir, "input.bin")
	require.NoError(b, os.WriteFile(inputPath, data, 0o644))
	data = nil //nolint:ineffassign,wastedassign // hint GC to free 1GB before benchmark loop

	codecs := []struct {
		name  string
		codec CompressionType
		level int
	}{
		{"zstd1", CompressionZstd, 1},
		{"zstd2", CompressionZstd, 2},
		{"zstd3", CompressionZstd, 3},
		{"lz4", CompressionLZ4, 0},
	}
	workerCounts := []int{1, 2, 4, 8}

	for _, codec := range codecs {
		for _, workers := range workerCounts {
			name := fmt.Sprintf("%s/w%d", codec.name, workers)
			b.Run(name, func(b *testing.B) {
				compCfg := CompressConfig{
					Enabled:            true,
					Type:               codec.codec.String(),
					Level:              codec.level,
					EncoderConcurrency: 1,
					FrameEncodeWorkers: workers,
					FrameSizeKB:        2 * 1024,
					MinPartSizeMB:      50,
				}

				b.SetBytes(int64(dataSize))
				b.ResetTimer()

				for range b.N {
					outDir := b.TempDir()
					outPath := filepath.Join(outDir, "output.dat")
					obj := &fsObject{path: outPath}

					fullFT, _, err := obj.StoreFile(b.Context(), inputPath, WithCompressConfig(compCfg))
					if err != nil {
						b.Fatal(err)
					}
					ft := fullFT.Table()

					b.ReportMetric(float64(ft.CompressedSize())/float64(ft.UncompressedSize()), "ratio")
				}
			})
		}
	}

	b.Run("uncompressed", func(b *testing.B) {
		b.SetBytes(int64(dataSize))
		b.ResetTimer()

		for range b.N {
			outDir := b.TempDir()
			outPath := filepath.Join(outDir, "output.dat")

			in, err := os.Open(inputPath)
			if err != nil {
				b.Fatal(err)
			}
			out, err := os.Create(outPath)
			if err != nil {
				in.Close()
				b.Fatal(err)
			}
			if _, err := io.Copy(out, in); err != nil {
				in.Close()
				out.Close()
				b.Fatal(err)
			}
			in.Close()
			out.Close()
		}
	})
}
