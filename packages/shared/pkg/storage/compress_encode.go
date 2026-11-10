package storage

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
	lz4 "github.com/pierrec/lz4/v4"
)

// compressor compresses individual frames. Implementations are pooled and
// reused across frames within a single CompressStream call.
//
// The returned slice is owned by the caller and sized to the compressed
// output. Frames wait in memory until their part is uploaded and the cache
// writer may still hold them after that, so returning the encoder's
// frame-sized working buffer would keep a full frame allocated for every
// frame, however small its output.
type compressor interface {
	compress(src []byte) ([]byte, error)
}

// lz4Compressor wraps a pooled lz4.Writer. The writer is reused via Reset
// between frames to avoid re-allocating internal hash tables (~64KB).
type lz4Compressor struct {
	w       *lz4.Writer
	scratch bytes.Buffer
}

func (c *lz4Compressor) compress(src []byte) ([]byte, error) {
	c.scratch.Reset()
	c.scratch.Grow(lz4.CompressBlockBound(len(src)))
	c.w.Reset(&c.scratch)

	if _, err := c.w.Write(src); err != nil {
		return nil, fmt.Errorf("lz4 compress: %w", err)
	}

	if err := c.w.Close(); err != nil {
		return nil, fmt.Errorf("lz4 compress close: %w", err)
	}

	return exactCopy(c.scratch.Bytes()), nil
}

// zstdCompressor wraps a pooled zstd.Encoder using EncodeAll.
type zstdCompressor struct {
	enc     *zstd.Encoder
	scratch []byte
}

func (z *zstdCompressor) compress(src []byte) ([]byte, error) { //nolint:unparam // satisfies compressor interface
	z.scratch = z.enc.EncodeAll(src, z.scratch[:0])

	return exactCopy(z.scratch), nil
}

// exactCopy returns a copy with cap == len; bytes.Clone may round the
// capacity up.
func exactCopy(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)

	return out
}

// compressorPoolKey holds every config field that changes how a compressor
// from newCompressorPool encodes or what it returns. Uploads whose configs
// have equal keys share one pool. Keep it in step with newCompressorPool.
type compressorPoolKey struct {
	compressionType    CompressionType
	level              int
	frameSize          int
	encoderConcurrency int
}

func newCompressorPoolKey(cfg CompressConfig) compressorPoolKey {
	return compressorPoolKey{
		compressionType:    cfg.CompressionType(),
		level:              cfg.Level,
		frameSize:          cfg.FrameSize(),
		encoderConcurrency: cfg.EncoderConcurrency,
	}
}

// compressorPools maps a compressorPoolKey to its *sync.Pool for the life of
// the process. Keys come from the few distinct compression configs, so the
// map stays small. sync.Pool drops idle compressors on GC, so a quiet process
// keeps no encoders, and a compressor's scratch buffer lives no longer than
// the compressor.
var compressorPools sync.Map

// sharedCompressorPool returns the pool shared by every upload with cfg's
// key, creating and validating it on first use. Building a pool per upload
// would allocate a fresh encoder for each of its workers, about 6 MiB each for
// zstd, and drop them when the upload ends.
func sharedCompressorPool(cfg CompressConfig) (*sync.Pool, error) {
	key := newCompressorPoolKey(cfg)
	if pool, ok := compressorPools.Load(key); ok {
		return pool.(*sync.Pool), nil
	}

	pool, err := newCompressorPool(cfg)
	if err != nil {
		return nil, err
	}
	actual, _ := compressorPools.LoadOrStore(key, pool)

	return actual.(*sync.Pool), nil
}

// newCompressorPool returns a pool of compressors for the given config.
// Both LZ4 and zstd encoders are pooled and reused via Reset/EncodeAll.
// The config is validated eagerly — if zstd options are invalid, an error
// is returned immediately rather than deferred to pool.Get().
func newCompressorPool(cfg CompressConfig) (*sync.Pool, error) {
	pool := &sync.Pool{}

	switch cfg.CompressionType() {
	case CompressionZstd:
		zstdOpts := []zstd.EOption{
			zstd.WithEncoderLevel(zstd.EncoderLevel(cfg.Level)),
			zstd.WithEncoderCRC(true),
		}
		if cfg.FrameSize() > 0 {
			zstdOpts = append(zstdOpts, zstd.WithWindowSize(cfg.FrameSize()))
		}
		if cfg.EncoderConcurrency > 0 {
			zstdOpts = append(zstdOpts, zstd.WithEncoderConcurrency(cfg.EncoderConcurrency))
		}

		// Validate options by creating one encoder upfront.
		first, err := zstd.NewWriter(nil, zstdOpts...)
		if err != nil {
			return nil, fmt.Errorf("zstd encoder: %w", err)
		}
		pool.Put(&zstdCompressor{enc: first})

		pool.New = func() any {
			// Options are already validated; NewWriter won't fail.
			enc, _ := zstd.NewWriter(nil, zstdOpts...)

			return &zstdCompressor{enc: enc}
		}
	case CompressionLZ4:
		lz4Opts := []lz4.Option{
			lz4.BlockSizeOption(lz4.Block4Mb),
			lz4.BlockChecksumOption(true),
			lz4.ChecksumOption(false),
			lz4.ConcurrencyOption(1),
			lz4.CompressionLevelOption(lz4.Fast),
		}

		// Validate options by creating one encoder upfront.
		first := lz4.NewWriter(nil)
		if err := first.Apply(lz4Opts...); err != nil {
			return nil, fmt.Errorf("lz4 encoder: %w", err)
		}
		pool.Put(&lz4Compressor{w: first})

		pool.New = func() any {
			w := lz4.NewWriter(nil)
			_ = w.Apply(lz4Opts...) //nolint:errcheck // options validated above

			return &lz4Compressor{w: w}
		}
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", cfg.CompressionType())
	}

	return pool, nil
}

func CompressBytes(ctx context.Context, data []byte, cfg CompressConfig) (*FullFrameTable, []byte, [32]byte, error) {
	up := &memPartUploader{}

	const compressBytesConcurrency = 1
	ft, checksum, err := compressStream(ctx, bytes.NewReader(data), cfg, up, compressBytesConcurrency, nil)
	if err != nil {
		return nil, nil, [32]byte{}, err
	}

	return ft, up.Assemble(), checksum, nil
}
