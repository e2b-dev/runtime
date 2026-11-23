package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/lock"
)

// openReaderCompressed handles the compressed cache path for OpenRangeReader.
// NFS stores compressed frames (.frm); on hit we decompress, on miss we fetch
// raw compressed bytes and tee them to NFS on Close.
func (c *cachedSeekable) openReaderCompressed(ctx context.Context, offsetU int64, frameTable *FrameTable) (RangeReader, Source, error) {
	rng, err := frameTable.LocateCompressed(offsetU)
	if err != nil {
		return nil, UnknownSource, fmt.Errorf("frame lookup for offset %d: %w", offsetU, err)
	}

	path := makeFrameFilename(c.path, rng)
	ct := frameTable.CompressionType()

	// Cache hit: open the compressed frame from NFS, validate its size, and
	// decompress. A size mismatch drops the stale file; on any miss/error we fall
	// through to a refetch.
	start := time.Now()
	var dec RangeReader
	f, err := os.Open(path)
	if err == nil {
		var fi os.FileInfo
		if fi, err = f.Stat(); err != nil {
			f.Close()
		} else if fi.Size() != int64(rng.Length) {
			f.Close()
			_ = os.Remove(path)
			err = fmt.Errorf("cached frame %s size %d != expected %d", path, fi.Size(), rng.Length)
		} else if dec, err = NewDecompressReader(NewRangeReader(f), ct, SourceNFS, c.objType); err != nil {
			f.Close()
			err = fmt.Errorf("decompress cached frame: %w", err)
		}
	}
	RecordReadOpen(ctx, time.Since(start), c.objType, SourceNFS, ct, err)
	if err == nil {
		c.index.touch()
		// A cached frame that decodes cleanly returns here. NewDecompressReader's
		// Close drains and CRC-verifies the frame, so a non-nil Close error means
		// the cached bytes no longer decode (bit rot / torn write that still has
		// the right size, which the size check above cannot catch) — evict it so
		// the next read refetches instead of failing forever.
		return &closeHookReader{RangeReader: dec, onClose: func(_ context.Context, err error) {
			if err != nil {
				_ = os.Remove(path)
			}
		}}, SourceNFS, nil
	}

	// Cache miss: fetch raw compressed bytes via OpenRangeReader(nil frameTable).
	raw, innerSource, err := c.inner.OpenRangeReader(ctx, rng.Offset, int64(rng.Length), nil)
	if err != nil {
		return nil, innerSource, fmt.Errorf("raw fetch at C=%d: %w", rng.Offset, err)
	}

	// The captureReader tees the raw compressed bytes into `captured`; write
	// them back only if the frame decoded cleanly (Close error == nil), so a
	// corrupt-but-right-sized frame is never cached.
	var captured []byte
	capturing := !skipCacheWriteback(ctx)
	frameReader := raw
	if capturing {
		frameReader = newCaptureReader(raw, rng.Length, true, func(_ context.Context, frame []byte) {
			captured = frame
		})
	}

	dec, err = NewDecompressReader(frameReader, ct, innerSource, c.objType)
	if err != nil {
		raw.Close(ctx)

		return nil, innerSource, fmt.Errorf("create decompressor: %w", err)
	}

	return &closeHookReader{RangeReader: dec, onClose: func(ctx context.Context, err error) {
		if err != nil || !capturing {
			return
		}
		c.writeFrameBack(ctx, path, offsetU, rng.Length, innerSource, ct, captured)
	}}, innerSource, nil
}

// closeHookReader runs onClose exactly once when the wrapped reader is closed,
// passing its Close error. The compressed cache relies on NewDecompressReader's
// Close draining and CRC-verifying the frame, so that error is the decode
// verdict: on a miss, write the frame back only when it's nil; on a hit, evict
// when it isn't. Reads pass straight through the embedded RangeReader.
type closeHookReader struct {
	RangeReader

	onClose func(ctx context.Context, err error)
}

func (r *closeHookReader) Close(ctx context.Context) (*ReadStats, error) {
	stats, err := r.RangeReader.Close(ctx)
	if r.onClose != nil {
		r.onClose(ctx, err)
		r.onClose = nil
	}

	return stats, err
}

// writeFrameBack persists a fully-read compressed frame to the NFS cache in a
// detached goroutine. Best-effort: a short frame is logged and skipped — the
// caller already has valid decompressed bytes.
func (c *cachedSeekable) writeFrameBack(ctx context.Context, framePath string, offset int64, expectedSize int, src Source, codec CompressionType, frame []byte) {
	if !isCompleteRead(len(frame), expectedSize, nil) {
		logger.L().Warn(ctx, "compressed frame cache writeback short, skipping",
			zap.Int("got", len(frame)), zap.Int("expected", expectedSize), zap.String("path", framePath))

		return
	}

	c.goCtx(ctx, func(ctx context.Context) {
		ctx, span := c.tracer.Start(ctx, "write compressed frame back to cache")
		defer span.End()

		start := time.Now()
		err := c.writeToCache(ctx, offset, framePath, frame)
		recordWriteback(ctx, time.Since(start), int64(len(frame)), c.objType, src, codec, TriggerRead, err)

		if err != nil && !errors.Is(err, lock.ErrLockAlreadyHeld) {
			recordError(span, err)
			logger.L().Warn(ctx, "failed to write frame back to cache", zap.Error(err))
		}
	})
}

// makeFrameFilename returns the NFS cache path for a compressed frame.
// Format: {cacheBasePath}/{016xStart}-{xLength}.frm
func makeFrameFilename(cacheBasePath string, r Range) string {
	return fmt.Sprintf("%s/%016x-%x.frm", cacheBasePath, r.Offset, uint32(r.Length))
}
