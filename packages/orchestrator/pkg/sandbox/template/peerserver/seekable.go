//go:build linux

package peerserver

import (
	"context"
	"fmt"
	"io"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
)

var _ SeekableSource = &seekableSource{}

// seekableSource serves seekable diff files (memfile, rootfs.ext4).
// Supports Size and random-access streaming via offset/length.
type seekableSource struct {
	diff build.Diff
}

func (f *seekableSource) Size(ctx context.Context) (int64, error) {
	return f.diff.Size(ctx)
}

func (f *seekableSource) Exists(_ context.Context) (bool, error) {
	return false, ErrNotSupported
}

func (f *seekableSource) Stream(ctx context.Context, offset, length int64, sender Sender) error {
	ctx, span := tracer.Start(ctx, "stream-seekable-file", trace.WithAttributes(
		attribute.Int64("offset", offset),
		attribute.Int64("length", length),
	))
	defer span.End()

	if length <= 0 {
		return nil
	}

	blockSize := f.diff.BlockSize()
	if blockSize <= 0 {
		return fmt.Errorf("invalid diff block size %d", blockSize)
	}
	for sent := int64(0); sent < length; {
		take := min(blockSize, length-sent)
		// Send may retain the bytes for lazy consumers after returning. Each
		// message owns its buffer; later reads must not overwrite earlier sends.
		buf := make([]byte, take)
		// P2P always serves uncompressed bytes — pass nil FrameTable.
		var n int
		err := block.RunFaultSafe(ctx, func() error {
			var err error
			n, err = f.diff.ReadAt(ctx, buf[:take], offset+sent, nil)

			return err
		})
		if err != nil {
			span.RecordError(err)

			return fmt.Errorf("read diff at offset %d: %w", offset+sent, err)
		}
		if int64(n) != take {
			span.RecordError(io.ErrUnexpectedEOF)

			return fmt.Errorf("read diff at offset %d: %w", offset+sent, io.ErrUnexpectedEOF)
		}
		if err := sender.Send(buf[:n]); err != nil {
			span.RecordError(err)

			return fmt.Errorf("send diff chunk: %w", err)
		}
		sent += int64(n)
	}

	return nil
}
