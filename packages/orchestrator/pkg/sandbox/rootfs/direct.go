//go:build linux

package rootfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/edsrzf/mmap-go"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

type DirectProvider struct {
	header *header.Header

	path      string
	blockSize int64

	// TODO: Remove when the snapshot flow is improved
	finishedOperations chan struct{}
	// TODO: Remove when the snapshot flow is improved
	closed atomic.Bool

	mmap *mmap.MMap

	logger logger.Logger
}

func NewDirectProvider(ctx context.Context, rootfs block.ReadonlyDevice, path string, lg logger.Logger) (Provider, error) {
	blockSize := rootfs.BlockSize()

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()

	size, err := rootfs.Size(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting size: %w", err)
	}

	mm, err := mmap.MapRegion(f, int(size), unix.PROT_READ|unix.PROT_WRITE, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("error mapping region: %w", err)
	}

	return &DirectProvider{
		header: rootfs.Header(),

		path:      path,
		blockSize: blockSize,

		finishedOperations: make(chan struct{}, 1),

		mmap:   &mm,
		logger: lg,
	}, nil
}

func (o *DirectProvider) Start(_ context.Context) error {
	return nil
}

func (o *DirectProvider) ExportDiff(
	ctx context.Context,
	out *os.File,
	stopSandbox func(context.Context) error,
) (*header.DiffMetadata, error) {
	ctx, childSpan := tracer.Start(ctx, "direct-provider-export")
	defer childSpan.End()

	if !o.closed.CompareAndSwap(false, true) {
		return nil, errors.New("direct provider close is already in progress")
	}

	defer func() {
		err := o.mmap.Unmap()
		if err != nil {
			o.logger.Error(ctx, "error unmapping mmap", zap.Error(err))
		}
	}()

	// the error is already logged in go routine in SandboxCreate handler
	go func() {
		err := stopSandbox(ctx)
		if err != nil {
			o.logger.Error(ctx, "error stopping sandbox on cow export", zap.Error(err))
		}
	}()

	select {
	case <-o.finishedOperations:
	case <-ctx.Done():
		return nil, errors.New("timeout waiting for overlay device to be released")
	}
	telemetry.ReportEvent(ctx, "sandbox stopped")

	m, err := o.exportToDiff(ctx, out)
	if err != nil {
		return nil, fmt.Errorf("error building diff metadata: %w", err)
	}

	telemetry.ReportEvent(ctx, "cache exported")

	return m, nil
}

func (o *DirectProvider) PrepareExportDiff(_ context.Context, _ func(context.Context) error) (*block.Cache, error) {
	return nil, ErrDeferredExportNotSupported
}

func (o *DirectProvider) ExportDiffInPlace(_ context.Context, _ *os.File) (*header.DiffMetadata, error) {
	return nil, errors.New("direct provider does not support in-place export")
}

func (o *DirectProvider) SwapForBackgroundSeal(_ context.Context) (*block.Cache, error) {
	return nil, ErrDeferredExportNotSupported
}

func (o *DirectProvider) FoldSealed(_ context.Context) (*block.Cache, error) {
	return nil, nil
}

func (o *DirectProvider) Close(ctx context.Context) error {
	o.finishedOperations <- struct{}{}

	if !o.closed.CompareAndSwap(false, true) {
		return nil
	}

	return errors.Join(o.sync(ctx), o.mmap.Unmap())
}

func (o *DirectProvider) Path() (string, error) {
	return o.path, nil
}

func (o *DirectProvider) exportToDiff(ctx context.Context, out io.Writer) (*header.DiffMetadata, error) {
	err := o.sync(ctx)
	if err != nil {
		return nil, fmt.Errorf("error flushing path: %w", err)
	}

	builder := header.NewDiffMetadataBuilder(o.blockSize)

	f, err := os.Open(o.path)
	if err != nil {
		return nil, fmt.Errorf("error opening path: %w", err)
	}
	defer f.Close()

	block := make([]byte, o.blockSize)
	for i := int64(0); i < int64(o.header.Metadata.Size); i += o.blockSize {
		n, err := f.ReadAt(block, i)
		if err != nil {
			return nil, fmt.Errorf("error reading from file: %w", err)
		}

		err = builder.Process(ctx, block[:n], out, i)
		if err != nil {
			return nil, fmt.Errorf("error processing block %d: %w", i, err)
		}
	}

	return builder.Build(), nil
}

func (o *DirectProvider) sync(ctx context.Context) error {
	err := o.mmap.Flush()
	if err != nil {
		return fmt.Errorf("error flushing mmap: %w", err)
	}

	return flush(ctx, o.path)
}
