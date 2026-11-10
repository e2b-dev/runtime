//go:build linux

package template

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.uber.org/zap"

	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	oldMemfileHugePageSize = 2 << 20 // 2 MiB
	oldRootfsBlockSize     = 4 << 10 // 4 KiB
)

type Storage struct {
	source *build.File
}

func NewStorage(
	ctx context.Context,
	store *build.DiffStore,
	buildId string,
	fileType build.DiffType,
	h *header.Header,
	persistence storage.StorageProvider,
	metrics blockmetrics.Metrics,
) (*Storage, error) {
	paths := storage.Paths{BuildID: buildId}

	var (
		hdrPath   string
		headerErr error
	)
	if h == nil {
		switch fileType {
		case build.Memfile:
			hdrPath = paths.MemfileHeader()
		case build.Rootfs:
			hdrPath = paths.RootfsHeader()
		default:
			return nil, build.UnknownDiffTypeError{DiffType: fileType}
		}

		h, _, headerErr = header.LoadHeader(ctx, persistence, hdrPath)
		if headerErr != nil && !errors.Is(headerErr, storage.ErrObjectNotExist) {
			return nil, headerErr
		}
	}

	// If we can't find the diff header in storage, we try to find the "old" style template without a header as a fallback.
	if h == nil {
		var dataPath string
		switch fileType {
		case build.Memfile:
			dataPath = paths.Memfile()
		case build.Rootfs:
			dataPath = paths.Rootfs()
		default:
			return nil, build.UnknownDiffTypeError{DiffType: fileType}
		}

		object, err := persistence.OpenSeekable(ctx, dataPath)
		if err != nil {
			return nil, fmt.Errorf("headerless fallback: header %q not loadable (%w), data %q open failed: %w", hdrPath, headerErr, dataPath, err)
		}

		size, err := object.Size(ctx)
		if err != nil {
			return nil, fmt.Errorf("headerless fallback: header %q not loadable (%w), data %q size failed: %w", hdrPath, headerErr, dataPath, err)
		}

		logger.L().Debug(ctx, "template header not found; using legacy headerless fallback",
			logger.WithBuildID(buildId),
			zap.String("file_type", string(fileType)),
			zap.String("header_path", hdrPath),
			zap.NamedError("header_error", headerErr),
		)

		id, err := uuid.Parse(buildId)
		if err != nil {
			return nil, fmt.Errorf("failed to parse build id: %w", err)
		}

		// TODO: This is a workaround for the old style template without a header.
		// We don't know the block size of the old style template, so we set it manually.
		var blockSize uint64
		switch fileType {
		case build.Memfile:
			blockSize = oldMemfileHugePageSize
		case build.Rootfs:
			blockSize = oldRootfsBlockSize
		default:
			return nil, fmt.Errorf("unsupported file type: %s", fileType)
		}

		h, err = header.NewHeader(&header.Metadata{
			// The version is always 1 for the old style template without a header.
			Version:     1,
			BuildId:     id,
			BaseBuildId: id,
			Size:        uint64(size),
			BlockSize:   blockSize,
			Generation:  1,
		}, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create header for old style template: %w", err)
		}
	}

	recordHeaderShape(ctx, fileType, h)

	b := build.NewFile(h, store, fileType, persistence, metrics)

	return &Storage{
		source: b,
	}, nil
}

func (d *Storage) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	return d.source.ReadAt(ctx, p, off)
}

func (d *Storage) Size(_ context.Context) (int64, error) {
	return int64(d.source.Header().Metadata.Size), nil
}

func (d *Storage) BlockSize() int64 {
	return int64(d.source.Header().Metadata.BlockSize)
}

func (d *Storage) Slice(ctx context.Context, off, length int64) ([]byte, error) {
	return d.source.Slice(ctx, off, length)
}

// IsCached forwards to the underlying build.File so dedup best-effort can
// peek the chunker cache through this wrapper.
func (d *Storage) IsCached(ctx context.Context, off, length int64) bool {
	return d.source.IsCached(ctx, off, length)
}

func (d *Storage) PageResident(ctx context.Context, off int64) (resident, known bool) {
	return d.source.PageResident(ctx, off)
}

func (d *Storage) Header() *header.Header {
	return d.source.Header()
}

func (d *Storage) SwapHeader(h *header.Header) {
	d.source.SwapHeader(h)
}

func (d *Storage) SwapHeaderIfCurrent(old, next *header.Header) bool {
	return d.source.SwapHeaderIfCurrent(old, next)
}

func (d *Storage) SetDurableHeader(f *utils.SetOnce[*header.Header]) {
	d.source.SetDurableHeader(f)
}

func (d *Storage) DurableHeader(ctx context.Context) (*header.Header, error) {
	return d.source.DurableHeader(ctx)
}

func (d *Storage) DurableHeaderNow() (*header.Header, bool) {
	return d.source.DurableHeaderNow()
}

func (d *Storage) Close() error {
	return nil
}
