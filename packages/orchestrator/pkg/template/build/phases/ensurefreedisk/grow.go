//go:build linux

package ensurefreedisk

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/filesystem"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
)

const offlineCleanupTimeout = time.Minute

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases/ensurefreedisk")

type growResult struct {
	freeBefore int64
	freeAfter  int64
	target     int64
}

func (b *EnsureFreeDiskBuilder) growAndExport(
	ctx context.Context,
	sourceRootfs block.ReadonlyDevice,
	buildID uuid.UUID,
	targetFreeMB int64,
) (build.Diff, *header.Header, growResult, error) {
	if targetFreeMB < 0 || targetFreeMB > math.MaxInt64>>units.MBShift {
		return nil, nil, growResult{}, fmt.Errorf("disk target overflows: %d MiB", targetFreeMB)
	}
	target := units.MBToBytes(targetFreeMB)

	// Storage.Size and BlockSize read header metadata directly, so validate the
	// pointers before calling either method.
	sourceHeader := sourceRootfs.Header()
	if sourceHeader == nil || sourceHeader.Metadata == nil {
		return nil, nil, growResult{}, errors.New("source rootfs header metadata is missing")
	}

	// Validate the source device before exposing it to host filesystem tools.
	sourceSize, err := sourceRootfs.Size(ctx)
	if err != nil {
		return nil, nil, growResult{}, fmt.Errorf("get source rootfs size: %w", err)
	}
	blockSize := sourceRootfs.BlockSize()
	if err := validateSourceGeometry(
		sourceSize,
		blockSize,
		sourceHeader.Metadata.Size,
		sourceHeader.Metadata.BlockSize,
	); err != nil {
		return nil, nil, growResult{}, err
	}

	// Measure the latest durable ext4 state without modifying the source layer.
	freeBefore, err := b.measureFree(ctx, sourceRootfs, sourceSize, blockSize, buildID)
	if err != nil {
		return nil, nil, growResult{}, err
	}
	result := growResult{freeBefore: freeBefore, freeAfter: freeBefore, target: target}
	if freeBefore >= target {
		// The filesystem already satisfies the request, but this phase still needs
		// its own cacheable artifact. Otherwise a forced rebuild would execute the
		// phase and then have nothing new to publish under the ensure-layer hash.
		//
		// This is an empty-diff header, not an empty rootfs. ToDiffHeader carries
		// forward every mapping from the source header while advancing it to a new
		// generation and build ID. Paired with NoDiff, the upload stores this header
		// and the metadata without uploading rootfs data that did not change.
		noChangeHeader, err := header.NewDiffMetadata(blockSize, nil, nil).ToDiffHeader(ctx, sourceHeader, buildID)
		if err != nil {
			return nil, nil, growResult{}, fmt.Errorf("build unchanged rootfs header: %w", err)
		}
		if err := noChangeHeader.Mapping.Validate(noChangeHeader.Metadata.Size, header.PageSize); err != nil {
			return nil, nil, growResult{}, fmt.Errorf("validate unchanged rootfs mapping: %w", err)
		}

		return &build.NoDiff{}, noChangeHeader, result, nil
	}

	// Grow once by the measured deficit plus the ext4 overhead allowance, rounded up to a whole MiB.
	newSize, err := computeGrownSize(sourceSize, target, freeBefore)
	if err != nil {
		return nil, nil, growResult{}, err
	}
	// Resize in a separate COW overlay and export only the changed blocks.
	diff, grownHeader, freeAfter, err := b.resizeAndExport(
		ctx, sourceRootfs, sourceHeader, buildID, sourceSize, newSize,
	)
	if err != nil {
		return nil, nil, growResult{}, err
	}
	result.freeAfter = freeAfter

	return diff, grownHeader, result, nil
}

func (b *EnsureFreeDiskBuilder) measureFree(
	ctx context.Context,
	source block.ReadonlyDevice,
	size, blockSize int64,
	buildID uuid.UUID,
) (free int64, e error) {
	// The phase mounts twice, here and in resizeOffline, and the two fail
	// differently: this one holds a device across a journal replay and a
	// debugfs read of the free-space counters. Without a span of its own its
	// teardown nests straight under the phase, which is also resizeOffline's
	// ancestor, so a trace cannot say which of the two mounts stalled.
	ctx, span := tracer.Start(ctx, "measure-free")
	defer span.End()

	cache, err := block.NewCache(size, blockSize, filepath.Join(b.BuilderConfig.DefaultCacheDir, resizeDiskName+"-"+buildID.String()+"-measure.cache"), false)
	if err != nil {
		return 0, fmt.Errorf("create measurement cache: %w", err)
	}
	defer func() { e = errors.Join(e, cache.Close()) }()

	device, err := b.openOfflineDevice(ctx, block.NewOverlay(source, cache), buildID)
	if err != nil {
		return 0, err
	}
	defer func() { e = errors.Join(e, device.close(ctx)) }()

	if _, err := filesystem.ReplayJournal(ctx, device.path); err != nil {
		return 0, fmt.Errorf("replay source journal: %w", err)
	}
	// Make the recovered block-group metadata visible from the backend before
	// debugfs reopens the device and reads its free-space counters.
	if err := device.mnt.Flush(ctx); err != nil {
		return 0, fmt.Errorf("flush recovered source journal: %w", err)
	}
	free, err = filesystem.GetFreeSpace(ctx, device.path, blockSize)
	if err != nil {
		return 0, fmt.Errorf("measure source free space: %w", err)
	}

	return free, nil
}

func (b *EnsureFreeDiskBuilder) resizeAndExport(
	ctx context.Context,
	source block.ReadonlyDevice,
	sourceHeader *header.Header,
	buildID uuid.UUID,
	sourceSize, newSize int64,
) (d build.Diff, h *header.Header, freeAfter int64, e error) {
	blockSize := source.BlockSize()
	// Use a sparse writable overlay at the final size; the source stays immutable.
	cache, err := block.NewCache(newSize, blockSize, filepath.Join(b.BuilderConfig.DefaultCacheDir, resizeDiskName+"-"+buildID.String()+"-grow.cache"), false)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("create grow cache: %w", err)
	}
	cacheOpen := true
	defer func() {
		if cacheOpen {
			e = errors.Join(e, cache.Close())
		}
	}()

	// Represent the new device tail as zeroes before ext4 grows into it.
	if _, err := cache.WriteZeroesAt(sourceSize, newSize-sourceSize); err != nil {
		return nil, nil, 0, fmt.Errorf("zero-fill grown tail: %w", err)
	}

	freeAfter, err = b.resizeOffline(ctx, block.NewOverlay(source, cache), buildID, newSize)
	if err != nil {
		return nil, nil, 0, err
	}

	// Serialize only changed extents and compose them over the source header.
	diffFile, err := build.NewLocalDiffFile(b.BuilderConfig.DefaultCacheDir, buildID.String(), build.Rootfs)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("create rootfs diff file: %w", err)
	}
	diffFileOwned := true
	defer func() {
		if diffFileOwned {
			e = errors.Join(e, cleanupDiffFile(diffFile))
		}
	}()

	diffMetadata, err := cache.ExportToDiff(ctx, diffFile.File)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("export grown overlay: %w", err)
	}
	grownHeader, err := diffMetadata.ToResizedDiffHeader(ctx, sourceHeader, buildID, uint64(newSize))
	if err != nil {
		return nil, nil, 0, fmt.Errorf("build grown rootfs header: %w", err)
	}

	// Remove the temporary overlay before handing the finalized diff to the caller.
	cacheOpen = false
	if err := cache.Close(); err != nil {
		return nil, nil, 0, fmt.Errorf("close grow cache: %w", err)
	}
	rootfsDiff, err := diffFile.CloseToDiff(blockSize)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("convert rootfs diff file: %w", err)
	}
	diffFileOwned = false

	return rootfsDiff, grownHeader, freeAfter, nil
}

// resizeOffline owns the NBD connection for the complete filesystem operation.
// Its deferred close finishes before this function returns, so the caller cannot
// export the backing cache while the NBD server may still be using it. A close
// failure is joined into the return error and therefore also prevents export.
func (b *EnsureFreeDiskBuilder) resizeOffline(
	ctx context.Context,
	backend block.Device,
	layerBuildID uuid.UUID,
	newSize int64,
) (freeAfter int64, e error) {
	// Group the two e2fsck runs and the resize2fs so the grow target and result
	// are visible without drilling into individual e2fsprogs spans.
	ctx, span := tracer.Start(ctx, "resize-offline")
	span.SetAttributes(attribute.Int64("template.resize_disk.new_size_bytes", newSize))
	defer func() {
		if e != nil {
			span.RecordError(e)
			span.SetStatus(codes.Error, e.Error())
		}
		span.End()
	}()

	// Expose the detached overlay as a normal host block device for e2fsprogs.
	device, err := b.openOfflineDevice(ctx, backend, layerBuildID)
	if err != nil {
		return 0, err
	}
	defer func() { e = errors.Join(e, device.close(ctx)) }()

	// Repair before resizing, then verify the enlarged filesystem.
	if _, err := filesystem.CheckIntegrity(ctx, device.path, true); err != nil {
		return 0, fmt.Errorf("pre-resize e2fsck: %w", err)
	}
	if _, err := filesystem.Resize(ctx, device.path, newSize); err != nil {
		return 0, fmt.Errorf("resize rootfs: %w", err)
	}
	if _, err := filesystem.CheckIntegrity(ctx, device.path, true); err != nil {
		return 0, fmt.Errorf("post-resize e2fsck: %w", err)
	}

	// Measure and flush the final state before the deferred close detaches it.
	freeAfter, err = filesystem.GetFreeSpace(ctx, device.path, backend.BlockSize())
	if err != nil {
		return 0, fmt.Errorf("measure grown free space: %w", err)
	}
	span.SetAttributes(attribute.Int64("template.resize_disk.free_after_bytes", freeAfter))
	if err := device.mnt.Flush(ctx); err != nil {
		return 0, fmt.Errorf("flush grown device: %w", err)
	}

	return freeAfter, nil
}

type offlineDevice struct {
	mnt  *nbd.DirectPathMount
	path string
}

// offlineDeviceRuntime is the identity the phase's mounts log under. The phase
// runs no sandbox, so they are attributed to the build being built — the one
// every build sandbox reports, not the layer artifact this phase produces.
func (b *EnsureFreeDiskBuilder) offlineDeviceRuntime() sandboxtypes.RuntimeMetadata {
	return sandboxtypes.RuntimeMetadata{
		TemplateID: b.Config.TemplateID,
		TeamID:     b.Config.TeamID,
		BuildID:    b.Template.BuildID,
	}
}

func (b *EnsureFreeDiskBuilder) openOfflineDevice(ctx context.Context, backend block.Device, layerBuildID uuid.UUID) (*offlineDevice, error) {
	lg := b.offlineDeviceRuntime().Logger().
		With(zap.String("layer_build_id", layerBuildID.String()))

	mnt := b.sandboxFactory.NewDirectPathMount(backend, lg)
	idx, err := mnt.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open NBD device: %w", err)
	}

	return &offlineDevice{mnt: mnt, path: nbd.GetDevicePath(idx)}, nil
}

func (d *offlineDevice) close(ctx context.Context) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), offlineCleanupTimeout)
	defer cancel()

	return d.mnt.Close(cleanupCtx)
}

func cleanupDiffFile(f *build.LocalDiffFile) error {
	closeErr := f.File.Close()
	if errors.Is(closeErr, os.ErrClosed) {
		closeErr = nil
	}
	removeErr := os.Remove(f.Name())
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}

	return errors.Join(closeErr, removeErr)
}
