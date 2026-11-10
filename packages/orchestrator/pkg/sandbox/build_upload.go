//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	headers "github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	deadStructureOutcomeCounter = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorDeadStructureOutcomeCounterName))

	attrUploadProvisionalHeaderDropped = uploadProvisionalHeaderOutcomeAttr("dropped")
	attrUploadProvisionalHeaderFlagOff = uploadProvisionalHeaderOutcomeAttr("flag_off")
	attrUploadProvisionalHeaderNone    = uploadProvisionalHeaderOutcomeAttr("none")
)

func uploadProvisionalHeaderOutcomeAttr(outcome string) metric.MeasurementOption {
	return metric.WithAttributeSet(attribute.NewSet(
		attribute.String("structure", "upload_provisional_header"),
		attribute.String("outcome", outcome),
	))
}

type Upload struct {
	buildID        uuid.UUID
	snap           *Snapshot
	paths          storage.Paths
	uploads        *Uploads
	store          storage.StorageProvider
	mem            storage.CompressConfig
	root           storage.CompressConfig
	useCase        string
	objectMetadata storage.ObjectMetadata
	future         *utils.ErrorOnce
	useV4          bool
	headerVersion  uint64

	// finishTemplate reports how the upload ended and returns the template
	// cache pin that keeps the snapshot's local entry resident while the
	// upload reads it. finished makes the upload's terminal outcome, Finish or
	// Abandon, happen once.
	finishTemplate func(template.UploadOutcome)
	finished       atomic.Bool
}

func NewUpload(
	ctx context.Context,
	uploads *Uploads,
	snap *Snapshot,
	store storage.StorageProvider,
	cfg storage.CompressConfig,
	ff *featureflags.Client,
	useCase string,
	objectMetadata storage.ObjectMetadata,
	// finishTemplate returns the pin on the snapshot's template cache entry,
	// with how the upload ended. The upload owns it from here and calls it
	// once, at its terminal outcome — landed, failed or abandoned — or with
	// UploadAbandoned when NewUpload fails. nil means there is no pin.
	finishTemplate func(template.UploadOutcome),
) (_ *Upload, err error) {
	if finishTemplate == nil {
		finishTemplate = func(template.UploadOutcome) {}
	}
	defer func() {
		if err != nil {
			finishTemplate(template.UploadAbandoned)
		}
	}()

	// The provisional header only feeds AddSnapshot, which a caller that
	// caches the snapshot runs before this; AddSnapshot counts a caller that
	// does not. The upload never reads the header, but keeps the snapshot for
	// its whole retry budget. It gets no bytes value: the template's holder
	// carries the same allocation, so counting it here would count it twice.
	switch {
	case snap.MemorySnapshot.ProvisionalDiffHeader == nil:
		deadStructureOutcomeCounter.Add(ctx, 1, attrUploadProvisionalHeaderNone)
	case ff != nil && ff.BoolFlag(ctx, featureflags.SnapshotCacheDropProvisionalHeaderFlag):
		snap.MemorySnapshot.ProvisionalDiffHeader = nil
		deadStructureOutcomeCounter.Add(ctx, 1, attrUploadProvisionalHeaderDropped)
	default:
		deadStructureOutcomeCounter.Add(ctx, 1, attrUploadProvisionalHeaderFlagOff)
	}

	// Filesystem-only snapshots have no memfile (NoDiff, block size 0), so
	// resolving its compress config would fail validation ("block size must be
	// positive"). The memfile body and header are never uploaded anyway.
	var mem storage.CompressConfig
	var memV4 bool
	if !snap.FilesystemSnapshot {
		mem, memV4, err = resolveCompressConfig(ctx, cfg, ff, storage.MemfileName, snap.MemorySnapshot.BlockSize, useCase)
		if err != nil {
			return nil, fmt.Errorf("resolve memfile compress config: %w", err)
		}
	}
	root, rootV4, err := resolveCompressConfig(ctx, cfg, ff, storage.RootfsName, snap.RootfsBlockSize, useCase)
	if err != nil {
		return nil, fmt.Errorf("resolve rootfs compress config: %w", err)
	}

	if useCase != "" {
		ctx = featureflags.AddToContext(ctx, featureflags.CompressUseCaseContext(useCase))
	}
	headerVersion := uint64(headers.MetadataVersionV4)
	if ff != nil && ff.BoolFlag(ctx, featureflags.HeaderV5WriteFlag) {
		headerVersion = headers.MetadataVersionV5
	}

	u := &Upload{
		buildID:        snap.BuildID,
		snap:           snap,
		paths:          storage.Paths{BuildID: snap.BuildID.String()},
		uploads:        uploads,
		store:          store,
		mem:            mem,
		root:           root,
		useCase:        useCase,
		objectMetadata: objectMetadata,
		useV4:          memV4 || rootV4 || headerVersion == headers.MetadataVersionV5,
		headerVersion:  headerVersion,

		finishTemplate: finishTemplate,
	}

	if uploads != nil {
		fut, err := uploads.Start(snap.BuildID)
		if err != nil {
			return nil, err
		}
		u.future = fut
	}

	return u, nil
}

// layerSizeMetadata adds the layer's logical, mapped, and diff sizes (all
// uncompressed, from the diff header) to the base object metadata. They live on
// the data object because the memfile values depend on the async dedup header.
func (u *Upload) layerSizeMetadata(h *headers.Header) storage.ObjectMetadata {
	md := maps.Clone(u.objectMetadata)
	if md == nil {
		md = make(storage.ObjectMetadata)
	}
	if h == nil || h.Metadata == nil {
		return md
	}

	bytesByBuild := h.Mapping.BytesByBuild()
	var mapped uint64
	for _, b := range bytesByBuild {
		mapped += b
	}
	md[storage.ObjectMetadataLogicalSize] = strconv.FormatUint(h.Metadata.Size, 10)
	md[storage.ObjectMetadataMappedSize] = strconv.FormatUint(mapped, 10)
	md[storage.ObjectMetadataDiffSize] = strconv.FormatUint(bytesByBuild[h.Metadata.BuildId], 10)

	return md
}

func (u *Upload) Run(ctx context.Context) error {
	// Attach the upload use case so flag reads can target it (e.g. write-through only for builds).
	ctx = featureflags.AddToContext(ctx, featureflags.CompressUseCaseContext(u.useCase))

	if !u.mem.IsCompressionEnabled() && !u.root.IsCompressionEnabled() && !u.useV4 {
		return u.runV3(ctx)
	}

	return u.runV4(ctx)
}

// Wait blocks until the upload has reached its terminal outcome (the future set
// by Finish) or ctx is done, returning the upload error. It lets a caller order
// work after the snapshot has durably landed — e.g. re-uploading the metadata
// object without racing the upload's own metadata write.
func (u *Upload) Wait(ctx context.Context) error {
	if u.future == nil {
		return nil
	}

	return u.future.WaitWithContext(ctx)
}

// Finish signals the upload's terminal outcome. Same-orch waiters wake on the
// future; cross-orch waiters wake on the Redis hint published here. The
// template cache pin is returned either way, with the outcome: a failed upload
// leaves its layer marked unlanded, so the cache never releases the entry that
// is the only copy of the snapshot and it stays on its TTL, counted as
// unlanded if it is a pause layer. Only an upload whose snapshot the API
// already relies on ends this way; a caller whose snapshot nothing will read
// after a failure ends it with Abandon instead. Only the first of Finish and Abandon has an effect.
func (u *Upload) Finish(ctx context.Context, uploadErr error) {
	if !u.finished.CompareAndSwap(false, true) {
		return
	}

	u.signal(ctx, uploadErr)

	outcome := template.UploadLanded
	if uploadErr != nil {
		outcome = template.UploadFailed
	}
	u.finishTemplate(outcome)
}

// Abandon ends an upload whose snapshot nothing will read — one that will
// never run, or one that failed where the snapshot is discarded — failing its
// waiters with err and returning the template cache pin with the layer marked
// unlanded but not counted: nothing will read it. A no-op once Finish or
// Abandon has run.
func (u *Upload) Abandon(ctx context.Context, err error) {
	if !u.finished.CompareAndSwap(false, true) {
		return
	}

	u.signal(ctx, err)
	u.finishTemplate(template.UploadAbandoned)
}

func (u *Upload) signal(ctx context.Context, uploadErr error) {
	if u.future != nil {
		_ = u.future.SetError(uploadErr)
	}
	if u.uploads != nil {
		u.uploads.publishUploadDoneToRedis(ctx, u.buildID, uploadErr)
	}
}

// publish swaps a finalized header into the local cached device so peers and
// Wait()ers see the build as complete. ErrBuildNotInCache is the one acceptable
// failure mode: nothing was cached locally, nothing to swap.
func (u *Upload) publish(ctx context.Context, t build.DiffType, h *headers.Header) error {
	if u.uploads == nil {
		return nil
	}

	dev, release, err := u.uploads.find(ctx, u.buildID, t)
	defer release()
	if errors.Is(err, ErrBuildNotInCache) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load %s for swap: %w", t, err)
	}

	dev.SwapHeader(h)

	return nil
}

// resolveCompressConfig returns the effective compression config for a given
// file type and use case, plus whether the V4 header layout should be used for
// an uncompressed upload. Feature flags override the base config when active.
// Returns zero-value CompressConfig when compression is disabled. fileType,
// useCase are added to the LD evaluation context; blockSize constrains legal
// frame sizes — see validateCompressConfig.
func resolveCompressConfig(ctx context.Context, base storage.CompressConfig, ff *featureflags.Client, fileType string, blockSize uint64, useCase string) (storage.CompressConfig, bool, error) {
	resolved := base
	var useV4 bool

	if ff != nil {
		var extra []ldcontext.Context
		if fileType != "" {
			extra = append(extra, featureflags.CompressFileTypeContext(fileType))
		}
		if useCase != "" {
			extra = append(extra, featureflags.CompressUseCaseContext(useCase))
		}
		ctx = featureflags.AddToContext(ctx, extra...)

		useV4 = ff.BoolFlag(ctx, featureflags.V4HeaderForUncompressedFlag)

		v := ff.JSONFlag(ctx, featureflags.CompressConfigFlag).AsValueMap()
		if v.Get("compressBuilds").BoolValue() {
			ct := v.Get("compressionType").StringValue()
			ldCfg := storage.CompressConfig{
				Enabled:            true,
				Type:               ct,
				Level:              v.Get("compressionLevel").IntValue(),
				FrameSizeKB:        v.Get("frameSizeKB").IntValue(),
				MinPartSizeMB:      v.Get("minPartSizeMB").IntValue(),
				FrameEncodeWorkers: v.Get("frameEncodeWorkers").IntValue(),
				EncoderConcurrency: v.Get("encoderConcurrency").IntValue(),
			}
			if ldCfg.CompressionType() != storage.CompressionNone {
				resolved = ldCfg
			}
		}
	}

	if !resolved.IsCompressionEnabled() {
		return storage.CompressConfig{}, useV4, nil
	}

	if err := validateCompressConfig(resolved, blockSize); err != nil {
		return storage.CompressConfig{}, false, err
	}

	return resolved, useV4, nil
}

// validateCompressConfig checks that the resolved config is internally
// consistent for the given block size. Frame size must be a positive multiple
// of blockSize so that every block-sized read served by the chunker lies
// inside one frame — otherwise Chunker.fetch fetches only the start frame and
// cache.sliceDirect returns uninitialized mmap bytes for the tail.
func validateCompressConfig(c storage.CompressConfig, blockSize uint64) error {
	fs := c.FrameSize()
	if fs <= 0 {
		return fmt.Errorf("frame size must be positive, got %d KB", c.FrameSizeKB)
	}
	if blockSize == 0 {
		return errors.New("block size must be positive")
	}
	if uint64(fs)%blockSize != 0 {
		return fmt.Errorf("frame size (%d) must be a multiple of block size (%d)", fs, blockSize)
	}

	return nil
}
