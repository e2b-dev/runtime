//go:build linux

// Package ensurefreedisk grows a quiescent rootfs after user steps and before
// finalize cold-boots it.
package ensurefreedisk

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/layer"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/storage/cache"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
)

const (
	resizeDiskName    = "resize-disk"
	resizeDiskTimeout = time.Hour
)

type EnsureFreeDiskBuilder struct {
	buildcontext.BuildContext

	sandboxFactory *sandbox.Factory
	layerExecutor  *layer.LayerExecutor
	templateCache  *sbxtemplate.Cache
	index          cache.Index
}

func New(
	buildContext buildcontext.BuildContext,
	sandboxFactory *sandbox.Factory,
	layerExecutor *layer.LayerExecutor,
	templateCache *sbxtemplate.Cache,
	index cache.Index,
) *EnsureFreeDiskBuilder {
	return &EnsureFreeDiskBuilder{
		BuildContext:   buildContext,
		sandboxFactory: sandboxFactory,
		layerExecutor:  layerExecutor,
		templateCache:  templateCache,
		index:          index,
	}
}

func (b *EnsureFreeDiskBuilder) Prefix() string {
	return resizeDiskName
}

func (b *EnsureFreeDiskBuilder) String(context.Context) (string, error) {
	return "Resizing disk", nil
}

func (b *EnsureFreeDiskBuilder) Metadata() phases.PhaseMeta {
	return phases.PhaseMeta{
		Phase:    metrics.PhaseResizeDisk,
		StepType: resizeDiskName,
	}
}

func (b *EnsureFreeDiskBuilder) Hash(_ context.Context, sourceLayer phases.LayerResult) (string, error) {
	return cache.HashKeys(
		sourceLayer.Hash,
		resizeDiskName,
		strconv.FormatInt(b.Config.FreeDiskSizeMB, 10),
	), nil
}

func (b *EnsureFreeDiskBuilder) Layer(
	ctx context.Context,
	sourceLayer phases.LayerResult,
	hash string,
) (phases.LayerResult, error) {
	finalMetadata := sourceLayer.Metadata
	finalMetadata.Template = metadata.TemplateMetadata{
		BuildID:            uuid.NewString(),
		KernelVersion:      b.Config.KernelVersion,
		FirecrackerVersion: b.Config.FirecrackerVersion,
	}

	notCachedResult := phases.LayerResult{
		Metadata: finalMetadata,
		Cached:   false,
		Hash:     hash,
	}

	// A forced or freshly-built source must flow through this phase instead of
	// reconnecting the build to an older ensure artifact with the same recipe hash.
	if !sourceLayer.Cached || (b.Config.Force != nil && *b.Config.Force) {
		return notCachedResult, nil
	}

	layerMeta, err := b.index.LayerMetaFromHash(ctx, hash)
	if err != nil {
		logger.L().Info(ctx, "resize-disk layer unavailable in cache, building it",
			zap.Error(err),
			zap.String("hash", hash),
		)

		return notCachedResult, nil //nolint:nilerr // A failed hash lookup is a cache miss; rebuild and refresh it.
	}

	meta, err := b.index.Cached(ctx, layerMeta.Template.BuildID)
	if err != nil {
		logger.L().Info(ctx, "resize-disk artifact unavailable in cache, building it",
			zap.Error(err),
			zap.String("hash", hash),
			zap.String("build_id", layerMeta.Template.BuildID),
		)

		return notCachedResult, nil //nolint:nilerr // An unavailable cached artifact must be rebuilt.
	}

	return phases.LayerResult{
		Metadata: meta,
		Cached:   true,
		Hash:     hash,
	}, nil
}

func (b *EnsureFreeDiskBuilder) Build(
	ctx context.Context,
	userLogger logger.Logger,
	_ string,
	sourceLayer phases.LayerResult,
	currentLayer phases.LayerResult,
) (phases.LayerResult, error) {
	ctx, cancel := context.WithTimeout(ctx, resizeDiskTimeout)
	defer cancel()

	// Load the last user-step rootfs as the immutable parent of the resized layer.
	sourceTemplate, releaseTemplate, err := b.templateCache.GetTemplatePinned(ctx, sourceLayer.Metadata.Template.BuildID, false, true)
	defer releaseTemplate()
	if err != nil {
		return phases.LayerResult{}, fmt.Errorf("get source template: %w", err)
	}

	sourceRootfs, err := sourceTemplate.Rootfs()
	if err != nil {
		return phases.LayerResult{}, fmt.Errorf("get source rootfs: %w", err)
	}

	layerBuildID, err := uuid.Parse(currentLayer.Metadata.Template.BuildID)
	if err != nil {
		return phases.LayerResult{}, fmt.Errorf("parse resize-disk layer build id: %w", err)
	}

	// Measure the source and produce either an empty or grown rootfs layer.
	rootfsDiff, rootfsHeader, res, err := b.growAndExport(ctx, sourceRootfs, layerBuildID, b.Config.FreeDiskSizeMB)
	if err != nil {
		return phases.LayerResult{}, fmt.Errorf("resize disk: %w", err)
	}
	telemetry.SetAttributes(ctx,
		attribute.Int64("template.resize_disk.requested_free_bytes", res.target),
		attribute.Int64("template.resize_disk.free_before_bytes", res.freeBefore),
		attribute.Int64("template.resize_disk.free_after_bytes", res.freeAfter),
	)

	// Keep ownership until it is transferred to the snapshot below.
	defer func() {
		if rootfsDiff != nil {
			if err := rootfsDiff.Close(); err != nil {
				logger.L().Warn(ctx, "failed to close unowned resize-disk diff", zap.Error(err))
			}
		}
	}()

	if res.freeBefore >= res.target {
		userLogger.Info(ctx, fmt.Sprintf(
			"resize-disk: %d MiB free already >= target %d MiB, no resize needed",
			units.BytesToMB(res.freeBefore), units.BytesToMB(res.target),
		))
	} else {
		if res.freeAfter < res.target {
			// ext4 block-group metadata may consume part of the added capacity. Exact
			// top-up is intentionally deferred; publish the best-effort result.
			logger.L().Warn(ctx, "resize-disk target undershot after ext4 resize",
				zap.Int64("requested_free_bytes", res.target),
				zap.Int64("free_before_bytes", res.freeBefore),
				zap.Int64("free_after_bytes", res.freeAfter),
			)
		}

		userLogger.Info(ctx, fmt.Sprintf(
			"resize-disk: free %d -> %d MiB",
			units.BytesToMB(res.freeBefore), units.BytesToMB(res.freeAfter),
		))
	}

	// The VM-less layer has no matching memory snapshot, so finalize must cold-boot it.
	meta := currentLayer.Metadata.MarkFilesystemOnly(true)

	cachePaths, err := storage.Paths{BuildID: layerBuildID.String()}.Cache(b.BuilderConfig.StorageConfig)
	if err != nil {
		return phases.LayerResult{}, fmt.Errorf("create cache paths: %w", err)
	}
	metafile := sbxtemplate.NewLocalFileLink(cachePaths.CacheMetadata())
	defer func() {
		if metafile != nil {
			if err := metafile.Close(); err != nil {
				logger.L().Warn(ctx, "failed to close unowned resize-disk metadata", zap.Error(err))
			}
		}
	}()
	if err := meta.ToFile(metafile.Path()); err != nil {
		return phases.LayerResult{}, fmt.Errorf("write resize-disk layer metadata: %w", err)
	}

	// Bundle the rootfs layer and metadata, transferring their cleanup ownership.
	snapshot := sandbox.NewFilesystemOnlySnapshot(
		ctx,
		layerBuildID,
		rootfsDiff,
		rootfsHeader,
		metafile,
	)
	rootfsDiff = nil
	metafile = nil

	// Publish resized and no-op results through the same normal layer path. For a
	// no-op, NoDiff skips the rootfs data body while the new header and metadata
	// still refresh the ensure hash mapping, including after a forced rebuild.
	if err := b.layerExecutor.UploadSnapshot(
		ctx,
		userLogger,
		snapshot,
		currentLayer.Hash,
		meta,
		storage.ObjectOriginTemplateBuildCache,
	); err != nil {
		return phases.LayerResult{}, fmt.Errorf("persist resized rootfs layer: %w", err)
	}

	return phases.LayerResult{
		Metadata: meta,
		Cached:   sourceLayer.Cached,
		Hash:     currentLayer.Hash,
	}, nil
}
