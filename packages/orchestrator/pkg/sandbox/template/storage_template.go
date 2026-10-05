//go:build linux

package template

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/scheduling"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

type storageTemplate struct {
	paths storage.CachePaths

	memfile  *utils.SetOnce[block.ReadonlyDevice]
	rootfs   *utils.SetOnce[block.ReadonlyDevice]
	snapfile *utils.SetOnce[File]
	metafile *utils.SetOnce[File]

	// memfileHeader is atomic because the footprint gauge reads it from the
	// metrics goroutine without a lock.
	memfileHeader atomic.Pointer[utils.SetOnce[*header.Header]]
	rootfsHeader  *utils.SetOnce[*header.Header]
	// durableMemfileHeader, when non-nil, is the header the memfile will settle
	// on (the deduped header while a provisional header is served); Fetch wires
	// it into the memfile device as its durable header before publishing the
	// device, so a pause parents off it rather than the provisional header.
	durableMemfileHeader *utils.SetOnce[*header.Header]
	// dropProvisionalHeader, set only on a template built from a provisional
	// memfile header, tells Fetch to clear the holder once it has read the
	// header.
	dropProvisionalHeader bool
	localSnapfile         File
	localMetafile         File

	metrics     blockmetrics.Metrics
	persistence storage.StorageProvider

	// kind is set before the template is admitted and never changes after.
	kind layerKind
	// layerMark is guarded by the cache's extendMu.
	layerMark layerMark

	closeOnce sync.Once
	closeErr  error
}

func newTemplateFromStorage(
	config cfg.BuilderConfig,
	buildId string,
	memfileHeader *utils.SetOnce[*header.Header],
	rootfsHeader *utils.SetOnce[*header.Header],
	persistence storage.StorageProvider,
	metrics blockmetrics.Metrics,
	localSnapfile File,
	localMetafile File,
	durableMemfileHeader *utils.SetOnce[*header.Header],
) (*storageTemplate, error) {
	paths, err := storage.Paths{
		BuildID: buildId,
	}.Cache(config.StorageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache paths: %w", err)
	}

	t := &storageTemplate{
		paths:                paths,
		localSnapfile:        localSnapfile,
		localMetafile:        localMetafile,
		rootfsHeader:         rootfsHeader,
		durableMemfileHeader: durableMemfileHeader,
		metrics:              metrics,
		persistence:          persistence,
		kind:                 layerKindFetched,
		memfile:              utils.NewSetOnce[block.ReadonlyDevice](),
		rootfs:               utils.NewSetOnce[block.ReadonlyDevice](),
		snapfile:             utils.NewSetOnce[File](),
		metafile:             utils.NewSetOnce[File](),
	}
	t.memfileHeader.Store(memfileHeader)

	return t, nil
}

func (t *storageTemplate) Fetch(ctx context.Context, buildStore *build.DiffStore) {
	ctx, span := tracer.Start(ctx, "fetch storage template", trace.WithAttributes(
		telemetry.WithBuildID(t.paths.BuildID),
	))
	defer span.End()

	var wg errgroup.Group

	wg.Go(func() error {
		if t.localSnapfile != nil {
			if err := t.snapfile.SetValue(t.localSnapfile); err != nil {
				return fmt.Errorf("failed to set local snapfile: %w", err)
			}

			return nil
		}

		snapfile, snapfileErr := newStorageFile(
			ctx,
			t.persistence,
			t.paths.Snapfile(),
			t.paths.CacheSnapfile(),
		)
		if snapfileErr != nil {
			errMsg := fmt.Errorf("failed to fetch snapfile: %w", snapfileErr)

			if err := t.snapfile.SetError(errMsg); err != nil {
				return fmt.Errorf("failed to set snapfile error: %w", errors.Join(errMsg, err))
			}

			return nil
		}

		if err := t.snapfile.SetValue(snapfile); err != nil {
			return fmt.Errorf("failed to set snapfile: %w", err)
		}

		return nil
	})

	wg.Go(func() error {
		if t.localMetafile != nil {
			if err := t.metafile.SetValue(t.localMetafile); err != nil {
				return fmt.Errorf("failed to set local metafile: %w", err)
			}

			return nil
		}

		meta, err := newStorageFile(
			ctx,
			t.persistence,
			t.paths.Metadata(),
			t.paths.CacheMetadata(),
		)
		if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
			sourceErr := fmt.Errorf("failed to fetch metafile: %w", err)
			if err := t.metafile.SetError(sourceErr); err != nil {
				return fmt.Errorf("failed to set metafile error: %w", errors.Join(sourceErr, err))
			}

			return nil
		}

		if err != nil {
			// If we can't find the metadata, we still want to return the metafile.
			// This is used for templates that don't have metadata, like v1 templates.
			logger.L().Info(ctx, "failed to fetch metafile, falling back to v1 template metadata",
				logger.WithBuildID(t.paths.BuildID),
				zap.Error(err),
			)
			oldTemplateMetadata := metadata.V1TemplateVersion()
			err := oldTemplateMetadata.ToFile(t.paths.CacheMetadata())
			if err != nil {
				sourceErr := fmt.Errorf("failed to write v1 template metadata to a file: %w", err)
				if err := t.metafile.SetError(sourceErr); err != nil {
					return fmt.Errorf("failed to set metafile error: %w", errors.Join(sourceErr, err))
				}

				return nil
			}

			if err := t.metafile.SetValue(&storageFile{
				path: t.paths.CacheMetadata(),
			}); err != nil {
				return fmt.Errorf("failed to set metafile v1: %w", err)
			}

			return nil
		}

		if err := t.metafile.SetValue(meta); err != nil {
			return fmt.Errorf("failed to set metafile value: %w", err)
		}

		return nil
	})

	wg.Go(func() error {
		holder := t.memfileHeader.Load()
		if holder == nil {
			// Only a Fetch after the one that dropped the holder gets here.
			errMsg := errors.New("memfile header holder already dropped")
			if err := t.memfile.SetError(errMsg); err != nil {
				return fmt.Errorf("failed to set memfile error: %w", errors.Join(errMsg, err))
			}

			return nil
		}
		memHdr, hdrErr := holder.WaitWithContext(ctx)
		// Before the device is published, so a caller that has the memfile
		// also observes the drop.
		t.dropProvisionalMemfileHeader(ctx, memHdr)
		if hdrErr != nil {
			errMsg := fmt.Errorf("failed to resolve memfile header: %w", hdrErr)
			if err := t.memfile.SetError(errMsg); err != nil {
				return fmt.Errorf("failed to set memfile error: %w", errors.Join(errMsg, err))
			}

			return nil
		}

		memfileStorage, memfileErr := NewStorage(
			ctx,
			buildStore,
			t.paths.BuildID,
			build.Memfile,
			memHdr,
			t.persistence,
			t.metrics,
		)

		if memfileErr != nil {
			errMsg := fmt.Errorf("failed to create memfile storage: %w", memfileErr)

			logger.L().Warn(ctx, "caching template with failed memfile resolution; reused until cache eviction",
				logger.WithBuildID(t.paths.BuildID),
				zap.Duration("min_cache_ttl", templateExpiration),
				zap.Error(memfileErr),
			)

			if err := t.memfile.SetError(errMsg); err != nil {
				return fmt.Errorf("failed to set memfile error: %w", errors.Join(errMsg, err))
			}

			return nil
		}

		// Wire the durable header before publishing the device (SetValue), so no
		// caller can observe the memfile with its durable header unset: a pause
		// then always parents off the deduped header rather than the provisional
		// one being served.
		if t.durableMemfileHeader != nil {
			memfileStorage.SetDurableHeader(t.durableMemfileHeader)
		}

		if err := t.memfile.SetValue(memfileStorage); err != nil {
			return fmt.Errorf("failed to set memfile value: %w", err)
		}

		return nil
	})

	wg.Go(func() error {
		rootHdr, hdrErr := t.rootfsHeader.WaitWithContext(ctx)
		if hdrErr != nil {
			errMsg := fmt.Errorf("failed to resolve rootfs header: %w", hdrErr)
			if err := t.rootfs.SetError(errMsg); err != nil {
				return fmt.Errorf("failed to set rootfs error: %w", errors.Join(errMsg, err))
			}

			return nil
		}

		rootfsStorage, rootfsErr := NewStorage(
			ctx,
			buildStore,
			t.paths.BuildID,
			build.Rootfs,
			rootHdr,
			t.persistence,
			t.metrics,
		)
		if rootfsErr != nil {
			errMsg := fmt.Errorf("failed to create rootfs storage: %w", rootfsErr)

			logger.L().Warn(ctx, "caching template with failed rootfs resolution; reused until cache eviction",
				logger.WithBuildID(t.paths.BuildID),
				zap.Duration("min_cache_ttl", templateExpiration),
				zap.Error(rootfsErr),
			)

			if err := t.rootfs.SetError(errMsg); err != nil {
				return fmt.Errorf("failed to set rootfs error: %w", errors.Join(errMsg, err))
			}

			return nil
		}

		if err := t.rootfs.SetValue(rootfsStorage); err != nil {
			return fmt.Errorf("failed to set rootfs value: %w", err)
		}

		return nil
	})

	err := wg.Wait()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		logger.L().Error(ctx, "failed to fetch template storage",
			logger.WithBuildID(t.paths.BuildID),
			zap.Error(err),
		)

		return
	}
}

// dropProvisionalMemfileHeader clears the holder of a provisional memfile
// header once Fetch has read the header h from it, and records the outcome.
// Fetch builds the memfile device from h, and the device keeps it until the
// deduped or the published header replaces it; headerFootprint is the
// holder's only other reader. Only AddSnapshot builds a template from a
// provisional header, and only it sets durableMemfileHeader.
func (t *storageTemplate) dropProvisionalMemfileHeader(ctx context.Context, h *header.Header) {
	if t.durableMemfileHeader == nil {
		deadStructureOutcomeMetric.Add(ctx, 1, attrTemplateProvisionalHeaderNone)

		return
	}

	var size int64
	if h != nil {
		size = int64(h.Mapping.ByteSize())
	}
	if !t.dropProvisionalHeader {
		deadStructureOutcomeMetric.Add(ctx, 1, attrTemplateProvisionalHeaderFlagOff)
		deadStructureBytesMetric.Add(ctx, size, attrTemplateProvisionalHeaderFlagOff)

		return
	}

	t.memfileHeader.Store(nil)
	deadStructureOutcomeMetric.Add(ctx, 1, attrTemplateProvisionalHeaderDropped)
	deadStructureBytesMetric.Add(ctx, size, attrTemplateProvisionalHeaderDropped)
}

// Close is idempotent and safe to call concurrently. The cache can reach one
// instance from more than one close path — a retired entry's last release and
// an eviction callback queued by Invalidate — so the teardown runs once and
// every caller gets its result.
func (t *storageTemplate) Close(ctx context.Context) error {
	t.closeOnce.Do(func() { t.closeErr = t.close(ctx) })

	return t.closeErr
}

func (t *storageTemplate) close(ctx context.Context) error {
	err := closeTemplate(ctx, t)

	// closeTemplate only removes the files it holds handles for, which leaves the
	// metafile and the directory itself behind; nothing else reclaims them, since
	// the startup sweep covers DefaultCacheDir and these live under
	// TemplateCacheDir. The directory is private to this instance — CachePaths
	// mints a fresh identifier per template — so removing it cannot touch another
	// instance's files.
	if pathsErr := t.paths.Close(); pathsErr != nil {
		err = errors.Join(err, fmt.Errorf("failed to remove template cache dir: %w", pathsErr))
	}

	return err
}

func (t *storageTemplate) Files() storage.CachePaths {
	return t.paths
}

// SchedulingMetadata reads the headers from the resolved memfile/rootfs devices
// rather than the header holders, which stay unset for templates loaded from
// storage (the headers are resolved internally by NewStorage during Fetch).
func (t *storageTemplate) SchedulingMetadata(ctx context.Context) *orchestrator.SchedulingMetadata {
	// The rootfs is always present; its header carries the build ID and is the
	// minimum needed for scheduling metadata.
	rootfs, rootfsErr := t.rootfs.WaitWithContext(ctx)
	if rootfsErr != nil {
		return nil
	}

	rh := rootfs.Header()
	if rh == nil || rh.Metadata == nil {
		return nil
	}

	// Filesystem-only snapshots have no memfile object, so memfile.WaitWithContext
	// errors on reload. Tolerate that and report rootfs-only scheduling metadata
	// (FromHeaders treats a nil memfile header as rootfs-only) instead of
	// dropping the rootfs affinity data too.
	var mh *header.Header
	if memfile, memfileErr := t.memfile.WaitWithContext(ctx); memfileErr == nil {
		// Use the durable (deduped) header, not the live one: during the
		// provisional window memfile.Header() maps dirty pages to a synthetic build
		// id that is never uploaded or registered, which would put a nonexistent
		// layer into scheduling metadata. Non-blocking: this runs on the
		// create/resume response path, so while a provisional swap is still pending
		// (deduped header not yet known) report rootfs-only metadata rather than
		// block the response for the whole dedup. No swap pending → live header.
		if dh, ok := memfile.(interface {
			DurableHeaderNow() (*header.Header, bool)
		}); ok {
			if h, ready := dh.DurableHeaderNow(); ready {
				mh = h
			}
		} else {
			mh = memfile.Header()
		}
	}

	return scheduling.FromHeaders(rh.Metadata.BuildId, mh, rh, 0)
}

// headerFootprint reports the mapping entry count and approximate heap bytes
// held by this template's resolved headers. It never blocks: the headers live
// on the memfile/rootfs devices (the header holders stay unset for templates
// loaded from storage — see SchedulingMetadata), and a device whose SetOnce has
// not resolved yet is skipped. The gauge therefore undercounts still-fetching
// templates rather than stalling the metrics callback on them.
func (t *storageTemplate) headerFootprint() (entries int, bytes int) {
	// Deduplicated by the mapping's backing storage rather than by header
	// identity, because one allocation reaches this function by two routes. The
	// holders below usually resolve to the very headers the devices carry; and
	// once a snapshot's upload publishes, the device carries a CloneForUpload
	// while the holder still carries the source — two distinct *Header sharing
	// one Mapping, since the clone copies the struct and the copy shares its
	// slices. Counting that allocation twice would overstate the number this
	// gauge exists to make trustworthy, and it would do so as uploads land,
	// which reads like retention growth rather than a counting artifact.
	seen := make(map[*header.Header]struct{}, 4)
	counted := make([]header.Mapping, 0, 4)

	add := func(h *header.Header) {
		if h == nil {
			return
		}
		if _, dup := seen[h]; dup {
			return
		}
		seen[h] = struct{}{}

		for _, m := range counted {
			if m.SharesStorageWith(h.Mapping) {
				return
			}
		}
		counted = append(counted, h.Mapping)

		entries += h.Mapping.Len()
		bytes += h.Mapping.ByteSize()
	}

	if dev, err := t.memfile.Result(); err == nil && dev != nil {
		add(dev.Header())
	}

	if dev, err := t.rootfs.Result(); err == nil && dev != nil {
		add(dev.Header())
	}

	// The holders are not always the headers the devices ended up on, and the
	// difference is retained memory. A template built from a provisional memfile
	// header keeps that header alive in memfileHeader after SwapHeaderIfCurrent
	// has moved the device on to the deduped one, unless Fetch dropped the
	// holder, so a paused-and-deduped template can hold two distinct mappings
	// while the device reports one. Count every distinct header the template
	// still references.
	for _, holder := range []*utils.SetOnce[*header.Header]{t.memfileHeader.Load(), t.rootfsHeader, t.durableMemfileHeader} {
		if holder == nil {
			continue
		}

		if h, err := holder.Result(); err == nil {
			add(h)
		}
	}

	return entries, bytes
}

// generation is the chain generation of the template's headers, read without
// blocking: from the memfile or rootfs device once resolved, else from the
// header it was built from, else 0 while none has resolved.
func (t *storageTemplate) generation() uint64 {
	var headers []*header.Header
	for _, s := range []*utils.SetOnce[block.ReadonlyDevice]{t.memfile, t.rootfs} {
		if dev, err := s.Result(); err == nil && dev != nil {
			headers = append(headers, dev.Header())
		}
	}
	for _, holder := range []*utils.SetOnce[*header.Header]{t.memfileHeader.Load(), t.rootfsHeader} {
		if holder == nil {
			continue
		}
		if h, err := holder.Result(); err == nil {
			headers = append(headers, h)
		}
	}

	for _, h := range headers {
		if h != nil && h.Metadata != nil {
			return h.Metadata.Generation
		}
	}

	return 0
}

func (t *storageTemplate) Memfile(ctx context.Context) (block.ReadonlyDevice, error) {
	_, span := tracer.Start(ctx, "storage-template-memfile")
	defer span.End()

	return t.memfile.Wait()
}

func (t *storageTemplate) Rootfs() (block.ReadonlyDevice, error) {
	return t.rootfs.Wait()
}

func (t *storageTemplate) Snapfile() (File, error) {
	return t.snapfile.Wait()
}

func (t *storageTemplate) Metadata() (metadata.Template, error) {
	metafile, err := t.metafile.Wait()
	if err != nil {
		return metadata.Template{}, fmt.Errorf("failed to get metafile: %w", err)
	}

	return metadata.FromFile(metafile.Path())
}

func (t *storageTemplate) UpdateMetadata(meta metadata.Template) error {
	metafile, err := t.metafile.Wait()
	if err != nil {
		return fmt.Errorf("failed to get metafile: %w", err)
	}

	return meta.ReplaceFile(metafile.Path())
}
