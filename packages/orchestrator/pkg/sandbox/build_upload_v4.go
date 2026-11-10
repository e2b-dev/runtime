//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	headers "github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

func (u *Upload) runV4(ctx context.Context) error {
	memSrc, err := u.snap.MemorySnapshot.Diff.CachePath(ctx)
	if err != nil {
		return fmt.Errorf("memfile diff path: %w", err)
	}

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		h, err := u.snap.MemorySnapshot.DiffHeader.WaitWithContext(ctx)
		if err != nil {
			return fmt.Errorf("wait memfile diff header: %w", err)
		}
		if h == nil {
			return nil
		}

		return u.uploadFramed(ctx, build.Memfile, memSrc, h, u.mem)
	})

	eg.Go(func() error {
		// Resolve the rootfs diff path inside the group: with deferred rootfs
		// export it blocks on the background seal, so doing it here lets the
		// memfile/snapfile/metadata uploads overlap the reflink instead of
		// waiting behind it.
		rootfsSrc, err := u.snap.RootfsDiff.CachePath(ctx)
		if err != nil {
			return fmt.Errorf("rootfs diff path: %w", err)
		}

		h, err := u.snap.RootfsDiffHeader.WaitWithContext(ctx)
		if err != nil {
			return fmt.Errorf("wait rootfs diff header: %w", err)
		}
		if h == nil {
			return nil
		}

		return u.uploadFramed(ctx, build.Rootfs, rootfsSrc, h, u.root)
	})

	meta := storage.WithMetadata(u.objectMetadata)

	eg.Go(func() error {
		// Filesystem-only snapshots resume by reboot, not snapfile restore, so
		// the snapfile (created only for its disk-flush side effect) is not uploaded.
		if u.snap.FilesystemSnapshot {
			return nil
		}

		return uploadBlobWithMetrics(ctx, u.store, u.paths.Snapfile(), u.snap.Snapfile.Path(), uploadFileSnap, meta)
	})

	eg.Go(func() error {
		return uploadBlobWithMetrics(ctx, u.store, u.paths.Metadata(), u.snap.Metafile.Path(), uploadFileMeta, meta)
	})

	return eg.Wait()
}

func (u *Upload) uploadFramed(
	ctx context.Context,
	fileType build.DiffType,
	srcPath string,
	srcHeader *headers.Header,
	cfg storage.CompressConfig,
) error {
	var selfBuild headers.BuildData

	if srcPath != "" {
		fullFT, checksum, err := storage.UploadFramed(ctx, u.store, u.paths.DataFile(string(fileType), cfg.CompressionType()), srcPath, storage.WithCompressConfig(cfg), storage.WithMetadata(u.layerSizeMetadata(srcHeader)), storage.WithChecksumSHA256())
		if err != nil {
			return fmt.Errorf("%s upload: %w", fileType, err)
		}

		// Compressed: frame-table byte count, since sparse memfile diffs stream
		// fewer bytes than they occupy on disk. Uncompressed has no table.
		ft := fullFT.Table()
		size := ft.UncompressedSize()
		compressedSize := ft.CompressedSize()
		if !ft.IsCompressed() {
			info, statErr := os.Stat(srcPath)
			if statErr != nil {
				return fmt.Errorf("%s stat: %w", fileType, statErr)
			}
			size = info.Size()
			compressedSize = size
		}

		dataFileType := uploadFileMemfile
		if fileType == build.Rootfs {
			dataFileType = uploadFileRootfs
		}
		recordUploadCompression(ctx, dataFileType, cfg, size, compressedSize)
		selfBuild = headers.BuildData{Size: size, Checksum: checksum, FrameData: ft}
	}

	h := srcHeader.CloneForUpload(u.headerVersion)
	h.IncompletePendingUpload = false
	if h.Builds == nil {
		h.Builds = make(map[uuid.UUID]headers.BuildData)
	}

	if err := u.appendAncestorBuilds(ctx, h.Builds, srcHeader.Mapping, fileType); err != nil {
		return err
	}
	h.Builds[u.buildID] = selfBuild

	headerFileType := uploadFileMemfileHeader
	if fileType == build.Rootfs {
		headerFileType = uploadFileRootfsHeader
	}
	if err := storeHeaderWithMetrics(ctx, u.store, u.paths.HeaderFile(string(fileType)), headerFileType, h, storage.WithMetadata(u.objectMetadata)); err != nil {
		return fmt.Errorf("store %s header: %w", fileType, err)
	}

	return u.publish(ctx, fileType, h)
}

// ancestorResolution says what appendAncestorBuilds left in the child
// header's Builds map for one ancestor.
type ancestorResolution string

const (
	// resolutionOverwrite: Wait's header supplied the entry.
	resolutionOverwrite ancestorResolution = "overwrite"
	// resolutionInherited: the child already carried the entry and kept it.
	resolutionInherited ancestorResolution = "inherited"
	// resolutionStorageHeal: the entry was copied from the ancestor's header
	// as loaded from storage, which includes LoadHeader's backfill on every
	// verdict whose heal keeps it.
	resolutionStorageHeal ancestorResolution = "storage_heal"
	// resolutionLegacy: the ancestor's header predates the Builds map, so the
	// empty sentinel entry was written.
	resolutionLegacy ancestorResolution = "legacy"
	// resolutionSelfEntryAbsent: a heal that skipped the backfill found no
	// entry for the ancestor in its stored header and left the child without
	// one, as the header the ancestor's upload published does.
	resolutionSelfEntryAbsent ancestorResolution = "self_entry_absent"
	// resolutionAbsent: the child carries no entry — none was found, or a heal
	// failed without failing the upload.
	resolutionAbsent ancestorResolution = "absent"
	// resolutionNone: nothing was resolved into a Builds map — the walk had no
	// map to fill, or it failed the upload.
	resolutionNone ancestorResolution = "none"
)

// appendAncestorBuilds waits on every unique buildID referenced by mappings
// (excluding self) — gating publish on parents' header finalization — and,
// when dst is non-nil, writes the freshest BuildData into it. An existing dst
// entry is overwritten when Wait returns a header that carries one (Wait is
// more authoritative than CloneForUpload) or that predates the Builds map, and
// kept otherwise.
//
// V3 ancestors carry no Builds map, so a sentinel empty BuildData{} is
// written — the entry's presence alone is what matters: GetBuildFrameData
// returns UncompressedFrameTable (nil FrameData → sentinel), and createDiff's
// hasEntry branch handles size=0 by falling back to upstream.Size. We avoid
// computing the diff size here on purpose: it's not in Metadata.Size (that's
// the virtual size), and asking storage at upload time across a long
// ancestor chain would multiply roundtrips. The fallback amortizes into the
// read that's about to happen anyway.
//
// V3 callers pass dst=nil — they need the barrier but have no Builds map.
//
// When Wait returns nil for a build dst still lacks (a gap inherited from the
// source header), the entry is recovered from the build's own stored header so
// the gap stops propagating to descendant headers; builds with no header file
// (legacy uncompressed) stay absent, resolved by the read path. The heal is
// best-effort: a failed load leaves the gap rather than failing the upload,
// unless the context is already done.
// After verdictFutureNoEntry, and on every heal of a build this node's
// template cache released, the heal reads the stored header without the
// uncompressed-build backfill, so a V4+ ancestor whose stored header carries
// no entry for itself stays absent, as the header its upload published
// leaves it. Every other heal keeps LoadHeader's backfill. The heal reads no
// flag: the release's own record decides, so the bytes do not depend on how the
// flags read in this upload's context.
//
// Every ancestor is counted once per walk on the ancestor-resolutions
// counter, by verdict and by what landed in dst, including the ancestor
// whose failure ends the walk. The verdict is Wait's, except that a walk that
// fails the upload counts as verdictError with resolutionNone, also when Wait
// succeeded and the heal after it failed on a done context.
//
// An ancestor's header comes from its local cache entry when one exists and
// has finished its upload; otherwise from storage, by a poll while a peer or
// the pending local entry finishes, or by one read for a heal the child needs.
// Sequential — the critical path is the slowest pending Wait either way.
func (u *Upload) appendAncestorBuilds(
	ctx context.Context,
	dst map[uuid.UUID]headers.BuildData,
	mappings headers.Mapping,
	fileType build.DiffType,
) error {
	if u.uploads == nil {
		return nil
	}

	// Mapping.Builds() is already deduplicated, so no local seen-set is needed.
	for _, buildID := range mappings.Builds() {
		if buildID == u.buildID || buildID == uuid.Nil {
			continue
		}

		verdict, resolution, err := u.resolveAncestor(ctx, dst, buildID, fileType)
		recordAncestorResolution(ctx, verdict, resolution)
		if err != nil {
			return err
		}
	}

	return nil
}

func (u *Upload) resolveAncestor(
	ctx context.Context,
	dst map[uuid.UUID]headers.BuildData,
	buildID uuid.UUID,
	fileType build.DiffType,
) (AncestorVerdict, ancestorResolution, error) {
	h, verdict, released, err := u.uploads.wait(ctx, buildID, fileType)
	if err != nil {
		return verdict, resolutionNone, fmt.Errorf("wait for ancestor %s/%s: %w", buildID, fileType, err)
	}
	if dst == nil {
		return verdict, resolutionNone, nil
	}

	healed, noBackfill := false, false
	if h == nil {
		if _, ok := dst[buildID]; ok {
			return verdict, resolutionInherited, nil
		}
		// A build this node released heals as its resident entry would have.
		noBackfill = released || verdict == verdictFutureNoEntry
		h, _, err = loadAncestorHeader(ctx, u.store, storage.Paths{BuildID: buildID.String()}.HeaderFile(string(fileType)), noBackfill)
		if errors.Is(err, storage.ErrObjectNotExist) {
			return verdict, resolutionAbsent, nil
		}
		if err != nil {
			// createDiff resolves an absent entry on its own, so don't fail
			// a snapshot over the heal — unless the context is already done.
			if ctx.Err() != nil {
				return verdictError, resolutionNone, fmt.Errorf("recover ancestor %s/%s build data: %w", buildID, fileType, err)
			}
			logger.L().Warn(ctx, "ancestor build data recovery failed, persisting header with the gap",
				logger.WithBuildID(buildID.String()),
				zap.String("file_type", string(fileType)),
				zap.Error(err),
			)

			return verdict, resolutionAbsent, nil
		}
		healed = true
	}

	if bd, ok := h.Builds[buildID]; ok {
		dst[buildID] = bd
		if healed {
			return verdict, resolutionStorageHeal, nil
		}

		return verdict, resolutionOverwrite, nil
	}

	if h.Metadata.Version < headers.MetadataVersionV4 {
		dst[buildID] = headers.BuildData{}

		return verdict, resolutionLegacy, nil
	}

	if _, ok := dst[buildID]; ok {
		return verdict, resolutionInherited, nil
	}
	if healed && noBackfill {
		return verdict, resolutionSelfEntryAbsent, nil
	}

	return verdict, resolutionAbsent, nil
}

// loadAncestorHeader loads the header a heal copies from. With noBackfill it
// skips LoadHeader's uncompressed-build backfill, so the ancestor's entry
// reaches the child only if the stored header carries it — as the header this
// node's upload published does, which its entry held unless a later load from
// storage replaced it. A header older than the Builds map still gives the
// empty entry either way. An absent entry is resolved by the read path; a
// backfilled one would assert "uncompressed" for a build whose header never
// said so.
func loadAncestorHeader(ctx context.Context, s storage.StorageProvider, path string, noBackfill bool) (*headers.Header, int, error) {
	if noBackfill {
		return headers.LoadStoredHeader(ctx, s, path)
	}

	return headers.LoadHeader(ctx, s, path)
}

func recordAncestorResolution(ctx context.Context, verdict AncestorVerdict, resolution ancestorResolution) {
	ancestorResolutionsCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("verdict", string(verdict)),
		attribute.String("resolution", string(resolution)),
	))
}
