package header

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// BuildData holds per-build metadata stored in V4 headers.
// Each layer's header carries a Builds map; child headers inherit parent
// entries for still-referenced build IDs via newDiffHeader.
type BuildData struct {
	Size      int64               // uncompressed file size
	Checksum  [32]byte            // SHA-256 of uncompressed data; zero value means unknown
	FrameData *storage.FrameTable // nil for uncompressed builds
}

const NormalizeFixVersion = 3

type Header struct {
	Metadata *Metadata
	// Builds maps build IDs to per-build metadata (size, checksum, FrameTable).
	// nil for V3 (uncompressed) headers; the read path falls back to a Size()
	// RPC and reads uncompressed data when nil.
	Builds map[uuid.UUID]BuildData

	// Mapping is the per-block source map. Stored compactly (9-10 B/entry vs 40
	// for a BuildMap) so long-lived cached headers don't dominate orchestrator
	// heap. Read via At / All / Slice / Len, not indexing.
	Mapping Mapping

	// IncompletePendingUpload is set on diff headers produced by ToDiffHeader and
	// cleared on the finalized headers swapped in by the upload pipeline. It
	// is in-memory only (never serialized), and signals that the build's data
	// has not yet reached object storage — readers must serve from the local
	// cache and skip FrameTable lookups for the still-missing self entry.
	IncompletePendingUpload bool
}

// CloneForUpload returns a clone the upload path can mutate for serialization
// without racing with concurrent readers of the original. The version is set
// on the clone and the Builds map is copied (the upload path adds the self
// entry). Mapping is shared by reference: it is immutable once a Header is
// constructed.
func (t *Header) CloneForUpload(version uint64) *Header {
	metaCopy := *t.Metadata
	metaCopy.Version = version

	clone := &Header{
		Metadata: &metaCopy,
		Mapping:  t.Mapping,
	}

	if t.Builds != nil {
		clone.Builds = make(map[uuid.UUID]BuildData, len(t.Builds))
		maps.Copy(clone.Builds, t.Builds)
	}

	return clone
}

// SetBuild adds or replaces build metadata for the given build ID.
func (t *Header) SetBuild(buildID uuid.UUID, bd BuildData) {
	if t.Builds == nil {
		t.Builds = make(map[uuid.UUID]BuildData)
	}

	t.Builds[buildID] = bd
}

func NewHeader(metadata *Metadata, mapping []BuildMap) (*Header, error) {
	if metadata.BlockSize == 0 {
		return nil, errors.New("block size cannot be zero")
	}

	if len(mapping) == 0 {
		length := (metadata.Size + PageSize - 1) / PageSize * PageSize
		mapping = []BuildMap{{
			Offset:             0,
			Length:             length,
			BuildId:            metadata.BuildId,
			BuildStorageOffset: 0,
		}}
	}

	compact, err := NewMapping(PageSize, mapping)
	if err != nil {
		return nil, fmt.Errorf("compact mapping: %w", err)
	}

	return &Header{
		Metadata: metadata,
		Mapping:  compact,
	}, nil
}

func newDiffHeader(metadata *Metadata, mapping []BuildMap, sourceBuilds map[uuid.UUID]BuildData) (*Header, error) {
	h, err := NewHeader(metadata, mapping)
	if err != nil {
		return nil, err
	}

	if sourceBuilds != nil {
		// h.Mapping.Builds() is already deduped at NewMapping construction;
		// no need for an oversized temp set keyed by len(mapping).
		h.Builds = make(map[uuid.UUID]BuildData, len(sourceBuilds))
		for _, id := range h.Mapping.Builds() {
			if bd, ok := sourceBuilds[id]; ok {
				h.Builds[id] = bd
			}
		}
	}

	h.IncompletePendingUpload = true

	return h, nil
}

func (t *Header) String() string {
	if t == nil {
		return "[nil Header]"
	}

	return fmt.Sprintf("[Header: version=%d, size=%d, blockSize=%d, generation=%d, buildId=%s, mappings=%d]",
		t.Metadata.Version,
		t.Metadata.Size,
		t.Metadata.BlockSize,
		t.Metadata.Generation,
		t.Metadata.BuildId.String(),
		t.Mapping.Len(),
	)
}

// IsNormalizeFixApplied is a helper method to soft fail for older versions of the header where fix for normalization was not applied.
// This should be removed in the future.
func (t *Header) IsNormalizeFixApplied() bool {
	return t.Metadata.Version >= NormalizeFixVersion
}

// GetShiftedMapping resolves a virtual offset to a build-local range.
// The read path uses this to find which build owns the data, then calls
// GetBuildFrameData to get the FrameTable for C-space lookup.
func (t *Header) GetShiftedMapping(ctx context.Context, offset int64) (BuildMap, error) {
	mapping, shift, err := t.getMapping(ctx, offset)
	if err != nil {
		return BuildMap{}, err
	}
	mappedLength := int64(mapping.Length) - shift

	b := BuildMap{
		Offset:  mapping.BuildStorageOffset + uint64(shift),
		Length:  uint64(mappedLength),
		BuildId: mapping.BuildId,
	}

	if mappedLength < 0 {
		if t.IsNormalizeFixApplied() {
			return BuildMap{}, fmt.Errorf("mapped length for offset %d is negative: %d", offset, mappedLength)
		}

		b.Length = 0
		logger.L().Warn(ctx, "mapped length is negative, but normalize fix is not applied",
			zap.Int64("offset", offset),
			zap.Int64("mappedLength", mappedLength),
			logger.WithBuildID(mapping.BuildId.String()),
		)
	}

	return b, nil
}

// GetBuildFrameData returns the FrameTable for a build: nil = no entry,
// storage.UncompressedFrameTable = authoritatively uncompressed, else
// compressed.
func (t *Header) GetBuildFrameData(buildID uuid.UUID) *storage.FrameTable {
	bd, ok := t.Builds[buildID]
	if !ok {
		return nil
	}
	if bd.FrameData == nil {
		return storage.UncompressedFrameTable
	}

	return bd.FrameData
}

// SelfBuildData returns the size and full FrameTable for the header's own
// build (t.Metadata.BuildId). Errors when the self entry is missing — only
// possible with peer-served incomplete headers, never with storage-uploaded
// ones (build_upload_v4 always populates self before publish).
//
// This is the *only* place the FullFrameTable upcast happens in production:
// Builds[id].FrameData is typed *FrameTable to match the trimmed-FT case
// where our header carries only the frames we mapped from an ancestor. For
// the self entry, build_upload_v4 always stores the complete table, so the
// upcast via storage.FullFromTable is sound.
func (t *Header) SelfBuildData() (int64, *storage.FullFrameTable, error) {
	bd, hasSelf := t.Builds[t.Metadata.BuildId]
	if !hasSelf {
		return 0, nil, fmt.Errorf("header for build %s has no self entry (peer-served incomplete?)", t.Metadata.BuildId)
	}

	return bd.Size, storage.FullFromTable(bd.FrameData), nil
}

func (t *Header) getMapping(ctx context.Context, offset int64) (BuildMap, int64, error) {
	if offset < 0 || offset >= int64(t.Metadata.Size) {
		if t.IsNormalizeFixApplied() {
			return BuildMap{}, 0, fmt.Errorf("offset %d is out of bounds (size: %d)", offset, t.Metadata.Size)
		}

		logger.L().Warn(ctx, "offset is out of bounds, but normalize fix is not applied",
			zap.Int64("offset", offset),
			zap.Int64("size", int64(t.Metadata.Size)),
			logger.WithBuildID(t.Metadata.BuildId.String()),
		)
	}
	if offset%PageSize != 0 {
		if t.IsNormalizeFixApplied() {
			return BuildMap{}, 0, fmt.Errorf("offset %d is not aligned to page size %d", offset, PageSize)
		}

		logger.L().Warn(ctx, "offset is not aligned to page size, but normalize fix is not applied",
			zap.Int64("offset", offset),
			zap.Int64("pageSize", PageSize),
			logger.WithBuildID(t.Metadata.BuildId.String()),
		)
	}

	i := t.Mapping.SearchOffset(offset)
	if i == 0 {
		return BuildMap{}, 0, fmt.Errorf("no source found for offset %d", offset)
	}

	mapping := t.Mapping.At(i - 1)
	shift := offset - int64(mapping.Offset)

	// Verify that the offset falls within this mapping's range
	if shift >= int64(mapping.Length) {
		if t.IsNormalizeFixApplied() {
			return BuildMap{}, 0, fmt.Errorf("offset %d is beyond the end of mapping at offset %d (ends at %d)",
				offset, mapping.Offset, mapping.Offset+mapping.Length)
		}

		logger.L().Warn(ctx, "offset is beyond the end of mapping, but normalize fix is not applied",
			zap.Int64("offset", offset),
			zap.Uint64("mappingOffset", mapping.Offset),
			zap.Uint64("mappingEnd", mapping.Offset+mapping.Length),
			logger.WithBuildID(t.Metadata.BuildId.String()),
		)
	}

	return mapping, shift, nil
}
