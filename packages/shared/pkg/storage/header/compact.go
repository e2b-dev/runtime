package header

import (
	"errors"
	"fmt"
	"iter"
	"math"

	"github.com/google/uuid"
)

// Mapping is the compact in-memory representation of a Header's mapping list.
// A merged Header is cached for hours, so on snapshot-heavy nodes these slices
// dominate host RAM. It shrinks each entry from 40 bytes (a BuildMap) to 9 or
// 10 by encoding offset/storage as uint32 block indices, deduplicating BuildId
// into a per-header table addressed by one byte (two once a header references
// more than 255 builds), storing the columns as parallel slices, and deriving
// each entry's length from where the next one starts. Immutable; read via
// At / All / Slice.
type Mapping struct {
	blockSize uint64
	builds    []uuid.UUID
	offsets   []uint32
	// lengths is nil when every entry starts where the previous one ends: the
	// length is then offsets[i+1]-offsets[i], and endBlocks-offsets[i] for the
	// last entry. Only a mapping with gaps or overlaps between entries (legacy
	// headers predating NormalizeFixVersion) stores the column explicitly.
	lengths []uint32
	storage []uint32
	// Exactly one build-index column is set for a non-empty mapping: buildIdx8
	// while the header references at most maxBuilds8 builds, buildIdx16
	// otherwise. Each addresses builds, with its own sentinel for an empty
	// (zero) region.
	buildIdx8  []uint8
	buildIdx16 []uint16
	// endBlocks is the block at which the last entry ends.
	endBlocks uint64
}

const (
	nilBuildIdx8  = math.MaxUint8
	nilBuildIdx16 = math.MaxUint16

	// maxBuilds8 is how many builds the one-byte index column can address,
	// with nilBuildIdx8 reserved for empty regions.
	maxBuilds8 = nilBuildIdx8
	// maxBuildsPerHeader leaves nilBuildIdx16 reserved for empty regions.
	maxBuildsPerHeader = nilBuildIdx16
)

// maxBlockIdx is the largest block index representable by the uint32 columns.
// At PageSize granularity this caps a single file (memfile/rootfs) at ~16 TiB,
// well above any sandbox; NewMapping errors if a value exceeds it.
const maxBlockIdx = math.MaxUint32

// NewMapping packs src into the compact representation. blockSize is the unit
// for the block indices and must divide every Offset, Length, and
// BuildStorageOffset in src (callers pass PageSize, the universal granularity).
func NewMapping(blockSize uint64, src []BuildMap) (Mapping, error) {
	if blockSize == 0 {
		return Mapping{}, errors.New("compact mapping: block size cannot be zero")
	}
	if len(src) == 0 {
		return Mapping{blockSize: blockSize}, nil
	}

	idxByBuild := make(map[uuid.UUID]uint16, 8)
	builds := make([]uuid.UUID, 0, 8)
	offsets := make([]uint32, len(src))
	storage := make([]uint32, len(src))
	// Wide scratch column; narrowed to one byte per entry once the build count
	// is known (see setBuildIdx).
	buildIdx := make([]uint16, len(src))

	contiguous := true
	var prevEnd uint64
	for i, m := range src {
		if m.Offset%blockSize != 0 {
			return Mapping{}, fmt.Errorf("compact mapping: offset %d at index %d not block-aligned to %d", m.Offset, i, blockSize)
		}
		if m.Length%blockSize != 0 {
			return Mapping{}, fmt.Errorf("compact mapping: length %d at index %d not block-aligned to %d", m.Length, i, blockSize)
		}
		if m.BuildStorageOffset%blockSize != 0 {
			return Mapping{}, fmt.Errorf("compact mapping: build storage offset %d at index %d not block-aligned to %d", m.BuildStorageOffset, i, blockSize)
		}

		offBlocks := m.Offset / blockSize
		lenBlocks := m.Length / blockSize
		stoBlocks := m.BuildStorageOffset / blockSize
		if offBlocks > maxBlockIdx || lenBlocks > maxBlockIdx || stoBlocks > maxBlockIdx {
			return Mapping{}, fmt.Errorf("compact mapping: block index out of uint32 range at entry %d", i)
		}
		if i > 0 && offBlocks != prevEnd {
			contiguous = false
		}
		prevEnd = offBlocks + lenBlocks

		idx := uint16(nilBuildIdx16)
		if m.BuildId != uuid.Nil {
			var ok bool
			idx, ok = idxByBuild[m.BuildId]
			if !ok {
				if len(builds) >= maxBuildsPerHeader {
					return Mapping{}, fmt.Errorf("compact mapping: more than %d unique build IDs", maxBuildsPerHeader)
				}
				idx = uint16(len(builds))
				idxByBuild[m.BuildId] = idx
				builds = append(builds, m.BuildId)
			}
		}

		offsets[i] = uint32(offBlocks)
		storage[i] = uint32(stoBlocks)
		buildIdx[i] = idx
	}

	out := Mapping{
		blockSize: blockSize,
		builds:    builds,
		offsets:   offsets,
		storage:   storage,
		endBlocks: prevEnd,
	}
	out.buildIdx8, out.buildIdx16 = buildIdxColumns(len(builds), buildIdx)
	if !contiguous {
		out.lengths = make([]uint32, len(src))
		for i, m := range src {
			out.lengths[i] = uint32(m.Length / blockSize)
		}
	}

	return out, nil
}

// buildIdxColumns chooses the retained build-index column from its wide form:
// one byte per entry when every build fits, which every memfile header and
// nearly every rootfs header does, otherwise the wide column as is. Exactly
// one of the returned columns is non-nil.
func buildIdxColumns(nBuilds int, wide []uint16) ([]uint8, []uint16) {
	if nBuilds > maxBuilds8 {
		return nil, wide
	}

	narrow := make([]uint8, len(wide))
	for i, idx := range wide {
		if idx == nilBuildIdx16 {
			narrow[i] = nilBuildIdx8
		} else {
			narrow[i] = uint8(idx)
		}
	}

	return narrow, nil
}

// buildIndex returns the i-th entry's index into builds, or -1 for an empty
// region.
func (m Mapping) buildIndex(i int) int {
	if m.buildIdx8 != nil {
		if idx := m.buildIdx8[i]; idx != nilBuildIdx8 {
			return int(idx)
		}

		return -1
	}
	if idx := m.buildIdx16[i]; idx != nilBuildIdx16 {
		return int(idx)
	}

	return -1
}

// newMappingFromColumns builds a Mapping from already-decoded columns, avoiding
// the []BuildMap intermediate on the deserialize path. The entries must be
// contiguous, with the last one ending at endBlocks, so no lengths column is
// stored. All column slices must have the same length, and every buildIdx must
// index builds.
func newMappingFromColumns(blockSize uint64, builds []uuid.UUID, offsets, storage []uint32, buildIdx []uint16, endBlocks uint64) (Mapping, error) {
	n := len(offsets)
	if len(storage) != n || len(buildIdx) != n {
		return Mapping{}, fmt.Errorf("compact mapping: column length mismatch (offsets=%d storage=%d buildIdx=%d)", n, len(storage), len(buildIdx))
	}
	if n > 0 && uint64(offsets[n-1]) > endBlocks {
		return Mapping{}, fmt.Errorf("compact mapping: last offset block %d beyond end block %d", offsets[n-1], endBlocks)
	}
	for i, bi := range buildIdx {
		if bi == nilBuildIdx16 {
			continue
		}
		if int(bi) >= len(builds) {
			return Mapping{}, fmt.Errorf("compact mapping: buildIdx %d at entry %d out of range (%d builds)", bi, i, len(builds))
		}
	}

	out := Mapping{
		blockSize: blockSize,
		builds:    builds,
		offsets:   offsets,
		storage:   storage,
		endBlocks: endBlocks,
	}
	out.buildIdx8, out.buildIdx16 = buildIdxColumns(len(builds), buildIdx)

	return out, nil
}

// Len returns the number of entries.
func (m Mapping) Len() int { return len(m.offsets) }

// BlockSize returns the block size used for block<->byte conversions.
func (m Mapping) BlockSize() uint64 { return m.blockSize }

// ByteSize returns the approximate heap footprint of the mapping's columns, so
// callers can gauge how much RAM cached headers hold without knowing the
// encoding. It counts the per-entry columns — 9 bytes for the contiguous form,
// 4 more where lengths are stored explicitly and one more where the header
// needs the wide build index — plus the deduplicated build table (16 bytes per
// UUID); it excludes the struct header itself and any slice capacity beyond
// len.
func (m Mapping) ByteSize() int {
	bytesPerEntry := 4 + 4 + 1 // offsets, storage, build index
	if m.lengths != nil {
		bytesPerEntry += 4
	}
	if m.buildIdx16 != nil {
		bytesPerEntry++
	}

	return len(m.offsets)*bytesPerEntry + len(m.builds)*16
}

// SharesStorageWith reports whether m and other are backed by the same columns.
// Copying a Mapping by value — as Header.CloneForUpload does — shares its
// slices with the original, so two distinct Headers can hold one allocation
// between them. Callers measuring heap footprint need this to avoid counting
// that allocation once per Header. An empty mapping shares nothing: it holds no
// allocation to confuse.
func (m Mapping) SharesStorageWith(other Mapping) bool {
	if len(m.offsets) == 0 || len(other.offsets) == 0 {
		return false
	}

	return &m.offsets[0] == &other.offsets[0]
}

// Builds returns the deduplicated build IDs referenced by the mapping. The
// returned slice is shared with the Mapping; callers must not mutate it.
func (m Mapping) Builds() []uuid.UUID { return m.builds }

// Contiguous reports whether every entry starts where the previous one ends.
// Such a mapping derives its lengths from the offsets column instead of
// storing them.
func (m Mapping) Contiguous() bool { return m.lengths == nil }

// lengthBlocks returns the i-th entry's length in blocks.
func (m Mapping) lengthBlocks(i int) uint32 {
	if m.lengths != nil {
		return m.lengths[i]
	}
	if i+1 < len(m.offsets) {
		return m.offsets[i+1] - m.offsets[i]
	}

	return uint32(m.endBlocks - uint64(m.offsets[i]))
}

// At materializes the i-th entry as a BuildMap. Panics if i is out of range,
// matching `mapping[i]` semantics.
func (m Mapping) At(i int) BuildMap {
	buildID := uuid.Nil
	if bi := m.buildIndex(i); bi >= 0 {
		buildID = m.builds[bi]
	}

	return BuildMap{
		Offset:             uint64(m.offsets[i]) * m.blockSize,
		Length:             uint64(m.lengthBlocks(i)) * m.blockSize,
		BuildId:            buildID,
		BuildStorageOffset: uint64(m.storage[i]) * m.blockSize,
	}
}

// All iterates the mapping, materializing each entry as a BuildMap. This is
// the preferred read path for callers that don't need a backing []BuildMap.
func (m Mapping) All() iter.Seq2[int, BuildMap] {
	return func(yield func(int, BuildMap) bool) {
		for i := range m.offsets {
			if !yield(i, m.At(i)) {
				return
			}
		}
	}
}

// BytesByBuild sums the bytes attributed to each referenced build. It scans the
// columns directly (no BuildMap materialization or per-entry uuid hashing),
// accumulating into a small per-build slice, so it stays cheap even for
// mappings with millions of entries. Empty (nil-build) regions are skipped.
// The returned map is non-nil and addressable by the caller.
func (m Mapping) BytesByBuild() map[uuid.UUID]uint64 {
	sums := make([]uint64, len(m.builds))
	for i := range m.offsets {
		if bi := m.buildIndex(i); bi >= 0 {
			sums[bi] += uint64(m.lengthBlocks(i))
		}
	}

	out := make(map[uuid.UUID]uint64, len(m.builds))
	for bi, blocks := range sums {
		out[m.builds[bi]] = blocks * m.blockSize
	}

	return out
}

// Slice materializes the full mapping as []BuildMap (~40 bytes/entry). Use
// sparingly — for serialization fallbacks, CLI inspection, and tests. Hot
// paths and the cached form should use At / All instead.
func (m Mapping) Slice() []BuildMap {
	out := make([]BuildMap, len(m.offsets))
	for i := range m.offsets {
		out[i] = m.At(i)
	}

	return out
}

// Validate checks that entries are contiguous, block-aligned to blockSize, and
// cover exactly `size` bytes. It is the compact-form equivalent of
// ValidateMappings(m.Slice(), size, blockSize) without the materialization.
func (m Mapping) Validate(size, blockSize uint64) error {
	if blockSize == 0 {
		return errors.New("mapping validation failed: zero block size")
	}
	// The compact form stores in m.blockSize units; if those aren't a multiple
	// of the requested validation blockSize, fall back to the slice path so we
	// don't miss a misalignment.
	if m.blockSize%blockSize != 0 {
		return ValidateMappings(m.Slice(), size, blockSize)
	}

	var currentOffset uint64
	for i := range m.offsets {
		offset := uint64(m.offsets[i]) * m.blockSize
		length := uint64(m.lengthBlocks(i)) * m.blockSize
		if currentOffset != offset {
			return fmt.Errorf("mapping validation failed at index %d: expected offset %d (block %d), got %d (block %d)", i, currentOffset, currentOffset/blockSize, offset, offset/blockSize)
		}
		if currentOffset+length > size {
			return fmt.Errorf("mapping validation failed at index %d: %d (current offset) + %d (length) > %d (size)", i, currentOffset, length, size)
		}
		currentOffset += length
	}
	if currentOffset != size {
		return fmt.Errorf("mapping validation failed: total %d != size %d", currentOffset, size)
	}

	return nil
}

// SearchOffset returns the first index i whose byte offset is strictly greater
// than off, matching sort.Search semantics on Offset. It compares in block
// units, so entries are never materialized: entry.OffsetBlocks*blockSize > off
// iff entry.OffsetBlocks > off/blockSize (integer division).
func (m Mapping) SearchOffset(off int64) int {
	if off < 0 || len(m.offsets) == 0 {
		return 0
	}
	target := uint64(off) / m.blockSize
	lo, hi := 0, len(m.offsets)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if uint64(m.offsets[mid]) > target {
			hi = mid
		} else {
			lo = mid + 1
		}
	}

	return lo
}
