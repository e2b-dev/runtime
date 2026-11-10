package header

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
)

const maxV5MappingEntries = 8 << 20

// V5 keeps V4's framing — [Metadata][uint8 flags][uint32 uncompressedSize]
// [LZ4(block)] — and an identical Builds section. Only the mapping section
// changes: instead of N fixed 40-byte records, it is columnar and varint-coded.
//
// V4 mapping bytes are dominated by the offset/length/storage columns (three
// 8-byte little-endian integers each, mostly tiny or monotonic) and by a full
// 16-byte BuildId repeated on every entry. For page-granular memfile dedup a
// single header can hold millions of entries, blowing past the 64 MiB
// uncompressed cap and compressing poorly (interleaved unique 8-byte values
// defeat LZ4). V5 fixes both:
//
//   - block-indexed columns (offset/length/storage as deltas/values in block
//     units, varint-coded): tiny per entry, and storing each column
//     contiguously lets LZ4 collapse the highly regular runs.
//   - a per-header build-id table addressed by a varint index, so a 16-byte
//     UUID is stored once per distinct build instead of once per entry.

// serializeV5 mirrors serializeV4 but writes the columnar mapping section.
// Returns the assembled bytes and the uncompressed inner-block size.
func serializeV5(metadata *Metadata, builds map[uuid.UUID]BuildData, mapping Mapping, incomplete bool) ([]byte, int64, error) {
	var metaBuf bytes.Buffer
	if err := binary.Write(&metaBuf, binary.LittleEndian, metadata); err != nil {
		return nil, 0, fmt.Errorf("failed to write metadata: %w", err)
	}

	var block bytes.Buffer

	if err := writeV4BuildsSection(&block, builds, mapping); err != nil {
		return nil, 0, err
	}

	if err := writeV5MappingSection(&block, mapping); err != nil {
		return nil, 0, err
	}

	blockBytes := block.Bytes()
	compressed, err := compressLZ4(blockBytes)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to LZ4-compress v5 header block: %w", err)
	}

	var flags uint8
	if incomplete {
		flags |= v4FlagIncomplete
	}

	result := make([]byte, metadataSize+v4FlagsLen+v4SizePrefixLen+len(compressed))
	copy(result, metaBuf.Bytes())
	result[metadataSize] = flags
	binary.LittleEndian.PutUint32(result[metadataSize+v4FlagsLen:], uint32(len(blockBytes)))
	copy(result[metadataSize+v4FlagsLen+v4SizePrefixLen:], compressed)

	return result, int64(len(blockBytes)), nil
}

// writeV5MappingSection writes the columnar, varint-coded mapping:
//
//	uint32 M                       distinct non-empty build-id count
//	M × [16]byte                   build-id table
//	uint32 N                       non-empty entry count
//	N × uvarint                    offset-block deltas (offset[i]-offset[i-1])
//	N × uvarint                    length in blocks
//	N × varint                     storage-block deltas (can go negative when switching builds)
//	N × uvarint                    build-id table index
func writeV5MappingSection(block *bytes.Buffer, m Mapping) error {
	var n int
	for i := range m.offsets {
		if m.buildIndex(i) >= 0 {
			n++
		}
	}
	if err := binary.Write(block, binary.LittleEndian, uint32(len(m.builds))); err != nil {
		return fmt.Errorf("failed to write mapping build count: %w", err)
	}
	for _, id := range m.builds {
		if _, err := block.Write(id[:]); err != nil {
			return fmt.Errorf("failed to write mapping build id: %w", err)
		}
	}
	if err := binary.Write(block, binary.LittleEndian, uint32(n)); err != nil {
		return fmt.Errorf("failed to write mapping count: %w", err)
	}

	var buf [binary.MaxVarintLen64]byte

	var prevOffset uint64
	for i, v := range m.offsets {
		if m.buildIndex(i) < 0 {
			continue
		}
		off := uint64(v)
		block.Write(buf[:binary.PutUvarint(buf[:], off-prevOffset)])
		prevOffset = off
	}
	for i := range m.offsets {
		if m.buildIndex(i) < 0 {
			continue
		}
		block.Write(buf[:binary.PutUvarint(buf[:], uint64(m.lengthBlocks(i)))])
	}
	var prevStorage int64
	for i, v := range m.storage {
		if m.buildIndex(i) < 0 {
			continue
		}
		s := int64(v)
		block.Write(buf[:binary.PutVarint(buf[:], s-prevStorage)])
		prevStorage = s
	}
	for i := range m.offsets {
		bi := m.buildIndex(i)
		if bi < 0 {
			continue
		}
		block.Write(buf[:binary.PutUvarint(buf[:], uint64(bi))])
	}

	return nil
}

// deserializeV5 decompresses and reads the V5 block.
func deserializeV5(metadata *Metadata, blockData []byte) (*Header, error) {
	if metadata.BlockSize == 0 {
		return nil, errors.New("v5 header has zero block size")
	}
	if len(blockData) < v4FlagsLen+v4SizePrefixLen {
		return nil, fmt.Errorf("v5 header block too short for flags + size prefix: %d bytes", len(blockData))
	}

	flags := blockData[0]
	size := binary.LittleEndian.Uint32(blockData[v4FlagsLen:])
	if uint64(size) > uint64(v4MaxUncompressedHeaderSize) {
		return nil, fmt.Errorf("v5 header uncompressed size %d exceeds cap %d", size, v4MaxUncompressedHeaderSize)
	}

	decompressed, err := decompressLZ4(blockData[v4FlagsLen+v4SizePrefixLen:], int(size))
	if err != nil {
		return nil, fmt.Errorf("failed to LZ4-decompress v5 header block: %w", err)
	}
	if len(decompressed) != int(size) {
		return nil, fmt.Errorf("v5 header decompressed size %d != prefix %d", len(decompressed), size)
	}

	reader := bytes.NewReader(decompressed)

	builds, err := readV4BuildsSection(reader)
	if err != nil {
		return nil, err
	}

	// The compact mapping is encoded in PageSize units (see NewHeader), not the
	// file's logical block size, so decode it the same way.
	mapping, err := readV5MappingSection(reader, PageSize, metadata.Size)
	if err != nil {
		return nil, err
	}

	h := &Header{
		Metadata:                metadata,
		Builds:                  builds,
		Mapping:                 mapping,
		IncompletePendingUpload: flags&v4FlagIncomplete != 0,
	}

	return h, nil
}

// readV5MappingSection reads the columnar mapping written by
// writeV5MappingSection and reconstructs the compact Mapping directly.
func readV5MappingSection(reader *bytes.Reader, blockSize, size uint64) (Mapping, error) {
	sizeBlocks := size / blockSize
	if size%blockSize != 0 {
		sizeBlocks++
	}
	if sizeBlocks > maxBlockIdx {
		return Mapping{}, fmt.Errorf("mapping size block %d exceeds uint32", sizeBlocks)
	}

	var m, n uint32
	if err := binary.Read(reader, binary.LittleEndian, &m); err != nil {
		return Mapping{}, fmt.Errorf("failed to read mapping build count: %w", err)
	}
	if m > maxBuildsPerHeader {
		return Mapping{}, fmt.Errorf("mapping build count %d exceeds maximum %d", m, maxBuildsPerHeader)
	}
	if uint64(m) > uint64(reader.Len())/16 {
		return Mapping{}, fmt.Errorf("mapping build count %d exceeds remaining %d bytes", m, reader.Len())
	}

	builds := make([]uuid.UUID, m)
	for i := range builds {
		if _, err := io.ReadFull(reader, builds[i][:]); err != nil {
			return Mapping{}, fmt.Errorf("failed to read mapping build id %d: %w", i, err)
		}
	}
	if err := binary.Read(reader, binary.LittleEndian, &n); err != nil {
		return Mapping{}, fmt.Errorf("failed to read mapping count: %w", err)
	}
	if n > maxV5MappingEntries {
		return Mapping{}, fmt.Errorf("mapping count %d exceeds maximum %d", n, maxV5MappingEntries)
	}

	if n == 0 {
		if size == 0 {
			return newMappingFromColumns(blockSize, builds, nil, nil, nil, 0)
		}

		return newMappingFromColumns(blockSize, builds,
			[]uint32{0}, []uint32{0}, []uint16{nilBuildIdx16}, sizeBlocks)
	}
	// Each entry needs at least one byte in each of the four encoded columns,
	// and sparse input can reconstruct up to a gap plus mapped entry per
	// encoded entry. Bound the crafted count before allocating either form:
	// the decoded columns are transient and carry the wire lengths, the
	// reconstructed ones are retained and derive them.
	maxEntriesByBytes := uint64(reader.Len()) / 4
	const encodedEntryBytes = 3*4 + 2
	const compactEntryBytes = 2*4 + 2 // upper bound; the retained index column is one byte for most headers
	maxEntriesByMemory := uint64(v4MaxUncompressedHeaderSize) / (encodedEntryBytes + 2*compactEntryBytes)
	if maxEntriesByBytes > maxEntriesByMemory {
		maxEntriesByBytes = maxEntriesByMemory
	}
	if uint64(n) > maxEntriesByBytes {
		return Mapping{}, fmt.Errorf("mapping count %d exceeds remaining %d bytes", n, reader.Len())
	}

	encodedOffsets := make([]uint32, n)
	encodedLengths := make([]uint32, n)
	encodedStorage := make([]uint32, n)
	encodedBuildIdx := make([]uint16, n)

	var prevOffset uint64
	for i := range int(n) {
		d, err := binary.ReadUvarint(reader)
		if err != nil {
			return Mapping{}, fmt.Errorf("failed to read offset delta %d: %w", i, err)
		}
		// Bound the delta before adding so the running sum can't overflow uint64
		// and wrap past the maxBlockIdx check.
		if d > maxBlockIdx {
			return Mapping{}, fmt.Errorf("offset delta %d at entry %d exceeds uint32", d, i)
		}
		prevOffset += d
		if prevOffset > maxBlockIdx {
			return Mapping{}, fmt.Errorf("offset block %d at entry %d exceeds uint32", prevOffset, i)
		}
		encodedOffsets[i] = uint32(prevOffset)
	}
	for i := range int(n) {
		v, err := binary.ReadUvarint(reader)
		if err != nil {
			return Mapping{}, fmt.Errorf("failed to read length %d: %w", i, err)
		}
		if v > maxBlockIdx {
			return Mapping{}, fmt.Errorf("length block %d at entry %d exceeds uint32", v, i)
		}
		encodedLengths[i] = uint32(v)
	}
	var prevStorage int64
	for i := range int(n) {
		d, err := binary.ReadVarint(reader)
		if err != nil {
			return Mapping{}, fmt.Errorf("failed to read storage delta %d: %w", i, err)
		}
		prevStorage += d
		if prevStorage < 0 || prevStorage > maxBlockIdx {
			return Mapping{}, fmt.Errorf("storage block %d at entry %d out of range", prevStorage, i)
		}
		encodedStorage[i] = uint32(prevStorage)
	}
	for i := range int(n) {
		v, err := binary.ReadUvarint(reader)
		if err != nil {
			return Mapping{}, fmt.Errorf("failed to read build index %d: %w", i, err)
		}
		if v >= uint64(m) {
			return Mapping{}, fmt.Errorf("build index %d at entry %d out of range (%d builds)", v, i, m)
		}
		encodedBuildIdx[i] = uint16(v)
	}

	// Size the columns exactly: the Mapping is cached for ~25h, so an oversized
	// buffer would stay resident (Clip keeps the backing array, Clone adds a
	// transient copy). Count the mapped entries plus reconstructed nil gaps.
	total := int(n)
	var prevEnd uint64
	for i := range int(n) {
		if uint64(encodedOffsets[i]) > prevEnd {
			total++
		}
		prevEnd = uint64(encodedOffsets[i]) + uint64(encodedLengths[i])
	}
	if prevEnd < sizeBlocks {
		total++
	}

	// The reconstruction below fills every gap with a nil entry and rejects
	// overlaps, so the result is contiguous from block 0 to sizeBlocks and the
	// wire lengths need not be retained: each entry ends where the next starts.
	offsets := make([]uint32, 0, total)
	storageCol := make([]uint32, 0, total)
	buildIdx := make([]uint16, 0, total)
	appendEntry := func(off, storage uint32, idx uint16) {
		offsets = append(offsets, off)
		storageCol = append(storageCol, storage)
		buildIdx = append(buildIdx, idx)
	}

	var current uint32
	for i := range int(n) {
		off, length := encodedOffsets[i], encodedLengths[i]
		if length == 0 {
			return Mapping{}, fmt.Errorf("zero-length mapping at entry %d", i)
		}
		if off < current {
			return Mapping{}, fmt.Errorf("mapping offset %d at entry %d overlaps previous end %d", off, i, current)
		}
		if off > current {
			appendEntry(current, 0, nilBuildIdx16)
		}
		end := uint64(off) + uint64(length)
		if end > sizeBlocks {
			return Mapping{}, fmt.Errorf("mapping end block %d at entry %d exceeds size %d", end, i, sizeBlocks)
		}
		appendEntry(off, encodedStorage[i], encodedBuildIdx[i])
		current = uint32(end)
	}
	if uint64(current) < sizeBlocks {
		appendEntry(current, 0, nilBuildIdx16)
	}

	return newMappingFromColumns(blockSize, builds, offsets, storageCol, buildIdx, sizeBlocks)
}
