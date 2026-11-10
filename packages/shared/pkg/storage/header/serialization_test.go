package header

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// fragmentedHeader builds a V4 header with n alternating-build mappings so its
// uncompressed block is large and incompressible enough to exercise the cap.
func fragmentedHeader(t *testing.T, n int) *Header {
	t.Helper()
	bs := uint64(4096)
	a, b := uuid.New(), uuid.New()
	mappings := make([]BuildMap, n)
	var off, sa, sb uint64
	for i := range mappings {
		id, so := a, sa
		if i%2 == 1 {
			id, so = b, sb
		}
		mappings[i] = BuildMap{Offset: off, Length: bs, BuildId: id, BuildStorageOffset: so}
		off += bs
		if i%2 == 0 {
			sa += bs
		} else {
			sb += bs
		}
	}
	meta := &Metadata{Version: MetadataVersionV4, BlockSize: bs, Size: off, BuildId: a, BaseBuildId: b}
	h, err := NewHeader(meta, mappings)
	require.NoError(t, err)
	h.Builds = map[uuid.UUID]BuildData{a: {Size: int64(sa)}, b: {Size: int64(sb)}}

	return h
}

//nolint:paralleltest // mutates the package-global cap; must not run in parallel
func TestStoreHeader_RejectsOversizeOnWrite(t *testing.T) {
	orig := v4MaxUncompressedHeaderSize
	v4MaxUncompressedHeaderSize = 1024
	t.Cleanup(func() { v4MaxUncompressedHeaderSize = orig })

	// The guard returns before the storage provider is used, so a nil provider
	// is fine for the rejection path.
	h := fragmentedHeader(t, 100)
	_, _, _, err := StoreHeader(t.Context(), nil, "header", h) //nolint:dogsled // only err matters
	require.ErrorContains(t, err, "exceeds cap")
}

//nolint:paralleltest // mutates the package-global cap; must not run in parallel
func TestDeserialize_AboveOldCapRoundTrips(t *testing.T) {
	// A header above a lowered cap is rejected on read; raising the cap lets it
	// round-trip. Mirrors raising the production cap so already-uploaded large
	// headers become resumable again.
	orig := v4MaxUncompressedHeaderSize
	v4MaxUncompressedHeaderSize = 4096
	t.Cleanup(func() { v4MaxUncompressedHeaderSize = orig })

	h := fragmentedHeader(t, 1000) // ~40 KiB uncompressed block, over the 4 KiB cap
	data, err := SerializeHeader(h)
	require.NoError(t, err)

	_, err = DeserializeBytes(data)
	require.ErrorContains(t, err, "exceeds cap")

	v4MaxUncompressedHeaderSize = orig
	got, err := DeserializeBytes(data)
	require.NoError(t, err)
	require.Equal(t, 1000, got.Mapping.Len())
}

func mustMapping(t *testing.T, blockSize uint64, src []BuildMap) Mapping {
	t.Helper()
	m, err := NewMapping(blockSize, src)
	require.NoError(t, err)

	return m
}

func TestSerializeDeserialize_V3_RoundTrip(t *testing.T) {
	t.Parallel()

	buildID := uuid.New()
	baseID := uuid.New()
	metadata := &Metadata{
		Version:     3,
		BlockSize:   4096,
		Size:        8192,
		Generation:  7,
		BuildId:     buildID,
		BaseBuildId: baseID,
	}

	mappings := []BuildMap{
		{
			Offset:             0,
			Length:             4096,
			BuildId:            buildID,
			BuildStorageOffset: 0,
		},
		{
			Offset:             4096,
			Length:             4096,
			BuildId:            baseID,
			BuildStorageOffset: 8192,
		},
	}

	data, err := serializeV3(metadata, mustMapping(t, metadata.BlockSize, mappings))
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	require.Equal(t, metadata, got.Metadata)
	require.Equal(t, 2, got.Mapping.Len())
	require.Equal(t, uint64(0), got.Mapping.At(0).Offset)
	require.Equal(t, uint64(4096), got.Mapping.At(0).Length)
	require.Equal(t, buildID, got.Mapping.At(0).BuildId)
	require.Equal(t, uint64(0), got.Mapping.At(0).BuildStorageOffset)

	require.Equal(t, uint64(4096), got.Mapping.At(1).Offset)
	require.Equal(t, uint64(4096), got.Mapping.At(1).Length)
	require.Equal(t, baseID, got.Mapping.At(1).BuildId)
	require.Equal(t, uint64(8192), got.Mapping.At(1).BuildStorageOffset)

	// V3 headers have no Builds
	require.Nil(t, got.Builds)
}

func TestDeserialize_TruncatedMetadata(t *testing.T) {
	t.Parallel()

	_, err := DeserializeBytes([]byte{0x01, 0x02, 0x03})
	require.Error(t, err)
	require.Contains(t, err.Error(), "header too short")
}

func TestSerializeDeserialize_EmptyMappings_Defaults(t *testing.T) {
	t.Parallel()

	metadata := &Metadata{
		Version:     3,
		BlockSize:   4096,
		Size:        8192,
		Generation:  0,
		BuildId:     uuid.New(),
		BaseBuildId: uuid.New(),
	}

	data, err := serializeV3(metadata, Mapping{})
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	// NewHeader creates a default mapping when none provided
	require.Equal(t, 1, got.Mapping.Len())
	require.Equal(t, uint64(0), got.Mapping.At(0).Offset)
	require.Equal(t, metadata.Size, got.Mapping.At(0).Length)
	require.Equal(t, metadata.BuildId, got.Mapping.At(0).BuildId)
}

func TestDeserialize_BlockSizeZero(t *testing.T) {
	t.Parallel()

	metadata := &Metadata{
		Version:     3,
		BlockSize:   0,
		Size:        4096,
		Generation:  0,
		BuildId:     uuid.New(),
		BaseBuildId: uuid.New(),
	}

	data, err := serializeV3(metadata, Mapping{})
	require.NoError(t, err)

	_, err = DeserializeBytes(data)
	require.Error(t, err)
	require.Contains(t, err.Error(), "block size cannot be zero")
}

func TestSerializeDeserialize_V4_WithFrameTable(t *testing.T) {
	t.Parallel()

	buildID := uuid.New()
	baseID := uuid.New()
	metadata := &Metadata{
		Version:     4,
		BlockSize:   4096,
		Size:        8192,
		Generation:  1,
		BuildId:     buildID,
		BaseBuildId: baseID,
	}

	mappings := []BuildMap{
		{
			Offset:             0,
			Length:             4096,
			BuildId:            buildID,
			BuildStorageOffset: 0,
		},
		{
			Offset:             4096,
			Length:             4096,
			BuildId:            baseID,
			BuildStorageOffset: 0,
		},
	}

	checksum := sha256.Sum256([]byte("test-data"))

	h, err := NewHeader(metadata, mappings)
	require.NoError(t, err)
	h.Builds = map[uuid.UUID]BuildData{
		buildID: {
			Size: 12345, Checksum: checksum,
			FrameData: storage.NewFullFrameTable(storage.CompressionLZ4, []storage.FrameSize{
				{U: 2048, C: 1024},
				{U: 2048, C: 900},
			}).Table(),
		},
		baseID: {Size: 67890},
	}

	data, err := SerializeHeader(h)
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	require.Equal(t, uint64(4), got.Metadata.Version)
	require.Equal(t, 2, got.Mapping.Len())
	require.Equal(t, buildID, got.Mapping.At(0).BuildId)
	require.Equal(t, baseID, got.Mapping.At(1).BuildId)

	// Builds round-trip
	require.Len(t, got.Builds, 2)
	require.Equal(t, int64(12345), got.Builds[buildID].Size)
	require.Equal(t, checksum, got.Builds[buildID].Checksum)
	require.Equal(t, int64(67890), got.Builds[baseID].Size)

	// Frame data round-trip
	fd := got.Builds[buildID].FrameData
	require.NotNil(t, fd)
	require.Equal(t, storage.CompressionLZ4, fd.CompressionType())
	require.Equal(t, 2, fd.NumFrames())

	r, err := fd.LocateCompressed(0)
	require.NoError(t, err)
	require.Equal(t, int64(0), r.Offset)
	require.Equal(t, 1024, r.Length)

	r, err = fd.LocateCompressed(2048)
	require.NoError(t, err)
	require.Equal(t, int64(1024), r.Offset)
	require.Equal(t, 900, r.Length)

	// baseID has no frames
	require.Nil(t, got.Builds[baseID].FrameData)
}

func TestSerializeDeserialize_V4_Zstd(t *testing.T) {
	t.Parallel()

	buildID := uuid.New()
	metadata := &Metadata{
		Version:     4,
		BlockSize:   4096,
		Size:        4096,
		Generation:  0,
		BuildId:     buildID,
		BaseBuildId: buildID,
	}

	mappings := []BuildMap{
		{
			Offset:             0,
			Length:             4096,
			BuildId:            buildID,
			BuildStorageOffset: 8192,
		},
	}

	h, err := NewHeader(metadata, mappings)
	require.NoError(t, err)
	// 3 frames; only the third [8192, 12288) overlaps the mapping.
	h.Builds = map[uuid.UUID]BuildData{
		buildID: {
			FrameData: storage.NewFullFrameTable(storage.CompressionZstd, []storage.FrameSize{
				{U: 4096, C: 2000},
				{U: 4096, C: 3000},
				{U: 4096, C: 3500},
			}).Table(),
		},
	}

	data, err := SerializeHeader(h)
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	require.Equal(t, 1, got.Mapping.Len())
	require.Equal(t, uint64(8192), got.Mapping.At(0).BuildStorageOffset)

	require.Len(t, got.Builds, 1)
	fd := got.Builds[buildID].FrameData
	require.NotNil(t, fd)
	require.Equal(t, storage.CompressionZstd, fd.CompressionType())
	require.Equal(t, 1, fd.NumFrames())

	r, err := fd.LocateCompressed(8192)
	require.NoError(t, err)
	require.Equal(t, int64(2000+3000), r.Offset)
	require.Equal(t, 3500, r.Length)
}

func TestSerializeDeserialize_V4_NoFrames(t *testing.T) {
	t.Parallel()

	buildID := uuid.New()
	baseID := uuid.New()
	metadata := &Metadata{
		Version:     4,
		BlockSize:   4096,
		Size:        8192,
		Generation:  0,
		BuildId:     buildID,
		BaseBuildId: buildID,
	}

	mappings := []BuildMap{
		{
			Offset:             0,
			Length:             4096,
			BuildId:            buildID,
			BuildStorageOffset: 0,
		},
		{
			Offset:             4096,
			Length:             4096,
			BuildId:            baseID,
			BuildStorageOffset: 0,
		},
	}

	h, err := NewHeader(metadata, mappings)
	require.NoError(t, err)

	data, err := SerializeHeader(h)
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	require.Equal(t, 2, got.Mapping.Len())
	require.Nil(t, got.Builds)
}

func TestSerializeDeserialize_V4_ManyFrames(t *testing.T) {
	t.Parallel()

	buildID := uuid.New()
	const numFrames = 1000
	frames := make([]storage.FrameSize, numFrames)
	for i := range frames {
		frames[i] = storage.FrameSize{U: 4096, C: int32(2000 + i)}
	}

	metadata := &Metadata{
		Version:     4,
		BlockSize:   4096,
		Size:        4096 * numFrames,
		Generation:  0,
		BuildId:     buildID,
		BaseBuildId: buildID,
	}

	mappings := []BuildMap{
		{
			Offset:             0,
			Length:             4096 * numFrames,
			BuildId:            buildID,
			BuildStorageOffset: 0,
		},
	}

	h, err := NewHeader(metadata, mappings)
	require.NoError(t, err)
	h.Builds = map[uuid.UUID]BuildData{
		buildID: {FrameData: storage.NewFullFrameTable(storage.CompressionLZ4, frames).Table()},
	}

	data, err := SerializeHeader(h)
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	require.Equal(t, 1, got.Mapping.Len())
	require.NotNil(t, got.Builds)
	fd := got.Builds[buildID].FrameData
	require.NotNil(t, fd)
	require.Equal(t, numFrames, fd.NumFrames())

	r, err := fd.LocateCompressed(0)
	require.NoError(t, err)
	require.Equal(t, int64(0), r.Offset)
	require.Equal(t, 2000, r.Length)

	r, err = fd.LocateCompressed(int64(4096 * (numFrames - 1)))
	require.NoError(t, err)
	require.Equal(t, 2000+numFrames-1, r.Length)
}

func TestSerializeDeserialize_V4_NoBuilds(t *testing.T) {
	t.Parallel()

	buildID := uuid.New()
	metadata := &Metadata{
		Version:     4,
		BlockSize:   4096,
		Size:        4096,
		Generation:  0,
		BuildId:     buildID,
		BaseBuildId: buildID,
	}

	mappings := []BuildMap{
		{
			Offset:  0,
			Length:  4096,
			BuildId: buildID,
		},
	}

	h, err := NewHeader(metadata, mappings)
	require.NoError(t, err)
	// No Builds set (nil map)

	data, err := SerializeHeader(h)
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	require.Equal(t, 1, got.Mapping.Len())
	require.Nil(t, got.Builds)
}

func TestReadV4BuildsSection_RejectsOversizedBuildCount(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, uint32(2)))

	_, err := readV4BuildsSection(bytes.NewReader(buf.Bytes()))
	require.ErrorContains(t, err, "build count 2 exceeds remaining")
}

func TestDeserializeV4_RejectsOversizedMappingCount(t *testing.T) {
	t.Parallel()

	var block bytes.Buffer
	require.NoError(t, binary.Write(&block, binary.LittleEndian, uint32(0))) // builds
	require.NoError(t, binary.Write(&block, binary.LittleEndian, uint32(2))) // mappings

	compressed, err := compressLZ4(block.Bytes())
	require.NoError(t, err)

	metadata := &Metadata{
		Version:     MetadataVersionV4,
		BlockSize:   4096,
		Size:        4096,
		BuildId:     uuid.New(),
		BaseBuildId: uuid.New(),
	}
	var meta bytes.Buffer
	require.NoError(t, binary.Write(&meta, binary.LittleEndian, metadata))
	data := make([]byte, metadataSize+v4FlagsLen+v4SizePrefixLen+len(compressed))
	copy(data[:metadataSize], meta.Bytes())
	binary.LittleEndian.PutUint32(data[metadataSize+v4FlagsLen:], uint32(block.Len()))
	copy(data[metadataSize+v4FlagsLen+v4SizePrefixLen:], compressed)

	_, err = DeserializeBytes(data)
	require.ErrorContains(t, err, "mapping count 2 exceeds remaining")
}

func TestSerializeDeserialize_V4_MultiBuild_LocateCompressed(t *testing.T) {
	t.Parallel()

	buildA := uuid.New()
	buildB := uuid.New()

	// Build A: 3 frames, each 4096 uncompressed.
	//   Frame 0: U=[0,4096)   C=[0,1000)
	//   Frame 1: U=[4096,8192) C=[1000,2800)
	//   Frame 2: U=[8192,12288) C=[2800,5100)
	ftA := storage.NewFullFrameTable(storage.CompressionZstd, []storage.FrameSize{
		{U: 4096, C: 1000},
		{U: 4096, C: 1800},
		{U: 4096, C: 2300},
	})

	// Build B: 2 frames, each 4096 uncompressed.
	//   Frame 0: U=[0,4096)   C=[0,500)
	//   Frame 1: U=[4096,8192) C=[500,1700)
	ftB := storage.NewFullFrameTable(storage.CompressionLZ4, []storage.FrameSize{
		{U: 4096, C: 500},
		{U: 4096, C: 1200},
	})

	// Virtual layout (20480 bytes total):
	//   [0,4096)     → buildA offset 0
	//   [4096,12288) → buildB offset 0 (8192 bytes = both frames of B)
	//   [12288,20480)→ buildA offset 4096 (8192 bytes = frames 1..2 of A)
	metadata := &Metadata{
		Version:     4,
		BlockSize:   4096,
		Size:        20480,
		Generation:  2,
		BuildId:     buildA,
		BaseBuildId: buildA,
	}

	mappings := []BuildMap{
		{Offset: 0, Length: 4096, BuildId: buildA, BuildStorageOffset: 0},
		{Offset: 4096, Length: 8192, BuildId: buildB, BuildStorageOffset: 0},
		{Offset: 12288, Length: 8192, BuildId: buildA, BuildStorageOffset: 4096},
	}

	checksumA := sha256.Sum256([]byte("build-a"))
	checksumB := sha256.Sum256([]byte("build-b"))

	h, err := NewHeader(metadata, mappings)
	require.NoError(t, err)
	h.Builds = map[uuid.UUID]BuildData{
		buildA: {Size: 12288, Checksum: checksumA, FrameData: ftA.Table()},
		buildB: {Size: 8192, Checksum: checksumB, FrameData: ftB.Table()},
	}

	data, err := SerializeHeader(h)
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	require.Equal(t, uint64(4), got.Metadata.Version)
	require.Equal(t, 3, got.Mapping.Len())
	require.Len(t, got.Builds, 2)

	// Verify checksums round-trip.
	require.Equal(t, checksumA, got.Builds[buildA].Checksum)
	require.Equal(t, checksumB, got.Builds[buildB].Checksum)

	// --- Build A frame lookups via GetBuildFrameData ---
	fdA := got.GetBuildFrameData(buildA)
	require.NotNil(t, fdA)
	require.Equal(t, storage.CompressionZstd, fdA.CompressionType())
	// All 3 frames should survive trimming: frame 0 referenced by mapping 0,
	// frames 1-2 referenced by mapping 2.
	require.Equal(t, 3, fdA.NumFrames())

	// Frame 0 of A: U=0, C offset=0, C length=1000.
	r, err := fdA.LocateCompressed(0)
	require.NoError(t, err)
	require.Equal(t, int64(0), r.Offset)
	require.Equal(t, 1000, r.Length)

	// Frame 1 of A: U=4096, C offset=1000, C length=1800.
	r, err = fdA.LocateCompressed(4096)
	require.NoError(t, err)
	require.Equal(t, int64(1000), r.Offset)
	require.Equal(t, 1800, r.Length)

	// Frame 2 of A: U=8192, C offset=2800, C length=2300.
	r, err = fdA.LocateCompressed(8192)
	require.NoError(t, err)
	require.Equal(t, int64(2800), r.Offset)
	require.Equal(t, 2300, r.Length)

	// --- Build B frame lookups via GetBuildFrameData ---
	fdB := got.GetBuildFrameData(buildB)
	require.NotNil(t, fdB)
	require.Equal(t, storage.CompressionLZ4, fdB.CompressionType())
	require.Equal(t, 2, fdB.NumFrames())

	// Frame 0 of B: U=0, C offset=0, C length=500.
	r, err = fdB.LocateCompressed(0)
	require.NoError(t, err)
	require.Equal(t, int64(0), r.Offset)
	require.Equal(t, 500, r.Length)

	// Frame 1 of B: U=4096, C offset=500, C length=1200.
	r, err = fdB.LocateCompressed(4096)
	require.NoError(t, err)
	require.Equal(t, int64(500), r.Offset)
	require.Equal(t, 1200, r.Length)

	// Beyond end of B's frames.
	_, err = fdB.LocateCompressed(8192)
	require.Error(t, err)
}

func TestSerializeDeserialize_V4_TrimmedOffsets_Error(t *testing.T) {
	t.Parallel()

	buildID := uuid.New()

	// 4 frames, each 4096 uncompressed.
	ft := storage.NewFullFrameTable(storage.CompressionZstd, []storage.FrameSize{
		{U: 4096, C: 2000},
		{U: 4096, C: 3000},
		{U: 4096, C: 3500},
		{U: 4096, C: 1800},
	})

	metadata := &Metadata{
		Version:     4,
		BlockSize:   4096,
		Size:        4096,
		Generation:  0,
		BuildId:     buildID,
		BaseBuildId: buildID,
	}

	// Mapping references only frame 2 (BuildStorageOffset=8192, Length=4096).
	// Frames 0, 1, and 3 should be trimmed away.
	mappings := []BuildMap{
		{
			Offset:             0,
			Length:             4096,
			BuildId:            buildID,
			BuildStorageOffset: 8192,
		},
	}

	h, err := NewHeader(metadata, mappings)
	require.NoError(t, err)
	h.Builds = map[uuid.UUID]BuildData{
		buildID: {FrameData: ft.Table()},
	}

	data, err := SerializeHeader(h)
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	fd := got.Builds[buildID].FrameData
	require.NotNil(t, fd)
	require.Equal(t, 1, fd.NumFrames(), "only frame 2 should survive trimming")

	// The surviving frame covers U=[8192,12288). Lookup should succeed.
	r, err := fd.LocateCompressed(8192)
	require.NoError(t, err)
	require.Equal(t, int64(5000), r.Offset)
	require.Equal(t, 3500, r.Length)

	// Trimmed offsets should return errors.
	_, err = fd.LocateCompressed(0)
	require.Error(t, err, "frame at offset 0 was trimmed, should error")

	_, err = fd.LocateCompressed(4096)
	require.Error(t, err, "frame at offset 4096 was trimmed, should error")

	_, err = fd.LocateCompressed(12288)
	require.Error(t, err, "frame at offset 12288 was trimmed, should error")

	// Completely beyond original range.
	_, err = fd.LocateCompressed(16384)
	require.Error(t, err, "offset beyond all frames should error")
}

func TestFrameTable_LocateCompressed(t *testing.T) {
	t.Parallel()

	fd := storage.NewFullFrameTable(storage.CompressionZstd, []storage.FrameSize{
		{U: 2048, C: 1024},
		{U: 2048, C: 900},
		{U: 4096, C: 3500},
	}).Table()

	// Frame 0: U=[0,2048), C=[0,1024)
	r, err := fd.LocateCompressed(0)
	require.NoError(t, err)
	require.Equal(t, int64(0), r.Offset)
	require.Equal(t, 1024, r.Length)

	// A mid-frame offset is rejected: it must be frame-aligned via
	// LocateUncompressed, otherwise the whole frame would be mis-served from
	// its start.
	_, err = fd.LocateCompressed(2047)
	require.Error(t, err)
	require.Contains(t, err.Error(), "frame-aligned")

	// Frame 1: U=[2048,4096), C=[1024,1924)
	r, err = fd.LocateCompressed(2048)
	require.NoError(t, err)
	require.Equal(t, int64(1024), r.Offset)
	require.Equal(t, 900, r.Length)

	// Frame 2: U=[4096,8192), C=[1924,5424)
	r, err = fd.LocateCompressed(4096)
	require.NoError(t, err)
	require.Equal(t, int64(1924), r.Offset)
	require.Equal(t, 3500, r.Length)

	// Beyond end
	_, err = fd.LocateCompressed(8192)
	require.Error(t, err)
}

func TestFrameTable_LocateUncompressed(t *testing.T) {
	t.Parallel()

	fd := storage.NewFullFrameTable(storage.CompressionZstd, []storage.FrameSize{
		{U: 2048, C: 1024},
		{U: 4096, C: 3500},
	}).Table()

	// Frame 0: U=[0,2048)
	r, err := fd.LocateUncompressed(0)
	require.NoError(t, err)
	require.Equal(t, int64(0), r.Offset)
	require.Equal(t, 2048, r.Length)

	// Frame 1: U=[2048,6144)
	r, err = fd.LocateUncompressed(2048)
	require.NoError(t, err)
	require.Equal(t, int64(2048), r.Offset)
	require.Equal(t, 4096, r.Length)

	// Beyond end
	_, err = fd.LocateUncompressed(6144)
	require.Error(t, err)
}

func TestSerializeDeserialize_V4_SparseTrimming(t *testing.T) {
	t.Parallel()

	buildID := uuid.New()
	otherID := uuid.New()

	ft := storage.NewFullFrameTable(storage.CompressionLZ4, []storage.FrameSize{
		{U: 4096, C: 2000},
		{U: 4096, C: 3000},
		{U: 4096, C: 2500},
		{U: 4096, C: 1800},
	})

	metadata := &Metadata{
		Version:     4,
		BlockSize:   4096,
		Size:        4096 * 4,
		Generation:  0,
		BuildId:     buildID,
		BaseBuildId: buildID,
	}

	// Mapping only references frames 0 and 3 (gap at 1,2 due to otherID).
	mappings := []BuildMap{
		{Offset: 0, Length: 4096, BuildId: buildID, BuildStorageOffset: 0},
		{Offset: 4096, Length: 8192, BuildId: otherID, BuildStorageOffset: 0},
		{Offset: 12288, Length: 4096, BuildId: buildID, BuildStorageOffset: 12288},
	}

	h, err := NewHeader(metadata, mappings)
	require.NoError(t, err)
	h.Builds = map[uuid.UUID]BuildData{
		buildID: {FrameData: ft.Table(), Size: 16384},
		otherID: {Size: 8192},
	}

	data, err := SerializeHeader(h)
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	gotFT := got.Builds[buildID].FrameData
	require.NotNil(t, gotFT)
	require.Equal(t, 2, gotFT.NumFrames())

	// Frame 0
	r, err := gotFT.LocateCompressed(0)
	require.NoError(t, err)
	require.Equal(t, int64(0), r.Offset)
	require.Equal(t, 2000, r.Length)

	// Frame 3
	r, err = gotFT.LocateCompressed(12288)
	require.NoError(t, err)
	require.Equal(t, int64(2000+3000+2500), r.Offset)
	require.Equal(t, 1800, r.Length)

	// Gap
	_, err = gotFT.LocateCompressed(4096)
	require.Error(t, err)
}

func TestSerializeDeserialize_V4_Uncompressed_SelfEntry(t *testing.T) {
	t.Parallel()

	buildID := uuid.New()
	metadata := &Metadata{
		Version:     MetadataVersionV4,
		BlockSize:   4096,
		Size:        8192,
		Generation:  0,
		BuildId:     buildID,
		BaseBuildId: buildID,
	}

	mappings := []BuildMap{
		{Offset: 0, Length: 8192, BuildId: buildID, BuildStorageOffset: 0},
	}

	h, err := NewHeader(metadata, mappings)
	require.NoError(t, err)
	h.Builds = map[uuid.UUID]BuildData{
		buildID: {},
	}

	data, err := SerializeHeader(h)
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	require.Equal(t, uint64(MetadataVersionV4), got.Metadata.Version)
	require.Len(t, got.Builds, 1)
	require.Contains(t, got.Builds, buildID)
	require.Equal(t, storage.UncompressedFrameTable, got.GetBuildFrameData(buildID))
}

// Layered chain V4-uncompressed (self) → V4-compressed (mid) → V4-uncompressed
// (older) → V3 ancestor → V3 ancestor: V4 ancestors round-trip via Builds, V3
// ancestors stay absent.
func TestSerializeDeserialize_V4_MixedChain(t *testing.T) {
	t.Parallel()

	selfID := uuid.New()
	midID := uuid.New()
	olderID := uuid.New()
	v3aID := uuid.New()
	v3bID := uuid.New()

	midFT := storage.NewFullFrameTable(storage.CompressionZstd, []storage.FrameSize{
		{U: 4096, C: 1234},
	})

	metadata := &Metadata{
		Version:     MetadataVersionV4,
		BlockSize:   4096,
		Size:        4096 * 5,
		Generation:  4,
		BuildId:     selfID,
		BaseBuildId: v3bID,
	}

	mappings := []BuildMap{
		{Offset: 0, Length: 4096, BuildId: selfID, BuildStorageOffset: 0},
		{Offset: 4096, Length: 4096, BuildId: midID, BuildStorageOffset: 0},
		{Offset: 8192, Length: 4096, BuildId: olderID, BuildStorageOffset: 0},
		{Offset: 12288, Length: 4096, BuildId: v3aID, BuildStorageOffset: 0},
		{Offset: 16384, Length: 4096, BuildId: v3bID, BuildStorageOffset: 0},
	}

	h, err := NewHeader(metadata, mappings)
	require.NoError(t, err)
	h.Builds = map[uuid.UUID]BuildData{
		selfID:  {},
		midID:   {Size: 4096, FrameData: midFT.Table()},
		olderID: {Size: 4096},
	}

	data, err := SerializeHeader(h)
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	require.Equal(t, uint64(MetadataVersionV4), got.Metadata.Version)
	require.Equal(t, 5, got.Mapping.Len())
	require.Len(t, got.Builds, 3)

	require.Equal(t, storage.UncompressedFrameTable, got.GetBuildFrameData(selfID))
	require.Equal(t, storage.UncompressedFrameTable, got.GetBuildFrameData(olderID))

	gotMidFT := got.GetBuildFrameData(midID)
	require.NotNil(t, gotMidFT)
	require.Equal(t, storage.CompressionZstd, gotMidFT.CompressionType())
	require.Equal(t, 1, gotMidFT.NumFrames())

	_, hasV3a := got.Builds[v3aID]
	require.False(t, hasV3a)
	_, hasV3b := got.Builds[v3bID]
	require.False(t, hasV3b)

	require.Equal(t, selfID, got.Mapping.At(0).BuildId)
	require.Equal(t, midID, got.Mapping.At(1).BuildId)
	require.Equal(t, olderID, got.Mapping.At(2).BuildId)
	require.Equal(t, v3aID, got.Mapping.At(3).BuildId)
	require.Equal(t, v3bID, got.Mapping.At(4).BuildId)
}

// Layered chain with a compressed self entry:
// C-v4 (self) → U-v4 (mid) → C-v4 (older) → V3 → V3. After a serialize round
// trip, every virtual offset resolves to the right build via GetShiftedMapping
// and carries the expected compression (FrameData present iff compressed).
func TestSerializeDeserialize_V4_CompressedSelfChain(t *testing.T) {
	t.Parallel()

	selfID := uuid.New()
	midID := uuid.New()
	olderID := uuid.New()
	v3aID := uuid.New()
	v3bID := uuid.New()

	selfFT := storage.NewFullFrameTable(storage.CompressionZstd, []storage.FrameSize{
		{U: 4096, C: 1100},
		{U: 4096, C: 1200},
	})
	olderFT := storage.NewFullFrameTable(storage.CompressionZstd, []storage.FrameSize{
		{U: 4096, C: 1300},
	})

	metadata := &Metadata{
		Version:     MetadataVersionV4,
		BlockSize:   4096,
		Size:        4096 * 6,
		Generation:  4,
		BuildId:     selfID,
		BaseBuildId: v3bID,
	}

	// self spans two blocks to exercise the intra-build shift.
	mappings := []BuildMap{
		{Offset: 0, Length: 8192, BuildId: selfID, BuildStorageOffset: 0},
		{Offset: 8192, Length: 4096, BuildId: midID, BuildStorageOffset: 0},
		{Offset: 12288, Length: 4096, BuildId: olderID, BuildStorageOffset: 0},
		{Offset: 16384, Length: 4096, BuildId: v3aID, BuildStorageOffset: 0},
		{Offset: 20480, Length: 4096, BuildId: v3bID, BuildStorageOffset: 0},
	}

	h, err := NewHeader(metadata, mappings)
	require.NoError(t, err)
	h.Builds = map[uuid.UUID]BuildData{
		selfID:  {Size: 8192, FrameData: selfFT.Table()},
		midID:   {Size: 4096},
		olderID: {Size: 4096, FrameData: olderFT.Table()},
	}

	data, err := SerializeHeader(h)
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	require.Equal(t, uint64(MetadataVersionV4), got.Metadata.Version)
	require.Len(t, got.Builds, 3)

	// Walk every virtual block offset and check it resolves to the owning
	// build with the expected compression.
	cases := []struct {
		offset     int64
		wantBuild  uuid.UUID
		wantOffset uint64
		compressed bool
	}{
		{0, selfID, 0, true},
		{4096, selfID, 4096, true}, // shifted read inside self
		{8192, midID, 0, false},    // uncompressed V4
		{12288, olderID, 0, true},  // compressed ancestor
		{16384, v3aID, 0, false},   // V3 ancestor, no Builds entry
		{20480, v3bID, 0, false},   // V3 ancestor, no Builds entry
	}
	for _, tc := range cases {
		m, err := got.GetShiftedMapping(t.Context(), tc.offset)
		require.NoError(t, err, "offset %d", tc.offset)
		require.Equal(t, tc.wantBuild, m.BuildId, "offset %d build", tc.offset)
		require.Equal(t, tc.wantOffset, m.Offset, "offset %d storage offset", tc.offset)

		ct := got.GetBuildFrameData(m.BuildId).CompressionType()
		if tc.compressed {
			require.Equal(t, storage.CompressionZstd, ct, "offset %d", tc.offset)
		} else {
			require.Equal(t, storage.CompressionNone, ct, "offset %d", tc.offset)
		}
	}

	// V3 ancestors must not have leaked into Builds.
	_, hasV3a := got.Builds[v3aID]
	require.False(t, hasV3a)
	_, hasV3b := got.Builds[v3bID]
	require.False(t, hasV3b)
}

func TestDeserialize_V3_StaysV3(t *testing.T) {
	t.Parallel()

	buildID := uuid.New()
	metadata := &Metadata{
		Version:     3,
		BlockSize:   4096,
		Size:        4096,
		Generation:  0,
		BuildId:     buildID,
		BaseBuildId: buildID,
	}
	mappings := []BuildMap{{Offset: 0, Length: 4096, BuildId: buildID}}

	data, err := serializeV3(metadata, mustMapping(t, metadata.BlockSize, mappings))
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	require.Equal(t, uint64(3), got.Metadata.Version)
	require.Nil(t, got.Builds)
	require.Nil(t, got.GetBuildFrameData(buildID))
}

func TestFillMissingBuildsAsSentinels(t *testing.T) {
	t.Parallel()

	selfID := uuid.New()
	v3aID := uuid.New()
	v3bID := uuid.New()
	knownID := uuid.New()

	knownFT := storage.NewFullFrameTable(storage.CompressionZstd, []storage.FrameSize{{U: 4096, C: 1024}})

	meta := &Metadata{
		Version: MetadataVersionV5, BlockSize: 4096, Size: 4096 * 4,
		BuildId: selfID, BaseBuildId: v3bID,
	}
	h, err := NewHeader(meta, []BuildMap{
		{Offset: 0, Length: 4096, BuildId: selfID, BuildStorageOffset: 0},
		{Offset: 4096, Length: 4096, BuildId: knownID, BuildStorageOffset: 0},
		{Offset: 8192, Length: 4096, BuildId: v3aID, BuildStorageOffset: 0},
		{Offset: 12288, Length: 4096, BuildId: v3bID, BuildStorageOffset: 0},
	})
	require.NoError(t, err)
	h.Builds = map[uuid.UUID]BuildData{
		knownID: {Size: 4096, FrameData: knownFT.Table()},
	}

	backfillMissingV3UncompressedBuilds(h)

	require.Len(t, h.Builds, 2)
	require.Equal(t, BuildData{Size: 4096, FrameData: knownFT.Table()}, h.Builds[knownID])
	require.Equal(t, BuildData{}, h.Builds[selfID])
	require.NotContains(t, h.Builds, v3aID)
	require.NotContains(t, h.Builds, v3bID)
	// Absent must read back nil ("unknown"), never UncompressedFrameTable: nil
	// is what routes createDiff to the ancestor's own header instead of the
	// suffix-less object name.
	require.Nil(t, h.GetBuildFrameData(v3aID))
	require.Nil(t, h.GetBuildFrameData(v3bID))
}

// V3 is the only format the ancestor sentinels still apply to: it has no Builds
// section and no compression, so every gap is genuinely uncompressed.
func TestFillMissingBuildsAsSentinels_V3(t *testing.T) {
	t.Parallel()

	selfID := uuid.New()
	ancAID := uuid.New()
	ancBID := uuid.New()

	meta := &Metadata{
		Version: 3, BlockSize: 4096, Size: 4096 * 3,
		BuildId: selfID, BaseBuildId: ancBID,
	}
	h, err := NewHeader(meta, []BuildMap{
		{Offset: 0, Length: 4096, BuildId: selfID, BuildStorageOffset: 0},
		{Offset: 4096, Length: 4096, BuildId: ancAID, BuildStorageOffset: 0},
		{Offset: 8192, Length: 4096, BuildId: ancBID, BuildStorageOffset: 0},
	})
	require.NoError(t, err)

	backfillMissingV3UncompressedBuilds(h)

	require.Len(t, h.Builds, 3)
	require.Equal(t, BuildData{}, h.Builds[selfID])
	require.Equal(t, BuildData{}, h.Builds[ancAID])
	require.Equal(t, BuildData{}, h.Builds[ancBID])
	require.Equal(t, storage.UncompressedFrameTable, h.GetBuildFrameData(ancAID))
}

// A V4+ ancestor gap must survive a pause generation absent: ToDiffHeader
// copies only entries the parent has, the serializer writes only existing
// entries, and the next load's backfill leaves the gap alone. An entry
// fabricated at any step would persist "uncompressed" — the permanent 404.
func TestToDiffHeader_AncestorGapRoundTripsAbsent(t *testing.T) {
	t.Parallel()

	selfID := uuid.New()
	gapID := uuid.New()   // compressed ancestor whose entry was lost
	knownID := uuid.New() // ancestor with an intact entry
	childID := uuid.New()

	knownFT := storage.NewFullFrameTable(storage.CompressionZstd, []storage.FrameSize{{U: 4096, C: 1024}})

	meta := &Metadata{
		Version: MetadataVersionV5, BlockSize: 4096, Size: 4096 * 3,
		BuildId: selfID, BaseBuildId: gapID,
	}
	parent, err := NewHeader(meta, []BuildMap{
		{Offset: 0, Length: 4096, BuildId: selfID, BuildStorageOffset: 0},
		{Offset: 4096, Length: 4096, BuildId: knownID, BuildStorageOffset: 0},
		{Offset: 8192, Length: 4096, BuildId: gapID, BuildStorageOffset: 0},
	})
	require.NoError(t, err)
	parent.Builds = map[uuid.UUID]BuildData{
		selfID:  {Size: 4096},
		knownID: {Size: 4096, FrameData: knownFT.Table()},
	}

	// Pause: page 0 dirtied; the known and gap ranges are inherited untouched.
	dm := &DiffMetadata{Dirty: roaring.BitmapOf(0), Empty: roaring.New(), BlockSize: 4096}
	child, err := dm.ToDiffHeader(t.Context(), parent, childID)
	require.NoError(t, err)
	require.NotContains(t, child.Builds, gapID)
	require.Contains(t, child.Builds, knownID)

	// Upload adds the self entry and clears the in-flight flag.
	child.SetBuild(childID, BuildData{Size: 4096})
	child.IncompletePendingUpload = false

	data, err := SerializeHeader(child)
	require.NoError(t, err)
	got, err := DeserializeBytes(data)
	require.NoError(t, err)
	backfillMissingV3UncompressedBuilds(got) // what LoadHeader runs after deserialize

	// The gap ancestor still owns its range and still has no entry.
	m, err := got.GetShiftedMapping(t.Context(), 8192)
	require.NoError(t, err)
	require.Equal(t, gapID, m.BuildId)
	require.NotContains(t, got.Builds, gapID)
	require.Nil(t, got.GetBuildFrameData(gapID))

	require.Equal(t, storage.CompressionZstd, got.GetBuildFrameData(knownID).CompressionType())
}

func TestFillMissingBuildsAsSentinels_NilBuilds(t *testing.T) {
	t.Parallel()

	selfID := uuid.New()
	meta := &Metadata{
		Version: MetadataVersionV5, BlockSize: 4096, Size: 4096,
		BuildId: selfID, BaseBuildId: selfID,
	}
	h, err := NewHeader(meta, []BuildMap{
		{Offset: 0, Length: 4096, BuildId: selfID, BuildStorageOffset: 0},
	})
	require.NoError(t, err)
	require.Nil(t, h.Builds)

	backfillMissingV3UncompressedBuilds(h)

	require.NotNil(t, h.Builds)
	require.Equal(t, BuildData{}, h.Builds[selfID])
}

// TestDeserialize_LegacyGapKeepsExplicitLengths reads V3 and V4 headers whose
// mappings leave a gap, as headers predating NormalizeFixVersion may. The
// record-per-entry formats round-trip them unchanged, so the compact form must
// fall back to an explicit lengths column rather than derive them.
func TestDeserialize_LegacyGapKeepsExplicitLengths(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	a, b := uuid.New(), uuid.New()
	mappings := []BuildMap{
		{Offset: 0, Length: bs, BuildId: a, BuildStorageOffset: 0},
		{Offset: 2 * bs, Length: bs, BuildId: b, BuildStorageOffset: 0},
		{Offset: 3 * bs, Length: bs, BuildId: a, BuildStorageOffset: bs},
	}

	versions := map[string]uint64{"v2": 2, "v4": MetadataVersionV4}
	for name, version := range versions {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			meta := &Metadata{Version: version, BlockSize: bs, Size: 4 * bs, BuildId: a, BaseBuildId: b}
			h, err := NewHeader(meta, mappings)
			require.NoError(t, err)
			require.False(t, h.Mapping.Contiguous())
			if version >= MetadataVersionV4 {
				h.Builds = map[uuid.UUID]BuildData{a: {Size: int64(2 * bs)}, b: {Size: int64(bs)}}
			}

			data, err := SerializeHeader(h)
			require.NoError(t, err)
			got, err := DeserializeBytes(data)
			require.NoError(t, err)

			require.False(t, got.Mapping.Contiguous())
			require.Equal(t, mappings, got.Mapping.Slice())
			require.Error(t, got.Mapping.Validate(4*bs, PageSize))
		})
	}
}

// storedHeaderProvider serves data as the blob at path, for as many opens as
// the test makes.
func storedHeaderProvider(t *testing.T, path string, data []byte) storage.StorageProvider {
	t.Helper()

	provider := storage.NewMockStorageProvider(t)
	provider.EXPECT().OpenBlob(mock.Anything, path).RunAndReturn(func(context.Context, string) (storage.Blob, error) {
		blob := storage.NewMockBlob(t)
		blob.EXPECT().WriteTo(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, w io.Writer) (int64, error) {
			return io.Copy(w, bytes.NewReader(data))
		})

		return blob, nil
	})

	return provider
}

// LoadStoredHeader returns the Builds map exactly as serialized; LoadHeader,
// over the same bytes, still backfills the header's own missing entry. The
// fixture is the shape runV3 writes under a V4 parent: a V4 header whose
// mapping references its own build and whose Builds map has no entry for it.
func TestLoadStoredHeader_SkipsOwnBuildBackfill(t *testing.T) {
	t.Parallel()

	selfID := uuid.New()
	parentID := uuid.New()
	parentBD := BuildData{Size: 4096}

	h, err := NewHeader(&Metadata{
		Version: MetadataVersionV4, BlockSize: 4096, Size: 8192,
		BuildId: selfID, BaseBuildId: parentID,
	}, []BuildMap{
		{Offset: 0, Length: 4096, BuildId: selfID},
		{Offset: 4096, Length: 4096, BuildId: parentID},
	})
	require.NoError(t, err)
	h.Builds = map[uuid.UUID]BuildData{parentID: parentBD}
	data, err := SerializeHeader(h)
	require.NoError(t, err)

	const path = "build/memfile.header"
	provider := storedHeaderProvider(t, path, data)

	stored, n, err := LoadStoredHeader(t.Context(), provider, path)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, map[uuid.UUID]BuildData{parentID: parentBD}, stored.Builds)

	backfilled, _, err := LoadHeader(t.Context(), provider, path)
	require.NoError(t, err)
	require.Equal(t, map[uuid.UUID]BuildData{parentID: parentBD, selfID: {}}, backfilled.Builds)
}
