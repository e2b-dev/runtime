package header

import (
	"crypto/sha256"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

func v5Header(t *testing.T, metadata *Metadata, mappings []BuildMap, builds map[uuid.UUID]BuildData) *Header {
	t.Helper()
	metadata.Version = MetadataVersionV5
	h, err := NewHeader(metadata, mappings)
	require.NoError(t, err)
	h.Builds = builds

	return h
}

// TestV5_PageGranularUnderHugepage round-trips a memory-style header
// (BlockSize = 2 MiB) whose mappings are page-granular (4 KiB), the production
// case that the compact-mapping unit fix addresses.
func TestV5_PageGranularUnderHugepage(t *testing.T) {
	t.Parallel()

	const hugepage = uint64(2 << 20)
	a, b := uuid.New(), uuid.New()
	mappings := []BuildMap{
		{Offset: 0, Length: PageSize, BuildId: a, BuildStorageOffset: 0},
		{Offset: PageSize, Length: PageSize, BuildId: b, BuildStorageOffset: 0},
		{Offset: 2 * PageSize, Length: 2 * PageSize, BuildId: a, BuildStorageOffset: PageSize},
	}
	meta := &Metadata{BlockSize: hugepage, Size: 4 * PageSize, BuildId: a, BaseBuildId: b}
	h := v5Header(t, meta, mappings, map[uuid.UUID]BuildData{a: {Size: 100}, b: {Size: 50}})

	data, err := SerializeHeader(h)
	require.NoError(t, err)
	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	require.True(t, Equal(mappings, got.Mapping.Slice()))
	require.Equal(t, hugepage, got.Metadata.BlockSize)
}

func TestV5_RoundTrip(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	a := uuid.New()
	b := uuid.New()
	metadata := &Metadata{BlockSize: bs, Size: 6 * bs, Generation: 3, BuildId: a, BaseBuildId: b}
	mappings := []BuildMap{
		{Offset: 0, Length: 2 * bs, BuildId: a, BuildStorageOffset: 0},
		{Offset: 2 * bs, Length: bs, BuildId: uuid.Nil},
		{Offset: 3 * bs, Length: bs, BuildId: b, BuildStorageOffset: 0},
		{Offset: 4 * bs, Length: 2 * bs, BuildId: a, BuildStorageOffset: 2 * bs},
	}
	checksum := sha256.Sum256([]byte("a"))
	h := v5Header(t, metadata, mappings, map[uuid.UUID]BuildData{
		a: {Size: 100, Checksum: checksum, FrameData: storage.NewFullFrameTable(storage.CompressionZstd, []storage.FrameSize{{U: int32(bs), C: 1000}, {U: int32(2 * bs), C: 2000}}).Table()},
		b: {Size: 200},
	})

	data, err := SerializeHeader(h)
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	require.Equal(t, uint64(MetadataVersionV5), got.Metadata.Version)
	require.True(t, Equal(mappings, got.Mapping.Slice()))
	require.ElementsMatch(t, []uuid.UUID{a, b}, got.Mapping.Builds())
	require.Len(t, got.Builds, 2)
	require.Equal(t, int64(100), got.Builds[a].Size)
	require.Equal(t, checksum, got.Builds[a].Checksum)
	require.Equal(t, int64(200), got.Builds[b].Size)

	fd := got.Builds[a].FrameData
	require.NotNil(t, fd)
	require.Equal(t, storage.CompressionZstd, fd.CompressionType())
}

func TestV5_RoundTripNonAlignedSize(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	id := uuid.New()
	metadata := &Metadata{BlockSize: bs, Size: bs + 1, BuildId: id, BaseBuildId: id}

	h := v5Header(t, metadata, nil, nil)
	data, err := SerializeHeader(h)
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)
	require.Equal(t, metadata.Size, got.Metadata.Size)
	require.Equal(t, 2*bs, got.Mapping.At(0).Length)
}

// TestV5_MatchesV4Semantics serializes the same logical header as V4 and V5
// and asserts both deserialize to the same mappings and Builds.
func TestV5_MatchesV4Semantics(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	a := uuid.New()
	b := uuid.New()
	c := uuid.New()
	mappings := []BuildMap{
		{Offset: 0, Length: bs, BuildId: a, BuildStorageOffset: 0},
		{Offset: bs, Length: bs, BuildId: b, BuildStorageOffset: 0},
		{Offset: 2 * bs, Length: bs, BuildId: a, BuildStorageOffset: bs},
		{Offset: 3 * bs, Length: bs, BuildId: c, BuildStorageOffset: 0},
	}
	base := &Metadata{BlockSize: bs, Size: 4 * bs, Generation: 1, BuildId: a, BaseBuildId: c}
	builds := map[uuid.UUID]BuildData{a: {Size: 10}, b: {Size: 20}, c: {Size: 30}}

	metaV4 := *base
	metaV4.Version = MetadataVersionV4
	hV4, err := NewHeader(&metaV4, mappings)
	require.NoError(t, err)
	hV4.Builds = builds
	dataV4, err := SerializeHeader(hV4)
	require.NoError(t, err)
	gotV4, err := DeserializeBytes(dataV4)
	require.NoError(t, err)

	metaV5 := *base
	hV5 := v5Header(t, &metaV5, mappings, builds)
	dataV5, err := SerializeHeader(hV5)
	require.NoError(t, err)
	gotV5, err := DeserializeBytes(dataV5)
	require.NoError(t, err)

	require.True(t, Equal(gotV4.Mapping.Slice(), gotV5.Mapping.Slice()))
	require.Equal(t, gotV4.Builds, gotV5.Builds)
}

// TestV5_SmallerThanV4 asserts the headline win on a fragmented, alternating
// header: V5 serializes much smaller than V4.
func TestV5_SmallerThanV4(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	a := uuid.New()
	b := uuid.New()
	const runs = 50_000
	mappings := make([]BuildMap, 0, runs)
	var off, sa, sb uint64
	for i := range runs {
		id, so := a, sa
		if i%2 == 1 {
			id, so = b, sb
		}
		mappings = append(mappings, BuildMap{Offset: off, Length: bs, BuildId: id, BuildStorageOffset: so})
		off += bs
		if i%2 == 0 {
			sa += bs
		} else {
			sb += bs
		}
	}
	base := &Metadata{BlockSize: bs, Size: off, BuildId: a, BaseBuildId: b}
	builds := map[uuid.UUID]BuildData{a: {Size: int64(sa)}, b: {Size: int64(sb)}}

	metaV4 := *base
	metaV4.Version = MetadataVersionV4
	hV4, err := NewHeader(&metaV4, mappings)
	require.NoError(t, err)
	hV4.Builds = builds
	dataV4, err := SerializeHeader(hV4)
	require.NoError(t, err)

	metaV5 := *base
	hV5 := v5Header(t, &metaV5, mappings, builds)
	dataV5, err := SerializeHeader(hV5)
	require.NoError(t, err)

	t.Logf("v4=%d bytes, v5=%d bytes (%.1fx smaller)", len(dataV4), len(dataV5), float64(len(dataV4))/float64(len(dataV5)))
	assert.Less(t, len(dataV5), len(dataV4)/2, "v5 should be at least 2x smaller than v4 on fragmented headers")

	// And it round-trips.
	got, err := DeserializeBytes(dataV5)
	require.NoError(t, err)
	require.True(t, Equal(mappings, got.Mapping.Slice()))
}

// TestV5_ReconstructedColumnsExactlySized guards against cached headers
// retaining over-allocated mapping columns. Alternating mapped/nil entries
// force gap reconstruction, the path that previously over-allocated.
func TestV5_ReconstructedColumnsExactlySized(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	a := uuid.New()
	mappings := []BuildMap{
		{Offset: 0, Length: bs, BuildId: uuid.Nil},
		{Offset: bs, Length: bs, BuildId: a, BuildStorageOffset: 0},
		{Offset: 2 * bs, Length: bs, BuildId: uuid.Nil},
		{Offset: 3 * bs, Length: bs, BuildId: a, BuildStorageOffset: bs},
	}
	meta := &Metadata{BlockSize: bs, Size: 4 * bs, BuildId: a, BaseBuildId: a}
	h := v5Header(t, meta, mappings, map[uuid.UUID]BuildData{a: {Size: int64(2 * bs)}})

	data, err := SerializeHeader(h)
	require.NoError(t, err)
	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	m := got.Mapping
	assert.Equal(t, len(m.offsets), cap(m.offsets))
	assert.Equal(t, len(m.storage), cap(m.storage))
	assert.Equal(t, len(m.buildIdx8), cap(m.buildIdx8))
	assert.Nil(t, m.buildIdx16)
	assert.True(t, m.Contiguous(), "the reconstructed mapping is contiguous and stores no lengths column")
	assert.Nil(t, m.lengths)
}

func TestV5_RejectsOversizePrefix(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	id := uuid.New()
	meta := &Metadata{Version: MetadataVersionV5, BlockSize: bs, Size: bs, BuildId: id, BaseBuildId: id}
	h, err := NewHeader(meta, nil)
	require.NoError(t, err)
	data, err := SerializeHeader(h)
	require.NoError(t, err)

	// Corrupt the uncompressed-size prefix to exceed the cap.
	prefix := metadataSize + v4FlagsLen
	data[prefix] = 0xFF
	data[prefix+1] = 0xFF
	data[prefix+2] = 0xFF
	data[prefix+3] = 0xFF

	_, err = DeserializeBytes(data)
	require.ErrorContains(t, err, "exceeds cap")
}

// TestV5_RoundTripLargeSparse round-trips a large mapping whose nil regions
// are dropped on the wire and reconstructed on read, checking the result is
// entry-for-entry identical and stores no lengths column.
func TestV5_RoundTripLargeSparse(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	a, b := uuid.New(), uuid.New()
	const runs = 60_000
	mappings := make([]BuildMap, 0, runs)
	var off, sa, sb uint64
	for i := range runs {
		length := bs * uint64(1+i%3)
		switch i % 3 {
		case 0:
			mappings = append(mappings, BuildMap{Offset: off, Length: length, BuildId: a, BuildStorageOffset: sa})
			sa += length
		case 1:
			mappings = append(mappings, BuildMap{Offset: off, Length: length, BuildId: uuid.Nil})
		default:
			mappings = append(mappings, BuildMap{Offset: off, Length: length, BuildId: b, BuildStorageOffset: sb})
			sb += length
		}
		off += length
	}
	meta := &Metadata{BlockSize: bs, Size: off, BuildId: a, BaseBuildId: b}
	h := v5Header(t, meta, mappings, map[uuid.UUID]BuildData{a: {Size: int64(sa)}, b: {Size: int64(sb)}})
	require.True(t, h.Mapping.Contiguous())

	data, err := SerializeHeader(h)
	require.NoError(t, err)
	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	require.True(t, got.Mapping.Contiguous())
	require.Nil(t, got.Mapping.lengths)
	require.Equal(t, mappings, got.Mapping.Slice())
	require.Equal(t, h.Mapping.BytesByBuild(), got.Mapping.BytesByBuild())
	require.NoError(t, got.Mapping.Validate(off, PageSize))
}
