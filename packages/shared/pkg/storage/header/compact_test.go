package header

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestNewHeader_PageGranularMappingUnderHugepageBlockSize reproduces the
// production case: a memory file has BlockSize = 2 MiB (hugepage) but its diff
// mappings are page-granular (4 KiB), so the header carries 4 KiB-aligned
// offsets under a 2 MiB block size. The compact mapping must encode in PageSize
// units, not metadata.BlockSize, or header construction fails and bricks every
// sandbox create/resume.
func TestNewHeader_PageGranularMappingUnderHugepageBlockSize(t *testing.T) {
	t.Parallel()

	const hugepage = uint64(2 << 20) // 2 MiB memory block size
	a := uuid.New()
	b := uuid.New()

	// Page-granular (4 KiB) mappings, NOT aligned to the 2 MiB block size.
	mappings := []BuildMap{
		{Offset: 0, Length: PageSize, BuildId: a, BuildStorageOffset: 0},
		{Offset: PageSize, Length: PageSize, BuildId: b, BuildStorageOffset: 0},
		{Offset: 2 * PageSize, Length: 2 * PageSize, BuildId: a, BuildStorageOffset: PageSize},
	}
	meta := &Metadata{Version: MetadataVersionV4, BlockSize: hugepage, Size: 4 * PageSize, BuildId: a, BaseBuildId: b}

	h, err := NewHeader(meta, mappings)
	require.NoError(t, err, "page-granular mappings under a hugepage block size must be accepted")
	require.True(t, Equal(mappings, h.Mapping.Slice()))

	// And the offset lookup must resolve correctly at page granularity.
	m, err := h.GetShiftedMapping(t.Context(), PageSize)
	require.NoError(t, err)
	require.Equal(t, b, m.BuildId)
}

func TestNewMapping_RoundTrip(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	a := uuid.New()
	b := uuid.New()
	src := []BuildMap{
		{Offset: 0, Length: 2 * bs, BuildId: a, BuildStorageOffset: 0},
		{Offset: 2 * bs, Length: bs, BuildId: b, BuildStorageOffset: 0},
		{Offset: 3 * bs, Length: bs, BuildId: a, BuildStorageOffset: 2 * bs},
	}

	m, err := NewMapping(bs, src)
	require.NoError(t, err)
	require.Equal(t, len(src), m.Len())

	require.True(t, Equal(src, m.Slice()), "Slice must round-trip the input")
	for i, want := range src {
		require.Equal(t, want, m.At(i), "At(%d)", i)
	}

	// Builds deduplicated to {a, b}.
	require.ElementsMatch(t, []uuid.UUID{a, b}, m.Builds())
}

func TestNewMapping_RejectsUnaligned(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	id := uuid.New()

	_, err := NewMapping(bs, []BuildMap{{Offset: 123, Length: bs, BuildId: id}})
	require.ErrorContains(t, err, "offset")

	_, err = NewMapping(bs, []BuildMap{{Offset: 0, Length: 123, BuildId: id}})
	require.ErrorContains(t, err, "length")

	_, err = NewMapping(bs, []BuildMap{{Offset: 0, Length: bs, BuildId: id, BuildStorageOffset: 123}})
	require.ErrorContains(t, err, "build storage offset")

	_, err = NewMapping(0, []BuildMap{{Offset: 0, Length: bs, BuildId: id}})
	require.ErrorContains(t, err, "block size")
}

func TestMapping_SearchOffset(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	id := uuid.New()
	src := []BuildMap{
		{Offset: 0, Length: 2 * bs, BuildId: id},
		{Offset: 2 * bs, Length: 2 * bs, BuildId: id, BuildStorageOffset: 2 * bs},
		{Offset: 4 * bs, Length: bs, BuildId: id, BuildStorageOffset: 4 * bs},
	}
	m, err := NewMapping(bs, src)
	require.NoError(t, err)

	// SearchOffset must match sort.Search over the materialized offsets for
	// every page within range, including non-block-aligned probes.
	for off := int64(0); off < int64(5*bs); off += int64(PageSize) {
		want := 0
		for _, bm := range src {
			if int64(bm.Offset) > off {
				break
			}
			want++
		}
		require.Equal(t, want, m.SearchOffset(off), "off=%d", off)
	}

	require.Equal(t, 0, m.SearchOffset(-1))
}

func TestMapping_Validate(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	id := uuid.New()
	size := 4 * bs
	m, err := NewMapping(bs, []BuildMap{
		{Offset: 0, Length: 2 * bs, BuildId: id},
		{Offset: 2 * bs, Length: 2 * bs, BuildId: id, BuildStorageOffset: 2 * bs},
	})
	require.NoError(t, err)
	require.NoError(t, m.Validate(size, bs))

	// Wrong size is rejected.
	require.Error(t, m.Validate(size+bs, bs))

	// A gap is rejected.
	gap, err := NewMapping(bs, []BuildMap{
		{Offset: 0, Length: bs, BuildId: id},
		{Offset: 2 * bs, Length: bs, BuildId: id, BuildStorageOffset: 2 * bs},
	})
	require.NoError(t, err)
	require.Error(t, gap.Validate(3*bs, bs))
}

// columnBytes sums the backing arrays of the per-entry columns, the part of a
// Mapping that scales with its length.
func columnBytes(m Mapping) int {
	return cap(m.offsets)*4 + cap(m.lengths)*4 + cap(m.storage)*4 + cap(m.buildIdx8) + cap(m.buildIdx16)*2
}

func bytesByBuildFromSlice(src []BuildMap) map[uuid.UUID]uint64 {
	out := make(map[uuid.UUID]uint64)
	for _, bm := range src {
		if bm.BuildId != uuid.Nil {
			out[bm.BuildId] += bm.Length
		}
	}

	return out
}

// requireMappingMatches checks every read path of m against its source slice.
func requireMappingMatches(t *testing.T, src []BuildMap, m Mapping) {
	t.Helper()

	require.Equal(t, len(src), m.Len())
	require.Equal(t, src, m.Slice())
	for i, want := range src {
		require.Equal(t, want, m.At(i), "At(%d)", i)
	}
	i := 0
	for idx, got := range m.All() {
		require.Equal(t, i, idx)
		require.Equal(t, src[i], got, "All() entry %d", i)
		i++
	}
	require.Equal(t, len(src), i)
	require.Equal(t, bytesByBuildFromSlice(src), m.BytesByBuild())

	// SearchOffset must match sort.Search over the materialized offsets.
	last := src[len(src)-1]
	for off := int64(0); off <= int64(last.Offset+last.Length); off += int64(PageSize) {
		want := 0
		for _, bm := range src {
			if int64(bm.Offset) > off {
				break
			}
			want++
		}
		require.Equal(t, want, m.SearchOffset(off), "SearchOffset(%d)", off)
	}
}

func TestNewMapping_ContiguousDerivesLengths(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	a := uuid.New()
	b := uuid.New()
	// Mixed run lengths, a nil-build gap, a zero-length entry, and an unequal
	// last run: every length must come back from the offsets column alone.
	src := []BuildMap{
		{Offset: 0, Length: 2 * bs, BuildId: a, BuildStorageOffset: 0},
		{Offset: 2 * bs, Length: bs, BuildId: uuid.Nil},
		{Offset: 3 * bs, Length: 0, BuildId: b, BuildStorageOffset: 5 * bs},
		{Offset: 3 * bs, Length: 3 * bs, BuildId: b, BuildStorageOffset: 0},
		{Offset: 6 * bs, Length: bs, BuildId: a, BuildStorageOffset: 2 * bs},
	}

	m, err := NewMapping(bs, src)
	require.NoError(t, err)
	require.True(t, m.Contiguous())
	require.Nil(t, m.lengths, "a contiguous mapping must not store a lengths column")
	requireMappingMatches(t, src, m)
	require.NoError(t, m.Validate(7*bs, bs))
}

func TestNewMapping_NonContiguousKeepsLengths(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	a := uuid.New()
	b := uuid.New()

	tests := map[string][]BuildMap{
		"gap": {
			{Offset: 0, Length: bs, BuildId: a, BuildStorageOffset: 0},
			{Offset: 2 * bs, Length: bs, BuildId: b, BuildStorageOffset: 0},
			{Offset: 3 * bs, Length: bs, BuildId: a, BuildStorageOffset: bs},
		},
		"overlap": {
			{Offset: 0, Length: 2 * bs, BuildId: a, BuildStorageOffset: 0},
			{Offset: bs, Length: 2 * bs, BuildId: b, BuildStorageOffset: 0},
			{Offset: 3 * bs, Length: bs, BuildId: a, BuildStorageOffset: 2 * bs},
		},
		"gap before last entry": {
			{Offset: 0, Length: bs, BuildId: a, BuildStorageOffset: 0},
			{Offset: bs, Length: bs, BuildId: b, BuildStorageOffset: 0},
			{Offset: 3 * bs, Length: bs, BuildId: a, BuildStorageOffset: bs},
		},
	}
	for name, src := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			m, err := NewMapping(bs, src)
			require.NoError(t, err)
			require.False(t, m.Contiguous())
			require.Len(t, m.lengths, len(src), "a non-contiguous mapping keeps an explicit lengths column")
			requireMappingMatches(t, src, m)
			require.Error(t, m.Validate(4*bs, bs))
		})
	}
}

func TestMapping_Validate_ContiguousForm(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	id := uuid.New()

	// Contiguous but not starting at offset 0: derived lengths still reject it.
	shifted, err := NewMapping(bs, []BuildMap{
		{Offset: bs, Length: bs, BuildId: id},
		{Offset: 2 * bs, Length: bs, BuildId: id, BuildStorageOffset: bs},
	})
	require.NoError(t, err)
	require.True(t, shifted.Contiguous())
	require.ErrorContains(t, shifted.Validate(3*bs, bs), "expected offset 0")

	// Contiguous but overshooting the size is caught at the last entry.
	full, err := NewMapping(bs, []BuildMap{
		{Offset: 0, Length: bs, BuildId: id},
		{Offset: bs, Length: 2 * bs, BuildId: id, BuildStorageOffset: bs},
	})
	require.NoError(t, err)
	require.True(t, full.Contiguous())
	require.NoError(t, full.Validate(3*bs, bs))
	require.ErrorContains(t, full.Validate(2*bs, bs), "> 8192 (size)")
	require.ErrorContains(t, full.Validate(4*bs, bs), "total 12288 != size 16384")
}

// TestMapping_ColumnBytesPerEntry pins the retained footprint with the one-byte
// build index every real header gets: 9 bytes per entry for the contiguous form
// every pause and resume produces, 13 for the explicit-lengths fallback. The
// two-byte index a header with more than 255 builds falls back to adds one.
func TestMapping_ColumnBytesPerEntry(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	a := uuid.New()
	b := uuid.New()
	const n = 100_000
	src := make([]BuildMap, n)
	var off, sa, sb uint64
	for i := range src {
		id, so := a, sa
		if i%2 == 1 {
			id, so = b, sb
		}
		src[i] = BuildMap{Offset: off, Length: bs, BuildId: id, BuildStorageOffset: so}
		off += bs
		if i%2 == 0 {
			sa += bs
		} else {
			sb += bs
		}
	}

	contiguous, err := NewMapping(bs, src)
	require.NoError(t, err)
	require.True(t, contiguous.Contiguous())
	require.NotNil(t, contiguous.buildIdx8, "two builds fit the one-byte index column")
	require.Nil(t, contiguous.buildIdx16)
	require.Equal(t, 9*n, columnBytes(contiguous))
	require.Equal(t, 9*n+2*16, contiguous.ByteSize())
	t.Logf("contiguous: %d entries, %d column bytes, %d B/entry", n, columnBytes(contiguous), columnBytes(contiguous)/n)

	// Open a one-block gap after the first entry to force the fallback.
	gapped := make([]BuildMap, n)
	copy(gapped, src)
	for i := 1; i < n; i++ {
		gapped[i].Offset += bs
	}
	fallback, err := NewMapping(bs, gapped)
	require.NoError(t, err)
	require.False(t, fallback.Contiguous())
	require.Equal(t, 13*n, columnBytes(fallback))
	require.Equal(t, 13*n+2*16, fallback.ByteSize())
	t.Logf("explicit lengths: %d entries, %d column bytes, %d B/entry", n, columnBytes(fallback), columnBytes(fallback)/n)
}

// A header referencing more builds than one byte can address keeps the wide
// index column, and every entry still resolves to the right build.
func TestMapping_WideBuildIndexPastByteRange(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	const nBuilds = maxBuilds8 + 3
	builds := make([]uuid.UUID, nBuilds)
	for i := range builds {
		builds[i] = uuid.New()
	}
	// Interleave empty regions so both sentinels are exercised.
	src := make([]BuildMap, 0, 2*nBuilds)
	var off uint64
	for i, id := range builds {
		src = append(src, BuildMap{Offset: off, Length: bs, BuildId: id, BuildStorageOffset: uint64(i) * bs})
		off += bs
		if i%7 == 0 {
			src = append(src, BuildMap{Offset: off, Length: bs, BuildId: uuid.Nil})
			off += bs
		}
	}

	m, err := NewMapping(bs, src)
	require.NoError(t, err)
	require.Nil(t, m.buildIdx8)
	require.Len(t, m.buildIdx16, len(src))
	require.Equal(t, 10*len(src), columnBytes(m))
	require.Equal(t, 10*len(src)+nBuilds*16, m.ByteSize())
	require.Equal(t, src, m.Slice())

	// At the byte limit exactly, the narrow column is still used.
	narrowSrc := src[:0:0]
	off = 0
	for i := range maxBuilds8 {
		narrowSrc = append(narrowSrc, BuildMap{Offset: off, Length: bs, BuildId: builds[i], BuildStorageOffset: uint64(i) * bs})
		off += bs
	}
	narrowSrc = append(narrowSrc, BuildMap{Offset: off, Length: bs, BuildId: uuid.Nil})
	narrow, err := NewMapping(bs, narrowSrc)
	require.NoError(t, err)
	require.Len(t, narrow.buildIdx8, len(narrowSrc))
	require.Nil(t, narrow.buildIdx16)
	require.Equal(t, narrowSrc, narrow.Slice())
	require.Equal(t, uuid.Nil, narrow.At(len(narrowSrc)-1).BuildId, "the last entry is the empty region, not build 255")
}

// ByteSize is what the orchestrator's residency gauges are built on, and the
// whole reason for the compact encoding is that these mappings dominate host
// RAM. A sign error or an off-by-one here would silently misreport the number
// the sizing decisions are made from, so pin the arithmetic against explicitly
// counted entries and builds.
func TestMapping_ByteSize(t *testing.T) {
	t.Parallel()

	// Every case below is contiguous and references few enough builds for the
	// one-byte index, so no entry carries a lengths column or a wide index.
	const bytesPerEntry = 9  // offsets + storage (4 each) + build index (1)
	const bytesPerBuild = 16 // uuid.UUID

	bs := uint64(4096)
	a := uuid.New()
	b := uuid.New()

	tests := []struct {
		name    string
		src     []BuildMap
		entries int
		builds  int
	}{
		{name: "empty"},
		{
			name:    "one entry one build",
			src:     []BuildMap{{Offset: 0, Length: bs, BuildId: a}},
			entries: 1,
			builds:  1,
		},
		{
			name: "builds are deduplicated, entries are not",
			src: []BuildMap{
				{Offset: 0, Length: bs, BuildId: a},
				{Offset: bs, Length: bs, BuildId: b},
				{Offset: 2 * bs, Length: bs, BuildId: a, BuildStorageOffset: bs},
			},
			entries: 3,
			builds:  2,
		},
		{
			// An empty region carries no build, so it costs an entry and nothing
			// in the build table.
			name: "nil build ids cost no build table slot",
			src: []BuildMap{
				{Offset: 0, Length: bs, BuildId: uuid.Nil},
				{Offset: bs, Length: bs, BuildId: uuid.Nil},
			},
			entries: 2,
			builds:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, err := NewMapping(bs, tt.src)
			require.NoError(t, err)

			require.Equal(t, tt.entries, m.Len())
			require.Len(t, m.Builds(), tt.builds)
			require.Equal(t, tt.entries*bytesPerEntry+tt.builds*bytesPerBuild, m.ByteSize())
		})
	}
}

// The encoding's claim is 9 bytes per entry against a BuildMap's 40, so the
// gauge must scale with entry count and stay far under the uncompacted size.
func TestMapping_ByteSizeScalesWithEntries(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	id := uuid.New()

	src := make([]BuildMap, 1000)
	for i := range src {
		src[i] = BuildMap{Offset: uint64(i) * bs, Length: bs, BuildId: id}
	}

	m, err := NewMapping(bs, src)
	require.NoError(t, err)

	require.Equal(t, 1000*9+16, m.ByteSize())
	require.Less(t, m.ByteSize(), len(src)*40, "compact mapping must be smaller than the BuildMap slice it replaces")
}

// The footprint gauges rely on this to tell one allocation reached through two
// Headers from two allocations. CloneForUpload copies the Header struct, so the
// copy's Mapping shares the original's slices.
func TestMapping_SharesStorageWith(t *testing.T) {
	t.Parallel()

	bs := uint64(4096)
	id := uuid.New()
	src := []BuildMap{
		{Offset: 0, Length: bs, BuildId: id},
		{Offset: bs, Length: bs, BuildId: id, BuildStorageOffset: bs},
	}

	m, err := NewMapping(bs, src)
	require.NoError(t, err)

	shared := m
	require.True(t, m.SharesStorageWith(shared), "a copy shares the original's columns")
	require.True(t, shared.SharesStorageWith(m), "and the relation is symmetric")

	other, err := NewMapping(bs, src)
	require.NoError(t, err)
	require.False(t, m.SharesStorageWith(other), "separately built mappings are separate allocations")

	empty, err := NewMapping(bs, nil)
	require.NoError(t, err)
	require.False(t, empty.SharesStorageWith(empty), "an empty mapping holds no allocation to share")
	require.False(t, m.SharesStorageWith(empty))
}
