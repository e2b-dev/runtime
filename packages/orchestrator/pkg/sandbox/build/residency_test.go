//go:build linux

package build

import (
	"bytes"
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// A same-node resume reads its last pause's diff from this node, so those
// reads are reported as local rather than left unknown.
func TestLocalDiffReportsLocalSource(t *testing.T) {
	t.Parallel()

	data := bytes.Repeat([]byte{0x5a}, int(testBlockSize))
	diff, err := NewLocalDiffFromCache(GetDiffStoreKey(uuid.NewString(), Memfile), &headerSwapBytesSource{data: data})
	require.NoError(t, err)

	var sliced storage.SourceSink
	_, err = diff.Slice(storage.ReadSourceInto(t.Context(), &sliced), 0, testBlockSize, nil)
	require.NoError(t, err)
	assert.Equal(t, storage.SourceMmap, sliced.Source())

	var read storage.SourceSink
	_, err = diff.ReadAt(storage.ReadSourceInto(t.Context(), &read), make([]byte, testBlockSize), 0, nil)
	require.NoError(t, err)
	assert.Equal(t, storage.SourceMmap, read.Source())
}

// A range the header maps to no build is zero-filled on both read paths and
// reported as zero, so those faults do not land in unknown.
func TestFileReportsZeroSource(t *testing.T) {
	t.Parallel()

	f := newResidencyFile(t, uuid.Nil, nil)

	var sliced storage.SourceSink
	_, err := f.Slice(storage.ReadSourceInto(t.Context(), &sliced), 0, testBlockSize)
	require.NoError(t, err)
	assert.Equal(t, storage.SourceZero, sliced.Source())

	var read storage.SourceSink
	_, err = f.ReadAt(storage.ReadSourceInto(t.Context(), &read), make([]byte, testBlockSize), 0)
	require.NoError(t, err)
	assert.Equal(t, storage.SourceZero, read.Source())
}

func newResidencyFile(t *testing.T, mapsTo uuid.UUID, diff Diff) *File {
	t.Helper()

	store, err := NewDiffStore(mustParseCfg(t), flagsWithMaxBuildCachePercentage(t, 90), t.TempDir(), time.Hour, time.Minute)
	require.NoError(t, err)
	t.Cleanup(store.Close)
	if diff != nil {
		store.Add(diff)
	}
	m, err := blockmetrics.NewMetrics(noop.NewMeterProvider())
	require.NoError(t, err)

	return NewFile(buildHeader(t, uuid.New(), testBlockSize, mapsTo), store, Memfile, nil, m)
}

// The residency probe behind resume.prefetch.*_cached_pct: a local pause diff
// is resident, a build not on the node is not, and an unsealed deferred diff
// is unknown rather than uncached.
func TestFilePageResident(t *testing.T) {
	t.Parallel()

	local := uuid.New()
	localDiff, err := NewLocalDiffFromCache(GetDiffStoreKey(local.String(), Memfile), &headerSwapBytesSource{data: make([]byte, testBlockSize)})
	require.NoError(t, err)
	resident, known := newResidencyFile(t, local, localDiff).PageResident(t.Context(), 0)
	assert.True(t, known)
	assert.True(t, resident, "a local pause diff needs no fetch")
	assert.False(t, newResidencyFile(t, local, localDiff).IsCached(t.Context(), 0, testBlockSize),
		"the dedup predicate is unchanged")

	resident, known = newResidencyFile(t, uuid.Nil, nil).PageResident(t.Context(), 0)
	assert.True(t, known)
	assert.True(t, resident, "an all-zero mapping is resident")

	resident, known = newResidencyFile(t, uuid.New(), nil).PageResident(t.Context(), 0)
	assert.True(t, known)
	assert.False(t, resident, "a build not on the node must be fetched")

	deferred := uuid.New()
	inner := utils.NewSetOnce[Diff]()
	deferredDiff := NewDeferredDiff(GetDiffStoreKey(deferred.String(), Memfile), testBlockSize, inner)
	f := newResidencyFile(t, deferred, deferredDiff)
	_, known = f.PageResident(t.Context(), 0)
	assert.False(t, known, "an unsealed deferred diff cannot tell")

	sealed, err := NewLocalDiffFromCache(GetDiffStoreKey(deferred.String(), Memfile), &headerSwapBytesSource{data: make([]byte, testBlockSize)})
	require.NoError(t, err)
	require.NoError(t, inner.SetValue(sealed))
	resident, known = f.PageResident(t.Context(), 0)
	assert.True(t, known)
	assert.True(t, resident, "a sealed deferred diff answers for its inner diff")
}

func TestFilePageResidentFragmented(t *testing.T) {
	t.Parallel()

	local, missing := uuid.New(), uuid.New()
	diff, err := NewLocalDiffFromCache(GetDiffStoreKey(local.String(), Memfile), &headerSwapBytesSource{data: make([]byte, 2<<20)})
	require.NoError(t, err)
	f := newResidencyFile(t, local, diff)
	mappings := make([]header.BuildMap, (2<<20)/testBlockSize)
	for i := range mappings {
		id := local
		if i%2 == 1 {
			id = missing
		}
		mappings[i] = header.BuildMap{
			Offset:             uint64(int64(i) * testBlockSize),
			Length:             uint64(testBlockSize),
			BuildId:            id,
			BuildStorageOffset: uint64(int64(i) * testBlockSize),
		}
	}
	meta := header.NewTemplateMetadata(uuid.New(), uint64(testBlockSize), uint64(len(mappings))*uint64(testBlockSize))
	meta.Version = header.MetadataVersionV4
	h, err := header.NewHeader(meta, mappings)
	require.NoError(t, err)
	f.SwapHeader(h)

	for _, page := range []int64{0, 1, 510, 511} {
		resident, known := f.PageResident(t.Context(), page*testBlockSize)
		assert.True(t, known, "fragmentation does not make an individual page unknown")
		assert.Equal(t, page%2 == 0, resident)
	}

	_, known := f.PageResident(t.Context(), 2<<20)
	assert.False(t, known, "out-of-range pages are unknown")
}

type residencyCacheDiff struct {
	Diff

	cachedOffset int64
	length       int64
}

func (d *residencyCacheDiff) IsCached(_ context.Context, off, length int64) bool {
	d.length = length

	return off == d.cachedOffset
}

func TestFilePageResidentUsesCacheOffset(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	diff, err := NewLocalDiffFromCache(GetDiffStoreKey(id.String(), Memfile), nil)
	require.NoError(t, err)
	probe := &residencyCacheDiff{Diff: diff, cachedOffset: 2 * testBlockSize}
	f := newResidencyFile(t, id, probe)
	h, err := header.NewHeader(
		header.NewTemplateMetadata(id, uint64(testBlockSize), uint64(2*testBlockSize)),
		[]header.BuildMap{{Length: uint64(2 * testBlockSize), BuildId: id, BuildStorageOffset: uint64(testBlockSize)}},
	)
	require.NoError(t, err)
	f.SwapHeader(h)

	resident, known := f.PageResident(t.Context(), testBlockSize)
	assert.True(t, known)
	assert.True(t, resident, "the lookup uses the diff's offset, which differs from the guest offset")
	assert.EqualValues(t, header.PageSize, probe.length, "only the sampled page is checked")
	resident, known = f.PageResident(t.Context(), 0)
	assert.True(t, known)
	assert.False(t, resident)
}

func TestFilePageResidentDoesNotExtendExpiry(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	diff, err := NewLocalDiffFromCache(GetDiffStoreKey(id.String(), Memfile), nil)
	require.NoError(t, err)
	f := newResidencyFile(t, id, diff)

	synctest.Test(t, func(t *testing.T) {
		item := f.store.cache.Set(diff.CacheKey(), diff, time.Second)
		expires := item.ExpiresAt()
		time.Sleep(500 * time.Millisecond)
		resident, known := f.PageResident(t.Context(), 0)
		require.True(t, known)
		require.True(t, resident)
		require.Equal(t, expires, item.ExpiresAt(), "observing residency must not retain the diff longer")

		time.Sleep(600 * time.Millisecond)
		resident, known = f.PageResident(t.Context(), 0)
		require.True(t, known)
		require.False(t, resident, "the original expiry still applies after a probe")
	})
}
