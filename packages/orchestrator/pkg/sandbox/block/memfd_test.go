//go:build linux

package block

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func newTestMemfd(t *testing.T, size int64) (memfd *Memfd, data []byte) {
	t.Helper()

	fd, err := unix.MemfdCreate("test", 0)
	require.NoError(t, err)
	require.NoError(t, unix.Ftruncate(fd, size))

	data = make([]byte, size)
	_, err = rand.Read(data)
	require.NoError(t, err)

	_, err = unix.Pwrite(fd, data, 0)
	require.NoError(t, err)

	memfd, err = NewFromFd(fd)
	require.NoError(t, err)

	return memfd, data
}

// fullDirty returns a bitmap marking every block in [0, size/blockSize) dirty.
func fullDirty(size, blockSize int64) *roaring.Bitmap {
	b := roaring.New()
	b.AddRange(0, uint64(size/blockSize))

	return b
}

// fakeOriginalDevice satisfies ReadonlyDevice over a fixed byte buffer.
// Tracks Slice/ReadAt calls so dedup tests can assert fast-path skipping.
type fakeOriginalDevice struct {
	data  []byte
	hdr   *header.Header // optional; nil disables the dedup fast paths
	reads int
}

func (f *fakeOriginalDevice) ReadAt(_ context.Context, p []byte, off int64) (int, error) {
	f.reads++
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}

func (f *fakeOriginalDevice) Slice(_ context.Context, off, length int64) ([]byte, error) {
	f.reads++
	if off+length > int64(len(f.data)) {
		return nil, io.EOF
	}

	return f.data[off : off+length], nil
}

func (f *fakeOriginalDevice) Size(context.Context) (int64, error) { return int64(len(f.data)), nil }
func (f *fakeOriginalDevice) Close() error                        { return nil }
func (f *fakeOriginalDevice) BlockSize() int64                    { return int64(header.PageSize) }
func (f *fakeOriginalDevice) Header() *header.Header              { return f.hdr }
func (f *fakeOriginalDevice) SwapHeader(*header.Header)           {}

// erroringOriginalDevice returns sentinel from every ReadAt and Slice.
type erroringOriginalDevice struct {
	fakeOriginalDevice

	err error
}

func (e *erroringOriginalDevice) ReadAt(context.Context, []byte, int64) (int, error) {
	return 0, e.err
}

func (e *erroringOriginalDevice) Slice(context.Context, int64, int64) ([]byte, error) {
	return nil, e.err
}

// peekingOriginalDevice wraps fakeOriginalDevice with a programmable
// CachePeeker implementation, so dedup best-effort tests can force a
// "uncached" answer without touching real chunkers.
type peekingOriginalDevice struct {
	fakeOriginalDevice

	cached bool
}

func (p *peekingOriginalDevice) IsCached(context.Context, int64, int64) bool { return p.cached }

// pwritevAll must concatenate non-contiguous iovecs at off and survive a
// kernel short-write (the helper retries with the remaining tail).
func TestPwritevAllConcatenatesIovecs(t *testing.T) {
	t.Parallel()

	path := t.TempDir() + "/out"
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	a := bytes.Repeat([]byte{0xAA}, 13)
	b := bytes.Repeat([]byte{0xBB}, 7)
	c := bytes.Repeat([]byte{0xCC}, 5)

	require.NoError(t, pwritevAll(int(f.Fd()), 42, [][]byte{a, b, c}))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	expected := append(append(make([]byte, 42), a...), append(b, c...)...)
	require.Equal(t, expected, got)
}

func TestNewCacheFromMemfd_NonAdjacentBlocks(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	memfd, expected := newTestMemfd(t, pageSize*6)

	// Non-adjacent blocks: BitsetRanges emits three separate ranges; the
	// cache packs them in iteration order.
	dirty := roaring.New()
	dirty.AddMany([]uint32{0, 2, 5})

	cache, err := NewCacheFromMemfd(t.Context(), pageSize, t.TempDir()+"/cache", memfd, dirty)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })

	for i, srcBlock := range []int64{0, 2, 5} {
		got := make([]byte, pageSize)
		_, err := cache.ReadAt(got, int64(i)*pageSize)
		require.NoError(t, err)
		require.Equal(t, expected[srcBlock*pageSize:(srcBlock+1)*pageSize], got)
	}
}

// Regression: the copy loop used to index src[srcOff:...] with srcOff in
// guest-absolute space, panicking when the first range started past zero.
func TestNewCacheFromMemfd_NonZeroRangeStart(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	memfd, expected := newTestMemfd(t, pageSize*8)

	dirty := roaring.New()
	dirty.AddMany([]uint32{3, 4})

	cache, err := NewCacheFromMemfd(t.Context(), pageSize, t.TempDir()+"/cache", memfd, dirty)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })

	got := make([]byte, pageSize*2)
	_, err = cache.ReadAt(got, 0)
	require.NoError(t, err)
	require.Equal(t, expected[pageSize*3:pageSize*5], got)
}

// Async: the copy detaches from the request context (cancelling the parent
// ctx doesn't abort it). After Wait the file on disk has the full payload.
func TestNewCacheFromMemfdAsync_DetachesAndFlushes(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	numPages := uint32(16)
	memfd, expected := newTestMemfd(t, pageSize*int64(numPages))

	dirty := roaring.New()
	dirty.AddRange(0, uint64(numPages))

	ctx, cancel := context.WithCancel(t.Context())
	cachePath := t.TempDir() + "/cache"
	cache, err := NewCacheFromMemfdAsync(ctx, pageSize, cachePath, memfd, dirty)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })

	cancel()
	require.NoError(t, cache.Wait(t.Context()))

	fromFile, err := os.ReadFile(cachePath)
	require.NoError(t, err)
	require.Equal(t, expected, fromFile)
}

// Compare detaches: the header future resolves only after the goroutine
// runs, and the deduped cache (after Wait) holds only pages that differ.
func TestNewCacheFromMemfdDeduped_DetachesCompareAndDrain(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	numPages := uint32(8)
	size := pageSize * int64(numPages)

	memfd, srcData := newTestMemfd(t, size)
	baseData := make([]byte, size)
	copy(baseData, srcData)
	for _, p := range []uint32{1, 4} {
		off := int64(p) * pageSize
		for i := range pageSize {
			baseData[off+i] ^= 0xFF
		}
	}

	dirty := roaring.New()
	dirty.AddRange(0, uint64(numPages))

	ctx, cancel := context.WithCancel(t.Context())
	cachePath := t.TempDir() + "/dedup-async"
	metaOut := utils.NewSetOnce[*header.DiffMetadata]()
	cache, err := NewCacheFromMemfdDeduped(
		ctx, &fakeOriginalDevice{data: baseData}, pageSize, cachePath, memfd, dirty, false, false,
		DedupBudget{}, nil, metaOut, false, false,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })

	cancel()

	meta, err := metaOut.WaitWithContext(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, meta.Dirty.GetCardinality())

	_, err = cache.Wait(t.Context())
	require.NoError(t, err)

	got := make([]byte, pageSize*2)
	_, err = cache.ReadAt(got, 0)
	require.NoError(t, err)
	expected := append([]byte{}, srcData[pageSize:pageSize*2]...)
	expected = append(expected, srcData[pageSize*4:pageSize*5]...)
	require.Equal(t, expected, got)
}

// buildPackedIndex.translate maps a packed diff offset back to the absolute
// memfd offset for the dirty run it belongs to, and refuses ranges that cross
// a run boundary or fall outside any run.
func TestPackedIndexTranslate(t *testing.T) {
	t.Parallel()

	ps := int64(header.PageSize)
	// Dirty pages 0, 2, 5 pack contiguously as seg0=[0,ps)->abs 0,
	// seg1=[ps,2ps)->abs 2ps, seg2=[2ps,3ps)->abs 5ps.
	dirty := roaring.New()
	dirty.AddMany([]uint32{0, 2, 5})
	idx := buildPackedIndex(dirty)

	for _, tc := range []struct{ packed, abs int64 }{{0, 0}, {ps, 2 * ps}, {2 * ps, 5 * ps}} {
		abs, ok := idx.translate(tc.packed, ps)
		require.True(t, ok)
		require.Equal(t, tc.abs, abs)
	}

	// A range spanning two runs can't be served from one contiguous memfd span.
	_, ok := idx.translate(0, 2*ps)
	require.False(t, ok)
	// Past the last run.
	_, ok = idx.translate(3*ps, ps)
	require.False(t, ok)
}

// While the drain is in progress (done unresolved), reads are served from the
// still-mapped memfd via the packed→absolute index; once done resolves the
// inflight path is bypassed in favor of the drained cache.
func TestDedupedMemfdCache_InflightServesFromMemfd(t *testing.T) {
	t.Parallel()

	ps := int64(header.PageSize)
	memfd, data := newTestMemfd(t, ps*6)
	t.Cleanup(func() { _ = memfd.Close() })

	// Non-adjacent dirty set so packed offsets differ from absolute offsets.
	dirty := roaring.New()
	dirty.AddMany([]uint32{0, 2, 5})

	// Construct the post-compare, pre-drain state directly (no goroutine): the
	// memfd + index are published and done is unresolved.
	d := &DedupedMemfdCache{
		done:     utils.NewSetOnce[*Cache](),
		inflight: true,
		memfd:    memfd,
		index:    buildPackedIndex(dirty),
	}

	// ReadAt at packed offsets resolves to the right absolute memfd pages.
	for i, srcPage := range []int64{0, 2, 5} {
		got := make([]byte, ps)
		n, err := d.ReadAt(got, int64(i)*ps)
		require.NoError(t, err)
		require.Equal(t, int(ps), n)
		require.Equal(t, data[srcPage*ps:(srcPage+1)*ps], got)
	}

	// Slice takes the same path and returns a copy of the memfd bytes.
	s, err := d.Slice(ps, ps) // packed page 1 -> absolute page 2
	require.NoError(t, err)
	require.Equal(t, data[2*ps:3*ps], s)

	// Once the drain resolves done, the inflight path is bypassed.
	require.NoError(t, d.done.SetValue(nil))
	_, ok := d.tryInflightRead(make([]byte, ps), 0)
	require.False(t, ok)
}

// Pages served from the memfd increment inflight_serve_pages by the page count
// (not once per call) and are tagged by phase: the drain-window path
// (tryInflightRead, via ReadAt/Slice) as "drain", the provisional compare-window
// path (ServeMemfd) as "provisional". A read bypassed after the drain resolves
// must not be counted.
//
//nolint:paralleltest // swaps the package-level inflightServePagesCounter
func TestDedupedMemfdCache_InflightServeMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	prev := inflightServePagesCounter
	inflightServePagesCounter = utils.Must(mp.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block").
		Int64Counter("orchestrator.memfd.inflight_serve_pages"))
	t.Cleanup(func() { inflightServePagesCounter = prev })

	ps := int64(header.PageSize)
	memfd, _ := newTestMemfd(t, ps*6)
	t.Cleanup(func() { _ = memfd.Close() })

	dirty := roaring.New()
	dirty.AddMany([]uint32{0, 2, 5})

	d := &DedupedMemfdCache{
		done:     utils.NewSetOnce[*Cache](),
		inflight: true,
		memfd:    memfd,
		index:    buildPackedIndex(dirty),
	}

	// Drain-window path (tryInflightRead via ReadAt/Slice): 3 ReadAt + 1 Slice,
	// each one page → 4 pages tagged phase=drain.
	for i := range 3 {
		_, err := d.ReadAt(make([]byte, ps), int64(i)*ps)
		require.NoError(t, err)
	}
	_, err := d.Slice(ps, ps)
	require.NoError(t, err)

	// Provisional compare-window path (ServeMemfd): a two-page serve then a
	// one-page serve → 3 pages tagged phase=provisional, across 2 calls. The
	// two-page call proves pages are counted per page, not once per call.
	_, err = d.ServeMemfd(make([]byte, 2*ps), 0)
	require.NoError(t, err)
	_, err = d.ServeMemfd(make([]byte, ps), ps)
	require.NoError(t, err)

	// A read bypassed after the drain resolves is not served from the memfd.
	require.NoError(t, d.done.SetValue(nil))
	_, ok := d.tryInflightRead(make([]byte, ps), 0)
	require.False(t, ok)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	byPhase := collectCounterBy(t, rm, "orchestrator.memfd.inflight_serve_pages", "phase")
	assert.Equal(t, int64(4), byPhase["drain"], "drain pages: 3 ReadAt + 1 Slice")
	assert.Equal(t, int64(3), byPhase["provisional"], "provisional pages: a 2-page serve + a 1-page serve")
}

// collectCounterBy totals a named int64 monotonic-sum metric's datapoints,
// keyed by their values for the given labels joined with "/".
func collectCounterBy(t *testing.T, rm metricdata.ResourceMetrics, name string, labels ...string) map[string]int64 {
	t.Helper()

	out := map[string]int64{}
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			found = true
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "metric %q is not an int64 sum", name)
			for _, dp := range sum.DataPoints {
				values := make([]string, 0, len(labels))
				for _, label := range labels {
					v, ok := dp.Attributes.Value(attribute.Key(label))
					require.True(t, ok, "datapoint missing %s attribute", label)
					values = append(values, v.AsString())
				}
				out[strings.Join(values, "/")] += dp.Value
			}
		}
	}
	require.True(t, found, "metric %q not found in collected metrics", name)

	return out
}

// End-to-end: drive a real dedup goroutine with inflight serving on and hammer
// tryInflightRead from many goroutines across the drain window and the
// memfd->cache handover. Every served read must return the correct bytes, and
// under -race the drain's memfd close must never unmap beneath an in-flight
// reader. Base is all zeros so every (random) source page stays in the diff:
// the deduped dirty set is the full range and packed offsets equal absolute.
// TestDedupedMemfdCache_MemfdHeldUntilSwap covers the swap/release ordering: with
// inflight serving active, the memfd stays mapped past the drain — past the point
// the drained cache resolves — until MarkSwapped fires, so a provisional read
// served via ServeMemfd never hits a released memfd during the compare→swap
// window (which outlives the drain for a small dirty set + fragmented parent).
// Before the fix the memfd was closed immediately after the drain.
func TestDedupedMemfdCache_MemfdHeldUntilSwap(t *testing.T) {
	t.Parallel()

	ps := int64(header.PageSize)
	const numPages = 64
	size := ps * numPages

	memfd, srcData := newTestMemfd(t, size)
	base := &fakeOriginalDevice{data: make([]byte, size)} // all-zero base → every dirty page kept
	dirty := roaring.New()
	dirty.AddRange(0, numPages)

	metaOut := utils.NewSetOnce[*header.DiffMetadata]()
	cache, err := NewCacheFromMemfdDeduped(
		t.Context(), base, ps, t.TempDir()+"/dedup-held", memfd, dirty,
		false, false, DedupBudget{}, nil, metaOut, true, false,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })

	// The drained cache resolves once the drain completes...
	_, err = cache.Wait(t.Context())
	require.NoError(t, err)

	// ...but the memfd is still mapped (held for a pending provisional swap), so a
	// provisional identity read still returns the page bytes.
	buf := make([]byte, ps)
	n, err := cache.ServeMemfd(buf, 3*ps)
	require.NoError(t, err)
	require.Equal(t, int(ps), n)
	require.Equal(t, srcData[3*ps:4*ps], buf)

	// Once the swap is signaled the memfd is released and provisional reads
	// fail. The release runs on the drain goroutine, which parallel tests in
	// this package can starve on a loaded runner — the window is generous for
	// that reason (the poll exits early on success).
	cache.MarkSwapped()
	require.Eventually(t, func() bool {
		_, e := cache.ServeMemfd(buf, 3*ps)
		var bna BytesNotAvailableError

		return errors.As(e, &bna)
	}, 15*time.Second, 5*time.Millisecond)
}

func TestDedupedMemfdCache_InflightConcurrentDrainRace(t *testing.T) {
	t.Parallel()

	ps := int64(header.PageSize)
	const numPages = 4096
	size := ps * numPages

	memfd, srcData := newTestMemfd(t, size)
	base := &fakeOriginalDevice{data: make([]byte, size)}

	dirty := roaring.New()
	dirty.AddRange(0, numPages)

	metaOut := utils.NewSetOnce[*header.DiffMetadata]()
	cache, err := NewCacheFromMemfdDeduped(
		t.Context(), base, ps, t.TempDir()+"/dedup-concurrent", memfd, dirty,
		false, false, DedupBudget{}, nil, metaOut, true, false,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })

	var wg sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan error, 16)
	for g := range 8 {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			buf := make([]byte, ps)
			for i := seed; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				page := int64(i % numPages)
				n, ok := cache.tryInflightRead(buf, page*ps)
				if ok && (n != int(ps) || !bytes.Equal(buf, srcData[page*ps:(page+1)*ps])) {
					errCh <- fmt.Errorf("inflight page %d mismatch", page)

					return
				}
				runtime.Gosched()
			}
		}(g)
	}

	// Let compare+drain complete (memfd closes, done resolves) under reader load.
	_, err = cache.Wait(t.Context())
	require.NoError(t, err)

	// Post-handover: every page reads correctly from the drained cache.
	buf := make([]byte, ps)
	for page := range int64(numPages) {
		_, rErr := cache.ReadAt(buf, page*ps)
		require.NoError(t, rErr)
		require.Equal(t, srcData[page*ps:(page+1)*ps], buf)
	}

	close(stop)
	wg.Wait()
	select {
	case e := <-errCh:
		t.Fatal(e)
	default:
	}
}

// MemfdIdentitySource serves dirty pages from the memfd at identity offsets
// while it is mapped, and reports BytesNotAvailableError once it is released so
// the caller falls back to the drained cache (via the swapped-in deduped header).
func TestMemfdIdentitySource_ServesThenReleases(t *testing.T) {
	t.Parallel()

	ps := int64(header.PageSize)
	memfd, data := newTestMemfd(t, ps*4)
	d := &DedupedMemfdCache{done: utils.NewSetOnce[*Cache](), memfd: memfd}
	src := NewMemfdIdentitySource(d, ps*4)

	for _, page := range []int64{0, 2, 3} {
		b := make([]byte, ps)
		n, err := src.ReadAt(b, page*ps)
		require.NoError(t, err)
		require.Equal(t, int(ps), n)
		require.Equal(t, data[page*ps:(page+1)*ps], b)
	}
	// Slice takes the same identity path and returns a copy of the memfd bytes.
	sl, err := src.Slice(2*ps, ps)
	require.NoError(t, err)
	require.Equal(t, data[2*ps:3*ps], sl)
	require.True(t, src.IsCached(t.Context(), 0, ps*4))

	require.NoError(t, d.releaseMemfd(t.Context()))

	var bna BytesNotAvailableError
	_, err = src.ReadAt(make([]byte, ps), 0)
	require.ErrorAs(t, err, &bna)
	_, err = src.Slice(0, ps)
	require.ErrorAs(t, err, &bna)
	require.False(t, src.IsCached(t.Context(), 0, ps))
}

// TestNewCacheFromMemfdKeepOpen pins the ownership contract the in-place
// checkpoint depends on: the KeepOpen constructor borrows the memfd instead of
// consuming it, so the running VM's memory backing stays readable afterwards.
// The closing constructor is then run over the SAME memfd to prove the fd
// survived the first call.
func TestNewCacheFromMemfdKeepOpen(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	size := pageSize * 4
	memfd, expected := newTestMemfd(t, size)

	dirty := fullDirty(size, pageSize)

	kept, err := NewCacheFromMemfdKeepOpen(t.Context(), pageSize, t.TempDir()+"/kept", memfd, dirty)
	require.NoError(t, err)
	t.Cleanup(func() { _ = kept.Close() })

	got := make([]byte, size)
	_, err = kept.ReadAt(got, 0)
	require.NoError(t, err)
	require.Equal(t, expected, got)

	// The memfd must still be alive: the consuming constructor reads it again
	// (and closes it, matching production where a later destroy-path pause
	// takes ownership).
	second, err := NewCacheFromMemfd(t.Context(), pageSize, t.TempDir()+"/second", memfd, dirty)
	require.NoError(t, err, "memfd must remain readable after the KeepOpen constructor")
	t.Cleanup(func() { _ = second.Close() })

	got2 := make([]byte, size)
	_, err = second.ReadAt(got2, 0)
	require.NoError(t, err)
	require.Equal(t, expected, got2)
}

// swapDeadStructureCounters points the package-level dead-structure counters
// at a manual reader for the duration of the test. NOT parallel-safe.
func swapDeadStructureCounters(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).
		Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block")

	prevOutcome, prevBytes := deadStructureOutcomeCounter, deadStructureBytesCounter
	deadStructureOutcomeCounter = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorDeadStructureOutcomeCounterName))
	deadStructureBytesCounter = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorDeadStructureBytesCounterName))
	t.Cleanup(func() { deadStructureOutcomeCounter, deadStructureBytesCounter = prevOutcome, prevBytes })

	return reader
}

// deadStructureTotals returns the dead-structure outcome counts and bytes,
// each keyed by "structure/outcome". A counter nothing recorded on is empty.
func deadStructureTotals(t *testing.T, reader *sdkmetric.ManualReader) (outcomes, bytes map[string]int64) {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	collect := func(name telemetry.CounterType) map[string]int64 {
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == string(name) {
					return collectCounterBy(t, rm, string(name), "structure", "outcome")
				}
			}
		}

		return map[string]int64{}
	}

	return collect(telemetry.OrchestratorDeadStructureOutcomeCounterName), collect(telemetry.OrchestratorDeadStructureBytesCounterName)
}

// failingSliceDevice fails every base Slice, so the dedup compare fails on the
// first dirty page that is not zero.
type failingSliceDevice struct{ *fakeOriginalDevice }

func (failingSliceDevice) Slice(context.Context, int64, int64) ([]byte, error) {
	return nil, errors.New("base slice failed")
}

const freeIndexTestPages = 16

// newFreeIndexCache starts a real dedup of freeIndexTestPages random dirty
// pages. Over an all-zero base every dirty page is kept, so packed offsets
// equal absolute ones.
func newFreeIndexCache(t *testing.T, base ReadonlyDevice, inflight, freeIndex bool) (*DedupedMemfdCache, []byte) {
	t.Helper()

	ps := int64(header.PageSize)
	size := ps * freeIndexTestPages

	memfd, srcData := newTestMemfd(t, size)
	dirty := roaring.New()
	dirty.AddRange(0, freeIndexTestPages)
	if base == nil {
		base = &fakeOriginalDevice{data: make([]byte, size)}
	}

	cache, err := NewCacheFromMemfdDeduped(
		t.Context(), base, ps, t.TempDir()+"/dedup-free-index", memfd, dirty,
		false, false, DedupBudget{}, nil, utils.NewSetOnce[*header.DiffMetadata](), inflight, freeIndex,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })

	return cache, srcData
}

// waitMemfdReleased waits until the drain goroutine has released the memfd.
func waitMemfdReleased(t *testing.T, cache *DedupedMemfdCache) {
	t.Helper()

	require.Eventually(t, func() bool {
		cache.mu.RLock()
		defer cache.mu.RUnlock()

		return cache.memfd == nil
	}, 15*time.Second, time.Millisecond, "the drain goroutine never released the memfd")
}

// freeIndexTestIndexBytes is the size of the index the dedup builds for
// newFreeIndexCache's dirty set: its backing array, 24 bytes an entry.
func freeIndexTestIndexBytes() int64 {
	dirty := roaring.New()
	dirty.AddRange(0, freeIndexTestPages)

	return int64(cap(buildPackedIndex(dirty))) * packedSegBytes
}

func TestPackedSegBytesMatchesLayout(t *testing.T) {
	t.Parallel()

	assert.Equal(t, uintptr(packedSegBytes), unsafe.Sizeof(packedSeg{}))
}

// The packed index is useful only while the memfd is mapped. Every release
// records one outcome. With the flag on the index goes with the memfd; with it
// off the index stays exactly as it does without the flag. Both record the
// index's size. Without in-flight serving no index is built, which records
// none and no size. In every state a read after the release is served,
// correctly, by the drained cache.
//
//nolint:paralleltest // swaps the package-level dead-structure counters
func TestDedupedMemfdCache_FreeIndexWithMemfd(t *testing.T) {
	ps := int64(header.PageSize)
	indexBytes := freeIndexTestIndexBytes()

	for _, tc := range []struct {
		name      string
		inflight  bool
		freeIndex bool
		wantIndex bool
		outcome   string
		wantBytes int64
	}{
		{name: "flag on", inflight: true, freeIndex: true, wantIndex: false, outcome: "dropped", wantBytes: indexBytes},
		{name: "flag off", inflight: true, freeIndex: false, wantIndex: true, outcome: "flag_off", wantBytes: indexBytes},
		{name: "no inflight serving", inflight: false, freeIndex: true, wantIndex: false, outcome: "none"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := swapDeadStructureCounters(t)

			cache, srcData := newFreeIndexCache(t, nil, tc.inflight, tc.freeIndex)
			cache.MarkSwapped()
			_, err := cache.Wait(t.Context())
			require.NoError(t, err)
			waitMemfdReleased(t, cache)

			cache.mu.RLock()
			hasIndex := cache.index != nil
			cache.mu.RUnlock()
			assert.Equal(t, tc.wantIndex, hasIndex, "packed index after the memfd release")

			outcomes, bytes := deadStructureTotals(t, reader)
			assert.Equal(t, map[string]int64{"dedup_index/" + tc.outcome: 1}, outcomes, "one outcome per release")
			if tc.wantBytes > 0 {
				assert.Equal(t, map[string]int64{"dedup_index/" + tc.outcome: tc.wantBytes}, bytes)
			} else {
				assert.Empty(t, bytes, "no index, no size")
			}

			buf := make([]byte, ps)
			_, err = cache.ReadAt(buf, 5*ps)
			require.NoError(t, err)
			assert.Equal(t, srcData[5*ps:6*ps], buf, "a read after the release comes from the drained cache")
		})
	}
}

// A compare that fails releases the memfd before any index is built, so even
// with in-flight serving and the flag on the release records none.
//
//nolint:paralleltest // swaps the package-level dead-structure counters
func TestDedupedMemfdCache_FreeIndexCompareError(t *testing.T) {
	reader := swapDeadStructureCounters(t)

	size := int64(header.PageSize) * freeIndexTestPages
	cache, _ := newFreeIndexCache(t, failingSliceDevice{&fakeOriginalDevice{data: make([]byte, size)}}, true, true)
	_, err := cache.Wait(t.Context())
	require.Error(t, err)
	waitMemfdReleased(t, cache)

	outcomes, bytes := deadStructureTotals(t, reader)
	assert.Equal(t, map[string]int64{"dedup_index/none": 1}, outcomes)
	assert.Empty(t, bytes)
}

// With no swap signal, cancelling the context the release waits on releases
// the memfd, and with the flag on the index goes with it just as after a swap.
//
//nolint:paralleltest // swaps the package-level dead-structure counters
func TestDedupedMemfdCache_FreeIndexOnCancelledSwapWait(t *testing.T) {
	reader := swapDeadStructureCounters(t)

	cache, _ := newFreeIndexCache(t, nil, true, true)
	_, err := cache.Wait(t.Context())
	require.NoError(t, err)
	cache.cancel()
	waitMemfdReleased(t, cache)

	cache.mu.RLock()
	assert.Nil(t, cache.index)
	cache.mu.RUnlock()
	outcomes, bytes := deadStructureTotals(t, reader)
	assert.Equal(t, map[string]int64{"dedup_index/dropped": 1}, outcomes)
	assert.Equal(t, map[string]int64{"dedup_index/dropped": freeIndexTestIndexBytes()}, bytes)
}

// Readers hammer the in-flight path across the drain and the release that
// frees the index. Most of them fall back once the drain resolves, so this is
// a -race check of the lock discipline, not a proof that a read overlapped the
// free: every read either comes from the memfd with the right bytes or falls
// back.
func TestDedupedMemfdCache_FreeIndexUnderConcurrentReads(t *testing.T) {
	t.Parallel()

	ps := int64(header.PageSize)
	const numPages = 256
	size := ps * numPages

	memfd, srcData := newTestMemfd(t, size)
	dirty := roaring.New()
	dirty.AddRange(0, numPages)

	cache, err := NewCacheFromMemfdDeduped(
		t.Context(), &fakeOriginalDevice{data: make([]byte, size)}, ps, t.TempDir()+"/dedup-free-race", memfd, dirty,
		false, false, DedupBudget{}, nil, utils.NewSetOnce[*header.DiffMetadata](), true, true,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })
	cache.MarkSwapped()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan error, 8)
	for g := range 8 {
		wg.Go(func() {
			buf := make([]byte, ps)
			for i := g; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				page := int64(i % numPages)
				n, ok := cache.tryInflightRead(buf, page*ps)
				if ok && (n != int(ps) || !bytes.Equal(buf, srcData[page*ps:(page+1)*ps])) {
					errCh <- fmt.Errorf("inflight page %d mismatch", page)

					return
				}
				runtime.Gosched()
			}
		})
	}

	require.Eventually(t, func() bool {
		cache.mu.RLock()
		defer cache.mu.RUnlock()

		return cache.memfd == nil && cache.index == nil
	}, time.Minute, time.Millisecond, "the release never freed the memfd and its index")

	close(stop)
	wg.Wait()
	select {
	case e := <-errCh:
		t.Fatal(e)
	default:
	}
}
