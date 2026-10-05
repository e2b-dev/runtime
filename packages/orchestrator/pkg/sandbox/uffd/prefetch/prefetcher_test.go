//go:build linux

package prefetch

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// prefaultResult scripts one Prefault call of fakeBackend.
type prefaultResult struct {
	installed bool
	err       error
}

// fakeBackend implements only Prefault; the embedded nil interface panics on
// anything else copyWorker shouldn't touch.
type fakeBackend struct {
	uffd.MemoryBackend

	results chan prefaultResult
}

func (f *fakeBackend) Prefault(context.Context, int64, []byte) (bool, error) {
	r := <-f.results

	return r.installed, r.err
}

func newTestPrefetcher(t *testing.T, results ...prefaultResult) *Prefetcher {
	t.Helper()

	log, err := logger.NewDevelopmentLogger()
	require.NoError(t, err)

	ch := make(chan prefaultResult, len(results))
	for _, r := range results {
		ch <- r
	}

	return &Prefetcher{logger: log, uffd: &fakeBackend{results: ch}}
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("copyWorker did not return within budget")
	}
}

// ErrClosed (uffd gone: sandbox teardown) must cancel the whole run so fetch
// workers stop fetching and queueing pages nobody will copy, and must count
// nothing.
func TestCopyWorkerCancelsRunOnErrClosed(t *testing.T) {
	t.Parallel()

	p := newTestPrefetcher(t, prefaultResult{err: userfaultfd.ErrClosed})

	ctx, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()

	copyCh := make(chan prefetchData, 2)
	copyCh <- prefetchData{}
	copyCh <- prefetchData{} // must not be drained after ErrClosed

	var copied, skipped atomic.Uint64

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.copyWorker(ctx, cancelRun, copyCh, &copied, &skipped)
	}()

	waitDone(t, done)

	require.Error(t, ctx.Err(), "run context must be cancelled so fetch workers stop")
	require.Zero(t, copied.Load())
	require.Zero(t, skipped.Load())
	require.Len(t, copyCh, 1, "remaining queued pages must not be drained")
}

// Only installed pages count as copied; nil-error no-op prefaults (already
// resident / lost install race / deferred) and errors land in skipped, so
// stage="copied" matches prefault{result="installed"}.
func TestCopyWorkerCountsOnlyInstalledAsCopied(t *testing.T) {
	t.Parallel()

	p := newTestPrefetcher(t,
		prefaultResult{installed: true},               // copied
		prefaultResult{installed: false},              // no-op (skipped/present/deferred)
		prefaultResult{err: context.DeadlineExceeded}, // error
		prefaultResult{installed: true},               // copied
	)

	ctx, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()

	copyCh := make(chan prefetchData, 4)
	for range 4 {
		copyCh <- prefetchData{}
	}
	close(copyCh)

	var copied, skipped atomic.Uint64

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.copyWorker(ctx, cancelRun, copyCh, &copied, &skipped)
	}()

	waitDone(t, done)

	require.NoError(t, ctx.Err(), "non-ErrClosed outcomes must not cancel the run")
	require.EqualValues(t, 2, copied.Load())
	require.EqualValues(t, 2, skipped.Load())
}

// TestCoalesceIndices covers the extent grouping used to turn many small
// per-block fetches into fewer, larger sequential ones.
func TestCoalesceIndices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		indices   []uint64
		maxBlocks int
		want      []extent
	}{
		{
			name:      "a contiguous run coalesces into one extent",
			indices:   []uint64{10, 11, 12, 13},
			maxBlocks: 8,
			want:      []extent{{startIdx: 10, blocks: 4}},
		},
		{
			name:      "a gap breaks the run into separate extents",
			indices:   []uint64{10, 11, 20, 21, 22},
			maxBlocks: 8,
			want:      []extent{{startIdx: 10, blocks: 2}, {startIdx: 20, blocks: 3}},
		},
		{
			name:      "maxBlocks caps a long run into multiple extents",
			indices:   []uint64{0, 1, 2, 3, 4, 5},
			maxBlocks: 2,
			want:      []extent{{startIdx: 0, blocks: 2}, {startIdx: 2, blocks: 2}, {startIdx: 4, blocks: 2}},
		},
		{
			name:      "maxBlocks==1 is one extent per index (coalescing off)",
			indices:   []uint64{5, 6, 9},
			maxBlocks: 1,
			want:      []extent{{startIdx: 5, blocks: 1}, {startIdx: 6, blocks: 1}, {startIdx: 9, blocks: 1}},
		},
		{
			name:      "maxBlocks<1 is clamped to 1",
			indices:   []uint64{1, 2},
			maxBlocks: 0,
			want:      []extent{{startIdx: 1, blocks: 1}, {startIdx: 2, blocks: 1}},
		},
		{
			name:      "no indices yields no extents",
			indices:   nil,
			maxBlocks: 4,
			want:      []extent{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := coalesceIndices(tt.indices, tt.maxBlocks)
			require.Equal(t, tt.want, got)
		})
	}
}

// fakeSlicer is a deterministic, in-memory block.Slicer: Slice returns
// `length` bytes where byte i equals byte(off+i), so a fetched extent's data
// can be checked against the offset each page should carry.
type fakeSlicer struct {
	blockSize int64
}

func (f *fakeSlicer) Slice(_ context.Context, off, length int64) ([]byte, error) {
	data := make([]byte, length)
	for i := range data {
		data[i] = byte(off + int64(i))
	}

	return data, nil
}

func (f *fakeSlicer) BlockSize() int64 { return f.blockSize }

// TestFetchWorkerSplitsCoalescedExtentIntoPerPageCopies is the prefetcher-level
// check that coalescing the fetch (one source.Slice call spanning several
// blocks) doesn't change what gets queued for copy: the copy phase must still
// see exactly one page-sized prefetchData per block, each carrying that
// block's own bytes at its own offset — never the whole coalesced extent.
func TestFetchWorkerSplitsCoalescedExtentIntoPerPageCopies(t *testing.T) {
	t.Parallel()

	const blockSize = 4096

	log, err := logger.NewDevelopmentLogger()
	require.NoError(t, err)

	p := &Prefetcher{logger: log, source: &fakeSlicer{blockSize: blockSize}}

	const startIdx = 5
	const blocks = 3

	fetchCh := make(chan extent, 1)
	fetchCh <- extent{startIdx: startIdx, blocks: blocks}
	close(fetchCh)

	copyCh := make(chan prefetchData, blocks)

	var fetched, skipped atomic.Uint64
	p.fetchWorker(t.Context(), fetchCh, copyCh, blockSize, &fetched, &skipped)
	close(copyCh)

	require.EqualValues(t, blocks, fetched.Load(), "one coalesced extent still counts as its full block count")
	require.Zero(t, skipped.Load())

	var got []prefetchData
	for d := range copyCh {
		got = append(got, d)
	}
	require.Len(t, got, blocks, "the copy phase gets one entry per block, not one per extent")

	for i, d := range got {
		wantOffset := header.BlockOffset(int64(startIdx+i), blockSize)
		require.Equal(t, wantOffset, d.offset)
		require.Len(t, d.data, blockSize, "copy data must be exactly one page, never a multi-block extent")
		require.Equal(t, byte(wantOffset), d.data[0], "each page must carry its own block's bytes")
	}
}

// TestFetchWorkerFetchOnlyQueuesNothing checks the fetch-only path (Prefault
// off, nil copyCh): the worker still fetches every block to warm the cache but
// queues nothing for copy and does not block, so the guest is left to fault the
// (now-warm) pages itself.
func TestFetchWorkerFetchOnlyQueuesNothing(t *testing.T) {
	t.Parallel()

	const blockSize = 4096

	log, err := logger.NewDevelopmentLogger()
	require.NoError(t, err)

	p := &Prefetcher{logger: log, source: &fakeSlicer{blockSize: blockSize}}

	fetchCh := make(chan extent, 1)
	fetchCh <- extent{startIdx: 2, blocks: 3}
	close(fetchCh)

	var fetched, skipped atomic.Uint64
	// nil copyCh == fetch-only. Must return (not deadlock) and count fetches.
	p.fetchWorker(t.Context(), fetchCh, nil, blockSize, &fetched, &skipped)

	require.EqualValues(t, 3, fetched.Load(), "fetch-only still fetches every block to warm the cache")
	require.Zero(t, skipped.Load())
}

// countingBackend records how many times Prefault is called (it must be zero
// for a fetch-only run).
type countingBackend struct {
	uffd.MemoryBackend

	prefaults atomic.Int64
}

func (b *countingBackend) Prefault(context.Context, int64, []byte) (bool, error) {
	b.prefaults.Add(1)

	return true, nil
}

// TestStartFetchOnlyNeverPrefaults drives the whole Start() with Prefault off
// and asserts the D6 guarantee at the integration seam: the copy phase is
// skipped entirely (copyCh nil, no copy coordinator), so the backend is never
// prefaulted even though every block is fetched to warm the cache.
func TestStartFetchOnlyNeverPrefaults(t *testing.T) {
	t.Parallel()

	log, err := logger.NewDevelopmentLogger()
	require.NoError(t, err)

	ff, err := featureflags.NewClient("", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	const bs = int64(4096)
	be := &countingBackend{}
	p := &Prefetcher{
		logger:       log,
		source:       &fakeSlicer{blockSize: bs},
		uffd:         be,
		mapping:      &metadata.MemoryPrefetchMapping{Indices: []uint64{0, 1, 2, 3}, BlockSize: bs},
		Prefault:     false,
		featureFlags: ff,
	}

	require.NoError(t, p.Start(t.Context()))
	require.Zero(t, be.prefaults.Load(), "fetch-only run must never call Prefault")
}

// shortSlicer returns one byte fewer than requested with a nil error, modelling
// a truncated/partial source read.
type shortSlicer struct{ blockSize int64 }

func (s *shortSlicer) Slice(_ context.Context, _, length int64) ([]byte, error) {
	return make([]byte, length-1), nil
}

func (s *shortSlicer) BlockSize() int64 { return s.blockSize }

// TestFetchWorkerShortReadDoesNotPanic checks that a short read with a nil
// error is skipped, not sliced into per-page copies — which would panic (slice
// bounds out of range) and crash the orchestrator.
func TestFetchWorkerShortReadDoesNotPanic(t *testing.T) {
	t.Parallel()

	const blockSize = 4096

	log, err := logger.NewDevelopmentLogger()
	require.NoError(t, err)

	p := &Prefetcher{logger: log, source: &shortSlicer{blockSize: blockSize}}

	fetchCh := make(chan extent, 1)
	fetchCh <- extent{startIdx: 0, blocks: 2}
	close(fetchCh)

	copyCh := make(chan prefetchData, 2)

	var fetched, skipped atomic.Uint64
	require.NotPanics(t, func() {
		p.fetchWorker(t.Context(), fetchCh, copyCh, blockSize, &fetched, &skipped)
	})
	close(copyCh)

	require.Zero(t, fetched.Load(), "a short read is not counted as fetched")
	require.EqualValues(t, 2, skipped.Load(), "both blocks of the extent are skipped")
	require.Empty(t, copyCh, "nothing is queued for copy on a short read")
}
