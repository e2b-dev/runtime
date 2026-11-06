//go:build linux

package block

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

const (
	testBlockSize = header.PageSize // 4KB
	testFrameSize = 256 * 1024      // 256 KB per frame for fast tests
	testFileSize  = testFrameSize * 4
)

func newTestMetrics(tb testing.TB) metrics.Metrics {
	tb.Helper()

	m, err := metrics.NewMetrics(noop.NewMeterProvider())
	require.NoError(tb, err)

	return m
}

func makeTestData(size int) []byte {
	rng := rand.New(rand.NewPCG(42, 0)) //nolint:gosec // deterministic test data
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(rng.IntN(256))
	}

	return data
}

// fakeSeekable implements storage.Seekable backed by in-memory data.
// When ctrl is non-nil, reads are gated through its channels for concurrency tests.
type fakeSeekable struct {
	data        []byte
	failAfter   int64 // >0: truncate reads at this offset; 0 = disabled
	corrupt     bool  // enable corruptByte injection
	corruptByte int64 // absolute offset to XOR 0xFF in served bytes (requires corrupt=true)
	fetchCount  atomic.Int64
	ctrl        *testControl // nil = ungated immediate reads
}

var _ storage.Seekable = (*fakeSeekable)(nil)

// testControl provides channel-based flow control for fakeSeekable.
type testControl struct {
	advance  chan struct{} // close to release reads
	consumed chan struct{} // receives after each read step
	opened   chan struct{} // receives when OpenRangeReader is called
	closed   chan struct{} // receives when reader is closed (fetch done)
	onOpen   func()        // optional callback on OpenRangeReader
}

func newTestChunker(t *testing.T, size int64) *Chunker {
	t.Helper()
	c, err := NewChunker(&featureflags.Client{}, size, testBlockSize, t.TempDir()+"/cache", newTestMetrics(t), storage.MemfileObjectType, "test-build")
	require.NoError(t, err)

	return c
}

func (s *fakeSeekable) Size(_ context.Context) (int64, error) {
	return int64(len(s.data)), nil
}

func (s *fakeSeekable) StoreFile(context.Context, string, ...storage.PutOption) (*storage.FullFrameTable, [32]byte, error) {
	panic("not used")
}

func (s *fakeSeekable) OpenRangeReader(_ context.Context, offsetU int64, length int64, frameTable *storage.FrameTable) (storage.RangeReader, storage.Source, error) {
	s.fetchCount.Add(1)

	if s.ctrl != nil {
		if s.ctrl.onOpen != nil {
			s.ctrl.onOpen()
		}

		select {
		case s.ctrl.opened <- struct{}{}:
		default:
		}

		end := min(offsetU+length, int64(len(s.data)))

		return &controlledReader{
			data:     s.data[offsetU:end],
			step:     max(16*1024, testBlockSize),
			advance:  s.ctrl.advance,
			consumed: s.ctrl.consumed,
			closed:   s.ctrl.closed,
		}, storage.SourceFS, nil
	}

	var fetchOff, fetchLen int64
	if frameTable.IsCompressed() {
		r, err := frameTable.LocateCompressed(offsetU)
		if err != nil {
			return nil, storage.UnknownSource, fmt.Errorf("frame lookup: %w", err)
		}

		fetchOff = r.Offset
		fetchLen = int64(r.Length)
	} else {
		fetchOff = offsetU
		fetchLen = length
	}

	end := min(fetchOff+fetchLen, int64(len(s.data)))
	if s.failAfter > 0 {
		end = min(end, s.failAfter)
	}

	served := s.data[fetchOff:end]
	if s.corrupt && s.corruptByte >= fetchOff && s.corruptByte < end {
		served = bytes.Clone(served)
		served[s.corruptByte-fetchOff] ^= 0xFF
	}

	r := io.Reader(bytes.NewReader(served))
	if frameTable.IsCompressed() {
		dec, err := storage.NewDecompressReader(storage.NewRangeReader(io.NopCloser(r)), frameTable.CompressionType(), storage.UnknownSource, storage.UnknownSeekableObjectType)

		return dec, storage.SourceFS, err
	}

	return storage.NewRangeReader(io.NopCloser(r)), storage.SourceFS, nil
}

func makeCompressedTestData(tb testing.TB, data []byte) (*storage.FrameTable, *fakeSeekable) {
	tb.Helper()

	ft, compressed, _, err := storage.CompressBytes(tb.Context(), data, storage.CompressConfig{
		Enabled:            true,
		Type:               "lz4",
		EncoderConcurrency: 1,
		FrameEncodeWorkers: 1,
		FrameSizeKB:        testFrameSize / 1024,
		MinPartSizeMB:      50,
	})
	require.NoError(tb, err)

	return ft.Table(), &fakeSeekable{data: compressed}
}

type chunkerTestCase struct {
	name       string
	newChunker func(t *testing.T, data []byte) (*Chunker, storage.RangeOpener, *storage.FrameTable)
}

var allChunkerTestCases = []chunkerTestCase{
	{
		name: "Compressed",
		newChunker: func(t *testing.T, data []byte) (*Chunker, storage.RangeOpener, *storage.FrameTable) {
			t.Helper()
			ft, getter := makeCompressedTestData(t, data)

			return newTestChunker(t, int64(len(data))), getter, ft
		},
	},
	{
		name: "Uncompressed",
		newChunker: func(t *testing.T, data []byte) (*Chunker, storage.RangeOpener, *storage.FrameTable) {
			t.Helper()

			return newTestChunker(t, int64(len(data))), &fakeSeekable{data: data}, nil
		},
	},
}

func TestChunker_BasicSlice(t *testing.T) {
	t.Parallel()

	for _, tc := range allChunkerTestCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data := makeTestData(testFileSize)
			chunker, file, ft := tc.newChunker(t, data)
			defer chunker.Close()

			slice, err := chunker.Slice(t.Context(), 0, testBlockSize, file, ft)
			require.NoError(t, err)
			require.Equal(t, data[:testBlockSize], slice)
		})
	}
}

// TestChunker_CacheHit verifies that a second read of the same block
// is served from cache without an additional upstream fetch.
func TestChunker_CacheHit(t *testing.T) {
	t.Parallel()

	data := makeTestData(testFileSize)

	// Uncompressed only — we need direct access to the fakeSeekable to count fetches.
	file := &fakeSeekable{data: data}
	chunker := newTestChunker(t, int64(len(data)))
	defer chunker.Close()

	// First read triggers a fetch.
	slice1, err := chunker.Slice(t.Context(), 0, testBlockSize, file, nil)
	require.NoError(t, err)
	require.Equal(t, data[:testBlockSize], slice1)

	firstFetches := file.fetchCount.Load()
	require.Positive(t, firstFetches)

	// Second read of the same block — should hit cache.
	slice2, err := chunker.Slice(t.Context(), 0, testBlockSize, file, nil)
	require.NoError(t, err)
	require.Equal(t, data[:testBlockSize], slice2)
	require.Equal(t, firstFetches, file.fetchCount.Load(), "expected no additional upstream fetch")
}

// TestChunker_FullChunkCachedAfterPartialRequest verifies that requesting the
// first block triggers a full background fetch of the entire chunk/frame, so
// the last block becomes available without additional upstream fetches.
func TestChunker_FullChunkCachedAfterPartialRequest(t *testing.T) {
	t.Parallel()

	for _, tc := range allChunkerTestCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data := makeTestData(testFileSize)
			chunker, file, ft := tc.newChunker(t, data)
			defer chunker.Close()

			_, err := chunker.Slice(t.Context(), 0, testBlockSize, file, ft)
			require.NoError(t, err)

			// The second Slice joins the in-flight session (or hits
			// cache if the fetch already completed). Either way it blocks
			// until the data is available — no polling needed.
			lastOff := int64(testFileSize) - testBlockSize
			slice, err := chunker.Slice(t.Context(), lastOff, testBlockSize, file, ft)
			require.NoError(t, err)
			require.Equal(t, data[lastOff:lastOff+testBlockSize], slice)
		})
	}
}

// TestChunker_ConcurrentSameChunk verifies that concurrent requests for the same
// chunk don't cause duplicate upstream fetches.
func TestChunker_ConcurrentSameChunk(t *testing.T) {
	t.Parallel()

	data := makeTestData(testFileSize)

	var fetchCount atomic.Int64
	chunker := newControlledChunker(t, data)
	chunker.onOpen = func() { fetchCount.Add(1) }
	defer chunker.Close()

	const numGoroutines = 10

	var eg errgroup.Group
	started := make(chan struct{})
	for range numGoroutines {
		eg.Go(func() error {
			<-started
			_, sliceErr := chunker.Slice(t.Context(), 0, testBlockSize, nil)

			return sliceErr
		})
	}

	// Release goroutines, wait for the fetch to start (blocked on advance),
	// then release data.
	close(started)
	<-chunker.opened
	close(chunker.advance)

	require.NoError(t, eg.Wait())

	require.Equal(t, int64(1), fetchCount.Load(),
		"expected 1 fetch (dedup), got %d", fetchCount.Load())
}

func TestChunker_EarlyReturn(t *testing.T) {
	t.Parallel()

	data := makeTestData(testFileSize)
	chunker := newControlledChunker(t, data)
	defer chunker.Close()

	lastOff := int64(len(data)) - testBlockSize

	type result struct {
		data []byte
		err  error
	}

	earlyDone := make(chan result, 1)
	lateDone := make(chan result, 1)

	go func() {
		slice, sliceErr := chunker.Slice(t.Context(), 0, testBlockSize, nil)
		earlyDone <- result{data: bytes.Clone(slice), err: sliceErr} // clone: slice backed by mutable mmap
	}()
	go func() {
		slice, sliceErr := chunker.Slice(t.Context(), lastOff, testBlockSize, nil)
		lateDone <- result{data: bytes.Clone(slice), err: sliceErr}
	}()

	// Advance exactly one read step (16KB). This covers offset 0 but is
	// far from the last block, and no further reads can proceed until we
	// send more signals — eliminating the scheduling race.
	chunker.advance <- struct{}{}
	<-chunker.consumed

	// Offset 0 is within the first readBatch — should be available now.
	r := <-earlyDone
	require.NoError(t, r.err)
	require.Equal(t, data[:testBlockSize], r.data)

	// No more reads have been allowed, so the last offset is unreachable.
	select {
	case <-lateDone:
		t.Fatal("late reader completed before its data was delivered")
	default:
	}

	// Release all remaining reads so the late reader can complete.
	close(chunker.advance)
	r = <-lateDone
	require.NoError(t, r.err)
	require.Equal(t, data[lastOff:lastOff+testBlockSize], r.data)
}

// TestChunker_SessionOriginMismatch reproduces a corruption bug in fetch's
// readiness fast path. A fetch session created on one frame geometry (an
// uncompressed whole-file chunk) is reused by a later read that locates a
// different origin — as happens when a peer→storage transition swaps 4MB
// uncompressed peer chunks for smaller compressed frames, so getOrCreateSession
// returns a session framed on the old geometry. bytesReady counts from the
// session's chunkOff; the readiness check must use the same origin. The buggy
// code computed the threshold from the freshly located chunkOff, so a session
// that filled only its first block reported a not-yet-written deeper block as
// ready — serving stale (zero) mmap to the guest instead of fetching it.
//
// The scenario is built synchronously: a partially filled, then terminated,
// session stands in for the aborted peer fetch. With the fix, the shifted-origin
// read finds the block unready and falls through to the terminated session's
// error; with the bug, it short-circuits to SourceMmap and returns no error.
func TestChunker_SessionOriginMismatch(t *testing.T) {
	t.Parallel()

	data := makeTestData(testFileSize)
	// A compressed frame table with testFrameSize-aligned uncompressed frames:
	// locateChunk for the second frame yields chunkOff=testFrameSize, an origin
	// different from the uncompressed session's chunkOff of 0.
	compFT, upstream := makeCompressedTestData(t, data)
	shiftedOff := int64(testFrameSize)

	chunker := newTestChunker(t, int64(len(data)))
	defer chunker.Close()

	// An in-flight session on the uncompressed whole-file geometry that filled
	// only its first block, then terminated (as a peer fetch does when the peer
	// goes away mid-transition). bytesReady covers block 0 but not shiftedOff.
	sess := newFetchSession(0, int64(len(data)), chunker.cache)
	sess.advance(testBlockSize)
	sess.fail(errors.New("peer gone"))

	chunker.fetchMu.Lock()
	chunker.fetchSessions = append(chunker.fetchSessions, sess)
	chunker.fetchMu.Unlock()

	// The shifted-origin read reuses the session. shiftedOff is not ready, so
	// fetch must consult the (terminated) session and surface its error rather
	// than report the block ready and serve stale mmap.
	_, err := chunker.fetch(t.Context(), shiftedOff, testBlockSize, upstream, compFT)
	require.Error(t, err, "fetch reported an unfilled shifted-origin block as ready (served stale mmap)")
	require.ErrorContains(t, err, "peer gone")
}

// TestChunker_ErrorKeepsPartialData verifies that an upstream error at the
// midpoint of a chunk still allows data before the error to be served.
func TestChunker_ErrorKeepsPartialData(t *testing.T) {
	t.Parallel()

	data := makeTestData(testFileSize)

	file := &fakeSeekable{data: data, failAfter: int64(testFileSize / 2)}
	chunker := newTestChunker(t, int64(len(data)))
	defer chunker.Close()

	lastOff := int64(testFileSize) - testBlockSize
	_, err := chunker.Slice(t.Context(), lastOff, testBlockSize, file, nil)
	require.Error(t, err)

	slice, err := chunker.Slice(t.Context(), 0, testBlockSize, file, nil)
	require.NoError(t, err)
	require.Equal(t, data[:testBlockSize], slice)
}

// TestChunker_ContextCancellation verifies that a cancelled caller context
// doesn't kill the background fetch — another caller can still get data.
func TestChunker_ContextCancellation(t *testing.T) {
	t.Parallel()

	data := makeTestData(testFileSize)
	chunker := newControlledChunker(t, data)
	defer chunker.Close()

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan error, 1)
	go func() {
		_, sliceErr := chunker.Slice(ctx, 0, testBlockSize, nil)
		done <- sliceErr
	}()

	// Wait for the fetch goroutine to be blocked on the reader, then cancel.
	<-chunker.opened
	cancel()

	require.Error(t, <-done)

	// Release the fetch — it runs with context.WithoutCancel so it continues.
	close(chunker.advance)
	<-chunker.closed

	// Fetch completed — data is now cached.
	slice, err := chunker.Slice(t.Context(), 0, testBlockSize, nil)
	require.NoError(t, err)
	require.Equal(t, data[:testBlockSize], slice)
}

// TestChunker_LastBlockPartial verifies correct handling of a file whose size
// is not aligned to blockSize — the final block is shorter than blockSize.
func TestChunker_LastBlockPartial(t *testing.T) {
	t.Parallel()

	size := testFileSize - 100
	data := makeTestData(size)

	for _, tc := range allChunkerTestCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			chunker, file, ft := tc.newChunker(t, data)
			defer chunker.Close()

			lastBlockOff := (int64(size) / testBlockSize) * testBlockSize
			remaining := int64(size) - lastBlockOff

			slice, err := chunker.Slice(t.Context(), lastBlockOff, remaining, file, ft)
			require.NoError(t, err)
			require.Equal(t, data[lastBlockOff:], slice)
		})
	}
}

// panicSeekable panics during Read after delivering panicAfter bytes.
type panicSeekable struct {
	data       []byte
	panicAfter int64
}

var _ storage.Seekable = (*panicSeekable)(nil)

func (s *panicSeekable) Size(_ context.Context) (int64, error) {
	return int64(len(s.data)), nil
}

func (s *panicSeekable) StoreFile(context.Context, string, ...storage.PutOption) (*storage.FullFrameTable, [32]byte, error) {
	panic("not used")
}

func (s *panicSeekable) OpenRangeReader(_ context.Context, off int64, length int64, _ *storage.FrameTable) (storage.RangeReader, storage.Source, error) {
	end := min(off+length, int64(len(s.data)))

	return &panicReader{
		data:       s.data[off:end],
		panicAfter: int(s.panicAfter - off),
	}, storage.SourceFS, nil
}

type panicReader struct {
	data       []byte
	pos        int
	panicAfter int
}

func (r *panicReader) Read(p []byte) (int, error) {
	if r.pos >= r.panicAfter {
		panic("simulated upstream panic")
	}

	if r.pos >= len(r.data) {
		return 0, io.EOF
	}

	end := min(r.pos+len(p), len(r.data))
	n := copy(p, r.data[r.pos:end])
	r.pos += n

	return n, nil
}

func (r *panicReader) Close(context.Context) (*storage.ReadStats, error) {
	return nil, nil
}

func TestChunker_PanicRecovery(t *testing.T) {
	t.Parallel()

	data := makeTestData(testFileSize)
	panicAt := int64(testFileSize / 2)

	file := &panicSeekable{data: data, panicAfter: panicAt}
	chunker := newTestChunker(t, int64(len(data)))
	defer chunker.Close()

	// Request data past the panic point — should get an error, not hang or crash
	lastOff := int64(testFileSize) - testBlockSize
	_, err := chunker.Slice(t.Context(), lastOff, testBlockSize, file, nil)
	require.Error(t, err)

	// Data before the panic point should still be cached
	slice, err := chunker.Slice(t.Context(), 0, testBlockSize, file, nil)
	require.NoError(t, err)
	require.Equal(t, data[:testBlockSize], slice)
}

func TestChunker_ConcurrentStress(t *testing.T) {
	t.Parallel()

	for _, tc := range allChunkerTestCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data := makeTestData(testFileSize)
			chunker, file, ft := tc.newChunker(t, data)
			defer chunker.Close()

			const numGoroutines = 50
			const opsPerGoroutine = 5
			readLen := int64(testBlockSize)

			var eg errgroup.Group

			for i := range numGoroutines {
				eg.Go(func() error {
					for j := range opsPerGoroutine {
						off := int64(((i*opsPerGoroutine)+j)%(len(data)/int(readLen))) * readLen
						slice, err := chunker.Slice(t.Context(), off, readLen, file, ft)
						if err != nil {
							return fmt.Errorf("goroutine %d op %d: %w", i, j, err)
						}
						if !bytes.Equal(data[off:off+readLen], slice) {
							return fmt.Errorf("goroutine %d op %d: data mismatch at off=%d", i, j, off)
						}
					}

					return nil
				})
			}

			require.NoError(t, eg.Wait())
		})
	}
}

// controlledChunker bundles a Chunker, its upstream, and the channels
// gating reads through that upstream.
type controlledChunker struct {
	*Chunker
	*testControl

	file *fakeSeekable
}

// Slice forwards to the embedded Chunker with the bundled upstream — saves
// each test from passing cc.file by hand.
func (cc *controlledChunker) Slice(ctx context.Context, off, length int64, ft *storage.FrameTable) ([]byte, error) {
	return cc.Chunker.Slice(ctx, off, length, cc.file, ft)
}

func newControlledChunker(t *testing.T, data []byte) *controlledChunker {
	t.Helper()

	ctrl := &testControl{
		advance:  make(chan struct{}),
		consumed: make(chan struct{}, 10),
		opened:   make(chan struct{}, 10),
		closed:   make(chan struct{}, 10),
	}
	file := &fakeSeekable{data: data, ctrl: ctrl}

	return &controlledChunker{
		Chunker:     newTestChunker(t, int64(len(data))),
		testControl: ctrl,
		file:        file,
	}
}

// controlledReader yields data in fixed-size steps, blocking on advance
// before each Read. After advance is closed, reads proceed immediately.
type controlledReader struct {
	data     []byte
	pos      int
	step     int
	advance  chan struct{}
	consumed chan struct{}
	closed   chan struct{}
}

func (r *controlledReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}

	<-r.advance

	end := min(r.pos+min(len(p), r.step), len(r.data))
	n := copy(p, r.data[r.pos:end])
	r.pos += n

	select {
	case r.consumed <- struct{}{}:
	default:
	}

	return n, nil
}

func (r *controlledReader) Close(context.Context) (*storage.ReadStats, error) {
	select {
	case r.closed <- struct{}{}:
	default:
	}

	return nil, nil
}

// TestChunker_CorruptCompressedFrameNotServedOrCached verifies the integrity
// gap fix: a compressed frame whose CRC only fails at the footer (content
// decodes to plausible bytes) must not be released to waiters or marked
// cached. Without the fix, an exact-size read never triggers the codec's
// footer CRC, the Close error was ignored, and the frame was advanced to
// waiters and cached. Corrupts the final byte of the first compressed frame.
func TestChunker_CorruptCompressedFrameNotServedOrCached(t *testing.T) {
	t.Parallel()

	data := makeTestData(testFileSize)
	ft, file := makeCompressedTestData(t, data)

	// Corrupt the last byte of the first frame's compressed range (footer).
	r, err := ft.LocateCompressed(0)
	require.NoError(t, err)
	file.corrupt = true
	file.corruptByte = r.Offset + int64(r.Length) - 1

	chunker := newTestChunker(t, int64(len(data)))
	defer chunker.Close()

	_, err = chunker.Slice(t.Context(), 0, testBlockSize, file, ft)
	require.Error(t, err, "corrupt compressed frame must surface an error, not serve unverified bytes")
	require.False(t, chunker.IsCached(t.Context(), 0, testBlockSize),
		"corrupt compressed frame must not be marked cached")

	// A later read of a clean frame still works (chunker remains usable).
	lastOff := int64(testFileSize) - testBlockSize
	slice, err := chunker.Slice(t.Context(), lastOff, testBlockSize, file, ft)
	require.NoError(t, err)
	require.Equal(t, data[lastOff:], slice)
}
