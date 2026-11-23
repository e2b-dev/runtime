//go:build linux

package block

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

const raChunk = int64(storage.MemoryChunkSize)

// raUpstream serves data and records the chunk offsets it was asked for. A
// held offset blocks its open until released; a failOnce offset fails its
// first open.
type raUpstream struct {
	data []byte

	mu       sync.Mutex
	opened   []int64
	hold     map[int64]chan struct{}
	failOnce map[int64]bool
}

func newRAUpstream(chunks int) *raUpstream {
	data := make([]byte, int64(chunks)*raChunk)
	for i := range data {
		data[i] = byte(i*7 + i/int(raChunk))
	}

	return &raUpstream{data: data, hold: map[int64]chan struct{}{}, failOnce: map[int64]bool{}}
}

func (u *raUpstream) OpenRangeReader(ctx context.Context, off, length int64, _ *storage.FrameTable) (storage.RangeReader, storage.Source, error) {
	u.mu.Lock()
	u.opened = append(u.opened, off)
	gate := u.hold[off]
	fail := u.failOnce[off]
	delete(u.failOnce, off)
	u.mu.Unlock()

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, storage.SourceFS, ctx.Err()
		}
	}
	if fail {
		return nil, storage.SourceFS, errors.New("injected open failure")
	}

	end := min(off+length, int64(len(u.data)))

	return storage.NewRangeReader(io.NopCloser(bytes.NewReader(u.data[off:end]))), storage.SourceFS, nil
}

func (u *raUpstream) holdChunk(idx int64) func() {
	ch := make(chan struct{})
	u.mu.Lock()
	u.hold[idx*raChunk] = ch
	u.mu.Unlock()

	return sync.OnceFunc(func() { close(ch) })
}

// openedChunks returns the chunk indexes opened so far, sorted.
func (u *raUpstream) openedChunks() []int64 {
	u.mu.Lock()
	defer u.mu.Unlock()

	out := make([]int64, 0, len(u.opened))
	for _, off := range u.opened {
		out = append(out, off/raChunk)
	}
	slices.Sort(out)

	return out
}

func readAheadFlags(t *testing.T, window, nodeMax int) *featureflags.Client {
	t.Helper()

	return readAheadFlagValue(t, ldvalue.ObjectBuild().
		Set("chunks", ldvalue.Int(window)).
		Set("nodeMaxInflight", ldvalue.Int(nodeMax)).
		Build())
}

// readAheadFlagValue serves v as the rootfs-read-ahead flag.
func readAheadFlagValue(t *testing.T, v ldvalue.Value) *featureflags.Client {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.RootfsReadAheadFlag.Key()).ValueForAll(v))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return ff
}

// readAheadFlagsWithStreams is readAheadFlags with the streams field set too;
// readAheadFlags leaves it out, so it takes its default.
func readAheadFlagsWithStreams(t *testing.T, window, nodeMax, streams int) *featureflags.Client {
	t.Helper()

	return readAheadFlagValue(t, ldvalue.ObjectBuild().
		Set("chunks", ldvalue.Int(window)).
		Set("nodeMaxInflight", ldvalue.Int(nodeMax)).
		Set("streams", ldvalue.Int(streams)).
		Build())
}

// raCountsByChunker holds, per chunker, its read-ahead bytes by outcome
// counted in chunks.
var raCountsByChunker sync.Map

type raCounts struct {
	mu     sync.Mutex
	chunks map[string]int64
	// streams counts follow's events, in reads.
	streams map[string]int64
}

// newRAChunker returns a chunker over u with its own node limiter, so
// parallel tests don't share the node cap, and its own outcome counts.
func newRAChunker(t *testing.T, ff *featureflags.Client, objType storage.SeekableObjectType, u *raUpstream) *Chunker {
	t.Helper()

	c, err := NewChunker(ff, int64(len(u.data)), testBlockSize, t.TempDir()+"/cache", newTestMetrics(t), objType, "test-build")
	require.NoError(t, err)
	c.readAhead.node = &readAheadLimiter{}

	counts := &raCounts{chunks: map[string]int64{}, streams: map[string]int64{}}
	c.readAhead.record = func(_ context.Context, n int64, o readAheadOutcome) {
		counts.mu.Lock()
		defer counts.mu.Unlock()
		counts.chunks[o.name] += n / raChunk
	}
	c.readAhead.recordStream = func(_ context.Context, ev streamEvent) {
		if name := streamEventNames[ev]; name != "" {
			counts.mu.Lock()
			defer counts.mu.Unlock()
			counts.streams[name]++
		}
	}
	raCountsByChunker.Store(c, counts)

	return c
}

// streamEventNames keys follow's events in the counts, next to the outcomes.
var streamEventNames = map[streamEvent]string{
	streamSequential: "stream_sequential",
	streamNew:        "stream_new",
	streamReplaced:   "stream_replaced",
}

// countsOf returns c's read-ahead outcomes so far, in chunks.
func countsOf(t *testing.T, c *Chunker) map[string]int64 {
	t.Helper()

	v, ok := raCountsByChunker.Load(c)
	require.True(t, ok)
	counts := v.(*raCounts)
	counts.mu.Lock()
	defer counts.mu.Unlock()

	return maps.Clone(counts.chunks)
}

// streamCountsOf returns follow's events for c so far, in reads.
func streamCountsOf(t *testing.T, c *Chunker) map[string]int64 {
	t.Helper()

	v, ok := raCountsByChunker.Load(c)
	require.True(t, ok)
	counts := v.(*raCounts)
	counts.mu.Lock()
	defer counts.mu.Unlock()

	return maps.Clone(counts.streams)
}

// readChunk reads the first block of chunk idx and checks its bytes.
func readChunk(t *testing.T, c *Chunker, u *raUpstream, idx int64) {
	t.Helper()

	off := idx * raChunk
	got, err := c.Slice(t.Context(), off, testBlockSize, u, nil)
	require.NoError(t, err)
	require.Equal(t, u.data[off:off+testBlockSize], got)
}

// waitReadAheadIdle waits until no fetch of c is in flight or still listed.
func waitReadAheadIdle(t *testing.T, c *Chunker) {
	t.Helper()

	require.Eventually(t, func() bool {
		c.readAhead.mu.Lock()
		raIdle := c.readAhead.inflight == 0 && c.readAhead.node.inflight.Load() == 0
		c.readAhead.mu.Unlock()

		return raIdle && len(sessionChunks(c, false)) == 0
	}, 5*time.Second, time.Millisecond)
}

// sessionChunks returns the chunk indexes of c's fetch sessions, sorted;
// with readAheadOnly, only those of read-ahead fetches. A session is listed
// when its fetch starts, before its upstream open runs.
func sessionChunks(c *Chunker, readAheadOnly bool) []int64 {
	c.fetchMu.Lock()
	defer c.fetchMu.Unlock()

	out := make([]int64, 0, len(c.fetchSessions))
	for _, s := range c.fetchSessions {
		if s.readAhead || !readAheadOnly {
			out = append(out, s.chunkOff/raChunk)
		}
	}
	slices.Sort(out)

	return out
}

// unreadChunks returns the chunks read ahead that no read has reached yet.
func unreadChunks(c *Chunker) []int64 {
	c.readAhead.mu.Lock()
	defer c.readAhead.mu.Unlock()

	out := make([]int64, 0, len(c.readAhead.chunks))
	for off, chunk := range c.readAhead.chunks {
		if !chunk.read {
			out = append(out, off/raChunk)
		}
	}
	slices.Sort(out)

	return out
}

func TestReadAheadFollow(t *testing.T) {
	t.Parallel()

	const cs = raChunk
	stream := readAheadStream{valid: true, lastOff: 4 * cs, lastEnd: 5 * cs, aheadEnd: 8 * cs, lastUse: 1}

	tests := []struct {
		name  string
		first int64 // chunk index of the read's first byte
		last  int64 // chunk index of the read's last byte
		ev    streamEvent
		same  bool // the read stays in the stream's slot
	}{
		{name: "next chunk", first: 5, last: 5, ev: streamSequential, same: true},
		{name: "chunk being read ahead", first: 6, last: 6, ev: streamSequential, same: true},
		{name: "first chunk past the window", first: 8, last: 8, ev: streamSequential, same: true},
		{name: "read spanning into the next chunk", first: 4, last: 5, ev: streamSequential, same: true},
		{name: "same chunk again", first: 4, last: 4, ev: streamSame, same: true},
		{name: "beyond the window", first: 9, last: 9, ev: streamNew, same: false},
		{name: "backwards", first: 3, last: 3, ev: streamNew, same: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ra := readAhead{streams: make([]readAheadStream, readAheadMaxStreams), clock: 1}
			ra.streams[0] = stream

			s, ev := ra.follow(tc.first*cs, tc.last*cs, (tc.last+1)*cs, 4)
			assert.Equal(t, tc.ev, ev)
			assert.Equal(t, tc.same, s == &ra.streams[0])
			assert.Equal(t, tc.last*cs, s.lastOff)
			assert.Equal(t, (tc.last+1)*cs, s.lastEnd)
			if !tc.same {
				assert.Equal(t, stream, ra.streams[0], "an unrelated read must leave the stream alone")
				assert.Zero(t, s.aheadEnd)
			}
		})
	}
}

func TestReadAheadFollowReplacesLeastRecentlyUsed(t *testing.T) {
	t.Parallel()

	const nStreams = 4
	ra := readAhead{streams: make([]readAheadStream, readAheadMaxStreams)}
	// Fill every used slot with a stream, slot i used at time i+1, so slot 0 is the oldest.
	for i := range nStreams {
		_, ev := ra.follow(int64(i)*100*raChunk, int64(i)*100*raChunk, (int64(i)*100+1)*raChunk, nStreams)
		require.Equal(t, streamNew, ev, "slot %d was free", i)
	}
	ra.follow(100*raChunk, 100*raChunk, 101*raChunk, nStreams) // touch slot 1 again

	s, ev := ra.follow(1000*raChunk, 1000*raChunk, 1001*raChunk, nStreams)
	assert.Equal(t, streamReplaced, ev, "every used slot was taken")
	assert.Same(t, &ra.streams[0], s, "the least recently used slot is reused")
	assert.Equal(t, int64(100)*raChunk, ra.streams[1].lastOff, "a recently used stream survives")
	assert.False(t, ra.streams[nStreams].valid, "slots past nStreams stay unused")
}

func TestReadAhead_StreamsIsClamped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		unset   bool // leave the flag at its default
		streams int
		wants   int
	}{
		{name: "default", unset: true, wants: 4},
		{name: "set", streams: 6, wants: 6},
		{name: "zero", streams: 0, wants: 1},
		{name: "negative", streams: -3, wants: 1},
		{name: "above the bound", streams: 1000, wants: readAheadMaxStreams},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ff := readAheadFlags(t, 1, 32)
			if !tc.unset {
				ff = readAheadFlagsWithStreams(t, 1, 32, tc.streams)
			}
			u := newRAUpstream(1)
			c := newRAChunker(t, ff, storage.RootFSObjectType, u)
			defer c.Close()

			assert.Equal(t, tc.wants, c.readAheadSettings(t.Context()).streams)
		})
	}
}

// A crowd of readers walking one file in lockstep: with fewer stream slots
// than readers, each cold read takes the slot of a reader that has not come
// back yet, so no reader is ever seen reading in order.
func TestReadAhead_MoreReadersThanStreams(t *testing.T) {
	t.Parallel()

	const readers = 6
	tests := []struct {
		name    string
		streams int
		issued  int64
		events  map[string]int64
	}{
		// 4 slots: the first 4 cold reads start streams, every later one takes a live reader's slot.
		{name: "fewer slots than readers", streams: 4, issued: 0, events: map[string]int64{"stream_new": 4, "stream_replaced": 8}},
		{name: "a slot per reader", streams: readers, issued: readers, events: map[string]int64{"stream_new": readers, "stream_sequential": readers}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			u := newRAUpstream(4 * readers)
			c := newRAChunker(t, readAheadFlagsWithStreams(t, 1, 32, tc.streams), storage.RootFSObjectType, u)
			defer c.Close()

			// Reader i walks chunks 4i, 4i+1, ...; all read their first chunk, then all their second.
			for step := range int64(2) {
				for i := range int64(readers) {
					readChunk(t, c, u, 4*i+step)
				}
			}
			waitReadAheadIdle(t, c)

			assert.Equal(t, tc.issued, countsOf(t, c)["issued"])
			streams := streamCountsOf(t, c)
			for _, name := range streamEventNames {
				assert.Equal(t, tc.events[name], streams[name], name)
			}
			if tc.issued > 0 {
				for i := range int64(readers) {
					assert.Contains(t, u.openedChunks(), 4*i+2, "reader %d gets its next chunk read ahead", i)
				}
			}
		})
	}
}

func TestReadAhead_SequentialReaderFetchesAhead(t *testing.T) {
	t.Parallel()

	u := newRAUpstream(10)
	c := newRAChunker(t, readAheadFlags(t, 3, 32), storage.RootFSObjectType, u)

	readChunk(t, c, u, 0)
	waitReadAheadIdle(t, c)
	assert.Equal(t, []int64{0}, u.openedChunks(), "one cold read is not a sequential reader yet")

	readChunk(t, c, u, 1)
	waitReadAheadIdle(t, c)
	assert.Equal(t, []int64{0, 1, 2, 3, 4}, u.openedChunks(), "the second chunk in a row reads 3 chunks ahead")
	assert.Equal(t, []int64{2, 3, 4}, unreadChunks(c))

	// Reading the fetched-ahead chunks is served from the cache and counts them as used.
	readChunk(t, c, u, 2)
	readChunk(t, c, u, 3)
	readChunk(t, c, u, 3)
	assert.Equal(t, []int64{0, 1, 2, 3, 4}, u.openedChunks())
	assert.Equal(t, []int64{4}, unreadChunks(c))
	assert.Equal(t, map[string]int64{"issued": 3, "used": 2}, countsOf(t, c))

	// Chunk 4 was never read: unused once the file closes.
	require.NoError(t, c.Close())
	assert.Equal(t, map[string]int64{"issued": 3, "used": 2, "unused": 1}, countsOf(t, c))
}

func TestReadAhead_ReaderPastTheWindowReadsAheadAgain(t *testing.T) {
	t.Parallel()

	u := newRAUpstream(10)
	c := newRAChunker(t, readAheadFlags(t, 2, 32), storage.RootFSObjectType, u)
	defer c.Close()

	readChunk(t, c, u, 0)
	readChunk(t, c, u, 1)
	waitReadAheadIdle(t, c)
	require.Equal(t, []int64{0, 1, 2, 3}, u.openedChunks())

	// 2 and 3 come from the cache, unseen by read-ahead; the cold read of 4 is
	// the reader arriving at the end of the window.
	readChunk(t, c, u, 2)
	readChunk(t, c, u, 3)
	readChunk(t, c, u, 4)
	waitReadAheadIdle(t, c)
	assert.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6}, u.openedChunks())
}

func TestReadAhead_RandomReadsDoNotReadAhead(t *testing.T) {
	t.Parallel()

	u := newRAUpstream(12)
	c := newRAChunker(t, readAheadFlags(t, 4, 32), storage.RootFSObjectType, u)
	defer c.Close()

	for _, idx := range []int64{5, 1, 9, 3, 11, 0} {
		readChunk(t, c, u, idx)
	}
	waitReadAheadIdle(t, c)
	assert.Equal(t, []int64{0, 1, 3, 5, 9, 11}, u.openedChunks())
	assert.Empty(t, unreadChunks(c))
}

func TestReadAhead_TwoInterleavedReaders(t *testing.T) {
	t.Parallel()

	u := newRAUpstream(12)
	c := newRAChunker(t, readAheadFlags(t, 1, 32), storage.RootFSObjectType, u)
	defer c.Close()

	for _, idx := range []int64{0, 6, 1, 7} {
		readChunk(t, c, u, idx)
	}
	waitReadAheadIdle(t, c)
	assert.Equal(t, []int64{0, 1, 2, 6, 7, 8}, u.openedChunks(), "each reader gets its own read-ahead")
}

func TestReadAhead_OffByDefault(t *testing.T) {
	t.Parallel()

	u := newRAUpstream(6)
	c := newRAChunker(t, &featureflags.Client{}, storage.RootFSObjectType, u)
	defer c.Close()

	for idx := range int64(4) {
		readChunk(t, c, u, idx)
		require.Empty(t, sessionChunks(c, true))
	}
	assert.Equal(t, []int64{0, 1, 2, 3}, u.openedChunks())
}

func TestReadAhead_OnlyForRootfs(t *testing.T) {
	t.Parallel()

	u := newRAUpstream(6)
	c := newRAChunker(t, readAheadFlags(t, 3, 32), storage.MemfileObjectType, u)
	defer c.Close()

	for idx := range int64(4) {
		readChunk(t, c, u, idx)
		require.Empty(t, sessionChunks(c, true))
	}
	assert.Equal(t, []int64{0, 1, 2, 3}, u.openedChunks())
}

func TestReadAhead_WindowIsClamped(t *testing.T) {
	t.Parallel()

	u := newRAUpstream(1)
	c := newRAChunker(t, readAheadFlags(t, 1000, 32), storage.RootFSObjectType, u)
	defer c.Close()

	assert.Equal(t, readAheadMaxChunks, c.readAheadSettings(t.Context()).window)
}

func TestReadAhead_NodeCap(t *testing.T) {
	t.Parallel()

	u := newRAUpstream(8)
	c := newRAChunker(t, readAheadFlags(t, 3, 1), storage.RootFSObjectType, u)
	defer c.Close()
	release := u.holdChunk(2)
	defer release()

	readChunk(t, c, u, 0)
	readChunk(t, c, u, 1)

	assert.Equal(t, []int64{2}, sessionChunks(c, true), "with a node cap of 1, only one chunk is read ahead")
	assert.Equal(t, int64(1), c.readAhead.node.inflight.Load())

	// A second cold read of the stream retries chunk 3, still capped; it counts once.
	c.readAheadOnColdRead(t.Context(), 2*raChunk, testBlockSize, u, nil)
	assert.Equal(t, []int64{2}, sessionChunks(c, true))
	assert.Equal(t, map[string]int64{"issued": 1, "capped": 1}, countsOf(t, c))

	release()
	waitReadAheadIdle(t, c)

	// The reader moves on into the window; the chunks the cap skipped are read ahead now.
	readChunk(t, c, u, 2)
	readChunk(t, c, u, 3)
	waitReadAheadIdle(t, c)
	assert.Contains(t, u.openedChunks(), int64(4))
}

func TestReadAhead_PerFileCap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		unset   bool // leave the streams flag at its default
		streams int64
	}{
		{name: "default streams", unset: true, streams: 4},
		{name: "streams flag", streams: 6},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Window 1: the file may have one read-ahead in flight per followed stream.
			ff := readAheadFlags(t, 1, 32)
			if !tc.unset {
				ff = readAheadFlagsWithStreams(t, 1, 32, int(tc.streams))
			}
			u := newRAUpstream(int(4 * (tc.streams + 1)))
			c := newRAChunker(t, ff, storage.RootFSObjectType, u)
			defer c.Close()

			// One reader per stream reads two chunks in a row and gets one
			// chunk read ahead, held in flight.
			var held []int64
			var releases []func()
			defer func() {
				for _, release := range releases {
					release()
				}
				waitReadAheadIdle(t, c)
			}()
			for i := range tc.streams {
				start := i * 4
				releases = append(releases, u.holdChunk(start+2))
				readChunk(t, c, u, start)
				readChunk(t, c, u, start+1)
				held = append(held, start+2)
			}
			require.Equal(t, held, sessionChunks(c, true))

			// One more reader takes a stream slot, but the file is at its cap.
			last := tc.streams * 4
			readChunk(t, c, u, last)
			readChunk(t, c, u, last+1)
			assert.Equal(t, held, sessionChunks(c, true), "chunk %d waits for a free read-ahead slot", last+2)
			// Counted as held by the file's cap, once: reading the chunk again retries it without a new count.
			readChunk(t, c, u, last+1)
			assert.Equal(t, int64(1), countsOf(t, c)["file_capped"])
			assert.Zero(t, countsOf(t, c)["capped"], "the node cap was not reached")
		})
	}
}

func TestReadAhead_ReadWhileFetchingAheadCountsAsUsed(t *testing.T) {
	t.Parallel()

	u := newRAUpstream(6)
	c := newRAChunker(t, readAheadFlags(t, 1, 32), storage.RootFSObjectType, u)
	defer c.Close()
	release := u.holdChunk(2)
	defer release()

	readChunk(t, c, u, 0)
	readChunk(t, c, u, 1)
	require.Eventually(t, func() bool { return slices.Contains(u.openedChunks(), 2) }, 5*time.Second, time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := c.Slice(context.WithoutCancel(t.Context()), 2*raChunk, testBlockSize, u, nil)
		assert.NoError(t, err)
	}()
	// The read marks chunk 2 as read, then slides the window on to chunk 3.
	require.Eventually(t, func() bool {
		return !slices.Contains(unreadChunks(c), 2) && countsOf(t, c)["issued"] == 2
	}, 5*time.Second, time.Millisecond)

	assert.Equal(t, map[string]int64{"issued": 2}, countsOf(t, c), "used only once the fetch succeeds")

	release()
	<-done
	waitReadAheadIdle(t, c)
	assert.Equal(t, 1, countOf(u.openedChunks(), 2), "the read joins the fetch in flight")
	assert.Equal(t, map[string]int64{"issued": 2, "used": 1}, countsOf(t, c))
}

func TestReadAhead_FailedFetchIsNotPendingAndIsRefetched(t *testing.T) {
	t.Parallel()

	u := newRAUpstream(6)
	u.failOnce[2*raChunk] = true
	c := newRAChunker(t, readAheadFlags(t, 1, 32), storage.RootFSObjectType, u)
	defer c.Close()

	readChunk(t, c, u, 0)
	readChunk(t, c, u, 1)
	waitReadAheadIdle(t, c)
	require.Equal(t, []int64{0, 1, 2}, u.openedChunks())
	assert.Empty(t, unreadChunks(c), "a failed read-ahead is not waiting to be used")
	assert.Equal(t, map[string]int64{"issued": 1, "failed": 1}, countsOf(t, c))

	// The cold read of 2 fetches it again on demand, and reads 3 ahead.
	readChunk(t, c, u, 2)
	waitReadAheadIdle(t, c)
	assert.Equal(t, []int64{0, 1, 2, 2, 3}, u.openedChunks())
}

func TestReadAhead_ReadFindingAFailedReadAheadFetchesItself(t *testing.T) {
	t.Parallel()

	u := newRAUpstream(4)
	c := newRAChunker(t, &featureflags.Client{}, storage.RootFSObjectType, u)
	defer c.Close()

	// A read-ahead fetch of chunk 2 that failed and is still listed, as it
	// is from its failure until its fetch goroutine removes it.
	failed := newFetchSession(2*raChunk, raChunk, c.cache)
	failed.readAhead = true
	failed.fail(errors.New("injected read-ahead failure"))
	c.fetchMu.Lock()
	c.fetchSessions = append(c.fetchSessions, failed)
	c.fetchMu.Unlock()

	readChunk(t, c, u, 2)
	assert.Equal(t, []int64{2}, u.openedChunks(), "the read fetched the chunk itself")
}

func TestReadAhead_CloseCancelsReadAheadInFlight(t *testing.T) {
	t.Parallel()

	u := newRAUpstream(6)
	c := newRAChunker(t, readAheadFlags(t, 1, 32), storage.RootFSObjectType, u)
	release := u.holdChunk(2)
	defer release()

	readChunk(t, c, u, 0)
	readChunk(t, c, u, 1)
	require.Equal(t, []int64{2}, sessionChunks(c, true))

	c.closeReadAhead()
	waitReadAheadIdle(t, c)
	assert.Equal(t, map[string]int64{"issued": 1, "unused": 1}, countsOf(t, c))
	require.NoError(t, c.Close())
}

func TestReadAhead_CloseStopsReadAhead(t *testing.T) {
	t.Parallel()

	u := newRAUpstream(6)
	c := newRAChunker(t, readAheadFlags(t, 2, 32), storage.RootFSObjectType, u)

	readChunk(t, c, u, 0)
	readChunk(t, c, u, 1)
	waitReadAheadIdle(t, c)
	require.Equal(t, []int64{2, 3}, unreadChunks(c))

	c.closeReadAhead()
	assert.Empty(t, unreadChunks(c))
	assert.False(t, c.readAhead.anyUnread(0, c.size))
	assert.Equal(t, map[string]int64{"issued": 2, "unused": 2}, countsOf(t, c))

	// The next cold read continues the stream, but nothing is read ahead after close.
	c.readAheadOnColdRead(t.Context(), 4*raChunk, testBlockSize, u, nil)
	assert.Empty(t, sessionChunks(c, true))
	waitReadAheadIdle(t, c)
	require.NoError(t, c.Close())
}

func TestReadAhead_RetryAfterAFailedReadAheadSharesOneFetchTimeout(t *testing.T) {
	t.Parallel()

	const timeout = 500 * time.Millisecond
	u := newRAUpstream(6)
	c := newRAChunker(t, readAheadFlags(t, 1, 32), storage.RootFSObjectType, u)
	c.fetchTimeout = timeout
	defer c.Close()
	release := u.holdChunk(2)

	readChunk(t, c, u, 0)
	readChunk(t, c, u, 1)
	require.Equal(t, []int64{2}, sessionChunks(c, true))

	// The read joins the read-ahead fetch of 2, which hangs until its timeout
	// and fails. The retry must not add a second full timeout, or the read
	// answers after the NBD request timeout.
	start := time.Now()
	_, err := c.Slice(t.Context(), 2*raChunk, testBlockSize, u, nil)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Less(t, elapsed, timeout*3/2)

	release()
	waitReadAheadIdle(t, c)
}

func TestReadAhead_ReadJoiningALaterReadAheadCountsAsUsed(t *testing.T) {
	t.Parallel()

	u := newRAUpstream(8)
	c := newRAChunker(t, readAheadFlags(t, 2, 32), storage.RootFSObjectType, u)
	release := u.holdChunk(4)
	defer release()

	// Reader B reads across chunks 4 and 5 and waits on its fetch of 4. Nothing
	// is read ahead yet, so its read has no read-ahead chunk to mark.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := c.Slice(context.WithoutCancel(t.Context()), 5*raChunk-testBlockSize, 2*testBlockSize, u, nil)
		assert.NoError(t, err)
	}()
	require.Eventually(t, func() bool { return slices.Contains(u.openedChunks(), 4) }, 5*time.Second, time.Millisecond)

	// Reader A reads 2 and 3 in order: 4 is already being fetched, so 5 is read ahead.
	readChunk(t, c, u, 2)
	readChunk(t, c, u, 3)
	require.Eventually(t, func() bool {
		return slices.Contains(u.openedChunks(), 5) && len(sessionChunks(c, true)) == 0
	}, 5*time.Second, time.Millisecond)

	release()
	<-done
	waitReadAheadIdle(t, c)
	require.NoError(t, c.Close())
	assert.Equal(t, map[string]int64{"issued": 1, "used": 1}, countsOf(t, c))
}

func TestReadAhead_ReadAfterTheChunkGeometryChangedCountsAsUsed(t *testing.T) {
	t.Parallel()

	u := newRAUpstream(6)
	c := newRAChunker(t, readAheadFlags(t, 1, 32), storage.RootFSObjectType, u)

	readChunk(t, c, u, 0)
	readChunk(t, c, u, 1)
	waitReadAheadIdle(t, c)
	require.Equal(t, []int64{2}, unreadChunks(c))

	// The build is now read through 2 MiB frames, so the second half of chunk
	// 2 is a frame of its own. Its bytes are already in the cache.
	ft := raFrameTable(len(u.data), 2<<20)
	off := 2*raChunk + raChunk/2
	got, err := c.Slice(t.Context(), off, testBlockSize, u, ft)
	require.NoError(t, err)
	require.Equal(t, u.data[off:off+testBlockSize], got)

	require.NoError(t, c.Close())
	assert.Equal(t, map[string]int64{"issued": 1, "used": 1}, countsOf(t, c))
}

func TestReadAhead_FramesOffASlotBoundaryAreNotReadAhead(t *testing.T) {
	t.Parallel()

	u := newRAUpstream(2)
	c := newRAChunker(t, readAheadFlags(t, 4, 32), storage.RootFSObjectType, u)
	defer c.Close()

	// Two reads in a row make a sequential reader, but with 256 KiB frames the
	// next frame does not start on a readAheadSlot boundary.
	ft := raFrameTable(len(u.data), 256<<10)
	c.readAheadOnColdRead(t.Context(), 0, testBlockSize, u, ft)
	c.readAheadOnColdRead(t.Context(), 256<<10, testBlockSize, u, ft)

	assert.Empty(t, sessionChunks(c, true))
	assert.Empty(t, countsOf(t, c))
}

// raFrameTable returns a compressed frame table of size bytes in frames of frameSize.
func raFrameTable(size, frameSize int) *storage.FrameTable {
	frames := make([]storage.FrameSize, size/frameSize)
	for i := range frames {
		frames[i] = storage.FrameSize{U: int32(frameSize), C: int32(frameSize / 2)}
	}

	return storage.NewFullFrameTable(storage.CompressionZstd, frames).Table()
}

func countOf(s []int64, v int64) int {
	n := 0
	for _, x := range s {
		if x == v {
			n++
		}
	}

	return n
}

func TestLogLimiter(t *testing.T) {
	t.Parallel()

	l := &logLimiter{every: time.Minute}
	t0 := time.Unix(1_000_000, 0)

	ok, held := l.allow(t0)
	assert.True(t, ok, "the first one logs")
	assert.Zero(t, held)

	for i := range 3 {
		ok, _ = l.allow(t0.Add(time.Duration(i+1) * 10 * time.Second))
		assert.False(t, ok, "within a minute of the last log")
	}

	ok, held = l.allow(t0.Add(time.Minute))
	assert.True(t, ok, "a minute after the last log")
	assert.Equal(t, int64(3), held, "reports the ones held back")

	ok, held = l.allow(t0.Add(3 * time.Minute))
	assert.True(t, ok)
	assert.Zero(t, held, "the held-back count resets after each log")
}

func TestFetchAttrsMarkReadAheadOnly(t *testing.T) {
	t.Parallel()

	attrsOf := func(o metric.MeasurementOption) map[attribute.Key]attribute.Value {
		set := metric.NewRecordConfig([]metric.RecordOption{o}).Attributes()
		out := map[attribute.Key]attribute.Value{}
		for _, kv := range set.ToSlice() {
			out[kv.Key] = kv.Value
		}

		return out
	}
	failed := errors.New("boom")

	for _, err := range []error{nil, failed} {
		demand := attrsOf(fetchAttrs(&fetchSession{}, storage.RootFSObjectType, storage.SourceGCS, storage.CompressionNone, err))
		assert.NotContains(t, demand, attribute.Key(storage.AttrReadAhead), "demand fetches keep their existing series")
		assert.Equal(t, "gcs", demand[storage.AttrSource].AsString())

		ahead := attrsOf(fetchAttrs(&fetchSession{readAhead: true}, storage.RootFSObjectType, storage.SourceGCS, storage.CompressionNone, err))
		require.Contains(t, ahead, attribute.Key(storage.AttrReadAhead))
		assert.True(t, ahead[storage.AttrReadAhead].AsBool())
		assert.Equal(t, "rootfs", ahead[storage.AttrFileType].AsString())
		assert.Equal(t, "gcs", ahead[storage.AttrSource].AsString())
		assert.Equal(t, demand[storage.AttrOutcome], ahead[storage.AttrOutcome], "same outcome label as a demand fetch")
	}
}

func TestReadAhead_MalformedFlagIsOff(t *testing.T) {
	t.Parallel()

	for name, v := range map[string]ldvalue.Value{
		"a bare number":         ldvalue.Int(8),
		"chunks as a string":    ldvalue.Parse([]byte(`{"chunks": "8"}`)),
		"an array":              ldvalue.Parse([]byte(`[8, 16, 32]`)),
		"an object without any": ldvalue.Parse([]byte(`{}`)),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			u := newRAUpstream(6)
			c := newRAChunker(t, readAheadFlagValue(t, v), storage.RootFSObjectType, u)
			defer c.Close()

			for idx := range int64(4) {
				readChunk(t, c, u, idx)
			}
			waitReadAheadIdle(t, c)
			assert.Equal(t, []int64{0, 1, 2, 3}, u.openedChunks(), "a sequential reader gets no read-ahead")
			assert.Zero(t, c.readAheadSettings(t.Context()).window)
		})
	}
}
