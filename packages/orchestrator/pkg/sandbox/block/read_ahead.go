//go:build linux

package block

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// Read-ahead keeps chunk fetches running ahead of a reader that walks a rootfs
// file cold, in order. Without it such a reader has one fetch in flight at a
// time and waits a full storage round trip per chunk, so its throughput is one
// chunk per round trip however much bandwidth the node has.

const (
	// readAheadMaxStreams bounds how many sequential readers of one file are
	// followed at once whatever the flag says (several sandboxes can share a
	// build file, and one guest can read two files of its disk at once).
	readAheadMaxStreams = 32
	// readAheadMaxChunks bounds the window whatever the flag says.
	readAheadMaxChunks = 64
	// readAheadSlot is the granularity of the unread bitset. Every chunk
	// starts on a multiple of it: uncompressed chunks are MemoryChunkSize
	// aligned and compression frames are a multiple of the huge page size.
	readAheadSlot = int64(header.HugepageSize)
)

// readAheadOutcome is one value of the outcome label of the read-ahead bytes
// counter. Every chunk read ahead ends as exactly one of used, failed or
// unused; issued counts it when it starts, and capped (node cap) and
// file_capped (per-file cap) count chunks not started.
type readAheadOutcome struct {
	name string
	opt  metric.AddOption
}

func newReadAheadOutcome(name string) readAheadOutcome {
	return readAheadOutcome{name: name, opt: metric.WithAttributeSet(attribute.NewSet(attribute.String("outcome", name)))}
}

var (
	readAheadBytes = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorBlockReadAheadBytes))

	readAheadIssued     = newReadAheadOutcome("issued")
	readAheadUsed       = newReadAheadOutcome("used")
	readAheadFailed     = newReadAheadOutcome("failed")
	readAheadUnused     = newReadAheadOutcome("unused")
	readAheadCapped     = newReadAheadOutcome("capped")
	readAheadFileCapped = newReadAheadOutcome("file_capped")

	readAheadStreamEvents = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorBlockReadAheadStreams))
	// readAheadFailLog limits the read-ahead failure warning per node.
	readAheadFailLog = &logLimiter{every: time.Minute}

	// nodeReadAhead counts the read-ahead fetches in flight across every file
	// on the orchestrator, for the node cap.
	nodeReadAhead = &readAheadLimiter{}
	_             = utils.Must(telemetry.GetObservableUpDownCounter(meter, telemetry.OrchestratorBlockReadAheadInflight, observeReadAheadInflight))
)

func recordReadAhead(ctx context.Context, n int64, o readAheadOutcome) {
	readAheadBytes.Add(ctx, n, o.opt)
}

// streamEvent is what follow did with a cold read.
type streamEvent int

const (
	// streamSame: the read stayed in its stream's last chunk; not counted.
	streamSame streamEvent = iota
	// streamSequential: the read continued a stream in order.
	streamSequential
	// streamNew: the read started a stream in a free slot.
	streamNew
	// streamReplaced: the read started a stream in the slot of a live one.
	streamReplaced
)

var streamEventOpts = [...]metric.AddOption{
	streamSequential: metric.WithAttributeSet(attribute.NewSet(attribute.String("event", "sequential"))),
	streamNew:        metric.WithAttributeSet(attribute.NewSet(attribute.String("event", "new"))),
	streamReplaced:   metric.WithAttributeSet(attribute.NewSet(attribute.String("event", "replaced"))),
}

func recordStreamEvent(ctx context.Context, ev streamEvent) {
	if ev != streamSame {
		readAheadStreamEvents.Add(ctx, 1, streamEventOpts[ev])
	}
}

// logLimiter lets a log through at most once per every and counts the ones
// it held back, so a failing backend does not flood the logs.
type logLimiter struct {
	every time.Duration

	mu         sync.Mutex
	last       time.Time
	suppressed int64
}

// allow reports whether to log at now and, if so, how many were held back
// since the last one.
func (l *logLimiter) allow(now time.Time) (bool, int64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.last.IsZero() && now.Sub(l.last) < l.every {
		l.suppressed++

		return false, 0
	}
	n := l.suppressed
	l.last, l.suppressed = now, 0

	return true, n
}

func observeReadAheadInflight(_ context.Context, observer metric.Int64Observer) error {
	observer.Observe(nodeReadAhead.inflight.Load())

	return nil
}

// readAheadLimiter admits read-ahead fetches up to a limit read at each
// admission, so the limit can change while fetches are in flight.
type readAheadLimiter struct {
	inflight atomic.Int64
}

func (l *readAheadLimiter) tryAcquire(limit int64) bool {
	for {
		n := l.inflight.Load()
		if n >= limit {
			return false
		}
		if l.inflight.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

func (l *readAheadLimiter) release() {
	l.inflight.Add(-1)
}

// readAheadStream is one reader being followed through a file.
type readAheadStream struct {
	valid bool
	// lastOff and lastEnd are the chunk of the stream's last cold read.
	lastOff, lastEnd int64
	// aheadEnd is the end of the furthest chunk read ahead for the stream, 0 if none.
	aheadEnd int64
	// cappedEnd is the end of the last chunk a cap kept from being read
	// ahead, so retrying the same chunk counts it as capped once.
	cappedEnd int64
	lastUse   uint64
}

// readAheadChunk is a chunk read ahead and not yet given its outcome.
type readAheadChunk struct {
	length  int64
	session *fetchSession
	// read is set when a read reaches the chunk while it is being fetched.
	read bool
	// fetched is set when the fetch succeeded before any read reached the chunk.
	fetched bool
	// cancel cancels the fetch, for close.
	cancel context.CancelFunc
}

// readAhead is a Chunker's read-ahead state. The fields below mu are guarded
// by it. unread is read without it.
type readAhead struct {
	node *readAheadLimiter
	// record counts bytes by outcome and recordStream follow's events;
	// variables so tests can see the counts.
	record       func(ctx context.Context, n int64, o readAheadOutcome)
	recordStream func(ctx context.Context, ev streamEvent)
	// unread has a bit per readAheadSlot of the file, set for every slot of a
	// chunk read ahead and not read since, so a read served from the cache
	// checks it without the lock. Nil for files that are never read ahead.
	unread []atomic.Uint64

	mu sync.Mutex
	// streams has readAheadMaxStreams slots; the flag picks how many of the
	// first ones are used. Nil for files that are never read ahead.
	streams []readAheadStream
	clock   uint64
	// inflight is this file's read-ahead fetches in flight, for the per-file
	// cap of a full window for each followed stream.
	inflight int
	// chunks holds the chunks read ahead and not given an outcome, by offset.
	chunks map[int64]*readAheadChunk
	closed bool
}

// init sets up read-ahead for a file of size bytes; only rootfs files get it.
func (ra *readAhead) init(objType storage.SeekableObjectType, size int64) {
	ra.node = nodeReadAhead
	ra.record = recordReadAhead
	ra.recordStream = recordStreamEvent
	if objType == storage.RootFSObjectType {
		slots := (size + readAheadSlot - 1) / readAheadSlot
		ra.unread = make([]atomic.Uint64, (slots+63)/64)
		ra.streams = make([]readAheadStream, readAheadMaxStreams)
	}
}

// setUnread sets or clears the unread bits of the chunk at [off, off+length).
// Must be called with mu held.
func (ra *readAhead) setUnread(off, length int64, unread bool) {
	for slot := off / readAheadSlot; slot <= (off+length-1)/readAheadSlot; slot++ {
		word, bit := slot/64, uint64(1)<<(slot%64)
		if word >= int64(len(ra.unread)) {
			return
		}
		if unread {
			ra.unread[word].Or(bit)
		} else {
			ra.unread[word].And(^bit)
		}
	}
}

// anyUnread reports whether [off, off+length) touches a chunk read ahead and
// not read since. Lock-free.
func (ra *readAhead) anyUnread(off, length int64) bool {
	for slot := off / readAheadSlot; slot <= (off+length-1)/readAheadSlot; slot++ {
		word := slot / 64
		if word >= int64(len(ra.unread)) {
			return false
		}
		if ra.unread[word].Load()&(uint64(1)<<(slot%64)) != 0 {
			return true
		}
	}

	return false
}

// follow records a cold read of the chunks [firstOff, lastEnd), lastOff being
// the start of its last chunk, and returns the stream it belongs to and what
// happened: streamSequential when the read continued that stream in order.
// Only the first nStreams slots are used. A read that continues no stream
// starts one in the least recently used of them.
// Must be called with mu held.
func (ra *readAhead) follow(firstOff, lastOff, lastEnd int64, nStreams int) (*readAheadStream, streamEvent) {
	ra.clock++

	var victim *readAheadStream
	for i := range ra.streams[:nStreams] {
		s := &ra.streams[i]
		if s.valid {
			if lastOff == s.lastOff {
				s.lastUse = ra.clock

				return s, streamSame
			}
			// Forward from the stream's last chunk to at most the end of what
			// was read ahead for it: the next chunk, a chunk still being
			// fetched ahead, or the first chunk past the fetched-ahead ones
			// (the reader read through them from the cache).
			if lastOff > s.lastOff && firstOff >= s.lastOff && firstOff <= max(s.lastEnd, s.aheadEnd) {
				s.lastOff, s.lastEnd, s.lastUse = lastOff, lastEnd, ra.clock

				return s, streamSequential
			}
		}
		if victim == nil || olderSlot(s, victim) {
			victim = s
		}
	}

	ev := streamNew
	if victim.valid {
		ev = streamReplaced
	}
	*victim = readAheadStream{valid: true, lastOff: lastOff, lastEnd: lastEnd, lastUse: ra.clock}

	return victim, ev
}

// olderSlot reports whether a is a better slot to reuse than b: an empty slot
// first, then the one used least recently.
func olderSlot(a, b *readAheadStream) bool {
	if a.valid != b.valid {
		return !a.valid
	}

	return a.lastUse < b.lastUse
}

// readAheadSettings is one evaluation of the rootfs-read-ahead flag for a cold
// read, clamped.
type readAheadSettings struct {
	// window is how many chunks to keep fetching ahead of a sequential reader
	// of this file, 0 if read-ahead is off for it.
	window int
	// streams is how many sequential readers of this file to follow at once,
	// between 1 and readAheadMaxStreams.
	streams int
	// nodeLimit caps the read-ahead fetches in flight on the node.
	nodeLimit int64
}

// readAheadSettings evaluates the rootfs-read-ahead flag once. Files that are
// never read ahead skip the evaluation and get window 0.
func (c *Chunker) readAheadSettings(ctx context.Context) readAheadSettings {
	if c.readAhead.unread == nil {
		return readAheadSettings{}
	}
	cfg := featureflags.GetRootfsReadAheadConfig(ctx, c.featureFlags)

	return readAheadSettings{
		window:    min(cfg.Chunks, readAheadMaxChunks),
		streams:   max(1, min(cfg.Streams, readAheadMaxStreams)),
		nodeLimit: int64(cfg.NodeMaxInflight),
	}
}

// readAheadOnColdRead runs for a read of [off, off+length) the cache could not
// serve whole. When the read continues a sequential stream, it starts fetches
// for the chunks up to window chunks past the read that are not cached or
// being fetched yet. It returns without waiting for them.
func (c *Chunker) readAheadOnColdRead(ctx context.Context, off, length int64, upstream storage.RangeOpener, ft *storage.FrameTable) {
	if length <= 0 {
		return
	}
	settings := c.readAheadSettings(ctx)
	window := settings.window
	if window <= 0 {
		return
	}
	firstOff, _, err := c.locateChunk(off, ft)
	if err != nil {
		return
	}
	lastOff, lastLen, err := c.locateChunk(off+length-1, ft)
	if err != nil || lastLen <= 0 {
		return
	}
	nodeLimit, nStreams := settings.nodeLimit, settings.streams

	ra := &c.readAhead
	ra.mu.Lock()
	defer ra.mu.Unlock()

	if ra.closed {
		return
	}

	s, ev := ra.follow(firstOff, lastOff, lastOff+lastLen, nStreams)
	ra.recordStream(ctx, ev)
	if ev != streamSequential {
		return
	}

	cur := s.lastEnd
	for range window {
		if cur >= c.size {
			return
		}
		chunkOff, chunkLen, err := c.locateChunk(cur, ft)
		// A frame off a slot boundary (frames under 2 MiB) ends read-ahead here.
		if err != nil || chunkLen <= 0 || chunkOff%readAheadSlot != 0 {
			return
		}
		chunkEnd := chunkOff + chunkLen
		if chunkEnd > s.aheadEnd {
			if !c.startReadAheadLocked(ctx, s, chunkOff, chunkLen, upstream, ft, window*nStreams, nodeLimit) {
				// A cap is full: leave aheadEnd here so the next cold read
				// of the stream tries this chunk again.
				return
			}
			s.aheadEnd = chunkEnd
		}
		cur = chunkEnd
	}
}

// startReadAheadLocked starts a background fetch of the chunk at
// [off, off+length) for stream s unless it is cached or already being
// fetched. It returns false, starting nothing, when this file already has
// fileLimit read-ahead fetches in flight (a window for each stream it can
// follow), or the node has nodeLimit.
// Must be called with c.readAhead.mu held.
func (c *Chunker) startReadAheadLocked(ctx context.Context, s *readAheadStream, off, length int64, upstream storage.RangeOpener, ft *storage.FrameTable, fileLimit int, nodeLimit int64) bool {
	ra := &c.readAhead

	c.fetchMu.Lock()
	defer c.fetchMu.Unlock()

	for _, session := range c.fetchSessions {
		if session.contains(off, length) {
			return true
		}
	}
	if c.cache.isCached(off, length) {
		return true
	}

	if ra.inflight >= fileLimit {
		ra.noteCapped(ctx, s, off, length, readAheadFileCapped)

		return false
	}
	if !ra.node.tryAcquire(nodeLimit) {
		ra.noteCapped(ctx, s, off, length, readAheadCapped)

		return false
	}
	ra.inflight++

	session := newFetchSession(off, length, c.cache)
	session.readAhead = true
	c.fetchSessions = append(c.fetchSessions, session)

	// Detached from the reader, which does not wait for it, but cancelled
	// when the file closes: nobody needs the chunk then.
	fetchCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))

	if ra.chunks == nil {
		ra.chunks = make(map[int64]*readAheadChunk)
	}
	ra.chunks[off] = &readAheadChunk{length: length, session: session, cancel: cancel}
	ra.setUnread(off, length, true)
	ra.record(ctx, length, readAheadIssued)

	go func() {
		defer cancel()
		c.runFetch(fetchCtx, session, upstream, ft)
	}()

	return true
}

// noteCapped counts a chunk a cap kept from being read ahead for stream s,
// once per stream and chunk: the cold reads that retry it are not counted
// again, whichever cap holds it then.
// Must be called with mu held.
func (ra *readAhead) noteCapped(ctx context.Context, s *readAheadStream, off, length int64, o readAheadOutcome) {
	if s.cappedEnd != off+length {
		s.cappedEnd = off + length
		ra.record(ctx, length, o)
	}
}

// readAheadFetchDone releases a finished read-ahead fetch's caps and gives
// its chunk an outcome, or leaves it waiting for a read or for close.
func (c *Chunker) readAheadFetchDone(ctx context.Context, session *fetchSession) {
	ra := &c.readAhead
	ra.node.release()

	failed := session.err() != nil

	ra.mu.Lock()
	ra.inflight--
	var outcome *readAheadOutcome
	chunk := ra.chunks[session.chunkOff]
	if chunk != nil && chunk.session == session {
		switch {
		case failed && !ra.closed:
			outcome = &readAheadFailed
		case failed, ra.closed && !chunk.read:
			// Cancelled at close, or fetched for a file nobody reads anymore.
			outcome = &readAheadUnused
		case chunk.read:
			outcome = &readAheadUsed
		default:
			chunk.fetched = true
		}
		if outcome != nil {
			delete(ra.chunks, session.chunkOff)
			ra.setUnread(session.chunkOff, session.chunkLen, false)
		}
	}
	ra.mu.Unlock()

	if outcome != nil {
		ra.record(ctx, session.chunkLen, *outcome)
	}
	if outcome == &readAheadFailed {
		// The read that reaches this chunk fetches it itself, so nothing else
		// reports the failure.
		if ok, held := readAheadFailLog.allow(time.Now()); ok {
			logger.L().Warn(ctx, "rootfs read-ahead fetch failed; the read will fetch the chunk itself",
				logger.WithBuildID(c.buildID),
				zap.Int64("offset", session.chunkOff),
				zap.Int64("length", session.chunkLen),
				zap.String("source", session.Source().String()),
				zap.Int64("failures_not_logged_since_last", held),
				zap.Error(session.err()))
		}
	}
}

// readAheadNoteRead marks the chunks read ahead that a read of
// [off, off+length) overlaps as read. A chunk already fetched counts as used
// now; one still being fetched counts as used when its fetch succeeds. Chunks
// are matched by overlap, not by the read's chunk geometry, which can change
// after a chunk was read ahead.
func (c *Chunker) readAheadNoteRead(ctx context.Context, off, length int64) {
	ra := &c.readAhead
	if length <= 0 || !ra.anyUnread(off, length) {
		return
	}

	var used int64

	ra.mu.Lock()
	for chunkOff, chunk := range ra.chunks {
		if chunk.read || chunkOff >= off+length || chunkOff+chunk.length <= off {
			continue
		}
		chunk.read = true
		ra.setUnread(chunkOff, chunk.length, false)
		if chunk.fetched {
			delete(ra.chunks, chunkOff)
			used += chunk.length
		}
	}
	ra.mu.Unlock()

	if used > 0 {
		ra.record(ctx, used, readAheadUsed)
	}
}

// closeReadAhead stops read-ahead for the file: it counts the chunks fetched
// ahead and never read as unused and cancels the fetches still in flight,
// which count as unused when they end.
func (c *Chunker) closeReadAhead() {
	ra := &c.readAhead
	if ra.unread == nil {
		return
	}

	var unused int64

	ra.mu.Lock()
	ra.closed = true
	for off, chunk := range ra.chunks {
		if chunk.fetched {
			unused += chunk.length
			delete(ra.chunks, off)
			ra.setUnread(off, chunk.length, false)
		} else {
			chunk.cancel()
		}
	}
	ra.mu.Unlock()

	if unused > 0 {
		ra.record(context.Background(), unused, readAheadUnused)
	}
}
