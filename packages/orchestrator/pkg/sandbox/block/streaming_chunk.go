//go:build linux

package block

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	// defaultFetchTimeout is the maximum time a single 4MB chunk fetch may take.
	// Acts as a safety net: if the upstream hangs, the goroutine won't live forever.
	defaultFetchTimeout = 60 * time.Second
)

type Chunker struct {
	cache        *Cache
	metrics      metrics.Metrics
	fetchTimeout time.Duration
	featureFlags *featureflags.Client
	objType      storage.SeekableObjectType
	// buildID names the build whose file this chunker reads. It goes on every
	// fetch span, so a fetch can be keyed by (build, file type, offset).
	buildID string

	size int64

	fetchMu       sync.Mutex
	fetchSessions []*fetchSession
}

func NewChunker(
	ff *featureflags.Client,
	size, blockSize int64,
	cachePath string,
	metrics metrics.Metrics,
	objType storage.SeekableObjectType,
	buildID string,
) (*Chunker, error) {
	cache, err := NewCache(size, blockSize, cachePath, false)
	if err != nil {
		return nil, fmt.Errorf("failed to create file cache: %w", err)
	}

	return &Chunker{
		size:         size,
		cache:        cache,
		metrics:      metrics,
		featureFlags: ff,
		fetchTimeout: defaultFetchTimeout,
		objType:      objType,
		buildID:      buildID,
	}, nil
}

// ReadAt and Slice take {upstream, ft} as a paired snapshot from the caller.
// The caller is responsible for keeping them consistent.
func (c *Chunker) ReadAt(ctx context.Context, b []byte, off int64, upstream storage.RangeOpener, ft *storage.FrameTable) (int, error) {
	slice, err := c.Slice(ctx, off, int64(len(b)), upstream, ft)
	if err != nil {
		return 0, fmt.Errorf("failed to slice cache at %d-%d: %w", off, off+int64(len(b)), err)
	}

	return copy(b, slice), nil
}

func (c *Chunker) Slice(ctx context.Context, off, length int64, upstream storage.RangeOpener, ft *storage.FrameTable) ([]byte, error) {
	ct := ft.CompressionType()
	sliceStart := time.Now()

	// Fast path: already cached.
	b, err := c.cache.Slice(off, length)
	if err == nil {
		c.metrics.ChunkSliceTimerFactory.Record(ctx, time.Since(sliceStart), length, storage.OKAttrs(c.objType, storage.SourceMmap, ct))
		storage.ReportSource(ctx, storage.SourceMmap)

		return b, nil
	}

	if !errors.As(err, &BytesNotAvailableError{}) {
		c.metrics.ChunkSliceTimerFactory.Record(ctx, time.Since(sliceStart), 0, storage.ErrAttrs(c.objType, storage.SourceMmap, ct, err))

		return nil, fmt.Errorf("failed read from cache at offset %d: %w", off, err)
	}

	// Fetch every chunk the range spans (one fetch session per chunk).
	var src storage.Source
	end := off + length
	for cur := off; cur < end; {
		chunkOff, chunkLen, lerr := c.locateChunk(cur, ft)
		if lerr != nil {
			c.metrics.ChunkSliceTimerFactory.Record(ctx, time.Since(sliceStart), 0, storage.ErrAttrs(c.objType, src, ct, lerr))

			return nil, fmt.Errorf("failed to locate chunk for offset %d: %w", cur, lerr)
		}
		chunkEnd := chunkOff + chunkLen
		rangeEnd := min(end, chunkEnd)
		s, err := c.fetch(ctx, cur, rangeEnd-cur, upstream, ft)
		if err != nil {
			c.metrics.ChunkSliceTimerFactory.Record(ctx, time.Since(sliceStart), 0, storage.ErrAttrs(c.objType, s, ct, err))

			return nil, fmt.Errorf("failed to ensure data at %d-%d: %w", cur, rangeEnd, err)
		}
		src = max(src, s)
		cur = chunkEnd
	}

	// sliceDirect skips isCached — the waiter already confirmed the data is in the mmap.
	b, cacheErr := c.cache.sliceDirect(off, length)
	if cacheErr != nil {
		c.metrics.ChunkSliceTimerFactory.Record(ctx, time.Since(sliceStart), 0, storage.ErrAttrs(c.objType, src, ct, cacheErr))

		return nil, fmt.Errorf("failed to read from cache after ensuring data at %d-%d: %w", off, off+length, cacheErr)
	}

	c.metrics.ChunkSliceTimerFactory.Record(ctx, time.Since(sliceStart), length, storage.OKAttrs(c.objType, src, ct))
	storage.ReportSource(ctx, src)

	return b, nil
}

// getOrCreateSession returns a fetch session for the chunk at [off, off+length),
// or (nil, true) if the data is already fully cached.
func (c *Chunker) getOrCreateSession(ctx context.Context, off, length int64, upstream storage.RangeOpener, ft *storage.FrameTable) (_ *fetchSession, cached bool) {
	c.fetchMu.Lock()
	defer c.fetchMu.Unlock()

	for _, s := range c.fetchSessions {
		if s.contains(off, length) {
			return s, false
		}
	}

	// Re-check cache under fetchMu. A fetch can finish (marking blocks
	// cached via setIsCached) and remove itself from sessions between
	// the lock-free Slice() and the session scan above. The lock
	// provides a happens-before guarantee that the bitmap writes are visible.
	if c.cache.isCached(off, length) {
		return nil, true
	}

	s := newFetchSession(off, length, c.cache)
	c.fetchSessions = append(c.fetchSessions, s)

	// Detach from the caller's cancel signal so the shared fetch goroutine
	// continues even if the first caller's context is cancelled. Trace/value
	// context is preserved for metrics. The (upstream, ft) pair is captured
	// by value here — in-flight sessions are unaffected by later swaps.
	go c.runFetch(context.WithoutCancel(ctx), s, upstream, ft)

	return s, false
}

// fetch ensures the chunk for [off, off+length) is fetched and waits
// for every block the range spans (a span can cross block boundaries
// after dedup; waiting only on the start block leaves the tail unfetched).
func (c *Chunker) fetch(ctx context.Context, off, length int64, upstream storage.RangeOpener, ft *storage.FrameTable) (storage.Source, error) {
	// (upstream, ft) is a per-call snapshot, so this read can be served by a
	// session created before a peer→storage switch and framed on a different
	// geometry (4MB uncompressed peer chunks vs smaller compressed frames).
	// bytesReady counts from the session's chunkOff, so all readiness math
	// below is in the session's frame.
	chunkOff, chunkLen, err := c.locateChunk(off, ft)
	if err != nil {
		return storage.UnknownSource, fmt.Errorf("failed to locate chunk for offset %d: %w", off, err)
	}

	session, justGotCached := c.getOrCreateSession(ctx, chunkOff, chunkLen, upstream, ft)
	if justGotCached {
		return storage.SourceMmap, nil
	}

	blockSize := c.cache.BlockSize()
	startBlock := (off / blockSize) * blockSize
	endBlock := ((off + length - 1) / blockSize) * blockSize

	chunkEnd := session.chunkOff + session.chunkLen

	// Already streamed past every byte we need: it's in the mmap, source=mmap.
	endByte := min(endBlock+blockSize, chunkEnd) - session.chunkOff
	if session.bytesReady.Load() >= endByte {
		return storage.SourceMmap, nil
	}

	for b := startBlock; b <= endBlock; b += blockSize {
		if b >= chunkEnd {
			break // tail belongs to the caller's next chunk fetch.
		}
		if err := session.registerAndWait(ctx, b); err != nil {
			return session.Source(), err
		}
	}

	return session.Source(), nil
}

// runFetch fetches data from storage into the mmap cache. Runs in a background goroutine.
func (c *Chunker) runFetch(ctx context.Context, s *fetchSession, upstream storage.RangeOpener, ft *storage.FrameTable) {
	ctx, cancel := context.WithTimeout(ctx, c.fetchTimeout)
	defer cancel()

	ctx, span := tracer.Start(ctx, "chunk.fetch")
	defer span.End()
	span.SetAttributes(
		attribute.Int64("off", s.chunkOff),
		attribute.Int64("len", s.chunkLen),
		telemetry.WithBuildID(c.buildID),
		attribute.String(storage.AttrFileType, c.objType.String()),
	)

	defer c.releaseSession(s)

	// Unconditionally terminate the session on exit so registerAndWait
	// never blocks forever — whether the fetch succeeded, failed, or panicked.
	defer func() {
		if r := recover(); r != nil {
			s.failIfRunning(fmt.Errorf("fetch panicked: %v", r))

			return
		}

		// Safety net: if no code path called setDone/fail, terminate now.
		s.failIfRunning(errors.New("fetch exited without completing"))
	}()

	mmapSlice, releaseLock, err := c.cache.addressBytes(s.chunkOff, s.chunkLen)
	if err != nil {
		s.fail(err)

		return
	}
	defer releaseLock()

	ct := ft.CompressionType()

	fetchStart := time.Now()

	res, err := c.progressiveFetch(ctx, s, mmapSlice, upstream, ft)
	var readBytes int64
	if res.stats != nil {
		readBytes = res.stats.DeliveredBytes
	}
	if err != nil {
		storage.RecordReadFetch(ctx, time.Since(fetchStart), readBytes, storage.ErrAttrs(c.objType, res.source, ct, err))

		s.fail(err)

		return
	}

	// Mark entire chunk as cached BEFORE releasing waiters.
	// This ensures isCached returns true before the session is removed from fetchSessions,
	// closing the TOCTOU window in getOrCreateSession.
	c.cache.setIsCached(s.chunkOff, s.chunkLen)

	fetchDuration := time.Since(fetchStart)
	storage.RecordReadFetch(ctx, fetchDuration, readBytes, storage.OKAttrs(c.objType, res.source, ct))

	// fetch wall / (open + read + decompress); >1 = unaccounted overhead.
	if res.stats != nil {
		if work := res.openDuration + res.stats.Read + res.stats.Decompress; work > 0 {
			ratio := fetchDuration.Seconds() / work.Seconds()
			storage.RecordPipelineEfficiency(ctx, ratio, storage.OKAttrs(c.objType, res.source, ct))
		}
	}

	s.setDone()
}

// fetchStats is one progressiveFetch outcome: the resolved source, the open/TTFB
// wall, and the read sub-stage stats (nil if the open failed).
type fetchStats struct {
	source       storage.Source
	openDuration time.Duration
	stats        *storage.ReadStats
}

func (c *Chunker) progressiveFetch(ctx context.Context, s *fetchSession, mmapSlice []byte, upstream storage.RangeOpener, ft *storage.FrameTable) (res fetchStats, err error) {
	openStart := time.Now()
	reader, source, err := upstream.OpenRangeReader(ctx, s.chunkOff, s.chunkLen, ft)
	res.source = source
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String(storage.AttrSource, source.String()))
	// A peer serves the uncompressed format, so without a compressed table the
	// stored size of the chunk is unknown and left out.
	if ft.IsCompressed() || source != storage.SourcePeer {
		span.SetAttributes(attribute.Int64("stored_len", storedLen(ft, s.chunkOff, s.chunkLen)))
	}
	res.openDuration = time.Since(openStart)
	s.setSource(source)
	if err != nil {
		return res, fmt.Errorf("failed to open range reader at %d: %w", s.chunkOff, err)
	}

	defer func() {
		var closeErr error
		res.stats, closeErr = reader.Close(context.WithoutCancel(ctx))

		// A compressed frame's CRC is only verified once its footer is consumed
		// on Close, so a Close error is a verification failure that must fail
		// the fetch (don't cache or release a corrupt frame). A read error, if
		// any, takes precedence.
		if err == nil {
			err = closeErr
		}

		ct := ft.CompressionType()
		attrs := storage.OKAttrs(c.objType, source, ct)
		if err != nil {
			attrs = storage.ErrAttrs(c.objType, source, ct, err)
		}

		var readDur time.Duration
		var readBytes int64
		if res.stats != nil {
			readDur, readBytes = res.stats.Read, res.stats.StoredBytes
		}
		storage.RecordReadRead(ctx, readDur, readBytes, attrs)
	}()

	blockSize := c.cache.BlockSize()
	readBatch := max(blockSize, int64(c.featureFlags.IntFlag(ctx, featureflags.MinChunkerReadSizeKB))*1024)

	var totalRead int64
	for totalRead < s.chunkLen {
		// Read in batches of max(blockSize, minReadBatchSize) to align notification
		// granularity with the read size and minimize lock/notify overhead.
		readEnd := min(totalRead+readBatch, s.chunkLen)
		n, readErr := io.ReadFull(reader, mmapSlice[totalRead:readEnd])
		totalRead += int64(n)

		if n > 0 && !ft.IsCompressed() {
			// Dirty marking is deferred to runFetch after the full chunk is fetched.
			// With coarse dirty granularity, marking here would expose partially-written data.
			//
			// Compressed chunks are a single frame whose CRC is only verified
			// once the footer is consumed on Close. Releasing waiters here
			// would hand out bytes that a later CRC failure proves corrupt, so
			// their release is deferred to setDone after verification.
			s.advance(totalRead)
		}

		if readErr != nil {
			if totalRead >= s.chunkLen {
				break // all bytes received; trailing EOF is expected
			}

			return res, fmt.Errorf("failed reading at offset %d after %d bytes: %w", s.chunkOff, totalRead, readErr)
		}
	}

	return res, nil
}

// releaseSession removes s from the active list (swap-delete).
func (c *Chunker) releaseSession(s *fetchSession) {
	c.fetchMu.Lock()
	defer c.fetchMu.Unlock()

	for i, a := range c.fetchSessions {
		if a == s {
			c.fetchSessions[i] = c.fetchSessions[len(c.fetchSessions)-1]
			c.fetchSessions[len(c.fetchSessions)-1] = nil
			c.fetchSessions = c.fetchSessions[:len(c.fetchSessions)-1]

			return
		}
	}
}

// locateChunk returns the aligned (offset, length) of the chunk containing off.
// For compressed data the frame table defines chunk boundaries; for
// uncompressed data chunks are MemoryChunkSize-aligned (for backwards
// compatibility) and clamped to file size.
// storedLen returns how many bytes the chunk at off occupies in storage: its
// compressed frame, or length itself when the data is uncompressed.
func storedLen(ft *storage.FrameTable, off, length int64) int64 {
	if !ft.IsCompressed() {
		return length
	}
	r, err := ft.LocateCompressed(off)
	if err != nil {
		return 0
	}

	return int64(r.Length)
}

func (c *Chunker) locateChunk(off int64, ft *storage.FrameTable) (chunkOff, chunkLen int64, err error) {
	if ft.IsCompressed() {
		r, err := ft.LocateUncompressed(off)
		if err != nil {
			return 0, 0, err
		}

		return r.Offset, int64(r.Length), nil
	}

	chunkOff = (off / storage.MemoryChunkSize) * storage.MemoryChunkSize

	return chunkOff, min(int64(storage.MemoryChunkSize), c.size-chunkOff), nil
}

func (c *Chunker) Close() error {
	return c.cache.Close()
}

// IsCached reports whether [off, off+length) is fully present in the local
// mmap cache (no remote fetch needed). Used by best-effort dedup.
func (c *Chunker) IsCached(_ context.Context, off, length int64) bool {
	return c.cache.isCached(off, length)
}

func (c *Chunker) Size() int64 {
	return c.size
}

func (c *Chunker) FileSize(ctx context.Context) (int64, error) {
	return c.cache.FileSize(ctx)
}
