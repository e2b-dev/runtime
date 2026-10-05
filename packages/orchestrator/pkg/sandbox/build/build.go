//go:build linux

package build

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	buildMeter = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build")

	// frameTableRefreshTimer measures ancestor-header refresh events as the
	// standard (duration, bytes, count) triple. A refresh is triggered when a
	// parent had no Builds[self] entry (proactive load at source resolution)
	// or a P2P peer transitions to storage mid-read.
	//
	// Attributes:
	//   cause     — proactive | peer_transitioned
	//   file_type — memfile | rootfs
	//   result    — success | failure (added by TimerFactory)
	frameTableRefreshTimer = utils.Must(telemetry.NewTimerFactory(buildMeter,
		"orchestrator.storage.diff.frame_table_refresh",
		"Duration of frame-table refresh header loads",
		"Bytes loaded during frame-table refreshes",
		"Frame-table refresh events",
	))

	fileReadAtTimer = utils.Must(telemetry.NewFloatTimerFactory(buildMeter, "orchestrator.file.read_at",
		"Time to serve a build.File ReadAt across all source builds",
		"Bytes read"))
)

type File struct {
	header atomic.Pointer[header.Header]
	// durable, when set, is the future for the header this file will eventually
	// settle on (the deduped header while a provisional header is being served).
	// DurableHeader waits on it so a pause never parents a snapshot off a
	// provisional (local-only) header — see SetDurableHeader.
	durable     atomic.Pointer[utils.SetOnce[*header.Header]]
	store       *DiffStore
	fileType    DiffType
	persistence storage.StorageProvider
	metrics     blockmetrics.Metrics

	okAttrs metric.MeasurementOption
}

func NewFile(
	header *header.Header,
	store *DiffStore,
	fileType DiffType,
	persistence storage.StorageProvider,
	metrics blockmetrics.Metrics,
) *File {
	f := &File{
		store:       store,
		fileType:    fileType,
		persistence: persistence,
		metrics:     metrics,
		okAttrs: metric.WithAttributes(
			attribute.String(storage.AttrFileType, string(fileType)),
			attribute.String(storage.AttrOutcome, storage.OutcomeOK),
		),
	}
	f.header.Store(header)

	return f
}

func (b *File) Header() *header.Header {
	return b.header.Load()
}

func (b *File) SwapHeader(h *header.Header) {
	b.header.Store(h)
}

// SwapHeaderIfCurrent atomically replaces the header with next only if it is
// still old, reporting whether it did. The provisional→deduped swap uses this so
// that, if it runs late, it can't clobber a newer header already installed by
// another writer (e.g. Upload.publish finalizing the build, which swaps
// unconditionally) with the older, still-incomplete deduped header.
func (b *File) SwapHeaderIfCurrent(old, next *header.Header) bool {
	return b.header.CompareAndSwap(old, next)
}

// SetDurableHeader records the future for the durable header this file will
// settle on, so DurableHeader can return it instead of the currently-installed
// (possibly provisional) header. Set once, before the file is shared.
func (b *File) SetDurableHeader(f *utils.SetOnce[*header.Header]) {
	b.durable.Store(f)
}

// DurableHeader returns the header safe to persist as a parent: the durable
// future's value if one was set (waiting for it), otherwise the currently
// installed header. Callers building an *uploaded* snapshot header (a Pause)
// must use this, never Header(), so they never inherit a provisional header's
// synthetic build-id mappings, which have no storage object.
func (b *File) DurableHeader(ctx context.Context) (*header.Header, error) {
	// Once the live header is finalized (the upload's publish cleared
	// IncompletePendingUpload), it is the most complete header for this build —
	// prefer it over the durable future, which resolves to the pre-finalize
	// deduped header and omits data the upload adds (e.g. the self build's frame
	// table). Only while the live header is still incomplete — it may be the
	// provisional header, whose synthetic build id has no storage object — do we
	// fall back to the durable future, so a parent never inherits provisional
	// mappings.
	if h := b.header.Load(); h != nil && !h.IncompletePendingUpload {
		return h, nil
	}

	if f := b.durable.Load(); f != nil {
		return f.WaitWithContext(ctx)
	}

	return b.Header(), nil
}

// DurableHeaderNow is the non-blocking form of DurableHeader: it returns
// (header, true) when the durable header is already known — no provisional swap
// pending, or the deduped future has resolved — and (nil, false) while a swap is
// still pending. Latency-sensitive callers (e.g. scheduling metadata on the
// create/resume response path) use it so they neither block on the dedup nor
// observe the provisional header's synthetic build id.
func (b *File) DurableHeaderNow() (*header.Header, bool) {
	// See DurableHeader: a finalized live header is the most complete, so prefer
	// it over the (pre-finalize) durable future.
	if h := b.header.Load(); h != nil && !h.IncompletePendingUpload {
		return h, true
	}

	f := b.durable.Load()
	if f == nil {
		return b.Header(), true
	}
	select {
	case <-f.Done:
		h, err := f.Wait()

		return h, err == nil
	default:
		return nil, false
	}
}

// ReadAt times file.read_at around readAt; Slice composes via readAt directly to
// avoid double counting.
func (b *File) ReadAt(ctx context.Context, p []byte, off int64) (n int, err error) {
	start := time.Now()
	n, err = b.readAt(ctx, p, off)
	// io.EOF is a normal short-read outcome (io.ReaderAt), not an I/O error.
	if err != nil && !errors.Is(err, io.EOF) {
		fileReadAtTimer.Record(ctx, time.Since(start), int64(n), metric.WithAttributes(
			attribute.String(storage.AttrFileType, string(b.fileType)),
			attribute.String(storage.AttrOutcome, storage.Outcome(err)),
		))

		return n, err
	}
	fileReadAtTimer.Record(ctx, time.Since(start), int64(n), b.okAttrs)

	return n, err
}

// readAt fills p from the mapped build segments, optionally in parallel.
// Cache eviction or a peer transition re-resolves and retries.
func (b *File) readAt(ctx context.Context, p []byte, off int64) (int, error) {
	maxParallel := b.store.flags.IntFlag(ctx, featureflags.MaxParallelBuildReadSegments)

	for {
		segments, n, distinctBuilds, err := b.planRead(ctx, p, off)
		if err == nil {
			err = b.readSegments(ctx, p, segments, maxParallel)
		}
		if err == nil {
			// Recorded only for the attempt that succeeded, so eviction /
			// transition retries don't double-count the read.
			recordReadFanout(ctx, b.fileType, len(segments), distinctBuilds)

			// Fewer bytes than requested means the mappings ran out: report the
			// bytes filled so far with io.EOF, matching io.ReaderAt semantics.
			if n < len(p) {
				return n, io.EOF
			}

			return n, nil
		}

		// A Diff can be evicted and closed between planning and reading. Re-plan
		// the whole read; reads are idempotent, so re-filling already-written
		// regions is safe and getBuild re-resolves the closed Diff.
		if _, ok := errors.AsType[*block.CacheClosedError](err); ok {
			continue
		}

		return 0, err
	}
}

type readSegment struct {
	dstOff int
	srcOff int64
	length int64
	diff   Diff

	// ft uses the nil-vs-empty convention: nil = no entry,
	// storage.UncompressedFrameTable = authoritatively uncompressed,
	// non-empty = see ft.compressionType.
	ft *storage.FrameTable
}

func (b *File) readSegments(ctx context.Context, p []byte, segments []readSegment, maxParallel int) error {
	if maxParallel > 1 && len(segments) > 1 {
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(maxParallel)
		for _, s := range segments {
			seg := s
			g.Go(func() error { return b.readSegmentFaultSafe(gctx, p, seg) })
		}

		return g.Wait()
	}

	for _, s := range segments {
		if err := b.readSegmentFaultSafe(ctx, p, s); err != nil {
			return err
		}
	}

	return nil
}

// readSegmentFaultSafe converts memory faults from the diff's mmap'd cache
// into errors, so a bad disk block fails one read, not the process.
func (b *File) readSegmentFaultSafe(ctx context.Context, p []byte, s readSegment) error {
	err := block.RunFaultSafe(ctx, func() error { return b.readSegment(ctx, p, s) })
	if faultErr, ok := errors.AsType[*block.MemoryFaultError](err); ok {
		cacheKey := string(s.diff.CacheKey())
		logger.L().Error(ctx, "memory fault reading build segment; local disk under the cache is likely failing",
			zap.Error(err),
			zap.String("diff_cache_key", cacheKey),
			zap.Uintptr("fault_addr", faultErr.Addr),
			zap.Int64("offset", s.srcOff),
			zap.Int64("length", s.length),
		)
	}

	return err
}

func (b *File) readSegment(ctx context.Context, p []byte, s readSegment) error {
	dst := p[s.dstOff : s.dstOff+int(s.length)]
	n, err := s.diff.ReadAt(ctx, dst, s.srcOff, s.ft)
	if err != nil {
		return err
	}
	if int64(n) != s.length {
		return io.ErrUnexpectedEOF
	}

	return nil
}

// planRead resolves the segments covering p, zero-filling uuid.Nil regions.
// A returned byte count below len(p) means the mappings ran out (EOF).
// distinctBuilds counts the distinct builds the segments reference; it
// saturates at buildCacheSize when a single read crosses more builds than the
// per-read cache holds (already deep in the "very fragmented" regime).
func (b *File) planRead(ctx context.Context, p []byte, off int64) (segments []readSegment, n int, distinctBuilds int, err error) {
	// Per-read Diff cache: avoids the DiffStore TTL cache mutex on every mapping.
	const buildCacheSize = 16
	var (
		underlyingIDs   [buildCacheSize]uuid.UUID
		underlyingDiffs [buildCacheSize]Diff
		cacheIDs        = underlyingIDs[:0]
		cacheDiffs      = underlyingDiffs[:0]
	)

	for n < len(p) {
		h := b.Header()
		mappedToBuild, err := h.GetShiftedMapping(ctx, off+int64(n))
		if err != nil {
			return nil, 0, 0, fmt.Errorf("failed to get mapping: %w", err)
		}
		readLength := min(int64(mappedToBuild.Length), int64(len(p)-n))
		// A zero-length mapping means off+n is past the last mapping (EOF); stop
		// and let the caller surface io.EOF for the bytes covered so far.
		if readLength <= 0 {
			return segments, n, len(cacheIDs), nil
		}
		// uuid.Nil marks an unmapped/empty region; zero-fill it in place.
		if mappedToBuild.BuildId == uuid.Nil {
			clear(p[n : n+int(readLength)])
			n += int(readLength)

			continue
		}

		diff, err := b.cachedBuild(ctx, mappedToBuild.BuildId, &cacheIDs, &cacheDiffs)
		if err != nil {
			return nil, 0, 0, err
		}
		segments = append(segments, readSegment{
			dstOff: n,
			srcOff: int64(mappedToBuild.Offset),
			length: readLength,
			diff:   diff,
			ft:     h.GetBuildFrameData(mappedToBuild.BuildId),
		})
		n += int(readLength)
	}

	return segments, n, len(cacheIDs), nil
}

func (b *File) cachedBuild(ctx context.Context, buildID uuid.UUID, ids *[]uuid.UUID, diffs *[]Diff) (Diff, error) {
	for i, id := range *ids {
		if id == buildID {
			return (*diffs)[i], nil
		}
	}

	diff, err := b.getBuild(ctx, buildID)
	if err != nil {
		return nil, fmt.Errorf("failed to get build: %w", err)
	}
	if len(*ids) < cap(*ids) {
		*ids = append(*ids, buildID)
		*diffs = append(*diffs, diff)
	}

	return diff, nil
}

// Slice returns [off, off+length). Zero-copy when the range fits in a
// single mapping; otherwise composes via ReadAt.
func (b *File) Slice(ctx context.Context, off, length int64) ([]byte, error) {
	if length > 0 {
		h := b.Header()
		m, err := h.GetShiftedMapping(ctx, off)
		if err == nil && int64(m.Length) >= length {
			if m.BuildId == uuid.Nil && length <= int64(len(header.EmptyHugePage)) {
				return header.EmptyHugePage[:length], nil
			}
			if m.BuildId != uuid.Nil {
				ft := h.GetBuildFrameData(m.BuildId)
				diff, derr := b.getBuild(ctx, m.BuildId)
				if derr != nil {
					logger.L().Warn(ctx, "failed to get build for slice fast path", zap.Error(derr))
				} else {
					slice, sErr := diff.Slice(ctx, int64(m.Offset), length, ft)
					if sErr == nil {
						return slice, nil
					}
					logger.L().Warn(ctx, "failed to slice build fast path", zap.Error(sErr))
				}
				// Errors fall through to ReadAt.
			}
		}
	}
	out := make([]byte, length)
	if _, err := b.readAt(ctx, out, off); err != nil {
		return nil, fmt.Errorf("failed to read at: %w", err)
	}

	return out, nil
}

// IsCached reports whether the range is fully resident locally; uuid.Nil
// counts as cached, uninitialized StorageDiffs as uncached. No I/O.
func (b *File) IsCached(ctx context.Context, off, length int64) bool {
	h := b.Header()
	if h == nil {
		return false
	}

	var n int64
	for n < length {
		m, err := h.GetShiftedMapping(ctx, off+n)
		if err != nil {
			return false
		}
		segLen := min(int64(m.Length), length-n)
		if segLen <= 0 {
			return false
		}

		if m.BuildId != uuid.Nil {
			diff, ok := b.store.Lookup(GetDiffStoreKey(m.BuildId.String(), b.fileType))
			if !ok {
				return false
			}
			peeker, ok := diff.(block.CachePeeker)
			if !ok || !peeker.IsCached(ctx, int64(m.Offset), segLen) {
				return false
			}
		}

		n += segLen
	}

	return true
}

// waitTransitionBackoff honors the peer's RetryAfter hint before the caller
// retries against base storage. Returns ctx.Err() if cancelled during sleep.
func waitTransitionBackoff(ctx context.Context, transErr *storage.PeerTransitionedError) error {
	if transErr.RetryAfter > 0 {
		timer := time.NewTimer(transErr.RetryAfter)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// getBuild returns the cached StorageDiff for buildID, constructing one via
// createDiff on miss. The singleflight inside GetOrCreate ensures createDiff
// runs at most once per key across concurrent callers.
func (b *File) getBuild(ctx context.Context, buildID uuid.UUID) (Diff, error) {
	key := GetDiffStoreKey(buildID.String(), b.fileType)

	return b.store.GetOrCreate(ctx, key, func(ctx context.Context) (Diff, error) {
		return b.createDiff(ctx, buildID)
	})
}
