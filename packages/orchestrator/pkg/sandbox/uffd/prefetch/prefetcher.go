//go:build linux

package prefetch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/prefetch")

type prefetchData struct {
	offset int64
	data   []byte
}

// extent is a run of contiguous block indices fetched from the source in a
// single source.Slice call. Coalescing contiguous indices into one extent
// turns many small sequential reads into fewer, larger ones. The copy phase
// always stays per-block (split out of the fetched extent) because
// Userfaultfd.Prefault installs one page per call (UFFDIO_COPY itself accepts
// a page-multiple length, so batching the copy is possible but not done here).
type extent struct {
	startIdx uint64
	blocks   int
}

// coalesceIndices groups sorted, deduplicated block indices into maximal
// contiguous runs, each capped at maxBlocks. With maxBlocks<=1 every extent is
// a single block, reproducing the per-block fetch exactly (coalescing off).
func coalesceIndices(indices []uint64, maxBlocks int) []extent {
	if maxBlocks < 1 {
		maxBlocks = 1
	}

	out := make([]extent, 0, len(indices))
	for i := 0; i < len(indices); {
		n := 1
		for i+n < len(indices) && indices[i+n] == indices[i+n-1]+1 && n < maxBlocks {
			n++
		}

		out = append(out, extent{startIdx: indices[i], blocks: n})
		i += n
	}

	return out
}

// Prefetcher handles background prefetching of memory pages.
// It proactively fetches pages that are known to be needed based on the prefetch mapping
// collected during template build.
//
// The prefetcher works in two parallel phases:
// 1. Fetch phase: Immediately starts fetching pages from the source (populates cache)
// 2. Copy phase: Once uffd is ready, copies the fetched pages to guest memory
//
// Both phases run with their own parallelism limits and don't block each other.
type Prefetcher struct {
	logger  logger.Logger
	source  block.Slicer
	uffd    uffd.MemoryBackend
	mapping *metadata.MemoryPrefetchMapping
	// Prefault installs each fetched page into the guest via UFFDIO_COPY (the
	// copy phase). New defaults it to true. Set it false for a "fetch-only"
	// run: the prefetcher only warms the shared chunk cache and the guest still
	// faults (served fast from the warm cache). Fetch-only is used for the
	// last-cycle diff, whose multi-GiB prefault would load UFFDIO_COPY onto the
	// resume-critical path for no workload gain and would regress warm resumes.
	Prefault bool
	// RestorePaused identifies prefetch for a VM restored without running its guest.
	RestorePaused bool
	featureFlags  *featureflags.Client
}

func New(
	logger logger.Logger,
	source block.Slicer,
	uffd uffd.MemoryBackend,
	mapping *metadata.MemoryPrefetchMapping,
	featureFlags *featureflags.Client,
) *Prefetcher {
	return &Prefetcher{
		logger:       logger,
		source:       source,
		uffd:         uffd,
		mapping:      mapping,
		Prefault:     true,
		featureFlags: featureFlags,
	}
}

// Start begins the background prefetching process.
// This is fire-and-forget - errors are logged but don't affect sandbox operation.
// The prefetcher will stop when the context is cancelled.
//
// The prefetcher starts fetching pages from the source immediately (to populate the cache),
// while simultaneously waiting for the uffd handler. Once the handler is ready, it starts
// copying the fetched pages to guest memory.
func (p *Prefetcher) Start(ctx context.Context) error {
	ctx = storage.WithSkipCacheWriteback(ctx)

	ctx, span := tracer.Start(ctx, "start prefetch")
	defer span.End()

	if p.mapping == nil {
		p.logger.Debug(ctx, "prefetch: no mapping provided, skipping")

		return nil
	}

	indices := p.mapping.Indices
	if len(indices) == 0 {
		p.logger.Debug(ctx, "prefetch: no pages to prefetch")

		return nil
	}

	// Teardown may cancel before the background run starts. Do not build the
	// full page-work queues for a run that will never consume them.
	if ctx.Err() != nil {
		return nil
	}

	// Get worker counts and coalescing config from feature flags at runtime
	maxFetchWorkers := p.featureFlags.IntFlag(ctx, featureflags.MemoryPrefetchMaxFetchWorkers)
	maxCopyWorkers := p.featureFlags.IntFlag(ctx, featureflags.MemoryPrefetchMaxCopyWorkers)
	coalesceMaxMB := p.featureFlags.IntFlag(ctx, featureflags.MemoryPrefetchCoalesceMaxMB)

	// cancelRun aborts the whole run early. Copy workers fire it on ErrClosed
	// (uffd gone: sandbox teardown) so fetch workers stop fetching and
	// queueing pages nobody will ever copy, instead of running the fetch
	// phase to completion for a dead sandbox.
	ctx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	activeRuns.Add(1)
	defer activeRuns.Add(-1)

	blockSize := p.mapping.BlockSize
	totalPages := len(indices)

	// Record the recorded working-set size for this run, tagged by mode so the
	// fetch-only (last-cycle) distribution is separable from the prefaulted
	// init trace. Available for rollout dashboards without Tempo sampling.
	modeAttr := modeFetchAttr
	if p.Prefault {
		modeAttr = modePrefaultAttr
	}
	restoreMode := metric.WithAttributes(attribute.Bool("restore_paused", p.RestorePaused))
	mappingBlocksHistogram.Record(ctx, int64(totalPages), modeAttr, restoreMode)

	span.SetAttributes(
		attribute.Int64("prefetch.total_pages", int64(totalPages)),
		attribute.Int64("prefetch.block_size", blockSize),
		attribute.Int64("prefetch.mapped_bytes", int64(totalPages)*blockSize),
		attribute.Bool("prefetch.restore_paused", p.RestorePaused),
		attribute.Int("prefetch.max_fetch_workers", maxFetchWorkers),
		attribute.Int("prefetch.max_copy_workers", maxCopyWorkers),
		attribute.Int("prefetch.coalesce_max_mb", coalesceMaxMB),
		attribute.Bool("prefetch.prefault", p.Prefault),
	)

	p.logger.Debug(ctx, "prefetch: starting background prefetch",
		zap.Int("total_pages", totalPages),
		zap.Int64("block_size", blockSize),
		zap.Int("max_fetch_workers", maxFetchWorkers),
		zap.Int("max_copy_workers", maxCopyWorkers),
		zap.Int("coalesce_max_mb", coalesceMaxMB),
	)

	// Coalesce contiguous blocks into extents. maxBlk<=1 (coalesceMaxMB<=0,
	// the default) reproduces the per-block fetch exactly.
	maxBlk := 1
	if coalesceMaxMB > 0 && blockSize > 0 {
		maxBlk = max(1, int(units.MBToBytes(int64(coalesceMaxMB))/blockSize))
	}
	extents := coalesceIndices(indices, maxBlk)
	span.SetAttributes(attribute.Int("prefetch.extents", len(extents)))

	// Channels for work distribution
	// Fetch channel: all extents to fetch (large buffer so main goroutine doesn't block)
	fetchCh := make(chan extent, len(extents))
	// Copy channel: pages fetched successfully, ready to install. Only created
	// when prefaulting; a fetch-only run leaves it nil and skips the copy phase,
	// so fetched bytes are dropped after warming the cache (no in-memory pile-up).
	var copyCh chan prefetchData
	if p.Prefault {
		copyCh = make(chan prefetchData, totalPages)
	}

	// Counters for statistics
	var fetchedCount atomic.Uint64
	var copiedCount atomic.Uint64
	var fetchSkippedCount atomic.Uint64
	var copySkippedCount atomic.Uint64

	var fetchWg sync.WaitGroup
	var copyWg sync.WaitGroup

	runStart := time.Now()
	// Set by the copy coordinator once uffd is ready; stays nil if the
	// context is cancelled before the copy phase ever starts. Holds a
	// *time.Time (not UnixNano) so the monotonic reading survives and the
	// duration below is immune to wall-clock steps.
	var copyStart atomic.Pointer[time.Time]

	// Queue all extents to fetch, in offset order.
	for _, e := range extents {
		fetchCh <- e
	}
	close(fetchCh)

	// Start fetch workers - they populate the cache and queue offsets for copy
	for range maxFetchWorkers {
		fetchWg.Go(func() {
			activeFetchWorkers.Add(1)
			defer activeFetchWorkers.Add(-1)

			p.fetchWorker(ctx, fetchCh, copyCh, blockSize, &fetchedCount, &fetchSkippedCount)
		})
	}

	// Start copy coordinator - waits for uffd ready, then spawns copy workers.
	// Skipped entirely for fetch-only runs.
	if p.Prefault {
		copyWg.Go(func() {
			p.startCopyWorkers(ctx, cancelRun, copyCh, maxCopyWorkers, &copyStart, &copiedCount, &copySkippedCount)
		})
	}

	// Wait for fetch workers to complete
	fetchWg.Wait()
	fetchDuration := time.Since(runStart)
	// Close copy channel when all fetch workers are done
	if copyCh != nil {
		close(copyCh)
	}

	// Wait for copy workers to complete
	copyWg.Wait()

	// Export the run stats: per-stage page counts plus phase durations. The
	// fetch and copy phases overlap; "total" spans the whole run.
	fetched := fetchedCount.Load()
	fetchSkipped := fetchSkippedCount.Load()
	copied := copiedCount.Load()
	copySkipped := copySkippedCount.Load()
	pagesCounter.Add(ctx, int64(fetched), stageFetchedAttr, restoreMode)
	pagesCounter.Add(ctx, int64(fetchSkipped), stageFetchSkippedAttr, restoreMode)
	pagesCounter.Add(ctx, int64(copied), stageCopiedAttr, restoreMode)
	pagesCounter.Add(ctx, int64(copySkipped), stageCopySkippedAttr, restoreMode)
	durationHistogram.Record(ctx, fetchDuration.Milliseconds(), phaseFetchAttr, restoreMode)
	span.SetAttributes(
		attribute.Int64("prefetch.fetched_pages", int64(fetched)),
		attribute.Int64("prefetch.fetched_logical_bytes", int64(fetched)*blockSize),
		attribute.Int64("prefetch.fetch_skipped_pages", int64(fetchSkipped)),
		attribute.Int64("prefetch.copied_pages", int64(copied)),
		attribute.Int64("prefetch.copy_skipped_pages", int64(copySkipped)),
		attribute.Float64("prefetch.fetch_ms", float64(fetchDuration)/float64(time.Millisecond)),
	)
	if cs := copyStart.Load(); cs != nil {
		copyDuration := time.Since(*cs)
		copyStartDelay := cs.Sub(runStart)
		durationHistogram.Record(ctx, copyDuration.Milliseconds(), phaseCopyAttr, restoreMode)
		durationHistogram.Record(ctx, copyStartDelay.Milliseconds(), phaseCopyStartDelayAttr, restoreMode)
		span.SetAttributes(
			attribute.Float64("prefetch.copy_ms", float64(copyDuration)/float64(time.Millisecond)),
			attribute.Float64("prefetch.copy_start_delay_ms", float64(copyStartDelay)/float64(time.Millisecond)),
		)
	}
	totalDuration := time.Since(runStart)
	durationHistogram.Record(ctx, totalDuration.Milliseconds(), phaseTotalAttr, restoreMode)
	span.SetAttributes(attribute.Float64("prefetch.total_ms", float64(totalDuration)/float64(time.Millisecond)))

	p.logger.Debug(ctx, "prefetch: completed",
		zap.Uint64("fetched", fetched),
		zap.Uint64("copied", copied),
		zap.Uint64("fetch_skipped", fetchSkipped),
		zap.Uint64("copy_skipped", copySkipped),
		zap.Int("total", totalPages),
	)

	return nil
}

// startCopyWorkers waits for uffd to be ready, then starts copy workers
func (p *Prefetcher) startCopyWorkers(
	ctx context.Context,
	cancelRun context.CancelFunc,
	copyCh chan prefetchData,
	maxCopyWorkers int,
	copyStart *atomic.Pointer[time.Time],
	copiedCount *atomic.Uint64,
	copySkippedCount *atomic.Uint64,
) {
	// Wait for uffd to be ready
	select {
	case <-ctx.Done():
		return
	case <-p.uffd.Ready():
	}

	now := time.Now()
	copyStart.Store(&now)

	p.logger.Debug(ctx, "prefetch: uffd ready, starting copy workers")

	// Start copy workers
	var copyWorkerWg sync.WaitGroup

	for range maxCopyWorkers {
		copyWorkerWg.Go(func() {
			activeCopyWorkers.Add(1)
			defer activeCopyWorkers.Add(-1)

			p.copyWorker(ctx, cancelRun, copyCh, copiedCount, copySkippedCount)
		})
	}

	copyWorkerWg.Wait()
}

// fetchWorker fetches extents from the source to populate the cache. An
// extent is one contiguous run of blocks fetched in a single source.Slice
// call; with coalescing off (the default) every extent is a single block,
// reproducing the plain per-block fetch. The copy phase always stays
// per-block: an extent is split into page-sized sub-slices because
// Userfaultfd.Prefault installs one page per call.
func (p *Prefetcher) fetchWorker(
	ctx context.Context,
	fetchCh <-chan extent,
	copyCh chan<- prefetchData,
	blockSize int64,
	fetchedCount *atomic.Uint64,
	skippedCount *atomic.Uint64,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-fetchCh:
			if !ok {
				return
			}

			baseOffset := header.BlockOffset(int64(e.startIdx), blockSize)

			// Fetch from source - this populates the cache. A multi-block
			// extent is one larger sequential read spanning e.blocks pages.
			data, err := p.source.Slice(ctx, baseOffset, blockSize*int64(e.blocks))
			if err != nil {
				p.logger.Debug(ctx, "prefetch: failed to fetch extent",
					zap.Int64("offset", baseOffset),
					zap.Int("blocks", e.blocks),
					zap.Error(err),
				)
				skippedCount.Add(uint64(e.blocks))

				continue
			}

			// Guard against a short read with a nil error: the per-page slicing
			// below indexes up to blockSize*e.blocks and would panic (slice
			// bounds out of range) — which crashes the whole orchestrator — if
			// the source returned fewer bytes than requested.
			if int64(len(data)) < blockSize*int64(e.blocks) {
				p.logger.Debug(ctx, "prefetch: short read from source",
					zap.Int64("offset", baseOffset),
					zap.Int("blocks", e.blocks),
					zap.Int("got_bytes", len(data)),
				)
				skippedCount.Add(uint64(e.blocks))

				continue
			}

			fetchedCount.Add(uint64(e.blocks))

			// Queue each page for copy (non-blocking - channel has enough
			// capacity). Fetch-only runs pass a nil copyCh: warming the cache is
			// the whole job, so the fetched bytes are dropped here.
			if copyCh == nil {
				continue
			}
			for b := range e.blocks {
				off := baseOffset + int64(b)*blockSize
				sub := data[int64(b)*blockSize : int64(b+1)*blockSize]
				select {
				case copyCh <- prefetchData{offset: off, data: sub}:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// copyWorker copies pages to guest memory via uffd
func (p *Prefetcher) copyWorker(
	ctx context.Context,
	cancelRun context.CancelFunc,
	copyCh <-chan prefetchData,
	copiedCount *atomic.Uint64,
	skippedCount *atomic.Uint64,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-copyCh:
			if !ok {
				return
			}

			installed, err := p.uffd.Prefault(ctx, d.offset, d.data)
			if errors.Is(err, userfaultfd.ErrClosed) {
				// The uffd is gone (sandbox teardown): every remaining queued
				// page would hit the same path. Cancel the run so the fetch
				// workers stop pulling pages nobody will copy, and count
				// nothing, keeping stage="copied" consistent with the
				// per-page prefault metric, which skips this path too.
				cancelRun()

				return
			}
			if err != nil {
				p.logger.Debug(ctx, "prefetch: failed to copy page",
					zap.Int64("offset", d.offset),
					zap.Error(err),
				)
				skippedCount.Add(1)

				continue
			}

			// A nil error doesn't mean this prefault copied the page: it may
			// have been skipped (already resident), lost the install race to
			// a demand fault, or deferred on EAGAIN. Count those as skipped so
			// stage="copied" matches prefault{result="installed"} exactly.
			if installed {
				copiedCount.Add(1)
			} else {
				skippedCount.Add(1)
			}
		}
	}
}
