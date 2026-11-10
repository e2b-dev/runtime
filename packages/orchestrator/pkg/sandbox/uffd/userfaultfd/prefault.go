//go:build linux

package userfaultfd

import (
	"context"
	"fmt"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// Prefault proactively copies a page to guest memory at the given offset
// to speed up sandbox starts. EEXIST (already mapped) is handled gracefully;
// a Prefault against an already-closed userfaultfd returns ErrClosed.
//
// installed reports whether THIS call copied the page into the guest. It is
// false on every nil-error path that didn't copy — tracker said the page is
// already resident (skipped), a demand fault won the install race (present),
// or the copy hit EAGAIN and the prefetcher won't retry (deferred) — so
// callers can keep "copied" accounting consistent with the per-page prefault
// metric's result label.
func (u *Userfaultfd) Prefault(ctx context.Context, offset int64, data []byte) (installed bool, e error) {
	// Record install latency / installed bytes / attempt count once per
	// prefault, tagged by outcome. Begun before the RLock so the latency
	// includes lock wait; with the data already in memory the remainder is
	// the UFFDIO_COPY itself, making this a host-contention proxy.
	sw := prefaultTimer.Begin()
	record := true
	result := faultResultInstalled
	var installedBytes int64
	defer func() {
		if record {
			sw.RecordRaw(ctx, installedBytes, prefaultAttrs[u.genBucket][result])
		}
	}()

	// Test hook: fires before settleRequests.RLock so that a test can park
	// the goroutine here, call Close() concurrently, then release and observe
	// that the closed check below returns ErrClosed without calling
	// UFFDIO_COPY.
	if h := u.testFaultHook.Load(); h != nil {
		(*h)(0, faultPhaseBeforePrefaultRLock)
	}

	u.settleRequests.RLock()
	defer u.settleRequests.RUnlock()

	// Close() sets closed under Lock(). Seeing it here under RLock means the
	// fd is already closed and its number may have been recycled by the OS —
	// skip instead of calling UFFDIO_COPY and getting EBADF/ENOTTY, and
	// report ErrClosed so the caller can stop and keep its own accounting
	// consistent. Not recorded: on sandbox stop the copy workers drain every
	// remaining queued page through this path, which would flood the metric
	// with teardown noise.
	if u.closed {
		record = false

		return false, ErrClosed
	}

	ctx, span := tracer.Start(ctx, "prefault page")
	defer span.End()

	addr, err := u.ma.GetHostVirtAddr(offset)
	if err != nil {
		result = faultResultError

		return false, fmt.Errorf("failed to get host virtual address: %w", err)
	}

	if len(data) != int(u.pageSize) {
		result = faultResultError

		return false, fmt.Errorf("data length (%d) does not match pagesize (%d)", len(data), u.pageSize)
	}

	idx := uint32(header.BlockIdx(offset, int64(u.pageSize)))
	state := u.pageTracker.Get(idx)
	if state != block.NotPresent && state != block.Installing {
		// Dirty/Zero: the page is already resident (or zero-installed) — a
		// demand fault got there first, so this prefetch arrived too late.
		// Removed: the guest freed the page (madvise); it must read back as
		// zeros, so installing source content would resurrect stale data.
		// Installing is not skipped: the bytes are in hand while the demand
		// worker that claimed the page may still be reading them, so this
		// install wakes the guest sooner; the race loser sees EEXIST and
		// MarkInstalled turns the claim into Clean from either side.
		result = faultResultSkipped

		return false, nil
	}

	// Prefault as a read so the page gets WP set. A concurrent on-demand
	// fault that installs the page first returns faultAlreadyPresent (EEXIST).
	outcome, err := u.faultPage(
		ctx,
		addr,
		offset,
		block.Read,
		directDataSource{data: data},
		nil,
	)
	if err != nil {
		result = faultResultError
		span.RecordError(err)

		return false, fmt.Errorf("failed to fault page: %w", err)
	}

	switch outcome {
	case faultInstalled, faultAlreadyPresent:
		if outcome == faultInstalled {
			// Page copied by this prefault → count its bytes, and record it
			// Clean: installed WP-armed with content identical to the source,
			// so the snapshot diff can inherit it from the parent layer until
			// a write promotes it to Dirty. MarkInstalled (not SetRange) so a
			// suspended prefault can't downgrade a page a concurrent WP fault
			// already promoted. On a lost install race (EEXIST) the winning
			// demand serve counted the bytes and records the state.
			installed = true
			installedBytes = int64(u.pageSize)
			u.pageTracker.MarkInstalled(idx, idx+1, block.Clean)
		} else {
			result = faultResultPresent
		}
		u.prefetchTracker.Add(offset, block.Prefetch)
	case faultDeferred:
		// The prefetcher does not retry: this page will not be prefaulted.
		result = faultResultDeferred
		span.AddEvent("prefault: write returned EAGAIN")
	case faultDiscarded:
		result = faultResultDiscarded
		span.AddEvent("prefault: discarded (process gone)")
	default:
		result = faultResultError

		return false, fmt.Errorf("unexpected faultOutcome: %#v", outcome)
	}

	return installed, nil
}

// directDataSource wraps a single page's bytes; off is ignored because the
// caller hands us exactly the page contents.
type directDataSource struct {
	data []byte
}

func (d directDataSource) ReadAt(_ context.Context, p []byte, _ int64) (int, error) {
	return copy(p, d.data), nil
}
