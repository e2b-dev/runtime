//go:build linux

package userfaultfd

import (
	"context"
	"os"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/testutils/testharness"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// skipIfDedupOff keeps the dedup assertions out of a UFFD_INSTALL_DEDUP=0
// suite run, which exercises the pre-dedup serve loop on purpose.
func skipIfDedupOff(t *testing.T) {
	t.Helper()
	if os.Getenv("UFFD_INSTALL_DEDUP") == "0" {
		t.Skip("UFFD_INSTALL_DEDUP=0: dedup disabled for this run")
	}
}

// forEachPageSize runs body as a parallel subtest per page size.
func forEachPageSize(t *testing.T, body func(t *testing.T, pagesize uint64)) {
	t.Helper()

	for name, pagesize := range map[string]uint64{"4k": header.PageSize, "hugepage": header.HugepageSize} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			body(t, pagesize)
		})
	}
}

// populate raises one fault on the page without the handler-side mutex, so
// readers and writers can be in flight on the same page at once. It does not
// touch the content: MADV_POPULATE_WRITE only makes the kernel take a write
// fault.
func populate(h *testHandler, offset int64, write bool) error {
	page := (*h.memoryArea)[offset : offset+int64(h.pagesize)]
	advice := unix.MADV_POPULATE_READ
	if write {
		advice = unix.MADV_POPULATE_WRITE
	}

	return unix.Madvise(page, advice)
}

// fault is populate from its own goroutine; the channel carries its result.
func fault(h *testHandler, offset int64, write bool) <-chan error {
	done := make(chan error, 1)
	go func() { done <- populate(h, offset, write) }()

	return done
}

// parkOwner raises a read fault on offset and holds its worker at phase, so the
// page is claimed while the install has not happened. It returns the
// barrier token to release and the fault's result channel.
func parkOwner(ctx context.Context, t *testing.T, h *testHandler, offset int64, phase faultPhase) (uint64, <-chan error) {
	t.Helper()

	addr := uintptr(unsafe.Pointer(&(*h.memoryArea)[offset]))
	token, err := h.installFaultBarrier(ctx, addr, phase)
	require.NoError(t, err)
	done := fault(h, offset, false)

	waitCtx, cancel := context.WithTimeout(ctx, barrierArrivalDeadline)
	defer cancel()
	require.NoError(t, h.waitFaultHeld(waitCtx, token), "owner did not park")

	return token, done
}

// unblocked fails the test naming the first fault that did not complete.
func unblocked(ctx context.Context, t *testing.T, faults map[string]<-chan error) {
	t.Helper()

	for name, ch := range faults {
		select {
		case err := <-ch:
			require.NoError(t, err, name)
		case <-ctx.Done():
			t.Fatalf("%s did not unblock", name)
		}
	}
}

// serveDrainDeadline bounds the wait for 64 workers to finish their serves
// and record them: with dedup off that is 64 hugepage reads and copies,
// which the race detector slows well past barrierArrivalDeadline.
const serveDrainDeadline = 20 * time.Second

// waitStats polls until every counter in want is reached. The serve counters
// are written by the worker's deferred bookkeeping, after the UFFDIO_COPY
// that released the faulting caller, so a caller that just returned may be
// ahead of them.
func waitStats(ctx context.Context, h *testHandler, want testharness.ServeStatsReply, deadline time.Duration) error {
	end := time.Now().Add(deadline)
	for {
		st, err := h.client.ServeStats()
		if err != nil {
			return err
		}
		if st.Pages >= want.Pages && st.Deduped >= want.Deduped && st.Deferred >= want.Deferred {
			return nil
		}
		if time.Now().After(end) {
			return context.DeadlineExceeded
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func stats(t *testing.T, h *testHandler) (testharness.SourceStatsReply, testharness.ServeStatsReply) {
	t.Helper()

	src, err := h.client.SourceStats()
	require.NoError(t, err)
	serve, err := h.client.ServeStats()
	require.NoError(t, err)

	return src, serve
}

// concurrentFaults raises n faults on page 0 with the first worker for the
// page parked before its install, so the page cannot become present until
// every contender has faulted: n-1 drops with dedup on, n dispatched
// workers with it off. A fixed source delay would race the callers'
// scheduling instead; a late caller finds the page present and raises no
// fault. Returns the counters once wantPages serves are recorded; the page
// content must then match the source.
func concurrentFaults(t *testing.T, h *testHandler, n int, write, dedup bool) (testharness.SourceStatsReply, testharness.ServeStatsReply) {
	t.Helper()
	ctx := t.Context()

	token, err := h.installFaultBarrier(ctx, uintptr(unsafe.Pointer(&(*h.memoryArea)[0])), faultPhaseBeforeFaultPage)
	require.NoError(t, err)

	var g errgroup.Group
	for range n {
		g.Go(func() error { return populate(h, 0, write) })
	}

	wantPages := int64(n)
	if dedup {
		wantPages = 1
		require.NoError(t, waitStats(ctx, h, testharness.ServeStatsReply{Deduped: int64(n - 1)}, barrierArrivalDeadline), "not every contender was dropped")
	} else {
		require.NoError(t, waitFaults(ctx, h, 0, n), "not every contender got a worker")
	}
	require.NoError(t, h.releaseFault(ctx, token))
	require.NoError(t, g.Wait())
	require.NoError(t, waitStats(ctx, h, testharness.ServeStatsReply{Pages: wantPages}, serveDrainDeadline), "not every serve was recorded")
	require.NoError(t, h.executeRead(ctx, operation{offset: 0, mode: operationModeRead}))

	return stats(t, h)
}

// waitFaults polls until the child has dispatched n workers for offset.
func waitFaults(ctx context.Context, h *testHandler, offset int64, n int) error {
	deadline := time.Now().Add(barrierArrivalDeadline)
	for {
		offsets, err := h.client.FaultOffsets()
		if err != nil {
			return err
		}
		if countOffset(offsets, offset) >= n {
			return nil
		}
		if time.Now().After(deadline) {
			return context.DeadlineExceeded
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

// Many faults on one not-yet-present page while the source is slow must cost
// one worker, one scratch buffer and one source read: the rest are dropped
// by the serve loop and woken by the owner's UFFDIO_COPY. Each worker holds
// a page-sized buffer for its whole read, so N duplicate reads of a hugepage
// pin N × 2 MiB until their reads finish.
func TestConcurrentFaultsOnOnePageInstallOnce(t *testing.T) {
	t.Parallel()
	skipIfDedupOff(t)

	const faults = 64

	forEachPageSize(t, func(t *testing.T, pagesize uint64) {
		t.Helper()

		for name, write := range map[string]bool{"read": false, "write": true} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				cfg := testConfig{pagesize: pagesize, numberOfPages: 1, barriers: true, sourceDelay: time.Millisecond}
				runMatrix(t, cfg, func(t *testing.T, cfg testConfig) {
					t.Helper()

					h, err := configureCrossProcessTest(t.Context(), t, cfg)
					require.NoError(t, err)

					src, serve := concurrentFaults(t, h, faults, write, true)
					assert.Equal(t, int64(1), src.Reads, "every duplicate fault read the source again")
					assert.Equal(t, int64(1), src.PeakInFlight, "duplicate reads each held a scratch buffer")
					// At least: a thread with a signal pending while it blocks
					// re-faults and queues a second message, which is dropped too.
					assert.GreaterOrEqual(t, serve.Deduped, int64(faults-1), "every fault after the first is dropped in the serve loop")
					assert.Equal(t, int64(1), serve.Pages, "only the owner serves the page")

					want := block.Clean
					if write {
						want = block.Dirty
					}
					states, err := h.awaitPageState(t.Context(), 0, want)
					require.NoError(t, err, "page did not reach %s: %+v", want, states)
				})
			})
		}
	})
}

// With the kill switch off the serve loop is the pre-dedup one: every fault
// message gets its own worker, buffer and source read.
func TestInstallDedupOffKeepsOneWorkerPerFault(t *testing.T) {
	t.Parallel()

	const faults = 64

	forEachPageSize(t, func(t *testing.T, pagesize uint64) {
		t.Helper()

		cfg := testConfig{pagesize: pagesize, numberOfPages: 1, barriers: true, sourceDelay: time.Millisecond, installDedupOff: true}
		h, err := configureCrossProcessTest(t.Context(), t, cfg)
		require.NoError(t, err)

		src, serve := concurrentFaults(t, h, faults, false, false)
		// At least: a thread with a signal pending while it blocks re-faults
		// and queues another message, which gets its own worker and read too.
		assert.GreaterOrEqual(t, src.Reads, int64(faults), "with dedup off every fault reads the source, each with its own buffer")
		assert.Zero(t, serve.Deduped)
		assert.GreaterOrEqual(t, serve.Pages, int64(faults))
	})
}

// A write fault dropped behind a read owner is not lost: the owner installs
// the page write-protected, the writer is woken, retries, takes a WP fault
// and is promoted to Dirty by the resolve.
func TestWriteFollowerBehindReadOwnerPromotesToDirty(t *testing.T) {
	t.Parallel()
	skipIfDedupOff(t)

	forEachPageSize(t, func(t *testing.T, pagesize uint64) {
		t.Helper()

		withRaceContext(t, func(ctx context.Context) {
			cfg := testConfig{pagesize: pagesize, numberOfPages: 2, barriers: true, syncWP: true, sourceDelay: time.Millisecond}
			h, err := configureCrossProcessTest(ctx, t, cfg)
			require.NoError(t, err)

			token, owner := parkOwner(ctx, t, h, 0, faultPhaseBeforeFaultPage)
			follower := fault(h, 0, true)
			require.NoError(t, waitStats(ctx, h, testharness.ServeStatsReply{Deduped: 1}, barrierArrivalDeadline), "the write fault was not deduped onto the read owner")

			require.NoError(t, h.releaseFault(ctx, token))
			unblocked(ctx, t, map[string]<-chan error{"read owner": owner, "write follower": follower})
			require.NoError(t, waitForState(ctx, h, 0, block.Dirty),
				"the dropped write was not promoted through the WP fault")

			src, _ := stats(t, h)
			assert.Equal(t, int64(1), src.Reads)
			require.NoError(t, h.executeRead(ctx, operation{offset: 0, mode: operationModeRead}))
		})
	})
}

// A REMOVE that lands between the serve loop's claim and the owner's RLock
// must win: the owner re-reads the state, zero-fills instead of installing
// source content, and the dropped follower is released by that install.
func TestRemoveBetweenClaimAndInstallZeroFills(t *testing.T) {
	t.Parallel()
	skipIfDedupOff(t)

	forEachPageSize(t, func(t *testing.T, pagesize uint64) {
		t.Helper()

		withRaceContext(t, func(ctx context.Context) {
			const sentinel = byte(0xC3)
			offset := int64(pagesize)
			cfg := testConfig{
				pagesize:      pagesize,
				numberOfPages: 4,
				barriers:      true,
				removeEnabled: true,
				sourceDelay:   time.Millisecond,
				sourcePatcher: func(content []byte) { content[offset] = sentinel },
			}
			h, err := configureCrossProcessTest(ctx, t, cfg)
			require.NoError(t, err)

			token, owner := parkOwner(ctx, t, h, offset, faultPhaseBeforeRLock)
			follower := fault(h, offset, false)
			require.NoError(t, waitStats(ctx, h, testharness.ServeStatsReply{Deduped: 1}, barrierArrivalDeadline), "the second fault was not deduped")

			require.NoError(t, h.executeRemove(operation{offset: offset, mode: operationModeRemove}))
			require.NoError(t, waitForState(ctx, h, uint64(offset), block.Removed),
				"the REMOVE batch did not replace the claim")

			require.NoError(t, h.releaseFault(ctx, token))
			unblocked(ctx, t, map[string]<-chan error{"owner": owner, "follower": follower})

			assert.Equal(t, byte(0), (*h.memoryArea)[offset], "the owner installed stale source content on a removed page")
			require.NoError(t, waitForState(ctx, h, uint64(offset), block.Zero))
			require.NoError(t, waitStats(ctx, h, testharness.ServeStatsReply{Pages: 1, Deduped: 1}, barrierArrivalDeadline))
			src, serve := stats(t, h)
			assert.Zero(t, src.Reads, "a removed page must not be read from the source")
			// Lower bounds: a re-fault (signal pending while blocked) adds a
			// drop or, once the page is Removed, a second zero-fill serve.
			assert.GreaterOrEqual(t, serve.Pages, int64(1))
			assert.GreaterOrEqual(t, serve.Deduped, int64(1))
		})
	})
}

// A prefault with the bytes in hand may install a page a demand worker has
// claimed: the guest is woken sooner, the owner loses the UFFDIO_COPY race
// with EEXIST, and the claim ends as Clean from whichever side gets there.
func TestPrefaultInstallsClaimedPage(t *testing.T) {
	t.Parallel()
	skipIfDedupOff(t)

	forEachPageSize(t, func(t *testing.T, pagesize uint64) {
		t.Helper()

		withRaceContext(t, func(ctx context.Context) {
			cfg := testConfig{pagesize: pagesize, numberOfPages: 2, barriers: true, sourceDelay: time.Millisecond}
			h, err := configureCrossProcessTest(ctx, t, cfg)
			require.NoError(t, err)

			token, owner := parkOwner(ctx, t, h, 0, faultPhaseBeforeFaultPage)

			installed, err := h.client.Prefault(0)
			require.NoError(t, err)
			assert.True(t, installed, "the prefault skipped a page that is only claimed, not installed")
			// Still parked: the prefault's install alone must wake the owner's thread.
			unblocked(ctx, t, map[string]<-chan error{"owner": owner})

			require.NoError(t, h.releaseFault(ctx, token))
			require.NoError(t, waitForState(ctx, h, 0, block.Clean))
			require.NoError(t, waitStats(ctx, h, testharness.ServeStatsReply{Pages: 1}, barrierArrivalDeadline), "the owner never finished as present")
			src, _ := stats(t, h)
			assert.Equal(t, int64(1), src.Reads, "the owner read once and lost the copy race")
			require.NoError(t, h.executeRead(ctx, operation{offset: 0, mode: operationModeRead}))
		})
	})
}

// waitEventPending polls until the child's uffd has an unread event.
func waitEventPending(ctx context.Context, h *testHandler) error {
	deadline := time.Now().Add(barrierArrivalDeadline)
	for {
		pending, err := h.client.EventPending()
		if err != nil {
			return err
		}
		if pending {
			return nil
		}
		if time.Now().After(deadline) {
			return context.DeadlineExceeded
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

// An owner whose UFFDIO_COPY is deferred (EAGAIN) must release its claim:
// the fault is re-served later, and a page still marked Installing would
// have that re-serve dropped as a duplicate of a worker that no longer
// exists, leaving the guest thread blocked for good. The EAGAIN is forced
// deterministically: the serve loop is parked after reading one REMOVE's
// event, before the settle lock, so a second REMOVE's event stays queued
// and keeps the kernel's mmap_changing set while the owner copies.
func TestDeferredOwnerReleasesClaim(t *testing.T) {
	t.Parallel()
	skipIfDedupOff(t)

	forEachPageSize(t, func(t *testing.T, pagesize uint64) {
		t.Helper()

		withRaceContext(t, func(ctx context.Context) {
			cfg := testConfig{pagesize: pagesize, numberOfPages: 4, barriers: true, removeEnabled: true, sourceDelay: time.Millisecond}
			h, err := configureCrossProcessTest(ctx, t, cfg)
			require.NoError(t, err)

			token, owner := parkOwner(ctx, t, h, 0, faultPhaseBeforeFaultPage)
			follower := fault(h, 0, false)
			require.NoError(t, waitStats(ctx, h, testharness.ServeStatsReply{Deduped: 1}, barrierArrivalDeadline), "the follower was not deduped")

			loopToken, err := h.installFaultBarrier(ctx, uintptr(unsafe.Pointer(&(*h.memoryArea)[2*pagesize])), faultPhaseBeforeSettleLock)
			require.NoError(t, err)
			require.NoError(t, h.executeRemove(operation{offset: 2 * int64(pagesize), mode: operationModeRemove}))
			waitCtx, cancel := context.WithTimeout(ctx, barrierArrivalDeadline)
			err = h.waitFaultHeld(waitCtx, loopToken)
			cancel()
			require.NoError(t, err, "the loop did not park before the settle lock")

			// Queued behind the parked loop: mmap_changing stays set until read.
			secondRemove := make(chan error, 1)
			go func() {
				secondRemove <- h.executeRemove(operation{offset: 3 * int64(pagesize), mode: operationModeRemove})
			}()
			require.NoError(t, waitEventPending(ctx, h), "the second REMOVE's event was not queued")

			// Park the owner right after it has re-queued the fault: the
			// claim must already be gone, or a loop draining the retry now
			// would drop it as a duplicate and leave the threads blocked.
			afterToken, err := h.installFaultBarrier(ctx, uintptr(unsafe.Pointer(&(*h.memoryArea)[0])), faultPhaseAfterDeferral)
			require.NoError(t, err)
			require.NoError(t, h.releaseFault(ctx, token))
			waitCtx, cancel = context.WithTimeout(ctx, barrierArrivalDeadline)
			err = h.waitFaultHeld(waitCtx, afterToken)
			cancel()
			require.NoError(t, err, "the owner's UFFDIO_COPY was not deferred")
			state, err := h.client.PageState(0)
			require.NoError(t, err)
			assert.Equal(t, block.NotPresent, block.State(state), "the claim must be released before the retry is published")
			require.NoError(t, h.releaseFault(ctx, afterToken))
			require.NoError(t, waitStats(ctx, h, testharness.ServeStatsReply{Deferred: 1}, barrierArrivalDeadline))
			require.NoError(t, h.releaseFault(ctx, loopToken))
			unblocked(ctx, t, map[string]<-chan error{"owner": owner, "follower": follower, "second REMOVE": secondRemove})

			require.NoError(t, waitForState(ctx, h, 0, block.Clean))
			require.NoError(t, waitStats(ctx, h, testharness.ServeStatsReply{Pages: 1, Deduped: 1}, barrierArrivalDeadline))
			src, serve := stats(t, h)
			// Lower bounds: a blocked thread with a signal pending re-faults
			// and queues another message, which lands as a drop while the
			// page is claimed or as an EEXIST "present" serve if it slips
			// into the window between the abort and the re-serve's claim.
			assert.GreaterOrEqual(t, serve.Pages, int64(1))
			assert.GreaterOrEqual(t, serve.Deduped, int64(1))
			assert.GreaterOrEqual(t, src.Reads, int64(2), "the re-serve must read the source again")
			require.NoError(t, h.executeRead(ctx, operation{offset: 0, mode: operationModeRead}))
		})
	})
}
