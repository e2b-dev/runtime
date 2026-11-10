//go:build linux

package userfaultfd

import (
	"context"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// Bounded budgets so a regression surfaces as a fast assertion, not a
// 30-minute CI hang. madviseBudget is the load-bearing one: madvise must
// return as soon as the handler drains the REMOVE event, which requires
// no lock — coupling readEvents to settleRequests would push us past it.
const (
	raceHappyPathBudget    = 30 * time.Second
	barrierArrivalDeadline = 2 * time.Second
	madviseBudget          = 2 * time.Second
)

func withRaceContext(t *testing.T, body func(ctx context.Context)) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), raceHappyPathBudget)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		body(ctx)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("race test exceeded happy-path budget of %s — handler is wedged", raceHappyPathBudget)
	}
}

// TestStaleSourceRaceMissingAndRemove deterministically reproduces the
// stale-source race: a worker that captured state == missing in the
// parent loop must not UFFDIO_COPY u.src after a concurrent REMOVE has
// transitioned the page to removed. The test plants a non-zero
// sentinel into source data, parks the worker at faultPhaseBeforeRLock,
// fires MADV_DONTNEED on the same page, releases the worker, and
// asserts the resulting page is zero-filled (regression: page[0]
// equals the sentinel).
func TestStaleSourceRaceMissingAndRemove(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pagesize uint64
	}{
		{name: "4k", pagesize: header.PageSize},
		{name: "hugepage", pagesize: header.HugepageSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			withRaceContext(t, func(ctx context.Context) {
				const sentinel = byte(0xC3)
				const pageIdx = 1
				pageOffset := int64(pageIdx) * int64(tt.pagesize)

				cfg := testConfig{
					pagesize:      tt.pagesize,
					numberOfPages: 4,
					barriers:      true,
					removeEnabled: true,
					sourcePatcher: func(content []byte) {
						content[pageOffset] = sentinel
					},
				}

				h, err := configureCrossProcessTest(ctx, t, cfg)
				require.NoError(t, err)

				memStart := uintptr(unsafe.Pointer(&(*h.memoryArea)[0]))
				addr := memStart + uintptr(pageIdx)*uintptr(tt.pagesize)

				token, err := h.installFaultBarrier(ctx, addr, faultPhaseBeforeRLock)
				require.NoError(t, err)

				// READ, not write: a write would overwrite the byte
				// we read below to distinguish the two outcomes.
				readErrCh := make(chan error, 1)
				go func() {
					readErrCh <- h.executeRead(ctx, operation{offset: pageOffset, mode: operationModeRead})
				}()

				waitCtx, waitCancel := context.WithTimeout(ctx, barrierArrivalDeadline)
				err = h.waitFaultHeld(waitCtx, token)
				waitCancel()
				require.NoError(t, err, "worker for page %d (addr %#x) did not park at barrier", pageIdx, addr)

				err = h.executeRemove(operation{offset: pageOffset, mode: operationModeRemove})
				require.NoError(t, err, "MADV_DONTNEED on page %d did not return — handler dispatch wedged", pageIdx)

				require.NoError(t, waitForState(ctx, h, uint64(pageOffset), block.Removed),
					"handler did not transition page %d to `removed` after MADV_DONTNEED", pageIdx)

				require.NoError(t, h.releaseFault(ctx, token))

				select {
				case <-readErrCh:
					// readErr is ignored; the page-content assertion
					// below is the bug-detection path.
				case <-ctx.Done():
					t.Fatalf("read of page %d did not unblock after barrier release", pageIdx)
				}

				page := (*h.memoryArea)[pageOffset : pageOffset+int64(tt.pagesize)]

				// MADV_POPULATE_READ resolves the page via the kernel
				// fault path while this goroutine sits in _Gsyscall, so
				// the subsequent direct load can never fault and can
				// never deadlock a concurrent STW.
				err = unix.Madvise(page, unix.MADV_POPULATE_READ)
				require.NoError(t, err, "madvise POPULATE_READ at offset %d", pageOffset)
				assert.Equalf(t, byte(0), page[0],
					"page %d first byte: want 0 (zero-fault for `removed`), got %#x — "+
						"if this equals the sentinel %#x, the worker used a stale source (regression)",
					pageIdx, page[0], sentinel,
				)

				pagemap, err := testutils.NewPagemapReader()
				require.NoError(t, err)
				defer pagemap.Close()
				entry, err := pagemap.ReadEntry(addr)
				require.NoError(t, err)
				assert.True(t, entry.IsPresent(), "page %d should be present after the racing read", pageIdx)
			})
		})
	}
}

// TestNoMadviseDeadlockWithInflightCopy is the liveness guard for
// MADV_DONTNEED while a worker holds settleRequests.RLock. madvise
// must return within madviseBudget because readEvents drains REMOVE
// events without taking any lock — any future change that couples
// readEvents to settleRequests fails this test at the budget boundary.
func TestNoMadviseDeadlockWithInflightCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pagesize uint64
	}{
		{name: "4k", pagesize: header.PageSize},
		{name: "hugepage", pagesize: header.HugepageSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			withRaceContext(t, func(ctx context.Context) {
				cfg := testConfig{
					pagesize:      tt.pagesize,
					numberOfPages: 4,
					barriers:      true,
					removeEnabled: true,
				}

				h, err := configureCrossProcessTest(ctx, t, cfg)
				require.NoError(t, err)

				const pageIdx = 2
				pageOffset := int64(pageIdx) * int64(tt.pagesize)

				memStart := uintptr(unsafe.Pointer(&(*h.memoryArea)[0]))
				addr := memStart + uintptr(pageIdx)*uintptr(tt.pagesize)

				token, err := h.installFaultBarrier(ctx, addr, faultPhaseBeforeFaultPage)
				require.NoError(t, err)

				writeErrCh := make(chan error, 1)
				go func() {
					writeErrCh <- h.executeWrite(ctx, operation{offset: pageOffset, mode: operationModeWrite})
				}()

				waitCtx, waitCancel := context.WithTimeout(ctx, barrierArrivalDeadline)
				err = h.waitFaultHeld(waitCtx, token)
				waitCancel()
				require.NoError(t, err, "worker for page %d (addr %#x) did not park at pre-COPY barrier", pageIdx, addr)

				// Worker is parked holding RLock; madvise must still complete.
				madviseDone := make(chan error, 1)
				go func() {
					madviseDone <- unix.Madvise((*h.memoryArea)[pageOffset:pageOffset+int64(tt.pagesize)], unix.MADV_DONTNEED)
				}()

				select {
				case err := <-madviseDone:
					require.NoError(t, err)
				case <-time.After(madviseBudget):
					_ = h.releaseFault(ctx, token)
					<-writeErrCh
					t.Fatalf("DEADLOCK: madvise(MADV_DONTNEED) on page %d did not return within %s "+
						"while a worker was parked holding settleRequests.RLock — readEvents must not require any lock",
						pageIdx, madviseBudget)
				}

				require.NoError(t, h.releaseFault(ctx, token))

				select {
				case err := <-writeErrCh:
					require.NoError(t, err)
				case <-ctx.Done():
					t.Fatalf("user-side write of page %d did not unblock after barrier release", pageIdx)
				}
			})
		})
	}
}

// TestFaultedShortCircuitOrdering is an end-state sanity check for a
// REMOVE + PAGEFAULT batch on disjoint pages: page 0 (already faulted)
// is REMOVE'd, page 1 (missing) gets a write fault, and after resume
// page 0 must be `removed` and page 1 must be `faulted`. The two
// orderings happen to commute on disjoint pages, so this test does
// not by itself prove drain-order; same-page ordering is covered by
// TestStaleSourceRaceMissingAndRemove.
func TestFaultedShortCircuitOrdering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pagesize uint64
	}{
		{name: "4k", pagesize: header.PageSize},
		{name: "hugepage", pagesize: header.HugepageSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			withRaceContext(t, func(ctx context.Context) {
				cfg := testConfig{
					pagesize:      tt.pagesize,
					numberOfPages: 2,
					gated:         true,
					removeEnabled: true,
					operations: []operation{
						{offset: 0, mode: operationModeRead},
						{mode: operationModeServePause},
						{offset: 0, mode: operationModeRemove, async: true},
						{mode: operationModeSleep},
						{offset: int64(tt.pagesize), mode: operationModeWrite, async: true},
						{mode: operationModeSleep},
						{mode: operationModeServeResume},
					},
				}

				h, err := configureCrossProcessTest(ctx, t, cfg)
				require.NoError(t, err)

				h.executeAll(t, cfg.operations) //nolint:contextcheck // executeAll uses t.Context() per-op for the bounded race wrapper above

				states, err := h.pageStates()
				require.NoError(t, err)

				assert.Contains(t, states.removed, uint(0),
					"page 0 should be `removed` after REMOVE batch (got removed=%v faulted=%v)",
					states.removed, states.faulted,
				)
				assert.Contains(t, states.faulted, uint(tt.pagesize),
					"page 1 (offset %d) should be `faulted` after the racing write was served (got removed=%v faulted=%v)",
					tt.pagesize, states.removed, states.faulted,
				)
			})
		})
	}
}

// waitForState polls the child's PageStates RPC until the page at
// `offset` reaches `want` or barrierArrivalDeadline elapses.
func waitForState(ctx context.Context, h *testHandler, offset uint64, want block.State) error {
	const pollInterval = 1 * time.Millisecond

	end := time.Now().Add(barrierArrivalDeadline)
	for {
		states, err := h.pageStates()
		if err != nil {
			return err
		}

		var bucket []uint
		switch want {
		case block.Zero:
			bucket = states.zero
		case block.Removed:
			bucket = states.removed
		case block.Dirty:
			bucket = states.faulted
		case block.Clean:
			bucket = states.clean
		default:
			return fmt.Errorf("waitForState: unsupported want=%d", want)
		}

		for _, off := range bucket {
			if uint64(off) == offset {
				return nil
			}
		}

		if time.Now().After(end) {
			return fmt.Errorf("page state at offset %d: want %d after %s — last seen removed=%v faulted=%v",
				offset, want, barrierArrivalDeadline, states.removed, states.faulted)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}
