//go:build linux

package userfaultfd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

func TestRemove(t *testing.T) {
	t.Parallel()

	tests := []testConfig{
		{
			name:          "4k read then remove",
			pagesize:      header.PageSize,
			numberOfPages: 2,
			removeEnabled: true,
			operations: []operation{
				{offset: 0, mode: operationModeRead},
				{offset: 0, mode: operationModeRemove},
				{mode: operationModeSleep},
			},
		},
		{
			name:          "hugepage read then remove",
			pagesize:      header.HugepageSize,
			numberOfPages: 2,
			removeEnabled: true,
			operations: []operation{
				{offset: 0, mode: operationModeRead},
				{offset: 0, mode: operationModeRemove},
				{mode: operationModeSleep},
			},
		},
		{
			name:          "4k write then remove",
			pagesize:      header.PageSize,
			numberOfPages: 2,
			removeEnabled: true,
			operations: []operation{
				{offset: 0, mode: operationModeWrite},
				{offset: 0, mode: operationModeRemove},
				{mode: operationModeSleep},
			},
		},
		{
			name:          "hugepage write then remove",
			pagesize:      header.HugepageSize,
			numberOfPages: 2,
			removeEnabled: true,
			operations: []operation{
				{offset: 0, mode: operationModeWrite},
				{offset: 0, mode: operationModeRemove},
				{mode: operationModeSleep},
			},
		},
		{
			name:          "4k selective remove",
			pagesize:      header.PageSize,
			numberOfPages: 2,
			removeEnabled: true,
			operations: []operation{
				{offset: 0, mode: operationModeRead},
				{offset: int64(header.PageSize), mode: operationModeWrite},
				{offset: 0, mode: operationModeRemove},
				{mode: operationModeSleep},
			},
		},
		{
			name:          "hugepage selective remove",
			pagesize:      header.HugepageSize,
			numberOfPages: 2,
			removeEnabled: true,
			operations: []operation{
				{offset: 0, mode: operationModeRead},
				{offset: int64(header.HugepageSize), mode: operationModeWrite},
				{offset: 0, mode: operationModeRemove},
				{mode: operationModeSleep},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, err := configureCrossProcessTest(t.Context(), t, tt)
			require.NoError(t, err)

			h.executeAll(t, tt.operations)

			states, err := h.pageStates()
			require.NoError(t, err)

			removedOffsets := getOperationsOffsets(tt.operations, operationModeRemove)
			assert.ElementsMatch(t, removedOffsets, states.removed)

			writeOffsets := getOperationsOffsets(tt.operations, operationModeWrite)
			for _, r := range removedOffsets {
				writeOffsets = removeOffset(writeOffsets, r)
			}
			assert.ElementsMatch(t, writeOffsets, states.faulted,
				"write installs materialize pages as Dirty")

			readOffsets := getOperationsOffsets(tt.operations, operationModeRead)
			for _, r := range removedOffsets {
				readOffsets = removeOffset(readOffsets, r)
			}
			for _, w := range writeOffsets {
				readOffsets = removeOffset(readOffsets, w)
			}
			assert.ElementsMatch(t, readOffsets, states.clean,
				"source read-installs are Clean (WP-armed, content == source)")

			h.checkDirtiness(t, tt.operations)
		})
	}
}

// TestRemoveMultiPage covers MADV_DONTNEED across a contiguous multi-page
// sub-range that spans both faulted and unfaulted pages — the production
// shape of free-page-reporting balloon deflate. Asserts every page in the
// range transitions to removed (faulted→removed and unfaulted→removed in
// the same event) while pages outside the range keep their prior state.
func TestRemoveMultiPage(t *testing.T) {
	t.Parallel()

	tests := []testConfig{
		{
			name:          "4k multi-page remove spans faulted and unfaulted",
			pagesize:      header.PageSize,
			numberOfPages: 6,
			removeEnabled: true,
			operations: []operation{
				{offset: 0, mode: operationModeRead},
				{offset: int64(header.PageSize) * 2, mode: operationModeRead},
				{offset: int64(header.PageSize), mode: operationModeRemove, pages: 4},
				{mode: operationModeSleep},
			},
		},
		{
			name:          "hugepage multi-page remove spans faulted and unfaulted",
			pagesize:      header.HugepageSize,
			numberOfPages: 6,
			removeEnabled: true,
			operations: []operation{
				{offset: 0, mode: operationModeRead},
				{offset: int64(header.HugepageSize) * 2, mode: operationModeRead},
				{offset: int64(header.HugepageSize), mode: operationModeRemove, pages: 4},
				{mode: operationModeSleep},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, err := configureCrossProcessTest(t.Context(), t, tt)
			require.NoError(t, err)

			h.executeAll(t, tt.operations)

			states, err := h.pageStates()
			require.NoError(t, err)

			expectedRemoved := []uint{
				uint(tt.pagesize),
				uint(tt.pagesize) * 2,
				uint(tt.pagesize) * 3,
				uint(tt.pagesize) * 4,
			}
			assert.ElementsMatch(t, expectedRemoved, states.removed,
				"all four pages in MADV_DONTNEED range should be removed (faulted page 2 and unfaulted pages 1,3,4)")

			assert.ElementsMatch(t, []uint{0}, states.clean,
				"page 0 (outside remove range) should keep its read-install (Clean) state")
			assert.Empty(t, states.faulted, "nothing was written, so no page is Dirty")

			outsideRange := uint(tt.pagesize) * 5
			assert.NotContains(t, states.clean, outsideRange,
				"page 5 was never touched and must not appear as clean")
			assert.NotContains(t, states.removed, outsideRange,
				"page 5 was never touched and must not appear as removed")

			h.checkDirtiness(t, tt.operations)
		})
	}
}

// TestRemoveThenFault asserts that after MADV_DONTNEED + a subsequent write,
// the handler re-faults the page (state transitions: faulted → removed → faulted).
func TestRemoveThenFault(t *testing.T) {
	t.Parallel()

	tests := []testConfig{
		{
			name:          "4k read, remove, write",
			pagesize:      header.PageSize,
			numberOfPages: 2,
			removeEnabled: true,
			operations: []operation{
				{offset: 0, mode: operationModeRead},
				{offset: 0, mode: operationModeRemove},
				{offset: 0, mode: operationModeWrite},
			},
		},
		{
			name:          "hugepage read, remove, write",
			pagesize:      header.HugepageSize,
			numberOfPages: 2,
			removeEnabled: true,
			operations: []operation{
				{offset: 0, mode: operationModeRead},
				{offset: 0, mode: operationModeRemove},
				{offset: 0, mode: operationModeWrite},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, err := configureCrossProcessTest(t.Context(), t, tt)
			require.NoError(t, err)

			h.executeAll(t, tt.operations)

			states, err := h.pageStates()
			require.NoError(t, err)

			assert.Empty(t, states.removed, "page should not be in removed state after re-fault")
			assert.Contains(t, states.faulted, uint(0), "page should be back in faulted state")

			h.checkDirtiness(t, tt.operations)
		})
	}
}

// TestRemoveThenWriteGated pins the gated-remove race: with the serve loop
// paused, MADV_DONTNEED queues a REMOVE and blocks the madviser in
// userfaultfd_event_wait_completion(), so nothing is zapped until the resumed
// loop read()s the event and a write issued inside that window hits the
// still-mapped page.
//
// Both arms run under WP_ASYNC (syncWP unset), where the kernel auto-resolves
// writes to the WP-armed page without delivering an event. The handler's fault
// log is therefore an exact discriminator. No new page-0 event means the write
// landed on the mapped page and the race ran — the only passing condition, and
// the resumed handler must then record the REMOVE and nothing else (tracker
// Removed, pagemap absent). A new page-0 event means the write ran after
// resume+zap and the race did not run: that attempt is sanity-checked (the
// re-served write must reinstall the page) and retried against the reinstalled
// page, and the test skips loudly rather than reporting success if the race
// never materializes. UFFDIO_COPY reinstalls the page present and WRITABLE,
// so only the first attempt writes to the WP-armed page and exercises the
// auto-resolve interaction the exactness argument above rests on; retries
// exercise the primary invariant — nothing is zapped until the resumed loop
// reads the event — against a plain mapped page, where the discriminator is
// exact for the simpler reason that an unfaulted write produces no event at
// all. The reinstall witnesses cannot tell a handler that read the REMOVE and
// skipped the Removed transition from one that recorded it, so they are
// consistency checks, not coverage.
//
// Exactness assumes WP_ASYNC auto-resolves hugetlb writes as well as 4k —
// supported by the write-first outcomes observed on real hugepage CI runs, not
// confirmed against the kernel source. A kernel delivering synchronous WP
// events there would resurrect the fault-blocked case on that arm, and the
// discriminator would then need REMOVE-batch membership: readEvents drains to
// EAGAIN, so a fault queued while paused arrives in the REMOVE's batch and a
// late one in a later batch.
func TestRemoveThenWriteGated(t *testing.T) {
	t.Parallel()

	tests := []testConfig{
		{
			name:          "4k gated remove with concurrent write",
			pagesize:      header.PageSize,
			numberOfPages: 2,
			gated:         true,
			removeEnabled: true,
		},
		{
			name:          "hugepage gated remove with concurrent write",
			pagesize:      header.HugepageSize,
			numberOfPages: 2,
			gated:         true,
			removeEnabled: true,
		},
	}

	const gatedRaceAttempts = 5

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, err := configureCrossProcessTest(ctx, t, tt)
			require.NoError(t, err)

			// A read installs page 0 present and WP-armed (Clean).
			require.NoError(t, h.executeOperation(ctx, operation{offset: 0, mode: operationModeRead}))

			for attempt := 1; attempt <= gatedRaceAttempts; attempt++ {
				// The install read and each retried attempt's re-served write
				// add page-0 events, so the baseline is per-attempt.
				baselineFaults, err := h.client.FaultOffsets()
				require.NoError(t, err)
				baselinePage0 := countOffset(baselineFaults, 0)

				require.NoError(t, h.client.Pause())

				removeCh := make(chan error, 1)
				go func() {
					removeCh <- h.executeOperation(ctx, operation{offset: 0, mode: operationModeRemove})
				}()
				time.Sleep(50 * time.Millisecond)

				writeCh := make(chan error, 1)
				go func() {
					writeCh <- h.executeOperation(ctx, operation{offset: 0, mode: operationModeWrite})
				}()
				time.Sleep(50 * time.Millisecond)

				require.NoError(t, h.client.Resume())

				select {
				case rerr := <-removeCh:
					require.NoError(t, rerr, "gated remove")
				case <-ctx.Done():
					require.Fail(t, "timed out waiting for the gated remove")
				}
				select {
				case werr := <-writeCh:
					require.NoError(t, werr, "concurrent write")
				case <-ctx.Done():
					require.Fail(t, "timed out waiting for the concurrent write")
				}

				// The fault hook fires before the install, so the log is
				// settled once both operations have drained.
				finalFaults, err := h.client.FaultOffsets()
				require.NoError(t, err)

				if countOffset(finalFaults, 0) == baselinePage0 {
					states, err := h.awaitPageState(ctx, 0, block.Removed)
					require.NoError(t, err, "the resumed handler must record the REMOVE")
					assert.Empty(t, states.faulted)
					h.checkDirtiness(t, []operation{{offset: 0, mode: operationModeRemove}})

					return
				}

				t.Logf("attempt %d/%d: the write ran after resume+zap, so the gated race did not run; retrying",
					attempt, gatedRaceAttempts)

				states, err := h.awaitPageState(ctx, 0, block.Dirty)
				require.NoError(t, err, "the re-served write must reinstall the page")
				require.Empty(t, states.removed)
				h.checkDirtiness(t, []operation{{offset: 0, mode: operationModeWrite}})
			}

			t.Skipf("the gated race never materialized in %d attempts: the write goroutine consistently ran after resume on this runner", gatedRaceAttempts)
		})
	}
}

// TestWriteThenRemoveGated verifies the serve loop's ordering guarantee:
// REMOVE events are processed before pagefaults even when the MISSING pagefault
// was queued first. The write to a missing page triggers MISSING (queued first),
// then MADV_DONTNEED triggers REMOVE (queued second). When the handler resumes,
// it processes REMOVE first, then MISSING — the write is not skipped.
func TestWriteThenRemoveGated(t *testing.T) {
	t.Parallel()

	tests := []testConfig{
		{
			name:          "4k write then remove in same batch",
			pagesize:      header.PageSize,
			numberOfPages: 2,
			gated:         true,
			removeEnabled: true,
			operations: []operation{
				{offset: 0, mode: operationModeRead},
				{mode: operationModeServePause},
				// MISSING for page 1 queued first
				{offset: int64(header.PageSize), mode: operationModeWrite, async: true},
				{mode: operationModeSleep},
				// REMOVE for page 0 queued second
				{offset: 0, mode: operationModeRemove, async: true},
				{mode: operationModeSleep},
				{mode: operationModeServeResume},
			},
		},
		{
			name:          "hugepage write then remove in same batch",
			pagesize:      header.HugepageSize,
			numberOfPages: 2,
			gated:         true,
			removeEnabled: true,
			operations: []operation{
				{offset: 0, mode: operationModeRead},
				{mode: operationModeServePause},
				{offset: int64(header.HugepageSize), mode: operationModeWrite, async: true},
				{mode: operationModeSleep},
				{offset: 0, mode: operationModeRemove, async: true},
				{mode: operationModeSleep},
				{mode: operationModeServeResume},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, err := configureCrossProcessTest(t.Context(), t, tt)
			require.NoError(t, err)

			h.executeAll(t, tt.operations)

			states, err := h.pageStates()
			require.NoError(t, err)

			assert.Contains(t, states.removed, uint(0))
			pageOffset := uint(tt.pagesize)
			assert.Contains(t, states.faulted, pageOffset,
				"write pagefault should not be skipped even when batched with REMOVE")
		})
	}
}
