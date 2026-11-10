//go:build linux

package userfaultfd

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/testutils"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/testutils/testharness"
)

var matrixModes = []struct {
	name          string
	removeEnabled bool
}{
	{"remove-off", false},
	{"remove-on", true},
}

// runMatrix runs body once per (removeEnabled) arm as a sequential pair
// of subtests. Arms are intentionally NOT parallel: each arm forks a
// uffd helper that mmaps the test region, and running both arms in
// parallel on top of the case-level fan-out has OOM'd memory-bound
// runners under -race.
func runMatrix(t *testing.T, tt testConfig, body func(t *testing.T, cfg testConfig)) {
	t.Helper()

	for _, m := range matrixModes {
		t.Run(m.name, func(t *testing.T) {
			cfg := tt
			cfg.removeEnabled = m.removeEnabled
			body(t, cfg)
		})
	}
}

// remove-only assert helper used by REMOVE-specific tests.
func removeOffset(offsets []uint, target uint) []uint {
	result := make([]uint, 0, len(offsets))
	for _, o := range offsets {
		if o != target {
			result = append(result, o)
		}
	}

	return result
}

type testConfig struct {
	name          string
	pagesize      uint64
	numberOfPages uint64
	operations    []operation
	// alwaysWP forces UFFDIO_COPY_MODE_WP on every fault.
	alwaysWP bool
	// removeEnabled toggles UFFD_FEATURE_EVENT_REMOVE in configureApi.
	removeEnabled bool
	// syncWP registers the uffd WITHOUT WP_ASYNC: writes to armed pages
	// block on a synchronous WP event until the serve loop resolves them
	// (production use-sync-wp). Under the default WP_ASYNC the kernel
	// auto-resolves such writes and the serve loop never sees them.
	syncWP bool
	// gated is a doc-only marker for tests that drive Pause/Resume.
	gated bool
	// barriers enables the per-worker fault hook (race tests only).
	barriers bool
	// sourcePatcher mutates the random source bytes before the child reads
	// them so race tests can plant a deterministic sentinel.
	sourcePatcher func([]byte)
	// sourceDelay slows every source read in the serving child.
	sourceDelay time.Duration
	// installDedupOff disables the one-installer-per-page claim in the child.
	installDedupOff bool
}

type operationMode uint32

const (
	operationModeRead operationMode = 1 << iota
	operationModeWrite
	operationModeRemove
	operationModeServePause
	operationModeServeResume
	// operationModeSleep pauses for a short duration to let async goroutines
	// enter their blocking syscalls before proceeding.
	operationModeSleep
)

type operation struct {
	// Offset in bytes. Must be smaller than the (numberOfPages-1) * pagesize as it reads a page and it must be aligned to the pagesize from the testConfig.
	offset int64
	mode   operationMode
	// async runs the operation in a background goroutine.
	async bool
	// pages spans the operation across N contiguous pages. Currently only
	// honored by operationModeRemove (multi-page MADV_DONTNEED). 0 == 1.
	pages uint64
}

type handlerPageStates struct {
	faulted []uint // block.Dirty: materialized by a write (or write reinstall)
	zero    []uint // block.Zero: installed zero page
	removed []uint // block.Removed: zapped by a REMOVE, not reinstalled
	clean   []uint // block.Clean: WP-armed install, content == source
}

// allAccessed returns the sorted union of every tracked (non-NotPresent)
// page offset.
func (s handlerPageStates) allAccessed() []uint {
	out := make([]uint, 0, len(s.faulted)+len(s.zero)+len(s.removed)+len(s.clean))
	out = append(out, s.faulted...)
	out = append(out, s.zero...)
	out = append(out, s.removed...)
	out = append(out, s.clean...)
	slices.Sort(out)

	return out
}

type testHandler struct {
	memoryArea *[]byte
	pagesize   uint64
	data       *MemorySlicer
	client     *testharness.Client
	mutex      sync.RWMutex
}

func (h *testHandler) pageStates() (handlerPageStates, error) {
	entries, err := h.client.PageStates()
	if err != nil {
		return handlerPageStates{}, err
	}

	var states handlerPageStates
	for _, e := range entries {
		switch block.State(e.State) {
		case block.Dirty:
			states.faulted = append(states.faulted, uint(e.Offset))
		case block.Zero:
			states.zero = append(states.zero, uint(e.Offset))
		case block.Removed:
			states.removed = append(states.removed, uint(e.Offset))
		case block.Clean:
			states.clean = append(states.clean, uint(e.Offset))
		}
	}
	slices.Sort(states.faulted)
	slices.Sort(states.zero)
	slices.Sort(states.removed)
	slices.Sort(states.clean)

	return states, nil
}

// awaitPageState polls the tracker until the page at offset reaches want. The
// handler records a transition only after the syscall that released the caller
// — the read() of a REMOVE, UFFDIO_COPY for a fault — so a state sampled the
// instant an operation returns can predate the bookkeeping it should show.
func (h *testHandler) awaitPageState(ctx context.Context, offset uint, want block.State) (handlerPageStates, error) {
	deadline := time.Now().Add(2 * time.Second)

	for {
		states, err := h.pageStates()
		if err != nil {
			return handlerPageStates{}, err
		}

		var got []uint
		switch want {
		case block.Dirty:
			got = states.faulted
		case block.Zero:
			got = states.zero
		case block.Removed:
			got = states.removed
		case block.Clean:
			got = states.clean
		}

		if slices.Contains(got, offset) {
			return states, nil
		}

		if time.Now().After(deadline) {
			return states, fmt.Errorf("page at offset %d never reached state %d: %+v", offset, want, states)
		}

		select {
		case <-ctx.Done():
			return states, ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func (h *testHandler) installFaultBarrier(_ context.Context, addr uintptr, phase faultPhase) (uint64, error) {
	return h.client.InstallBarrier(addr, testharness.Point(phase))
}

// waitFaultHeld blocks until the child reports a worker has reached the
// barrier identified by token, or ctx is cancelled. net/rpc's Call
// doesn't take a context, so we run it in a goroutine and race it.
func (h *testHandler) waitFaultHeld(ctx context.Context, token uint64) error {
	errCh := make(chan error, 1)
	go func() { errCh <- h.client.WaitFaultHeld(token) }()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *testHandler) releaseFault(_ context.Context, token uint64) error {
	return h.client.ReleaseFault(token)
}

func (h *testHandler) executeAll(t *testing.T, operations []operation) {
	t.Helper()

	var asyncErrors []chan error

	for i, op := range operations {
		if op.async {
			errCh := make(chan error, 1)
			asyncErrors = append(asyncErrors, errCh)

			go func() {
				errCh <- h.executeOperation(t.Context(), op)
			}()

			continue
		}

		err := h.executeOperation(t.Context(), op)
		require.NoError(t, err, "step %d: %v at offset %d", i, op.mode, op.offset)
	}

	for _, errCh := range asyncErrors {
		select {
		case err := <-errCh:
			require.NoError(t, err, "async operation")
		case <-t.Context().Done():
			t.Fatal("timed out waiting for async operation")
		}
	}
}

type pageExpectation uint8

const (
	expectClean   pageExpectation = iota // read-only: present + WP set
	expectDirty                          // written: present + WP cleared
	expectRemoved                        // removed: not present
)

func (h *testHandler) checkDirtiness(t *testing.T, operations []operation) {
	t.Helper()

	pagemap, err := testutils.NewPagemapReader()
	require.NoError(t, err)
	defer pagemap.Close()

	memStart := uintptr(unsafe.Pointer(&(*h.memoryArea)[0]))

	// Replay operations to derive the final expected state per offset.
	expected := make(map[uint]pageExpectation)

	for _, op := range operations {
		off := uint(op.offset)
		switch op.mode {
		case operationModeRead:
			// First read on a fresh / removed page lands clean (WP set);
			// a subsequent read on a dirty page must not downgrade.
			curr, seen := expected[off]
			if !seen || curr == expectRemoved {
				expected[off] = expectClean
			}
		case operationModeWrite:
			expected[off] = expectDirty
		case operationModeRemove:
			pages := op.pages
			if pages == 0 {
				pages = 1
			}
			for i := range pages {
				expected[off+uint(i*h.pagesize)] = expectRemoved
			}
		}
	}

	for off, expect := range expected {
		entry, err := pagemap.ReadEntry(memStart + uintptr(off))
		require.NoError(t, err, "pagemap read at offset %d", off)

		switch expect {
		case expectRemoved:
			assert.False(t, entry.IsPresent(), "removed page at offset %d should not be present", off)
		case expectDirty:
			assert.True(t, entry.IsPresent(), "written page at offset %d should be present", off)
			assert.False(t, entry.IsWriteProtected(), "written page at offset %d should be dirty", off)
		case expectClean:
			assert.True(t, entry.IsPresent(), "read-only page at offset %d should be present", off)
			assert.True(t, entry.IsWriteProtected(), "read-only page at offset %d should be clean", off)
		}
	}
}

func (h *testHandler) executeOperation(ctx context.Context, op operation) error {
	switch op.mode {
	case operationModeRead:
		return h.executeRead(ctx, op)
	case operationModeWrite:
		return h.executeWrite(ctx, op)
	case operationModeRemove:
		return h.executeRemove(op)
	case operationModeServePause:
		return h.client.Pause()
	case operationModeServeResume:
		return h.client.Resume()
	case operationModeSleep:
		time.Sleep(50 * time.Millisecond)

		return nil
	default:
		return fmt.Errorf("invalid operation mode: %d", op.mode)
	}
}

func (h *testHandler) executeRemove(op operation) error {
	pages := op.pages
	if pages == 0 {
		pages = 1
	}
	length := int64(pages * h.pagesize)
	region := (*h.memoryArea)[op.offset : op.offset+length]

	return unix.Madvise(region, unix.MADV_DONTNEED)
}

func (h *testHandler) executeRead(ctx context.Context, op operation) error {
	expectedBytes, err := h.data.Slice(ctx, op.offset, int64(h.pagesize))
	if err != nil {
		return err
	}

	h.mutex.RLock()
	defer h.mutex.RUnlock()

	readBytes := (*h.memoryArea)[op.offset : op.offset+int64(h.pagesize)]

	// MADV_POPULATE_READ faults via syscall: the goroutine sits in
	// _Gsyscall while the kernel waits for UFFDIO_COPY, so STW can preempt
	// it. A direct memory load would be _Grunning and would deadlock STW
	// against the matching uffd worker goroutine.
	if err := unix.Madvise(readBytes, unix.MADV_POPULATE_READ); err != nil {
		return fmt.Errorf("madvise POPULATE_READ at offset %d: %w", op.offset, err)
	}

	if !bytes.Equal(readBytes, expectedBytes) {
		idx, want, got := testutils.FirstDifferentByte(readBytes, expectedBytes)

		return fmt.Errorf("content mismatch: want '%x, got %x at index %d", want, got, idx)
	}

	return nil
}

func (h *testHandler) executeWrite(ctx context.Context, op operation) error {
	bytesToWrite, err := h.data.Slice(ctx, op.offset, int64(h.pagesize))
	if err != nil {
		return err
	}

	h.mutex.Lock()
	defer h.mutex.Unlock()

	writeBytes := (*h.memoryArea)[op.offset : op.offset+int64(h.pagesize)]

	// MADV_POPULATE_WRITE: STW preemption note in executeRead applies.
	if err := unix.Madvise(writeBytes, unix.MADV_POPULATE_WRITE); err != nil {
		return fmt.Errorf("madvise POPULATE_WRITE at offset %d: %w", op.offset, err)
	}

	n := copy(writeBytes, bytesToWrite)
	if n != int(h.pagesize) {
		return fmt.Errorf("copy length mismatch: want %d, got %d", h.pagesize, n)
	}

	return nil
}

func countOffset(offsets []int64, off int64) int {
	n := 0
	for _, o := range offsets {
		if o == off {
			n++
		}
	}

	return n
}

func getOperationsOffsets(ops []operation, m operationMode) []uint {
	b := roaring.New()

	for _, operation := range ops {
		if operation.mode&m != 0 {
			b.Add(uint32(operation.offset))
		}
	}

	result := make([]uint, 0, b.GetCardinality())
	b.Iterate(func(x uint32) bool {
		result = append(result, uint(x))

		return true
	})

	return result
}
