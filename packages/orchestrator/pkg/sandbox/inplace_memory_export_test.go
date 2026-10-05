//go:build linux

package sandbox

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// stubCoWExporter satisfies uffd.CoWExporter for driving the export paths
// without a live UFFD handler: BeginCoWExport installs a real CoWWindow over
// the given sink, reading pre-images from src.
type stubCoWExporter struct {
	src      io.ReaderAt
	pageSize int64

	window *userfaultfd.CoWWindow
	ended  bool
}

var _ uffd.CoWExporter = (*stubCoWExporter)(nil)

func (s *stubCoWExporter) BeginCoWExport(_ context.Context, dirty *roaring.Bitmap, sink io.WriterAt) (*userfaultfd.CoWWindow, error) {
	s.window = userfaultfd.NewCoWWindow(dirty, s.pageSize, s.src, sink, nil)

	return s.window, nil
}

func (s *stubCoWExporter) EndCoWExport(*userfaultfd.CoWWindow) { s.ended = true }

// TestRunDeferredMemoryExport_FailureSparesSandbox pins the memory-seal-latch
// decision: a failed or cancelled CoW window (the free-page-reporting REMOVE
// tripwire is the designed producer) loses ONLY that checkpoint's artifact.
// Memory has nothing to fold back — exports parent the original template and
// union inPlaceExportedDirty, and content comes from live guest RAM — so the
// seal signal must resolve SUCCESS once the window is released: neither a
// later pause (waitForMemorySeal) nor its pre-arm guard (EnsurePausable) may
// refuse, let alone kill, a fully exportable sandbox.
func TestRunDeferredMemoryExport_FailureSparesSandbox(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	pages := roaring.New()
	pages.AddMany([]uint32{0, 1})

	cache, err := block.NewCache(pageSize*2, pageSize, t.TempDir()+"/mem.cache", false)
	require.NoError(t, err)

	src := bytes.NewReader(make([]byte, pageSize*2))
	window := userfaultfd.NewCoWWindow(pages, pageSize, src, cache, nil)

	// The tripwire's shape: the window is cancelled before the sweep runs.
	cancelCause := errors.New("uffd REMOVE overlaps uncaptured CoW-window pages")
	window.Cancel(cancelCause)

	// The runner's failure logs carry sandbox identity, so the test Sandbox
	// needs Metadata for sbxlogger.
	s := &Sandbox{Metadata: &Metadata{Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "test-sbx"}}}
	ce := &stubCoWExporter{}
	diffPromise := utils.NewSetOnce[build.Diff]()
	sealDone := utils.NewSetOnce[struct{}]()
	s.memSealMu.Lock()
	s.memSealDone = sealDone
	s.memSealMu.Unlock()

	// Run synchronously (production spawns it on a goroutine).
	s.runDeferredMemoryExport(t.Context(), window, ce, cache, uuid.New(), diffPromise, sealDone, false)

	// The checkpoint's artifact is lost, tagged as the permanent seal failure
	// so the upload retry loop stops.
	_, diffErr := diffPromise.Result()
	require.Error(t, diffErr)
	require.ErrorIs(t, diffErr, build.ErrDeferredSealFailed)

	// The window was released...
	require.True(t, ce.ended, "the runner must EndCoWExport on the failure path")

	// ...and the SANDBOX is unharmed: the occupancy signal resolved SUCCESS,
	// so the next pause proceeds and the pre-arm guard does not kill.
	_, err = sealDone.Result()
	require.NoError(t, err, "a failed capture must resolve the seal signal success, not latch")
	require.NoError(t, s.waitForMemorySeal(t.Context()))
	require.NoError(t, s.EnsurePausable())
}

// TestSetupDeferredMemoryExport_AbortCleanupOrder is the memory twin of
// TestSetupDeferredRootfsExport_AbortCleanupOrder: when Pause aborts before
// startMemSeal runs, the LIFO cleanup stack must poison the deferred diff's
// promise BEFORE the diff's Close waits on it — the deferred diff's Close is
// registered inside setup, between the window cleanup and the resolver, and a
// caller-registered Close (appended after the resolver) would run first and
// deadlock the whole cleanup run on a promise nothing will ever resolve.
func TestSetupDeferredMemoryExport_AbortCleanupOrder(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	numPages := int64(4)
	size := pageSize * numPages

	dirty := roaring.New()
	dirty.AddMany([]uint32{0, 2})

	memfileHeader, err := header.NewHeader(
		header.NewTemplateMetadata(uuid.New(), uint64(pageSize), uint64(size)),
		nil,
	)
	require.NoError(t, err)
	diffMetadata := header.NewDiffMetadata(pageSize, dirty, roaring.New())

	ce := &stubCoWExporter{src: bytes.NewReader(make([]byte, size)), pageSize: pageSize}
	s := &Sandbox{config: cfg.BuilderConfig{DefaultCacheDir: t.TempDir()}}

	cleanup := NewCleanup()
	mem, startMemSeal, err := s.setupDeferredMemoryExport(t.Context(), uuid.New(), memfileHeader, diffMetadata, ce, false, cleanup)
	require.NoError(t, err)
	require.NotNil(t, mem.Diff)
	require.NotNil(t, startMemSeal)

	// The seal gate (async-checkpoint guard) must BLOCK while the export is
	// unsettled — answering nil here is exactly the artifact-less-record
	// hazard it exists to close.
	snap := &Snapshot{MemoryExportDeferred: true, MemorySnapshot: mem}
	waitCtx, cancelWait := context.WithTimeout(t.Context(), 50*time.Millisecond)
	require.ErrorIs(t, snap.WaitMemorySealed(waitCtx), context.DeadlineExceeded)
	cancelWait()

	// Do NOT call startMemSeal: simulate Pause aborting (e.g. ResumeInPlace
	// failed). The cleanup stack must complete promptly — a wrong ordering
	// deadlocks it on the unresolved promise.
	done := make(chan error, 1)
	go func() { done <- cleanup.Run(t.Context()) }()
	select {
	case runErr := <-done:
		require.NoError(t, runErr)
	case <-time.After(10 * time.Second):
		t.Fatal("cleanup deadlocked: deferred memory diff Close ran before the promise was poisoned")
	}

	// The promise was poisoned, so the diff's data methods resolve (to the
	// abort error) without blocking on a capture that never ran...
	_, err = mem.Diff.FileSize(t.Context())
	require.ErrorContains(t, err, "pause aborted before deferred memory export ran")

	// ...and the window was released.
	require.True(t, ce.ended, "the abort cleanup must EndCoWExport")

	// The seal gate surfaces the settled failure — never success.
	require.ErrorContains(t, snap.WaitMemorySealed(t.Context()),
		"pause aborted before deferred memory export ran")
}

// TestSnapshotWaitMemorySealed_SynchronousExport pins the gate's fast path: a
// snapshot whose memory was copied synchronously has nothing to wait for.
func TestSnapshotWaitMemorySealed_SynchronousExport(t *testing.T) {
	t.Parallel()

	require.NoError(t, (&Snapshot{}).WaitMemorySealed(t.Context()))
}

// TestSetupDeferredMemoryExport_EmptyDirtyIsNotDeferred pins the empty-set
// shape: no dirty pages installs no window and returns a NIL startMemSeal —
// Pause derives MemoryExportDeferred from that nil, so a NoDiff checkpoint
// must not be labeled deferred nor routed through the upload's seal gate.
func TestSetupDeferredMemoryExport_EmptyDirtyIsNotDeferred(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	memfileHeader, err := header.NewHeader(
		header.NewTemplateMetadata(uuid.New(), uint64(pageSize), uint64(pageSize*4)),
		nil,
	)
	require.NoError(t, err)
	diffMetadata := header.NewDiffMetadata(pageSize, roaring.New(), roaring.New())

	ce := &stubCoWExporter{pageSize: pageSize}
	s := &Sandbox{config: cfg.BuilderConfig{DefaultCacheDir: t.TempDir()}}

	cleanup := NewCleanup()
	mem, startMemSeal, err := s.setupDeferredMemoryExport(t.Context(), uuid.New(), memfileHeader, diffMetadata, ce, false, cleanup)
	require.NoError(t, err)
	require.Nil(t, startMemSeal, "an empty dirty set defers nothing")
	require.IsType(t, &build.NoDiff{}, mem.Diff)
	require.Nil(t, ce.window, "no window may be installed for an empty set")
	require.NoError(t, cleanup.Run(t.Context()))
}

// TestApplyInPlaceExportUnion_ExcludesFreedPages pins the union's REMOVE edge:
// a page a previous in-place checkpoint exported but the guest has since freed
// (present in the baseline, reported empty by the current readout) must stay
// EMPTY — not be re-dirtied into a set the deferred path would WP-arm and copy
// as a hole — while still-present baseline pages are re-exported, and the
// advanced baseline drops the freed page.
func TestApplyInPlaceExportUnion_ExcludesFreedPages(t *testing.T) {
	t.Parallel()

	s := &Sandbox{}
	baseline := roaring.New()
	baseline.AddMany([]uint32{0, 1, 2}) // exported by a prior checkpoint
	s.memSealMu.Lock()
	s.inPlaceExportedDirty = baseline
	s.memSealMu.Unlock()

	dirty := roaring.New()
	dirty.Add(3) // freshly written this interval
	empty := roaring.New()
	empty.Add(1) // page 1 was freed (REMOVE) since the last checkpoint
	meta := header.NewDiffMetadata(int64(header.PageSize), dirty, empty)

	s.applyInPlaceExportUnion(meta, true)

	require.ElementsMatch(t, []uint32{0, 2, 3}, meta.Dirty.ToArray(),
		"still-present baseline pages re-export; the freed page must not")
	require.ElementsMatch(t, []uint32{1}, meta.Empty.ToArray(),
		"the freed page stays empty — zeros for free, nothing to arm or copy")

	s.memSealMu.Lock()
	advanced := s.inPlaceExportedDirty.ToArray()
	s.memSealMu.Unlock()
	require.ElementsMatch(t, []uint32{0, 2, 3}, advanced,
		"the advanced baseline drops the freed page")
}

// TestRunFPRResume pins the resume retry/fence choreography without a live
// FC process (deps-injected, like pollFphDone): the inline attempt, the
// detached retry, the generation fence that makes a stale retry abandon
// itself once a newer window owns the pause, the FC-exit bail, and the
// abandoned terminal state — each landing on its outcome counter, since
// "abandoned" is the alertable signal for a leaked pause.
func TestRunFPRResume(t *testing.T) {
	t.Parallel()

	mkDeps := func(resume func(context.Context) error, gen func() uint64, exited <-chan struct{}) (fprResumeDeps, chan string) {
		outcomes := make(chan string, 4)

		return fprResumeDeps{
			resume:         resume,
			fcExited:       exited,
			pauseGen:       gen,
			attemptTimeout: 50 * time.Millisecond,
			retryDelay:     time.Millisecond,
			record:         func(o string) { outcomes <- o },
			logInfo:        func(string, ...zap.Field) {},
			logError:       func(string, ...zap.Field) {},
		}, outcomes
	}

	t.Run("inline success", func(t *testing.T) {
		t.Parallel()
		deps, outcomes := mkDeps(func(context.Context) error { return nil }, func() uint64 { return 1 }, nil)
		runFPRResume(t.Context(), deps)
		require.Equal(t, "inline", <-outcomes)
	})

	t.Run("retry success", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int32
		deps, outcomes := mkDeps(func(context.Context) error {
			if calls.Add(1) < 3 {
				return errors.New("transient")
			}

			return nil
		}, func() uint64 { return 1 }, nil)
		runFPRResume(t.Context(), deps)
		require.Equal(t, "retry", <-outcomes)
		require.Equal(t, int32(3), calls.Load())
	})

	t.Run("fence abandons stale retry", func(t *testing.T) {
		t.Parallel()
		var gen atomic.Uint64
		gen.Store(1)
		var calls atomic.Int32
		deps, outcomes := mkDeps(func(context.Context) error {
			// The retry loop captures the generation AFTER the failed inline
			// attempt, so bump it during the first retry: the fence check
			// before the second retry must then abandon the loop.
			if calls.Add(1) == 2 {
				gen.Store(2)
			}

			return errors.New("still failing")
		}, gen.Load, nil)
		runFPRResume(t.Context(), deps)
		require.Equal(t, "fenced", <-outcomes)
		require.Equal(t, int32(2), calls.Load(), "the stale retry must not touch FPR again after the fence moved")
	})

	t.Run("fc exit bails", func(t *testing.T) {
		t.Parallel()
		exited := make(chan struct{})
		close(exited)
		deps, outcomes := mkDeps(func(context.Context) error { return errors.New("down") }, func() uint64 { return 1 }, exited)
		runFPRResume(t.Context(), deps)
		require.Equal(t, "fc_exited", <-outcomes)
	})

	t.Run("abandoned after exhausting retries", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int32
		deps, outcomes := mkDeps(func(context.Context) error {
			calls.Add(1)

			return errors.New("never")
		}, func() uint64 { return 1 }, nil)
		runFPRResume(t.Context(), deps)
		require.Equal(t, "abandoned", <-outcomes)
		require.Equal(t, int32(5), calls.Load(), "one inline attempt plus four retries")
	})
}
