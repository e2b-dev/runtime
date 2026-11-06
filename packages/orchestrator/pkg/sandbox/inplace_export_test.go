//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// fakeInPlaceRootfs is a rootfs.Provider backed by a real block.Overlay so the
// swap/fold lifecycle behaves like production. Only the in-place seal methods are
// exercised; everything else nil-panics via the embedded interface (unused here).
type fakeInPlaceRootfs struct {
	rootfs.Provider

	overlay   *block.Overlay
	size      int64
	blockSize int64
	dir       string
	gen       int
}

func (f *fakeInPlaceRootfs) SwapForBackgroundSeal(context.Context) (*block.Cache, error) {
	f.gen++
	fresh, err := block.NewCache(f.size, f.blockSize, fmt.Sprintf("%s/fresh%d", f.dir, f.gen), false)
	if err != nil {
		return nil, err
	}

	return f.overlay.SwapCache(fresh)
}

func (f *fakeInPlaceRootfs) FoldSealed(context.Context) (*block.Cache, error) {
	return f.overlay.FoldSealing()
}

func (f *fakeInPlaceRootfs) ExportDiffInPlace(ctx context.Context, out *os.File) (*header.DiffMetadata, error) {
	return f.overlay.ExportDiffInPlace(ctx, out)
}

// inPlaceRODevice is a minimal block.ReadonlyDevice over a fixed byte buffer.
type inPlaceRODevice struct{ data []byte }

func (d *inPlaceRODevice) ReadAt(_ context.Context, p []byte, off int64) (int, error) {
	if off >= int64(len(d.data)) {
		return 0, nil
	}

	return copy(p, d.data[off:]), nil
}

func (d *inPlaceRODevice) Slice(_ context.Context, off, length int64) ([]byte, error) {
	return d.data[off : off+length], nil
}

func (d *inPlaceRODevice) Size(context.Context) (int64, error) { return int64(len(d.data)), nil }
func (d *inPlaceRODevice) Close() error                        { return nil }
func (d *inPlaceRODevice) BlockSize() int64                    { return int64(header.PageSize) }
func (d *inPlaceRODevice) Header() *header.Header              { return nil }
func (d *inPlaceRODevice) SwapHeader(*header.Header)           {}

func TestSynchronousInPlaceRootfsExportSurvivesSourceClose(t *testing.T) {
	t.Parallel()

	blockSize := int64(header.PageSize)
	size := 2 * blockSize
	base := &inPlaceRODevice{data: make([]byte, size)}
	cache, err := block.NewCache(size, blockSize, t.TempDir()+"/source-cache", false)
	require.NoError(t, err)
	overlay := block.NewOverlay(base, cache)
	content := make([]byte, blockSize)
	for i := range content {
		content[i] = 0xAB
	}
	_, err = overlay.WriteAt(content, 0)
	require.NoError(t, err)

	baseHeader, err := header.NewHeader(header.NewTemplateMetadata(uuid.New(), uint64(blockSize), uint64(size)), nil)
	require.NoError(t, err)
	provider := &fakeInPlaceRootfs{overlay: overlay}
	diff, _, err := pauseProcessRootfs(t.Context(), uuid.New(), baseHeader, &RootfsDiffCreator{rootfs: provider}, t.TempDir(), "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = diff.Close() })

	require.NoError(t, overlay.Close())
	path, err := diff.CachePath(t.Context())
	require.NoError(t, err)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, got)
}

// TestRunInPlaceRootfsExport verifies the in-place background lifecycle: the
// frozen (swapped-out) cache is sealed into the deferred diff, the diff resolves
// with the pre-swap bytes, the sealing cache is folded into the live writable
// cache and released, and the completion signal fires so a subsequent checkpoint
// can swap again.
func TestRunInPlaceRootfsExport(t *testing.T) {
	t.Parallel()

	blockSize := int64(header.PageSize)
	numBlocks := int64(4)
	size := blockSize * numBlocks
	base := &inPlaceRODevice{data: make([]byte, size)}

	c0, err := block.NewCache(size, blockSize, t.TempDir()+"/c0", false)
	require.NoError(t, err)
	overlay := block.NewOverlay(base, c0)

	// Pre-checkpoint write to block 0.
	blockA := make([]byte, blockSize)
	for i := range blockA {
		blockA[i] = 0xAA
	}
	_, err = overlay.WriteAt(blockA, 0)
	require.NoError(t, err)

	fake := &fakeInPlaceRootfs{overlay: overlay, size: size, blockSize: blockSize, dir: t.TempDir()}
	s := &Sandbox{
		Metadata:  &Metadata{},
		Resources: &Resources{rootfs: fake},
		config:    cfg.BuilderConfig{DefaultCacheDir: t.TempDir()},
	}

	// Swap (VM would be paused here); the returned cache is the frozen old cache.
	sealCache, err := fake.SwapForBackgroundSeal(t.Context())
	require.NoError(t, err)

	// Post-swap write to block 1 lands in the fresh writable cache.
	blockB := make([]byte, blockSize)
	for i := range blockB {
		blockB[i] = 0xBB
	}
	_, err = overlay.WriteAt(blockB, blockSize)
	require.NoError(t, err)

	meta, err := sealCache.DiffMetadata()
	require.NoError(t, err)

	buildID := uuid.New()
	diffPromise := utils.NewSetOnce[build.Diff]()
	sealDone := utils.NewSetOnce[struct{}]()

	// Run the seal synchronously (no goroutine) for a deterministic test.
	s.runInPlaceRootfsExport(t.Context(), sealCache, buildID, blockSize, meta, diffPromise, sealDone)

	// The completion signal fired.
	_, err = sealDone.Result()
	require.NoError(t, err)

	// The deferred diff resolved to the sealed bytes (block 0 = 0xAA).
	diff, err := diffPromise.Result()
	require.NoError(t, err)
	path, err := diff.CachePath(t.Context())
	require.NoError(t, err)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, blockA, got, "sealed diff must contain the pre-swap block")
	require.NoError(t, diff.Close())

	// The sealing cache was folded into the writable cache and released, so both
	// blocks resolve from the overlay and a second swap succeeds (slot freed).
	read := func(idx int64) []byte {
		buf := make([]byte, blockSize)
		_, rerr := overlay.ReadAt(t.Context(), buf, idx*blockSize)
		require.NoError(t, rerr)

		return buf
	}
	require.Equal(t, blockA, read(0), "folded pre-swap block still readable")
	require.Equal(t, blockB, read(1), "post-swap block readable")

	_, err = fake.SwapForBackgroundSeal(t.Context())
	require.NoError(t, err, "sealing slot must be free after fold")

	require.NoError(t, overlay.Close())
}

// TestRunInPlaceRootfsExport_SealFailureRecovers pins the failure-path
// contract: when the reflink into the diff file fails, the deferred diff must
// carry ErrDeferredSealFailed (the artifact is lost), but the SANDBOX must
// stay serviceable — the frozen cache is folded back so the writable cache is
// a complete diff again, the sealing slot frees for the next checkpoint, and
// the seal signal resolves SUCCESS so neither a later checkpoint nor a later
// pause aborts on a sandbox whose rootfs state is exportable.
func TestRunInPlaceRootfsExport_SealFailureRecovers(t *testing.T) {
	t.Parallel()

	blockSize := int64(header.PageSize)
	numBlocks := int64(4)
	size := blockSize * numBlocks
	base := &inPlaceRODevice{data: make([]byte, size)}

	c0, err := block.NewCache(size, blockSize, t.TempDir()+"/c0", false)
	require.NoError(t, err)
	overlay := block.NewOverlay(base, c0)

	blockA := make([]byte, blockSize)
	for i := range blockA {
		blockA[i] = 0xAA
	}
	_, err = overlay.WriteAt(blockA, 0)
	require.NoError(t, err)

	fake := &fakeInPlaceRootfs{overlay: overlay, size: size, blockSize: blockSize, dir: t.TempDir()}
	s := &Sandbox{
		Metadata:  &Metadata{},
		Resources: &Resources{rootfs: fake},
		// Non-existent cache dir: NewLocalDiffFile fails, so the reflink
		// (sealCacheToDiff) errors before touching the frozen cache.
		config: cfg.BuilderConfig{DefaultCacheDir: t.TempDir() + "/does/not/exist"},
	}

	sealCache, err := fake.SwapForBackgroundSeal(t.Context())
	require.NoError(t, err)

	meta, err := sealCache.DiffMetadata()
	require.NoError(t, err)

	buildID := uuid.New()
	diffPromise := utils.NewSetOnce[build.Diff]()
	sealDone := utils.NewSetOnce[struct{}]()

	s.runInPlaceRootfsExport(t.Context(), sealCache, buildID, blockSize, meta, diffPromise, sealDone)

	// The checkpoint's artifact is lost, tagged as the permanent seal failure.
	_, diffErr := diffPromise.Result()
	require.Error(t, diffErr)
	require.ErrorIs(t, diffErr, build.ErrDeferredSealFailed)

	// But the sandbox recovered: the seal signal resolved SUCCESS...
	_, err = sealDone.Result()
	require.NoError(t, err, "a failed seal must fold back and resolve success, not latch")

	// ...the writable cache is complete again (pre-swap block readable)...
	buf := make([]byte, blockSize)
	_, err = overlay.ReadAt(t.Context(), buf, 0)
	require.NoError(t, err)
	require.Equal(t, blockA, buf, "pre-swap dirty block must survive the fold-back")

	// ...and the sealing slot is free for the next checkpoint.
	nextSeal, err := fake.SwapForBackgroundSeal(t.Context())
	require.NoError(t, err, "sealing slot must be free after the fold-back")
	folded, err := overlay.FoldSealing()
	require.NoError(t, err)
	require.NoError(t, folded.Close())
	_ = nextSeal
}

// TestRunInPlaceRootfsExport_RepeatedCycles pins the repeated-checkpoint
// contract: each cycle seals only the blocks dirtied since the previous swap
// (the fold rebaselines the writable cache, and DiffMetadata of the NEXT
// frozen cache includes both folded and fresh blocks), the sealing slot frees
// between cycles, and the overlay stays complete throughout.
func TestRunInPlaceRootfsExport_RepeatedCycles(t *testing.T) {
	t.Parallel()

	blockSize := int64(header.PageSize)
	numBlocks := int64(4)
	size := blockSize * numBlocks
	base := &inPlaceRODevice{data: make([]byte, size)}

	c0, err := block.NewCache(size, blockSize, t.TempDir()+"/c0", false)
	require.NoError(t, err)
	overlay := block.NewOverlay(base, c0)

	fake := &fakeInPlaceRootfs{overlay: overlay, size: size, blockSize: blockSize, dir: t.TempDir()}
	s := &Sandbox{
		Metadata:  &Metadata{},
		Resources: &Resources{rootfs: fake},
		config:    cfg.BuilderConfig{DefaultCacheDir: t.TempDir()},
	}

	pattern := func(b byte) []byte {
		buf := make([]byte, blockSize)
		for i := range buf {
			buf[i] = b
		}

		return buf
	}

	runCycle := func(writeBlock int64, writeByte byte) build.Diff {
		_, werr := overlay.WriteAt(pattern(writeByte), writeBlock*blockSize)
		require.NoError(t, werr)

		sealCache, serr := fake.SwapForBackgroundSeal(t.Context())
		require.NoError(t, serr, "sealing slot must be free at each cycle start")

		meta, merr := sealCache.DiffMetadata()
		require.NoError(t, merr)

		diffPromise := utils.NewSetOnce[build.Diff]()
		sealDone := utils.NewSetOnce[struct{}]()
		s.runInPlaceRootfsExport(t.Context(), sealCache, uuid.New(), blockSize, meta, diffPromise, sealDone)

		_, derr := sealDone.Result()
		require.NoError(t, derr)
		diff, perr := diffPromise.Result()
		require.NoError(t, perr)

		return diff
	}

	// Cycle 1: block 0 dirty -> sealed diff carries exactly block 0.
	diff1 := runCycle(0, 0xAA)
	path1, err := diff1.CachePath(t.Context())
	require.NoError(t, err)
	got1, err := os.ReadFile(path1)
	require.NoError(t, err)
	require.Equal(t, pattern(0xAA), got1, "cycle 1 diff must hold only the pre-swap block")
	require.NoError(t, diff1.Close())

	// Cycle 2: block 1 dirty. The fold from cycle 1 rebaselined block 0 into
	// the writable cache, so cycle 2's frozen cache (and therefore its diff)
	// holds BOTH blocks — the cumulative diff a resume from this build needs.
	diff2 := runCycle(1, 0xBB)
	path2, err := diff2.CachePath(t.Context())
	require.NoError(t, err)
	got2, err := os.ReadFile(path2)
	require.NoError(t, err)
	require.Equal(t, append(pattern(0xAA), pattern(0xBB)...), got2,
		"cycle 2 diff must hold the folded block plus the new block")
	require.NoError(t, diff2.Close())

	// The overlay still serves everything after two full cycles.
	for idx, want := range map[int64][]byte{0: pattern(0xAA), 1: pattern(0xBB)} {
		buf := make([]byte, blockSize)
		_, rerr := overlay.ReadAt(t.Context(), buf, idx*blockSize)
		require.NoError(t, rerr)
		require.Equal(t, want, buf)
	}
	require.NoError(t, overlay.Close())
}

// TestWaitForRootfsSeal_SerializesNextSnapshot pins the mechanism that keeps a
// new checkpoint or pause from exporting while the previous in-place seal is
// still running: waitForRootfsSeal blocks until the seal signal resolves, then
// returns its outcome. This is the wait Sandbox.Pause performs before touching
// the rootfs, so "snapshot while the previous export is in flight" degrades to
// "wait, then proceed".
func TestWaitForRootfsSeal_SerializesNextSnapshot(t *testing.T) {
	t.Parallel()

	s := &Sandbox{}
	sealDone := utils.NewSetOnce[struct{}]()
	s.rootfsSealDone = sealDone

	returned := make(chan error, 1)
	go func() { returned <- s.waitForRootfsSeal(t.Context()) }()

	// Still sealing: the wait must not return.
	select {
	case err := <-returned:
		t.Fatalf("waitForRootfsSeal returned while the seal was in flight: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, sealDone.SetValue(struct{}{}))
	require.NoError(t, <-returned)

	// A latched failure surfaces to the waiter (the abort-rather-than-export case).
	s2 := &Sandbox{}
	failed := utils.NewSetOnce[struct{}]()
	require.NoError(t, failed.SetError(errors.New("seal failed")))
	s2.rootfsSealDone = failed
	require.Error(t, s2.waitForRootfsSeal(t.Context()))
}

// TestNewPauseOptions checks the combinations a retained pause rejects or
// changes. A retained guest never resumes to drain a deferred export, so a
// missed override would hold the source paused for the whole upload budget.
func TestNewPauseOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		opts            []PauseOption
		wantErr         bool
		wantDeferRootfs bool
		wantDeferMemory bool
	}{
		{
			name:            "in-place checkpoint keeps deferral",
			opts:            []PauseOption{WithMaintainSandbox(), WithDeferredRootfsExport()},
			wantDeferRootfs: true,
			wantDeferMemory: true,
		},
		{
			name: "retained pause disables deferral",
			opts: []PauseOption{WithRetainedPause(), WithDeferredRootfsExport()},
		},
		{
			name:    "retained pause rejects a filesystem-only snapshot",
			opts:    []PauseOption{WithRetainedPause(), WithFilesystemSnapshot()},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o, err := newPauseOptions(tt.opts...)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantDeferRootfs, o.deferRootfsExport)
			require.Equal(t, tt.wantDeferMemory, o.mayDeferMemoryExport())
			require.True(t, o.maintainSandbox)
		})
	}
}

// TestBeginEndCheckpoint pins the guard that keeps two Checkpoint RPCs
// from racing Pause/CreateSnapshot/ResumeInPlace on one FC process: Begin is a
// CAS that admits exactly one in-flight checkpoint until End releases it.
func TestBeginEndCheckpoint(t *testing.T) {
	t.Parallel()

	s := &Sandbox{}
	require.True(t, s.BeginCheckpoint(), "first Begin must win")
	require.False(t, s.BeginCheckpoint(), "second Begin must be refused while in flight")
	s.EndCheckpoint()
	require.True(t, s.BeginCheckpoint(), "Begin must win again after End")
}

// TestSetupInPlaceRootfsExport_AbortCleanupOrder is the in-place twin of
// TestSetupDeferredRootfsExport_AbortCleanupOrder: when Pause aborts before
// startSeal runs, the cleanup stack must (in order) settle the deferred diff's
// promise before its Close waits on it, FOLD the frozen cache back so the live
// writable cache is complete again (the sandbox keeps running after a failed
// in-place pause), resolve the seal signal SUCCESS, and free the sealing slot.
func TestSetupInPlaceRootfsExport_AbortCleanupOrder(t *testing.T) {
	t.Parallel()

	blockSize := int64(header.PageSize)
	numBlocks := int64(4)
	size := blockSize * numBlocks
	base := &inPlaceRODevice{data: make([]byte, size)}

	c0, err := block.NewCache(size, blockSize, t.TempDir()+"/c0", false)
	require.NoError(t, err)
	overlay := block.NewOverlay(base, c0)

	blockA := make([]byte, blockSize)
	for i := range blockA {
		blockA[i] = 0xAA
	}
	_, err = overlay.WriteAt(blockA, 0)
	require.NoError(t, err)

	originalHeader, err := header.NewHeader(
		header.NewTemplateMetadata(uuid.New(), uint64(blockSize), uint64(size)),
		nil,
	)
	require.NoError(t, err)

	fake := &fakeInPlaceRootfs{overlay: overlay, size: size, blockSize: blockSize, dir: t.TempDir()}
	s := &Sandbox{
		Metadata:  &Metadata{},
		Resources: &Resources{rootfs: fake},
		config:    cfg.BuilderConfig{DefaultCacheDir: t.TempDir()},
	}

	cleanup := NewCleanup()
	rootfsDiff, hdr, startSeal, err := s.setupInPlaceRootfsExport(t.Context(), uuid.New(), originalHeader, cleanup)
	require.NoError(t, err)
	require.NotNil(t, rootfsDiff)
	require.NotNil(t, hdr)
	require.NotNil(t, startSeal)

	// Do NOT call startSeal: simulate Pause aborting (e.g. ResumeInPlace failed).
	done := make(chan error, 1)
	go func() { done <- cleanup.Run(t.Context()) }()
	select {
	case runErr := <-done:
		require.NoError(t, runErr)
	case <-time.After(10 * time.Second):
		t.Fatal("cleanup deadlocked: deferred diff Close ran before the promise was poisoned")
	}

	// The aborted checkpoint's artifact fails without blocking...
	_, err = rootfsDiff.FileSize(t.Context())
	require.ErrorContains(t, err, "pause aborted before in-place rootfs export ran")

	// ...the fold-back made the writable cache complete again...
	buf := make([]byte, blockSize)
	_, err = overlay.ReadAt(t.Context(), buf, 0)
	require.NoError(t, err)
	require.Equal(t, blockA, buf, "pre-swap dirty block must survive the abort fold-back")

	// ...the seal signal resolved SUCCESS (a later pause/checkpoint proceeds)...
	require.NoError(t, s.waitForRootfsSeal(t.Context()))

	// ...and the sealing slot is free.
	_, err = fake.SwapForBackgroundSeal(t.Context())
	require.NoError(t, err, "sealing slot must be free after the abort fold-back")
	require.NoError(t, overlay.Close())
}

// TestSetupInPlaceRootfsExport_EmptyDirtySet pins the no-writes-between-
// checkpoints fast path: the swapped cache has no dirty blocks, so setup must
// fold it straight back, return NoDiff, resolve the seal signal, and leave the
// sealing slot free — if the immediate fold-back were wrong the slot would stay
// occupied and every later checkpoint would fail its swap.
func TestSetupInPlaceRootfsExport_EmptyDirtySet(t *testing.T) {
	t.Parallel()

	blockSize := int64(header.PageSize)
	numBlocks := int64(4)
	size := blockSize * numBlocks
	base := &inPlaceRODevice{data: make([]byte, size)}

	c0, err := block.NewCache(size, blockSize, t.TempDir()+"/c0", false)
	require.NoError(t, err)
	overlay := block.NewOverlay(base, c0)

	originalHeader, err := header.NewHeader(
		header.NewTemplateMetadata(uuid.New(), uint64(blockSize), uint64(size)),
		nil,
	)
	require.NoError(t, err)

	fake := &fakeInPlaceRootfs{overlay: overlay, size: size, blockSize: blockSize, dir: t.TempDir()}
	s := &Sandbox{
		Metadata:  &Metadata{},
		Resources: &Resources{rootfs: fake},
		config:    cfg.BuilderConfig{DefaultCacheDir: t.TempDir()},
	}

	cleanup := NewCleanup()
	rootfsDiff, hdr, startSeal, err := s.setupInPlaceRootfsExport(t.Context(), uuid.New(), originalHeader, cleanup)
	require.NoError(t, err)
	require.IsType(t, &build.NoDiff{}, rootfsDiff)
	require.NotNil(t, hdr)
	require.NotNil(t, startSeal, "the no-op startSeal must still be callable")
	startSeal(t.Context())

	// The seal signal resolved and the slot is free for the next checkpoint.
	require.NoError(t, s.waitForRootfsSeal(t.Context()))
	nextSeal, err := fake.SwapForBackgroundSeal(t.Context())
	require.NoError(t, err, "sealing slot must be free after the empty fold-back")
	require.NotNil(t, nextSeal)
	require.NoError(t, cleanup.Run(t.Context()))
	require.NoError(t, overlay.Close())
}

// TestFailSealSetup pins both halves of the setup-failure recovery contract.
// Fold-back succeeds: the cause passes through unchanged, nothing latches
// (later pauses proceed), and the slot frees. Fold-back fails: the returned
// error joins both causes and — crucially — the failure latches into
// rootfsSealDone tagged ErrDeferredSealFailed, because at that point the
// writable cache is genuinely missing blocks and a destroy-path export that
// could not see the latch would be silently incomplete.
func TestFailSealSetup(t *testing.T) {
	t.Parallel()

	blockSize := int64(header.PageSize)
	numBlocks := int64(4)
	size := blockSize * numBlocks

	newSwappedOverlay := func(t *testing.T) (*block.Overlay, *fakeInPlaceRootfs, *block.Cache, []byte) {
		t.Helper()
		base := &inPlaceRODevice{data: make([]byte, size)}
		c0, err := block.NewCache(size, blockSize, t.TempDir()+"/c0", false)
		require.NoError(t, err)
		overlay := block.NewOverlay(base, c0)

		blockA := make([]byte, blockSize)
		for i := range blockA {
			blockA[i] = 0xAA
		}
		_, err = overlay.WriteAt(blockA, 0)
		require.NoError(t, err)

		fake := &fakeInPlaceRootfs{overlay: overlay, size: size, blockSize: blockSize, dir: t.TempDir()}
		sealCache, err := fake.SwapForBackgroundSeal(t.Context())
		require.NoError(t, err)

		return overlay, fake, sealCache, blockA
	}

	t.Run("fold-back succeeds", func(t *testing.T) {
		t.Parallel()

		overlay, fake, sealCache, blockA := newSwappedOverlay(t)
		s := &Sandbox{Resources: &Resources{rootfs: fake}}

		cause := errors.New("header build failed")
		err := s.failSealSetup(t.Context(), sealCache, cause)
		require.ErrorIs(t, err, cause)

		// Nothing latched: a later pause of this still-running sandbox proceeds.
		require.NoError(t, s.waitForRootfsSeal(t.Context()))
		require.NoError(t, s.EnsurePausable())

		// The writable cache is complete again and the slot is free.
		buf := make([]byte, blockSize)
		_, err = overlay.ReadAt(t.Context(), buf, 0)
		require.NoError(t, err)
		require.Equal(t, blockA, buf)
		_, err = fake.SwapForBackgroundSeal(t.Context())
		require.NoError(t, err)
		require.NoError(t, overlay.Close())
	})

	t.Run("fold-back fails", func(t *testing.T) {
		t.Parallel()

		overlay, fake, sealCache, _ := newSwappedOverlay(t)
		s := &Sandbox{Resources: &Resources{rootfs: fake}}

		// Force the fold-back to fail: FillMissingFrom writes into the live
		// writable cache, so ejecting+closing it first makes the fold error.
		ejected, err := overlay.EjectCache()
		require.NoError(t, err)
		require.NoError(t, ejected.Close())

		cause := errors.New("header build failed")
		err = s.failSealSetup(t.Context(), sealCache, cause)
		require.ErrorIs(t, err, cause)

		// The unrecoverable state latched, tagged as the permanent seal failure:
		// both the pause wait and the pre-arm guard now refuse.
		latchErr := s.waitForRootfsSeal(t.Context())
		require.Error(t, latchErr)
		require.ErrorIs(t, latchErr, build.ErrDeferredSealFailed)
		require.Error(t, s.EnsurePausable())
	})
}
