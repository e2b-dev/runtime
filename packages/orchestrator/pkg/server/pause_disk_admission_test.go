//go:build linux

package server

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	testMiB = int64(1) << 20
	testGiB = int64(1) << 30
)

func diskAdmissionFlags(t *testing.T, headroomMiB int, more ...func(*ldtestdata.TestDataSource)) *featureflags.Client {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.PauseAdmissionDiskHeadroomMiB.Key()).ValueForAll(ldvalue.Int(headroomMiB)))
	for _, m := range more {
		m(td)
	}

	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return ff
}

// diskAdmissionTestServer is admissionTestServer with the dedup pre-flight off,
// the disk headroom set, the build filesystem's free space fixed, and template
// storage elsewhere.
func diskAdmissionTestServer(t *testing.T, headroomMiB int, available int64) *Server {
	t.Helper()

	s := admissionTestServer(t, nil)
	s.featureFlags = diskAdmissionFlags(t, headroomMiB)
	s.buildDisk = &buildDisk{
		available:     func() (int64, error) { return available, nil },
		storageShares: func() bool { return false },
	}

	return s
}

// diskAdmissionSandbox is admissionTestSandbox with a guest memory size; it has
// no rootfs, so the estimate is memory plus the snapfile allowance.
func diskAdmissionSandbox(t *testing.T, sandboxID string, slotIdx int, ramMB int64) *sandbox.Sandbox {
	t.Helper()

	slot, err := network.NewSlot("test", slotIdx, network.Config{}, network.NoopEgressProxy{})
	require.NoError(t, err)

	return &sandbox.Sandbox{
		LifecycleID: "lifecycle-1",
		Metadata: &sandbox.Metadata{
			Config: sandbox.NewConfig(sandbox.Config{
				RamMB:             ramMB,
				Envd:              sandbox.EnvdMetadata{Version: "9.9.9"},
				FirecrackerConfig: fc.Config{FirecrackerVersion: "v1.14.1", KernelVersion: "vmlinux-6.1"},
			}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: sandboxID},
		},
		Resources: &sandbox.Resources{Slot: slot},
		Template:  admissionTestTemplate{memfile: &admissionRODevice{durable: utils.NewSetOnce[*header.Header](), waiting: make(chan struct{})}},
	}
}

// A capture the build filesystem cannot hold is refused with ResourceExhausted
// BEFORE MarkStopping: the sandbox stays live, unmarked, with no stop reason,
// and nothing is reserved for it.
func TestPause_DiskAdmissionRefusesBeforeMarkStopping(t *testing.T) {
	t.Parallel()

	s := diskAdmissionTestServer(t, 1024, 1*testGiB)
	sbx := diskAdmissionSandbox(t, "sbx-disk-refuse", 21, 2048)
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	_, pauseErr := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: "sbx-disk-refuse"})

	st, ok := status.FromError(pauseErr)
	require.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, st.Code())
	assert.Contains(t, st.Message(), "disk")

	_, live := s.sandboxFactory.Sandboxes.Get("sbx-disk-refuse")
	assert.True(t, live, "a refused pause must leave the sandbox live")
	assert.Equal(t, sandbox.StopReasonCrashed, sbx.GetStopReason(), "no stop reason may be set by a refusal")
	assert.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), "sbx-disk-refuse", "lifecycle-1"), "a refused pause must leave the sandbox unmarked")
	assert.Zero(t, s.buildDisk.reserved.Load(), "a refused pause reserves nothing")
}

// A capture that fits is admitted, proceeds into the destructive path, and
// holds its estimate reserved until its upload ends (the fake template parks
// the pause before that, so the reservation is still held here).
func TestPause_DiskAdmissionAdmitsAndReservesTheCapture(t *testing.T) {
	t.Parallel()

	s := diskAdmissionTestServer(t, 1024, 10*testGiB)
	sbx := diskAdmissionSandbox(t, "sbx-disk-admit", 22, 1024)
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	go func() {
		_, _ = s.Pause(context.WithoutCancel(t.Context()), &orchestrator.SandboxPauseRequest{SandboxId: "sbx-disk-admit"})
	}()

	require.Eventually(t, func() bool {
		_, live := s.sandboxFactory.Sandboxes.Get("sbx-disk-admit")

		return !live
	}, 5*time.Second, 5*time.Millisecond, "an admitted pause must proceed to MarkStopping")
	assert.Equal(t, 1*testGiB+pauseSnapfileAllowance, s.buildDisk.reserved.Load(), "the admitted pause holds its estimate while the capture is being written")
}

// Pauses already admitted count against the free space the next one sees;
// releasing a reservation more than once releases it once.
func TestAdmitPauseDisk_CountsAdmittedPausesAndReleasesOnce(t *testing.T) {
	t.Parallel()

	s := diskAdmissionTestServer(t, 0, 4*testGiB)
	first := diskAdmissionSandbox(t, "sbx-disk-first", 23, 2048)
	second := diskAdmissionSandbox(t, "sbx-disk-second", 24, 2048)

	reservation, err := s.admitPauseDisk(t.Context(), first, false)
	require.NoError(t, err)
	assert.Equal(t, 2*testGiB+pauseSnapfileAllowance, s.buildDisk.reserved.Load())

	_, err = s.admitPauseDisk(t.Context(), second, false)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "the first pause's reservation counts against the second")

	reservation.Release()
	reservation.Release()
	assert.Zero(t, s.buildDisk.reserved.Load(), "a reservation is released once")

	secondReservation, err := s.admitPauseDisk(t.Context(), second, false)
	require.NoError(t, err, "with the first reservation gone the second fits")
	secondReservation.Release()
}

func TestAdmitPauseDisk_NegativeHeadroomDisablesTheCheck(t *testing.T) {
	t.Parallel()

	s := diskAdmissionTestServer(t, -1, 1)
	sbx := diskAdmissionSandbox(t, "sbx-disk-off", 25, 2048)

	reservation, err := s.admitPauseDisk(t.Context(), sbx, false)
	require.NoError(t, err)
	assert.Zero(t, s.buildDisk.reserved.Load(), "nothing is reserved when the check is off")
	reservation.Release()
}

// A filesystem-only pause writes no memory diff, so the guest's memory is not
// part of its estimate.
func TestAdmitPauseDisk_FilesystemOnlyExcludesTheMemory(t *testing.T) {
	t.Parallel()

	s := diskAdmissionTestServer(t, 0, 1*testGiB)
	sbx := diskAdmissionSandbox(t, "sbx-disk-fsonly", 26, 2048)

	reservation, err := s.admitPauseDisk(t.Context(), sbx, true)
	require.NoError(t, err)
	assert.Equal(t, pauseSnapfileAllowance, s.buildDisk.reserved.Load())
	reservation.Release()
}

// Whether template storage shares the build filesystem is asked at every
// admission: a local provider's directory appears with its first object, so a
// deployment that started before any upload is not stuck at "not sharing".
func TestAdmitPauseDisk_AsksWhetherStorageSharesTheDiskEachTime(t *testing.T) {
	t.Parallel()

	s := diskAdmissionTestServer(t, 0, 10*testGiB)
	sbx := diskAdmissionSandbox(t, "sbx-disk-shared-later", 34, 1024)

	var shared atomic.Bool
	s.buildDisk.storageShares = shared.Load

	first, err := s.admitPauseDisk(t.Context(), sbx, false)
	require.NoError(t, err)
	assert.Equal(t, 1*testGiB+pauseSnapfileAllowance, s.buildDisk.reserved.Load(), "the directory is not there yet: one copy")
	first.Release()

	shared.Store(true)
	second, err := s.admitPauseDisk(t.Context(), sbx, false)
	require.NoError(t, err)
	assert.Equal(t, 2*(1*testGiB+pauseSnapfileAllowance), s.buildDisk.reserved.Load(), "the directory appeared on the build filesystem: two copies")
	second.Release()
}

// With template storage on the build filesystem the upload writes a second
// copy of everything, so the estimate doubles.
func TestAdmitPauseDisk_DoublesWhenStorageSharesTheBuildDisk(t *testing.T) {
	t.Parallel()

	s := diskAdmissionTestServer(t, 0, 2*testGiB)
	sbx := diskAdmissionSandbox(t, "sbx-disk-shared", 27, 1024)

	s.buildDisk.storageShares = func() bool { return true }
	_, err := s.admitPauseDisk(t.Context(), sbx, false)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "two copies of 1 GiB plus the snapfile do not fit in 2 GiB")

	s.buildDisk.storageShares = func() bool { return false }
	reservation, err := s.admitPauseDisk(t.Context(), sbx, false)
	require.NoError(t, err, "one copy fits")
	reservation.Release()
}

// The check protects against a full disk; a filesystem it cannot read is not a
// reason to refuse, so the pause runs as it did before the check existed.
func TestAdmitPauseDisk_UnreadableFreeSpaceAdmits(t *testing.T) {
	t.Parallel()

	s := diskAdmissionTestServer(t, 0, 0)
	s.buildDisk.available = func() (int64, error) { return 0, errors.New("statfs: permission denied") }
	sbx := diskAdmissionSandbox(t, "sbx-disk-unreadable", 28, 2048)

	reservation, err := s.admitPauseDisk(t.Context(), sbx, false)
	require.NoError(t, err)
	reservation.Release()
}

func TestStorageSharesDisk(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	assert.True(t, storageSharesDisk(storage.Spec{Provider: storage.LocalStorageProvider, BasePath: t.TempDir()}, dir), "two directories on one filesystem share it")
	assert.False(t, storageSharesDisk(storage.Spec{Provider: storage.GCPStorageProvider, Bucket: "snapshots"}, dir), "a bucket is not on this disk")
	assert.True(t, storageSharesDisk(storage.Spec{Provider: storage.LocalStorageProvider, BasePath: filepath.Join(dir, "not", "yet", "created")}, dir), "a directory the provider has not created yet lands on its nearest existing ancestor's filesystem")
	assert.False(t, storageSharesDisk(storage.Spec{Provider: storage.LocalStorageProvider, BasePath: ""}, dir), "no path, no answer")
	assert.False(t, storageSharesDisk(storage.Spec{Provider: storage.LocalStorageProvider, BasePath: filepath.Join(dir, "missing")}, filepath.Join(dir, "missing-too")), "a build directory that cannot be read is not assumed to share")
}

// The admission asks once and uses the one answer for both the estimate and
// the second copy the reservation keeps for the upload, so the two cannot
// disagree when the answer changes between calls.
func TestAdmitPauseDisk_OneAnswerServesTheEstimateAndTheSecondCopy(t *testing.T) {
	t.Parallel()

	s := diskAdmissionTestServer(t, 0, 10*testGiB)
	sbx := diskAdmissionSandbox(t, "sbx-disk-one-answer", 35, 1024)

	var asked atomic.Int32
	s.buildDisk.storageShares = func() bool { return asked.Add(1) == 1 }

	reservation, err := s.admitPauseDisk(t.Context(), sbx, false)
	require.NoError(t, err)
	assert.Equal(t, int32(1), asked.Load(), "asked once per admission")
	need := 2 * (1*testGiB + pauseSnapfileAllowance)
	assert.Equal(t, need, s.buildDisk.reserved.Load())
	reservation.Written()
	assert.Equal(t, need/2, s.buildDisk.reserved.Load(), "the second copy follows the same answer as the estimate")
	reservation.Release()
	assert.Zero(t, s.buildDisk.reserved.Load())
}

func TestDiskAvailableBytes(t *testing.T) {
	t.Parallel()

	available, err := diskAvailableBytes(t.TempDir())
	require.NoError(t, err)
	assert.Positive(t, available)

	_, err = diskAvailableBytes(filepath.Join(t.TempDir(), "missing"))
	require.Error(t, err)
}

// Once the capture is on disk statfs counts it, so only the upload's second
// copy, when template storage shares the filesystem, stays reserved; the
// upload's end drops that too.
func TestPauseDiskReservation_WrittenKeepsOnlyTheUploadsCopy(t *testing.T) {
	t.Parallel()

	s := diskAdmissionTestServer(t, 0, 10*testGiB)
	sbx := diskAdmissionSandbox(t, "sbx-disk-written", 29, 1024)

	s.buildDisk.storageShares = func() bool { return true }
	shared, err := s.admitPauseDisk(t.Context(), sbx, false)
	require.NoError(t, err)
	need := 2 * (1*testGiB + pauseSnapfileAllowance)
	assert.Equal(t, need, s.buildDisk.reserved.Load())
	shared.Written()
	shared.Written()
	assert.Equal(t, need/2, s.buildDisk.reserved.Load(), "the upload's copy stays reserved once the capture is on disk")
	shared.Release()
	shared.Release()
	assert.Zero(t, s.buildDisk.reserved.Load())

	s.buildDisk.storageShares = func() bool { return false }
	remote, err := s.admitPauseDisk(t.Context(), sbx, false)
	require.NoError(t, err)
	remote.Written()
	assert.Zero(t, s.buildDisk.reserved.Load(), "with remote storage nothing stays reserved once the capture is on disk")
	remote.Release()
	assert.Zero(t, s.buildDisk.reserved.Load())
}

// The capture is written while the request runs, so the pause handler marks
// the reservation written as soon as the diffs resolve.
func TestReleasePauseDiskWhenWritten_WaitsForBothDiffs(t *testing.T) {
	t.Parallel()

	s := diskAdmissionTestServer(t, 0, 10*testGiB)
	sbx := diskAdmissionSandbox(t, "sbx-disk-seal", 30, 1024)
	reservation, err := s.admitPauseDisk(t.Context(), sbx, false)
	require.NoError(t, err)
	need := 1*testGiB + pauseSnapfileAllowance

	sealed := make(chan struct{})
	copied := make(chan struct{})
	res := &snapshotResult{
		memoryExportDeferred: true,
		waitMemorySealed: func(ctx context.Context) error {
			select {
			case <-sealed:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		rootfsDiff: &build.NoDiff{},
		memoryDiff: &pathWhenReadyDiff{ready: copied},
	}
	s.releasePauseDiskWhenWritten(t.Context(), res, reservation)
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, need, s.buildDisk.reserved.Load(), "the reservation holds while the memory diff is still sealing")

	close(sealed)
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, need, s.buildDisk.reserved.Load(), "the reservation holds while the memory diff is still being copied")

	close(copied)
	require.Eventually(t, func() bool { return s.buildDisk.reserved.Load() == 0 }, 2*time.Second, time.Millisecond, "the reservation goes once both diffs are on disk")
	reservation.Release()
	assert.Zero(t, s.buildDisk.reserved.Load())
}

// pathWhenReadyDiff resolves its path once ready closes, as the memfd
// background copy does when it has finished streaming.
type pathWhenReadyDiff struct {
	build.Diff

	ready chan struct{}
}

func (d *pathWhenReadyDiff) CachePath(ctx context.Context) (string, error) {
	select {
	case <-d.ready:
		return "memfile", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// Without a memfd the export holds the raw memory diff and its dedup output on
// disk at the same time, so the estimate counts the memory twice.
func TestAdmitPauseDisk_WithoutMemfdAndWithDedupCountsTheIntermediateCopy(t *testing.T) {
	t.Parallel()

	s := diskAdmissionTestServer(t, 0, 10*testGiB)
	s.featureFlags = diskAdmissionFlags(t, 0, func(td *ldtestdata.TestDataSource) {
		td.Update(td.Flag(featureflags.UseMemFdFlag.Key()).ValueForAll(ldvalue.Bool(false)))
		td.Update(td.Flag(featureflags.MemfileDiffDedupFlag.Key()).ValueForAll(ldvalue.FromJSONMarshal(map[string]any{"enabled": true})))
	})
	sbx := diskAdmissionSandbox(t, "sbx-disk-dedup", 31, 1024)

	reservation, err := s.admitPauseDisk(t.Context(), sbx, false)
	require.NoError(t, err)
	assert.Equal(t, 2*testGiB+pauseSnapfileAllowance, s.buildDisk.reserved.Load())
	reservation.Release()
}

// A capture landing on disk between an admission's free-space sample and its
// reservation would let the admission use a sample that no longer describes
// the disk; sampling and reserving happen under one lock.
func TestAdmitPauseDisk_NoReleaseLandsBetweenTheSampleAndTheReservation(t *testing.T) {
	t.Parallel()

	s := diskAdmissionTestServer(t, 0, 10*testGiB)
	first := diskAdmissionSandbox(t, "sbx-disk-order-first", 32, 1024)
	held, err := s.admitPauseDisk(t.Context(), first, false)
	require.NoError(t, err)

	sampling := make(chan struct{})
	proceed := make(chan struct{})
	var once sync.Once
	s.buildDisk.available = func() (int64, error) {
		once.Do(func() { close(sampling) })
		<-proceed

		return 10 * testGiB, nil
	}

	second := diskAdmissionSandbox(t, "sbx-disk-order-second", 33, 1024)
	admitted := make(chan struct{})
	go func() {
		defer close(admitted)
		r, err := s.admitPauseDisk(t.Context(), second, false)
		assert.NoError(t, err)
		r.Release()
	}()
	<-sampling

	written := make(chan struct{})
	go func() {
		held.Written()
		close(written)
	}()
	select {
	case <-written:
		t.Fatal("a capture landing on disk must wait for the admission in progress")
	case <-time.After(50 * time.Millisecond):
	}

	close(proceed)
	<-admitted
	<-written
	held.Release()
	assert.Zero(t, s.buildDisk.reserved.Load())
}
