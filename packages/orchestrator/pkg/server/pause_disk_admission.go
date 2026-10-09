//go:build linux

package server

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// pauseSnapfileAllowance bounds what a pause writes besides the two diffs: the
// Firecracker vmstate, a few MiB, and the metadata and header files, KiB.
const pauseSnapfileAllowance = int64(64) << 20

// diskAvailableBytes is what an unprivileged writer can still put on the
// filesystem holding path.
func diskAvailableBytes(path string) (int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", path, err)
	}

	return int64(st.Bavail) * st.Bsize, nil
}

// storageSharesDisk reports whether template storage is a directory on the
// filesystem that holds dir, in which case a snapshot's upload writes a second
// copy of it there. A local provider creates its directory with its first
// object, so a directory not there yet is judged by the nearest ancestor that
// is, where its objects will land. A path that cannot be read otherwise is
// not assumed to share, which is why it is asked again at every admission
// (buildDisk.storageShares): a mount can appear later.
func storageSharesDisk(spec storage.Spec, dir string) bool {
	if spec.Provider != storage.LocalStorageProvider {
		return false
	}

	storageStat, ok := statNearestExisting(spec.BasePath)
	if !ok {
		return false
	}
	var dirStat unix.Stat_t
	if err := unix.Stat(dir, &dirStat); err != nil {
		return false
	}

	return storageStat.Dev == dirStat.Dev
}

// statNearestExisting stats path or, while it does not exist, its parent, up
// to the root; any other failure, or an empty path, is reported as none.
func statNearestExisting(path string) (unix.Stat_t, bool) {
	var st unix.Stat_t
	if path == "" {
		return st, false
	}
	for p := filepath.Clean(path); ; p = filepath.Dir(p) {
		err := unix.Stat(p, &st)
		if err == nil {
			return st, true
		}
		if !errors.Is(err, unix.ENOENT) || p == filepath.Dir(p) {
			return st, false
		}
	}
}

// pauseDiskNeed bounds the bytes a pause writes under the build directory. The
// memory diff cannot exceed the guest's memory, and dirty pages are only
// countable once the guest is paused, which is too late to refuse; without a
// memfd the export writes the raw diff and dedups it into a second file before
// closing the first, so both exist at once. The rootfs diff is what the
// writable cache occupies on disk, which the export copies unless the
// filesystem reflinks. The snapfile and the small files are a flat allowance;
// they go under the template cache directory, which every shipped layout
// keeps on the build directory's filesystem (both under ORCHESTRATOR_BASE_PATH).
// With template storage on the same filesystem (shared) the upload writes a
// second copy of everything.
func (s *Server) pauseDiskNeed(ctx context.Context, sbx *sandbox.Sandbox, filesystemOnly, shared bool) (int64, error) {
	need := pauseSnapfileAllowance
	if !filesystemOnly {
		ram := sbx.Config.RamMB << 20
		need += ram
		if !s.memfdExport(ctx, sbx) && s.memfileDedupEnabled(ctx) {
			need += ram
		}
	}

	rootfs, err := sbx.RootfsCacheSize(ctx)
	if err != nil {
		return 0, fmt.Errorf("rootfs cache size: %w", err)
	}
	need += rootfs

	if shared {
		need *= 2
	}

	return need, nil
}

// memfdExport reports whether the sandbox's memory is exported from a memfd
// (fc.ExportMemory): one file, streamed straight into the build directory.
func (s *Server) memfdExport(ctx context.Context, sbx *sandbox.Sandbox) bool {
	return fc.FCSupportsMemfd(sbx.Config.FirecrackerConfig.FirecrackerVersion) &&
		s.featureFlags.BoolFlag(ctx, featureflags.UseMemFdFlag)
}

func (s *Server) memfileDedupEnabled(ctx context.Context) bool {
	return s.featureFlags.JSONFlag(ctx, featureflags.MemfileDiffDedupFlag).AsValueMap().Get("enabled").BoolValue()
}

// buildDisk is the filesystem holding the build directory as the pause disk
// admission (admitPauseDisk) sees it: what the pauses admitted on this node
// and not yet on disk are estimated to write, and the two things it asks the
// filesystem.
type buildDisk struct {
	// reserved is the sum of those estimates; mu orders a free-space sample
	// with the changes to it, so none lands between an admission's sample and
	// its reservation.
	reserved atomic.Int64
	mu       sync.Mutex

	// available reads the filesystem's free bytes. newBuildDisk sets statfs on
	// the build directory; tests fix a value.
	available func() (int64, error)
	// storageShares says whether template storage is a directory on this
	// filesystem, so an upload writes a second copy of the snapshot here.
	// Asked at every admission, not once at start: a local provider creates
	// its directory with the first object, so at start there may be nothing
	// to look at yet. newBuildDisk sets the stat; tests fix an answer.
	storageShares func() bool
}

// newBuildDisk asks the filesystem holding dir, with template storage
// compared against it.
func newBuildDisk(dir string, templateStorage storage.Spec) *buildDisk {
	return &buildDisk{
		available:     func() (int64, error) { return diskAvailableBytes(dir) },
		storageShares: func() bool { return storageSharesDisk(templateStorage, dir) },
	}
}

// buildDiskShortError is a reservation refused, with what the admission saw.
type buildDiskShortError struct {
	need, available, reserved, headroom int64
}

func (e *buildDiskShortError) Error() string {
	return fmt.Sprintf("build filesystem short of disk: %d MiB needed, %d MiB free, %d MiB reserved, %d MiB headroom",
		e.need>>20, e.available>>20, e.reserved>>20, e.headroom>>20)
}

// reserve holds need bytes against the filesystem's free space, less what is
// already reserved, when that leaves headroom free; secondCopy of them stay
// held until the upload ends. The sample and the reservation happen under mu:
// one admission at a time, and no reservation change (a capture landing on
// disk, an upload ending) in between. A refusal is a *buildDiskShortError; a
// filesystem whose free space cannot be read is that read's error.
func (d *buildDisk) reserve(need, secondCopy, headroom int64) (*pauseDiskReservation, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	available, err := d.available()
	if err != nil {
		return nil, err
	}

	reserved := d.reserved.Load()
	if available-reserved-need < headroom {
		return nil, &buildDiskShortError{need: need, available: available, reserved: reserved, headroom: headroom}
	}
	d.reserved.Add(need)

	return &pauseDiskReservation{disk: d, remaining: need, secondCopy: secondCopy}, nil
}

// pauseDiskReservation is the estimate an admitted pause holds against the
// build filesystem's free space. It covers bytes statfs cannot see yet: once
// the capture is on disk statfs counts those itself, so Written shrinks the
// reservation to the upload's second copy (there is one when template storage
// shares the filesystem) and Release drops what is left when the upload ends.
// Either may run more than once; a nil reservation is one the check did not
// take. Changes take the disk's mu, so none lands between an admission's
// free-space sample and its reservation.
type pauseDiskReservation struct {
	disk *buildDisk

	remaining  int64
	secondCopy int64
}

// Written says the memory and rootfs diffs are on disk.
func (r *pauseDiskReservation) Written() {
	if r == nil {
		return
	}

	r.disk.mu.Lock()
	defer r.disk.mu.Unlock()

	if r.remaining > r.secondCopy {
		r.disk.reserved.Add(r.secondCopy - r.remaining)
		r.remaining = r.secondCopy
	}
}

// Release says the upload has ended, whichever way.
func (r *pauseDiskReservation) Release() {
	if r == nil {
		return
	}

	r.disk.mu.Lock()
	defer r.disk.mu.Unlock()

	r.disk.reserved.Add(-r.remaining)
	r.remaining = 0
}

// admitPauseDisk refuses a pause, retryably and before anything destructive,
// when the build filesystem cannot hold its capture with the configured
// headroom left free, counting the captures already admitted and not yet on
// disk. An admitted pause's estimate is reserved until Written and Release on
// the returned reservation let it go. A negative headroom turns the check off;
// a filesystem whose free space cannot be read admits the pause, as before the
// check existed. Both return a nil reservation.
func (s *Server) admitPauseDisk(ctx context.Context, sbx *sandbox.Sandbox, filesystemOnly bool) (*pauseDiskReservation, error) {
	headroomMiB := s.featureFlags.IntFlag(ctx, featureflags.PauseAdmissionDiskHeadroomMiB)
	if headroomMiB < 0 {
		return nil, nil
	}

	// One answer serves the estimate and the reservation's second copy.
	shared := s.buildDisk.storageShares()
	need, err := s.pauseDiskNeed(ctx, sbx, filesystemOnly, shared)
	if err != nil {
		sbxlogger.I(sbx).Warn(ctx, "Cannot estimate the snapshot's size, admitting the pause", zap.Error(err))

		return nil, nil
	}
	var secondCopy int64
	if shared {
		secondCopy = need / 2
	}

	reservation, err := s.buildDisk.reserve(need, secondCopy, int64(headroomMiB)<<20)
	var short *buildDiskShortError
	switch {
	case errors.As(err, &short):
		s.recordPauseAdmission(ctx, "pause", sandbox.SnapshotAdmissionRefusedDisk, 0)
		// The sandbox's own log says only that the pause was refused: the
		// numbers describe the host and the other captures on it, and belong
		// to the internal stream.
		sbxlogger.E(sbx).Warn(ctx, "Refusing pause: the node is short of disk for the snapshot, retry later")
		sbxlogger.I(sbx).Warn(ctx, "Refusing pause: the build filesystem cannot hold the snapshot",
			zap.Int64("need_bytes", short.need),
			zap.Int64("available_bytes", short.available),
			zap.Int64("reserved_bytes", short.reserved),
			zap.Int64("headroom_bytes", short.headroom),
		)

		return nil, status.Errorf(codes.ResourceExhausted, "node is short of disk for the snapshot of sandbox '%s' (%d MiB needed, %d MiB free), please retry",
			sbx.Runtime.SandboxID, short.need>>20, (short.available-short.reserved)>>20)
	case err != nil:
		sbxlogger.I(sbx).Warn(ctx, "Cannot read the build filesystem's free space, admitting the pause", zap.Error(err))

		return nil, nil
	}

	return reservation, nil
}

// releasePauseDiskWhenWritten marks the reservation written once the capture
// is on disk: after the deferred seals waitSnapshotSealed knows, and after the
// memory diff's path resolves, which with the memfd background copy is when
// the copy has finished streaming. A seal or copy that failed writes nothing
// more either. ctx must outlive the request.
func (s *Server) releasePauseDiskWhenWritten(ctx context.Context, res *snapshotResult, reservation *pauseDiskReservation) {
	if reservation == nil {
		return
	}

	releaseWork := s.info.TrackWork()
	go func() {
		defer releaseWork()

		_ = waitSnapshotSealed(ctx, res)
		if res.memoryDiff != nil {
			_, _ = res.memoryDiff.CachePath(ctx)
		}
		reservation.Written()
	}()
}
