//go:build linux

package fsfreeze

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const (
	// backingFileSize is comfortably above ext4's minimum. The image is sparse,
	// so only the metadata mkfs writes is ever allocated.
	backingFileSize = 64 << 20

	// frozenWriteTimeout is how long a write is given to prove it is blocked. A
	// write to a frozen filesystem blocks until thaw, so any completion at all
	// means the freeze did not take; the wait only has to outlast the scheduling
	// of an unblocked write.
	frozenWriteTimeout = 2 * time.Second

	// thawedWriteTimeout bounds the wait for that same write once thawed. It is
	// generous because the write lands on a loop device under whatever else CI
	// is doing.
	thawedWriteTimeout = 30 * time.Second

	cleanupTimeout = 30 * time.Second
)

// scratchExt4 mounts a fresh ext4 filesystem on a loop device and returns its
// mountpoint.
//
// FIFREEZE needs a filesystem that implements freezing: tmpfs has no freeze
// hooks and the ioctl answers EOPNOTSUPP, so t.TempDir() will not do. Freezing
// the machine's own filesystems is obviously not an option either, which leaves
// a throwaway block-backed mount as the only way to exercise the real ioctls.
func scratchExt4(t *testing.T) string {
	t.Helper()

	// mount(2) needs CAP_SYS_ADMIN, and so does FIFREEZE itself.
	if os.Geteuid() != 0 {
		t.Skip("requires root (mount(2) and FIFREEZE both need CAP_SYS_ADMIN)")
	}

	for _, tool := range []string{"losetup", "mkfs.ext4"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("requires %s to build a scratch ext4 filesystem: %v", tool, err)
		}
	}

	// Probing the loop control device separates "this machine has no loop driver"
	// from a real failure in the setup below, which stays fatal. Opening it is
	// also what triggers the module autoload, so this is the check and the fix.
	control, err := os.Open("/dev/loop-control")
	if err != nil {
		t.Skipf("requires loop devices to build a scratch ext4 filesystem: %v", err)
	}

	require.NoError(t, control.Close())

	dir := t.TempDir()
	image := filepath.Join(dir, "scratch.img")

	require.NoError(t, os.WriteFile(image, nil, 0o600))
	require.NoError(t, os.Truncate(image, backingFileSize))

	loopDevice := strings.TrimSpace(run(t, t.Context(), "losetup", "--find", "--show", image))
	require.NotEmpty(t, loopDevice, "losetup should report the device it attached")

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), cleanupTimeout)
		defer cancel()

		if out, err := exec.CommandContext(ctx, "losetup", "--detach", loopDevice).CombinedOutput(); err != nil {
			t.Logf("detaching %s failed: %v: %s", loopDevice, err, out)
		}
	})

	run(t, t.Context(), "mkfs.ext4", "-q", loopDevice)

	mountpoint := filepath.Join(dir, "mnt")
	require.NoError(t, os.Mkdir(mountpoint, 0o755))
	require.NoError(t, unix.Mount(loopDevice, mountpoint, "ext4", 0, ""))

	t.Cleanup(func() {
		// A frozen filesystem has to be thawed before it can be unmounted, and a
		// failed assertion can leave it frozen. Thawing an unfrozen filesystem is
		// a no-op, so this is unconditional.
		if err := New().Thaw(mountpoint); err != nil {
			t.Logf("thawing %s before unmount failed: %v", mountpoint, err)
		}

		if err := unix.Unmount(mountpoint, 0); err != nil {
			// Something still holds the mount. Detach it so the loop device can
			// be released and the temp dir removed.
			t.Logf("unmounting %s failed (%v); detaching lazily", mountpoint, err)

			if err := unix.Unmount(mountpoint, unix.MNT_DETACH); err != nil {
				t.Logf("lazily detaching %s failed: %v", mountpoint, err)
			}
		}
	})

	return mountpoint
}

func run(t *testing.T, ctx context.Context, name string, args ...string) string {
	t.Helper()

	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	require.NoErrorf(t, err, "%s %s: %s", name, strings.Join(args, " "), out)

	return string(out)
}

// TestFreezeBlocksWritesAndThawReleasesThem exercises the real FIFREEZE/FITHAW
// ioctls against a live filesystem, which is the claim the filesystem-only pause
// rests on: freezing does not merely flush the filesystem, it stops writes, so no
// write can be acknowledged between the flush and the VM pause. The handler tests
// in internal/api run against a fake freezer and cannot say any of this.
//
// It also pins the idempotency both callers rely on — the orchestrator freezes
// before every filesystem-only pause and thaws on the rollback path without
// knowing the current state — against the kernel rather than against a fake: a
// second freeze really does return EBUSY and a second thaw really does return
// EINVAL, and Freeze/Thaw swallow exactly those.
func TestFreezeBlocksWritesAndThawReleasesThem(t *testing.T) {
	t.Parallel()

	mountpoint := scratchExt4(t)
	freezer := New()

	// Written and synced before the freeze so reading it back afterwards cannot
	// depend on writeback, which the freeze suspends.
	readable := filepath.Join(mountpoint, "readable")
	payload := []byte("readable while frozen")
	require.NoError(t, os.WriteFile(readable, payload, 0o644))
	unix.Sync()

	require.NoError(t, freezer.Freeze(mountpoint), "freezing a fresh ext4 mount should succeed")

	// A write to a frozen filesystem blocks in the kernel and cannot be
	// interrupted, so it is left running and collected after the thaw.
	blocked := make(chan error, 1)
	written := filepath.Join(mountpoint, "written-while-frozen")

	go func() {
		// Closing after the send lets the cleanup below wait on the same channel
		// whether or not the test body already collected the result.
		defer close(blocked)

		blocked <- os.WriteFile(written, []byte("released by thaw"), 0o644)
	}()

	t.Cleanup(func() {
		// Whatever went wrong above, release the writer and wait for it, so the
		// unmount in scratchExt4's cleanup does not race a write in flight.
		if err := freezer.Thaw(mountpoint); err != nil {
			t.Logf("thawing %s during cleanup failed: %v", mountpoint, err)
		}

		select {
		case <-blocked:
		case <-time.After(cleanupTimeout):
			assert.Fail(t, "the write blocked by the freeze never completed after the thaw")
		}
	})

	select {
	case err := <-blocked:
		require.Failf(t, "a write to the frozen filesystem completed; the freeze did not take", "write returned %v", err)
	case <-time.After(frozenWriteTimeout):
	}

	// Reads are unaffected: freezing is a write barrier, and envd has to stay
	// responsive while frozen — it is what serves the thaw.
	got, err := os.ReadFile(readable)
	require.NoError(t, err, "reads should still work while the filesystem is frozen")
	assert.Equal(t, payload, got)

	// The kernel answers EBUSY; Freeze reports success.
	assert.NoError(t, freezer.Freeze(mountpoint), "freezing an already-frozen filesystem should be a no-op")

	require.NoError(t, freezer.Thaw(mountpoint), "thawing a frozen filesystem should succeed")

	select {
	case err := <-blocked:
		require.NoError(t, err, "the write blocked by the freeze should succeed once thawed")
	case <-time.After(thawedWriteTimeout):
		require.Fail(t, "the write blocked by the freeze did not complete after the thaw")
	}

	// The kernel answers EINVAL; Thaw reports success.
	assert.NoError(t, freezer.Thaw(mountpoint), "thawing an unfrozen filesystem should be a no-op")

	content, err := os.ReadFile(written)
	require.NoError(t, err)
	assert.Equal(t, []byte("released by thaw"), content, "the released write should have landed")
}
