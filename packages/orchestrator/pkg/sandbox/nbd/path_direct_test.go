//go:build linux

package nbd

import (
	"crypto/rand"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

func TestPathDirect_Direct4MBWrite(t *testing.T) {
	t.Parallel()

	featureFlags, err := featureflags.NewClient("", "", "")
	require.NoError(t, err)
	size := int64(10 * 1024 * 1024)

	deviceFile, _ := setupNBDDevice(t, featureFlags, size, unix.O_DIRECT|unix.O_RDWR)

	const bs = 4 * 1024 * 1024
	buf, err := unix.Mmap(-1, 0, bs, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_ANON)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		unix.Munmap(buf)
	})

	n, err := deviceFile.WriteAt(buf, 0)
	require.NoError(t, err, "failed to write to device")
	require.Equal(t, len(buf), n, "partial write")

	readData := make([]byte, bs)
	n, err = deviceFile.ReadAt(readData, 0)
	require.NoError(t, err, "failed to read from device")
	require.Equal(t, len(readData), n, "partial read")
	require.Equal(t, buf, readData, "data mismatch")
}

// We usually see the 32MB write be split into smaller writes, even on O_DIRECT.
func TestPathDirect_Direct32MBWrite(t *testing.T) {
	t.Parallel()

	featureFlags, err := featureflags.NewClient("", "", "")
	require.NoError(t, err)
	size := int64(256 * 1024 * 1024)

	deviceFile, _ := setupNBDDevice(t, featureFlags, size, unix.O_DIRECT|unix.O_RDWR)

	const bs = 32 * 1024 * 1024
	buf, err := unix.Mmap(-1, 0, bs, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_ANON)
	require.NoError(t, err, "failed to mmap")

	t.Cleanup(func() {
		unix.Munmap(buf)
	})

	n, err := deviceFile.WriteAt(buf, 0)
	require.NoError(t, err, "failed to write to device")
	require.Equal(t, len(buf), n, "partial write")

	readData := make([]byte, bs)
	n, err = deviceFile.ReadAt(readData, 0)
	require.NoError(t, err, "failed to read from device")
	require.Equal(t, len(readData), n, "partial read")
	require.Equal(t, buf, readData, "data mismatch")
}

func TestPathDirect_Write(t *testing.T) {
	t.Parallel()

	featureFlags, err := featureflags.NewClient("", "", "")
	require.NoError(t, err)

	size := int64(5 * 1024 * 1024)

	deviceFile, _ := setupNBDDevice(t, featureFlags, size, os.O_RDWR)

	const writeSize = 1024 * 1024
	testData := make([]byte, writeSize)
	_, err = rand.Read(testData)
	require.NoError(t, err, "failed to generate random data")

	n, err := deviceFile.WriteAt(testData, 0)
	require.NoError(t, err, "failed to write data to device")
	require.Equal(t, len(testData), n, "partial write")

	readData := make([]byte, writeSize)
	n, err = deviceFile.ReadAt(readData, 0)
	require.NoError(t, err, "failed to read data from device")
	require.Equal(t, len(readData), n, "partial read")
	require.Equal(t, testData, readData, "data mismatch")
}

func TestPathDirect_WriteAtOffset(t *testing.T) {
	t.Parallel()

	featureFlags, err := featureflags.NewClient("", "", "")
	require.NoError(t, err)
	size := int64(5 * 1024 * 1024)

	deviceFile, _ := setupNBDDevice(t, featureFlags, size, os.O_RDWR)

	const writeSize = 512 * 1024
	const writeOffset = 512 * 1024
	testData := make([]byte, writeSize)
	_, err = rand.Read(testData)
	require.NoError(t, err, "failed to generate random data")

	n, err := deviceFile.WriteAt(testData, writeOffset)
	require.NoError(t, err, "failed to write data to device")
	require.Equal(t, len(testData), n, "partial write")

	readData := make([]byte, writeSize)
	n, err = deviceFile.ReadAt(readData, writeOffset)
	require.NoError(t, err, "failed to read data from device")
	require.Equal(t, len(readData), n, "partial read")
	require.Equal(t, testData, readData, "data mismatch")
}

func TestPathDirect_LargeWrite(t *testing.T) {
	t.Parallel()

	featureFlags, err := featureflags.NewClient("", "", "")
	require.NoError(t, err)

	size := int64(1200 * 1024 * 1024)

	deviceFile, _ := setupNBDDevice(t, featureFlags, size, os.O_RDWR)

	time.Sleep(1 * time.Second)
	cmd := exec.CommandContext(t.Context(), "dd", "if=/dev/zero", "of="+deviceFile.Name(), "bs=1G", "count=1")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	require.NoError(t, err, "failed to execute dd command")
}

func TestPathLargeRead(t *testing.T) {
	t.Parallel()

	featureFlags, err := featureflags.NewClient("", "", "")
	require.NoError(t, err)

	size := int64(1200 * 1024 * 1024)

	deviceFile, _ := setupNBDDevice(t, featureFlags, size, os.O_RDONLY)
	time.Sleep(1 * time.Second)

	cmd := exec.CommandContext(t.Context(), "dd", "if="+deviceFile.Name(), "of=/dev/null", "bs=1G", "count=1")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	require.NoError(t, err, "failed to execute dd command")
}

// Verifies BLKDISCARD/BLKZEROOUT round-trip through the dispatcher to punchHole and shrink the cache.
func TestPathDirect_HolePunchOnDiscardAndZeroOut(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		op   uintptr
	}{
		{"BLKDISCARD", unix.BLKDISCARD},
		{"BLKZEROOUT", unix.BLKZEROOUT},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			featureFlags, err := featureflags.NewClient("", "", "")
			require.NoError(t, err)

			const size = 16 * 1024 * 1024
			const region = 4 * 1024 * 1024

			deviceFile, cachePath := setupNBDDevice(t, featureFlags, size, os.O_RDWR)

			data := make([]byte, region)
			_, err = rand.Read(data)
			require.NoError(t, err)
			_, err = deviceFile.WriteAt(data, 0)
			require.NoError(t, err)
			require.NoError(t, deviceFile.Sync())

			before := allocatedBytes(t, cachePath)
			require.Positive(t, before)

			require.NoError(t, blkRangeIoctl(deviceFile.Fd(), tc.op, 0, region))
			// Check allocation before read-back: tmpfs reads refill st_blocks for punched ranges.
			require.Less(t, allocatedBytes(t, cachePath), before, "%s must hole-punch backing cache", tc.name)

			readBack := make([]byte, region)
			_, err = deviceFile.ReadAt(readBack, 0)
			require.NoError(t, err)
			require.Equal(t, make([]byte, region), readBack, "discarded range must read as zeros")
		})
	}
}

// blkRangeIoctl issues a Linux block-device range ioctl with a uint64[2]{offset, length} arg.
func blkRangeIoctl(fd, op uintptr, off, length uint64) error {
	rng := [2]uint64{off, length}
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, fd, op, uintptr(unsafe.Pointer(&rng[0])))
	runtime.KeepAlive(&rng)
	if errno != 0 {
		return errno
	}

	return nil
}

func allocatedBytes(t *testing.T, path string) int64 {
	t.Helper()

	var st syscall.Stat_t
	require.NoError(t, syscall.Stat(path, &st))

	return st.Blocks * 512
}

func setupNBDDevice(t *testing.T, featureFlags *featureflags.Client, size int64, flags int) (*os.File, string) {
	t.Helper()

	if os.Geteuid() != 0 {
		t.Skip("the nbd requires root privileges to run")
	}

	const blockSize = header.RootfsBlockSize

	emptyDevice, err := testutils.NewZeroDevice(size, blockSize)
	require.NoError(t, err, "failed to create zero device")

	cowCachePath := filepath.Join(os.TempDir(), fmt.Sprintf("test-rootfs.ext4.cow.cache-%s", uuid.New().String()))
	t.Cleanup(func() {
		os.RemoveAll(cowCachePath)
	})

	cache, err := block.NewCache(
		size,
		blockSize,
		cowCachePath,
		false,
	)
	require.NoError(t, err, "failed to create cache")

	overlay := block.NewOverlay(emptyDevice, cache)
	t.Cleanup(func() {
		overlay.Close()
	})

	_, devicePath := setupNBDMount(t, featureFlags, overlay)

	t.Logf("NBD device path: %s", devicePath)

	deviceFile, err := os.OpenFile(devicePath, flags, 0)
	require.NoError(t, err, "failed to open device")
	t.Cleanup(func() {
		deviceFile.Close()
	})

	return deviceFile, cowCachePath
}
