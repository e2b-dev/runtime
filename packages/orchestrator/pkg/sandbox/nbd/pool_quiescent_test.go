//go:build linux

package nbd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bits-and-blooms/bitset"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// quiescentPool builds a pool whose device-state reads resolve against a
// temporary /sys/block stand-in, so isDeviceFree can be driven without the nbd
// module loaded.
func quiescentPool(t *testing.T) *DevicePool {
	t.Helper()

	return &DevicePool{
		done:        make(chan struct{}),
		usedSlots:   bitset.New(16),
		slots:       make(chan DeviceSlot, 1),
		sysBlockDir: t.TempDir(),
	}
}

// writeDeviceState lays out /sys/block/nbd<slot>/{size,inflight} and the
// holders directory for a fake device. A negative holder count means the
// holders directory is absent entirely (older-kernel path).
func writeDeviceState(t *testing.T, dir string, slot DeviceSlot, size, inflight string, holders int) {
	t.Helper()

	base := filepath.Join(dir, "nbd"+itoa(slot))
	require.NoError(t, os.MkdirAll(base, 0o755))

	if size != "" {
		require.NoError(t, os.WriteFile(filepath.Join(base, "size"), []byte(size), 0o644))
	}
	if inflight != "" {
		require.NoError(t, os.WriteFile(filepath.Join(base, "inflight"), []byte(inflight), 0o644))
	}
	if holders >= 0 {
		holdersDir := filepath.Join(base, "holders")
		require.NoError(t, os.MkdirAll(holdersDir, 0o755))
		for i := 0; i < holders; i++ {
			require.NoError(t, os.MkdirAll(filepath.Join(holdersDir, "dm-"+itoa(DeviceSlot(i))), 0o755))
		}
	}
}

func itoa(v DeviceSlot) string {
	if v == 0 {
		return "0"
	}
	var buf [10]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}

	return string(buf[i:])
}

// A disconnected device that is fully quiescent -- size 0, no in-flight
// requests, no holders -- is free.
func TestIsDeviceFreeQuiescent(t *testing.T) {
	t.Parallel()

	pool := quiescentPool(t)
	writeDeviceState(t, pool.sysBlockDir, 0, "0\n", "       0        0\n", 0)

	free, err := pool.isDeviceFree(0)
	require.NoError(t, err)
	assert.True(t, free, "quiescent disconnected device should be free")
}

// In-flight requests mean the kernel has not finished draining the previous
// connection, so the device is not free even with size 0.
func TestIsDeviceFreeInflightNotFree(t *testing.T) {
	t.Parallel()

	pool := quiescentPool(t)
	writeDeviceState(t, pool.sysBlockDir, 0, "0\n", "       0        3\n", 0)

	free, err := pool.isDeviceFree(0)
	require.NoError(t, err)
	assert.False(t, free, "device with in-flight requests must not be free")
}

// A holder (partition probe, dm, mount) still referencing the device keeps it
// out of the free pool.
func TestIsDeviceFreeHeldNotFree(t *testing.T) {
	t.Parallel()

	pool := quiescentPool(t)
	writeDeviceState(t, pool.sysBlockDir, 0, "0\n", "       0        0\n", 1)

	free, err := pool.isDeviceFree(0)
	require.NoError(t, err)
	assert.False(t, free, "device with a holder must not be free")
}

// A non-zero size means the device is still connected/backed, so it is not free
// regardless of the quiescence signals.
func TestIsDeviceFreeNonZeroSizeNotFree(t *testing.T) {
	t.Parallel()

	pool := quiescentPool(t)
	writeDeviceState(t, pool.sysBlockDir, 0, "2048\n", "       0        0\n", 0)

	free, err := pool.isDeviceFree(0)
	require.NoError(t, err)
	assert.False(t, free, "device with non-zero size must not be free")
}

// When the kernel does not expose inflight/holders (older kernels), size 0 with
// no pid is sufficient: the quiescence check must not wedge on absent signals.
func TestIsDeviceFreeMissingSignalsFallsBack(t *testing.T) {
	t.Parallel()

	pool := quiescentPool(t)
	writeDeviceState(t, pool.sysBlockDir, 0, "0\n", "", -1)

	free, err := pool.isDeviceFree(0)
	require.NoError(t, err)
	assert.True(t, free, "absent inflight/holders signals should fall back to size-only free")
}
