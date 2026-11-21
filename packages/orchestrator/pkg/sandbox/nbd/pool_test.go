//go:build linux

package nbd

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bits-and-blooms/bitset"
	"github.com/stretchr/testify/require"
)

func TestFreeDeviceScanProbesBusySlotOnce(t *testing.T) {
	t.Parallel()

	pool := &DevicePool{
		done:        make(chan struct{}),
		sysBlockDir: t.TempDir(),
		usedSlots:   bitset.New(3).Set(0),
	}
	busy := filepath.Join(pool.sysBlockDir, "nbd1")
	free := filepath.Join(pool.sysBlockDir, "nbd2")
	require.NoError(t, os.Mkdir(busy, 0o755))
	require.NoError(t, os.Mkdir(free, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(free, "size"), []byte("0\n"), 0o644))

	// The pipe reports a nonzero size once; a second probe would read EOF.
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })
	_, err = writer.WriteString("1\n")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	busySize := filepath.Join(busy, "size")
	require.NoError(t, os.Symlink(fmt.Sprintf("/proc/self/fd/%d", reader.Fd()), busySize))

	slot, err := pool.getFreeDeviceSlot()
	require.NoError(t, err)
	require.Equal(t, DeviceSlot(2), *slot)

	require.NoError(t, os.Remove(busySize))
	require.NoError(t, os.WriteFile(busySize, []byte("0\n"), 0o644))
	slot, err = pool.getFreeDeviceSlot()
	require.NoError(t, err)
	require.Equal(t, DeviceSlot(1), *slot)
}
