//go:build linux

package nbd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

var errBackendWrite = errors.New("backend write rejected")

// failingWriteDevice serves reads normally and rejects every write, which is
// what the kernel sees when the backend cannot take a write or the connection
// times out: the dispatcher answers with an error and the page the guest
// already had acknowledged never reaches the backend.
type failingWriteDevice struct {
	block.Device
}

func (f *failingWriteDevice) WriteAt(_ []byte, _ int64) (int, error) {
	return 0, errBackendWrite
}

func (f *failingWriteDevice) WriteZeroesAt(_, _ int64) (int, error) {
	return 0, errBackendWrite
}

// A write that the backend rejected leaves the block device's page cache with a
// writeback error and nothing in the backend. Flush has to report it, because
// it is the only place the pause path can learn that the rootfs it is about to
// export is missing writes the guest was told had landed.
func TestPathDirect_FlushReportsFailedWriteback(t *testing.T) {
	t.Parallel()

	featureFlags, err := featureflags.NewClient()
	require.NoError(t, err)

	overlay := setupOverlay(t, 10*1024*1024)
	mnt, devicePath := setupNBDMount(t, featureFlags, &failingWriteDevice{Device: overlay})

	deviceFile, err := os.OpenFile(devicePath, os.O_RDWR, 0)
	require.NoError(t, err, "failed to open device")
	t.Cleanup(func() {
		deviceFile.Close()
	})

	// Buffered, so the write is acknowledged out of the page cache and only
	// fails once the kernel writes it back.
	_, err = deviceFile.WriteAt(newPattern(header.RootfsBlockSize), 0)
	require.NoError(t, err, "buffered write should be acknowledged from the page cache")

	require.Error(t, mnt.Flush(t.Context()), "flush must report the failed writeback")
}

// The failure that matters most happens while the guest is still running: a
// backend write fails, the kernel records it, and only later does the pause
// path flush. Because Linux samples the writeback-error sequence when a
// descriptor is opened, a descriptor opened at flush time would be blind to it
// - and reading it through another descriptor first must not hide it either.
func TestPathDirect_FlushReportsWritebackFailedBeforeFlush(t *testing.T) {
	t.Parallel()

	featureFlags, err := featureflags.NewClient()
	require.NoError(t, err)

	overlay := setupOverlay(t, 10*1024*1024)
	mnt, devicePath := setupNBDMount(t, featureFlags, &failingWriteDevice{Device: overlay})

	// A separate descriptor, standing in for the guest's writer: it writes, and
	// its own sync both triggers the failed writeback and consumes this
	// descriptor's copy of the error.
	writer, err := os.OpenFile(devicePath, os.O_RDWR, 0)
	require.NoError(t, err, "failed to open device")

	_, err = writer.WriteAt(newPattern(header.RootfsBlockSize), 0)
	require.NoError(t, err, "buffered write should be acknowledged from the page cache")
	require.Error(t, writer.Sync(), "the writer's own sync should see the failed writeback")
	require.NoError(t, writer.Close())

	require.Error(t, mnt.Flush(t.Context()), "flush must report a writeback that failed before it ran")
}

// The happy path: Flush pushes what is still cached by the kernel into the
// backend, so a diff exported straight from the backend sees it.
func TestPathDirect_FlushPushesBufferedWrites(t *testing.T) {
	t.Parallel()

	featureFlags, err := featureflags.NewClient()
	require.NoError(t, err)

	overlay := setupOverlay(t, 10*1024*1024)
	mnt, devicePath := setupNBDMount(t, featureFlags, overlay)

	deviceFile, err := os.OpenFile(devicePath, os.O_RDWR, 0)
	require.NoError(t, err, "failed to open device")
	t.Cleanup(func() {
		deviceFile.Close()
	})

	written := newPattern(header.RootfsBlockSize)
	_, err = deviceFile.WriteAt(written, 0)
	require.NoError(t, err, "failed to write to device")

	require.NoError(t, mnt.Flush(t.Context()))

	readBack := make([]byte, len(written))
	_, err = overlay.ReadAt(t.Context(), readBack, 0)
	require.NoError(t, err, "failed to read from the backend")
	require.Equal(t, written, readBack, "backend is missing the flushed write")
}

func setupOverlay(t *testing.T, size int64) *block.Overlay {
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

	cache, err := block.NewCache(size, blockSize, cowCachePath, false)
	require.NoError(t, err, "failed to create cache")

	overlay := block.NewOverlay(emptyDevice, cache)
	t.Cleanup(func() {
		overlay.Close()
	})

	return overlay
}

// setupNBDMount is setupNBDDevice's sibling for tests that drive the mount
// itself rather than the device path it exposes.
func setupNBDMount(t *testing.T, featureFlags *featureflags.Client, backend block.Device, mountOpts ...MountOption) (*DirectPathMount, string) {
	t.Helper()

	devicePool := newPartitionedPool(t)

	mnt := NewDirectPathMount(backend, devicePool, featureFlags, logger.L(), mountOpts...)

	deviceIndex, err := mnt.Open(t.Context())
	require.NoError(t, err, "failed to open nbd mount")

	// Registered after newPartitionedPool's cleanup, so the mount closes
	// before the pool does.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()

		if err := mnt.Close(ctx); err != nil {
			t.Logf("failed to close nbd mount: %v", err)
		}
	})

	return mnt, GetDevicePath(deviceIndex)
}

func newPattern(size int) []byte {
	p := make([]byte, size)
	for i := range p {
		p[i] = byte(i%251 + 1)
	}

	return p
}
