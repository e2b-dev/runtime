//go:build linux

package nbd

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

func TestNBDTimeoutDefaults(t *testing.T) {
	t.Parallel()

	mnt := NewDirectPathMount(nil, nil, nil, logger.L())
	require.Equal(t, 90*time.Second, mnt.ioTimeout, "request timeout must still cover backend fetches")
	require.Zero(t, mnt.deadconnTimeout, "there is no reconnect owner to wait for")

	custom := NewDirectPathMount(nil, nil, nil, logger.L(), WithDeadconnTimeout(5*time.Second))
	require.Equal(t, 5*time.Second, custom.deadconnTimeout, "explicit test overrides must remain supported")
	require.Equal(t, 90*time.Second, custom.ioTimeout)
}

// The lost-socket read must fail without the old 30s reconnect wait. This uses
// the real kernel NBD queue and requires a disposable Linux NBD test environment.
func TestNBDDeadConnectionReturnsWithoutReconnectWait(t *testing.T) {
	t.Parallel()

	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}

	flags, err := featureflags.NewClient("", "", "")
	require.NoError(t, err)
	base, err := testutils.NewZeroDevice(10*1024*1024, header.RootfsBlockSize)
	require.NoError(t, err)
	cache, err := block.NewCache(10*1024*1024, header.RootfsBlockSize, filepath.Join(t.TempDir(), "cache"), false)
	require.NoError(t, err)
	overlay := block.NewOverlay(base, cache)
	t.Cleanup(func() { require.NoError(t, overlay.Close()) })
	mnt, path := setupNBDMount(t, flags, overlay)
	file, err := os.OpenFile(path, os.O_RDONLY|unix.O_DIRECT, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	buffer, err := unix.Mmap(-1, 0, 4096, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, unix.Munmap(buffer)) })

	// No user requests are in flight: the backend has no producer to kill,
	// and the normal mount cleanup still flushes, drains, disconnects and joins.
	for _, server := range mnt.socksServer {
		require.NoError(t, server.Close())
	}
	mnt.handlersWg.Wait()

	result := make(chan error, 1)
	start := time.Now()
	go func() {
		// Bypass cached probe reads, which could succeed without a live backend.
		_, readErr := file.ReadAt(buffer, 0)
		result <- readErr
	}()
	select {
	case err := <-result:
		require.ErrorIs(t, err, unix.EIO, "a device without command connections must reject I/O")
		t.Logf("dead connection returned after %s: %v", time.Since(start), err)
	case <-time.After(10 * time.Second):
		t.Error("read is waiting for an unimplemented reconnect path")
		// Join even on the old-code control; never leave a blocked read behind.
		<-result
	}
}
