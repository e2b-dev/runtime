//go:build linux

package nbd

import (
	"context"
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
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// slowDevice wraps a ReadonlyDevice and adds a configurable delay to every
// ReadAt call. Used to simulate slow GCS/NFS backends in tests.
type slowDevice struct {
	inner     block.ReadonlyDevice
	readDelay time.Duration
}

var _ block.ReadonlyDevice = (*slowDevice)(nil)

func newSlowDevice(inner block.ReadonlyDevice, readDelay time.Duration) *slowDevice {
	return &slowDevice{inner: inner, readDelay: readDelay}
}

func (s *slowDevice) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	select {
	case <-time.After(s.readDelay):
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	return s.inner.ReadAt(ctx, p, off)
}

func (s *slowDevice) Size(ctx context.Context) (int64, error) {
	return s.inner.Size(ctx)
}

func (s *slowDevice) BlockSize() int64 {
	return s.inner.BlockSize()
}

func (s *slowDevice) Slice(ctx context.Context, off, length int64) ([]byte, error) {
	return s.inner.Slice(ctx, off, length)
}

func (s *slowDevice) Header() *header.Header {
	return s.inner.Header()
}

func (s *slowDevice) SwapHeader(h *header.Header) {
	s.inner.SwapHeader(h)
}

func (s *slowDevice) Close() error {
	return s.inner.Close()
}

// TestSlowBackend_ShortTimeout reproduces the EIO bug: when the
// backend read takes longer than the kernel NBD I/O timeout, the kernel
// declares the connection dead and all I/O returns EIO.
func TestSlowBackend_ShortTimeout(t *testing.T) {
	t.Parallel()

	if os.Geteuid() != 0 {
		t.Skip("the nbd requires root privileges to run")
	}

	featureFlags, err := featureflags.NewClient("", "")
	require.NoError(t, err)

	const (
		size      = int64(10 * 1024 * 1024)
		blockSize = header.RootfsBlockSize
	)

	emptyDevice, err := testutils.NewZeroDevice(size, blockSize)
	require.NoError(t, err)

	// Backend delays every read by 8 seconds — longer than the kernel timeout below.
	slow := newSlowDevice(emptyDevice, 8*time.Second)

	cowCachePath := filepath.Join(os.TempDir(), fmt.Sprintf("test-slow-timeout-%s", uuid.New().String()))
	t.Cleanup(func() { os.RemoveAll(cowCachePath) })

	cache, err := block.NewCache(size, blockSize, cowCachePath, false)
	require.NoError(t, err)

	overlay := block.NewOverlay(slow, cache)
	t.Cleanup(func() { overlay.Close() })

	// Kernel I/O timeout of 5s + deadconn 5s = 10s total.
	// The 8s backend delay exceeds the 5s I/O timeout, so the kernel
	// will declare the connection dead and return EIO.
	_, devicePath := setupNBDMount(
		t, featureFlags, overlay,
		WithIOTimeout(5*time.Second),
		WithDeadconnTimeout(5*time.Second),
	)

	deviceFile, err := os.OpenFile(devicePath, os.O_RDONLY, 0)
	require.NoError(t, err)
	t.Cleanup(func() { deviceFile.Close() })

	buf := make([]byte, 4096)
	_, err = deviceFile.ReadAt(buf, 0)
	require.Error(t, err, "expected EIO from kernel timeout, but read succeeded")
	t.Logf("got expected error: %v", err)
}

// TestSlowBackend_SufficientTimeout proves the fix: with a kernel timeout
// longer than the backend delay, reads succeed.
func TestSlowBackend_SufficientTimeout(t *testing.T) {
	t.Parallel()

	if os.Geteuid() != 0 {
		t.Skip("the nbd requires root privileges to run")
	}

	featureFlags, err := featureflags.NewClient("", "")
	require.NoError(t, err)

	const (
		size      = int64(10 * 1024 * 1024)
		blockSize = header.RootfsBlockSize
	)

	emptyDevice, err := testutils.NewZeroDevice(size, blockSize)
	require.NoError(t, err)

	// Backend delays every read by 3 seconds.
	slow := newSlowDevice(emptyDevice, 3*time.Second)

	cowCachePath := filepath.Join(os.TempDir(), fmt.Sprintf("test-slow-ok-%s", uuid.New().String()))
	t.Cleanup(func() { os.RemoveAll(cowCachePath) })

	cache, err := block.NewCache(size, blockSize, cowCachePath, false)
	require.NoError(t, err)

	overlay := block.NewOverlay(slow, cache)
	t.Cleanup(func() { overlay.Close() })

	// Kernel I/O timeout of 30s — well above the 3s backend delay.
	_, devicePath := setupNBDMount(
		t, featureFlags, overlay,
		WithIOTimeout(30*time.Second),
		WithDeadconnTimeout(30*time.Second),
	)

	deviceFile, err := os.OpenFile(devicePath, os.O_RDONLY, 0)
	require.NoError(t, err)
	t.Cleanup(func() { deviceFile.Close() })

	buf := make([]byte, 4096)
	n, err := deviceFile.ReadAt(buf, 0)
	require.NoError(t, err, "read should succeed when timeout > backend delay")
	require.Equal(t, 4096, n)
}
