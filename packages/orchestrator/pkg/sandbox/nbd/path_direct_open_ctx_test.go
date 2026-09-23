//go:build linux

package nbd

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// TestPathDirect_CloseFlushesAfterOpenContextEnds pins the data path's
// lifetime to Close, not to the context that opened the mount. A Close can
// run after that context is gone - t.Context is cancelled before Cleanup
// functions run, a cancelled build's deferred close outlives the build
// context - and the writeback flush Close starts with still has to reach the
// backend. A dispatcher bound to the Open context returns at its next
// request, which is that flush's, so the kernel holds the close until it
// abandons the connection (ioTimeout + deadconnTimeout) and fails the
// acknowledged writes with EIO.
func TestPathDirect_CloseFlushesAfterOpenContextEnds(t *testing.T) {
	t.Parallel()

	if os.Geteuid() != 0 {
		t.Skip("the nbd requires root privileges to run")
	}

	featureFlags, err := featureflags.NewClient()
	require.NoError(t, err)
	t.Cleanup(func() { _ = featureFlags.Close(context.WithoutCancel(t.Context())) })

	overlay := setupOverlay(t, 16*1024*1024)

	// Short kernel deadlines: a flush nobody answers returns only after
	// ioTimeout + deadconnTimeout, and the 10 s bound below has to be able to
	// fail before that.
	mnt := NewDirectPathMount(overlay, newPartitionedPool(t), featureFlags, logger.L(),
		WithIOTimeout(20*time.Second),
		WithDeadconnTimeout(10*time.Second),
	)

	openCtx, cancelOpen := context.WithCancel(t.Context())
	defer cancelOpen()

	deviceIndex, err := mnt.Open(openCtx)
	require.NoError(t, err)

	closed := false
	t.Cleanup(func() {
		if closed {
			return
		}

		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()

		_ = mnt.Close(ctx)
	})

	// A buffered write acknowledged from the page cache; Close's flush is
	// what carries it to the backend.
	pattern := newPattern(2 * header.RootfsBlockSize)

	writer, err := os.OpenFile(GetDevicePath(deviceIndex), os.O_RDWR, 0)
	require.NoError(t, err)

	_, err = writer.WriteAt(pattern, 0)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	cancelOpen()

	closeStart := time.Now()
	require.NoError(t, mnt.Close(t.Context()), "the flush must complete through a live data path")
	closed = true
	elapsed := time.Since(closeStart)

	require.Lessf(t, elapsed, 10*time.Second,
		"Close took %s: the flush ran against a dead data path and waited for the kernel to abandon it", elapsed)

	got := make([]byte, len(pattern))
	_, err = overlay.ReadAt(t.Context(), got, 0)
	require.NoError(t, err)
	require.Equal(t, pattern, got, "the acknowledged write must have reached the backend")
}
