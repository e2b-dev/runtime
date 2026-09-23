//go:build linux

package nbd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// TestCloseClosesTheDescriptorBeforeTheHandlerTeardown pins Close's order and
// its trace marks.
//
// The descriptor close is the block device's last close, where the kernel
// flushes writeback, so it must precede the context cancel and socket close
// that kill the data path the flush needs (see
// TestPathDirect_CloseReturnsWhileTheBackendStalls for what the reversed
// order costs). The event marks matter too: the descriptor close and Drain
// can each block without bound, and a stall is read by differencing the
// surrounding event timestamps -- if the descriptor event is missing or
// drifts away from the operation it marks, a two-minute stall becomes one
// unattributable interval, which a green suite would otherwise not notice.
func TestCloseClosesTheDescriptorBeforeTheHandlerTeardown(t *testing.T) {
	t.Parallel()

	if os.Geteuid() != 0 {
		t.Skip("the nbd requires root privileges to run")
	}

	// Close builds its span from the package tracer, so the recorder is the
	// package-wide one installed in TestMain. Selecting on this test's own
	// trace id is what keeps that safe to share with parallel tests.
	ctx, parent := otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd").Start(t.Context(), "test-parent")
	traceID := parent.SpanContext().TraceID()

	const size = int64(16 * 1024 * 1024)

	device, err := testutils.NewZeroDevice(size, header.RootfsBlockSize)
	require.NoError(t, err)

	cowPath := filepath.Join(t.TempDir(), fmt.Sprintf("events-%s.cow", uuid.New().String()))
	cache, err := block.NewCache(size, header.RootfsBlockSize, cowPath, false)
	require.NoError(t, err)

	overlay := block.NewOverlay(device, cache)
	t.Cleanup(func() { _ = overlay.Close() })

	featureFlags, err := featureflags.NewClient()
	require.NoError(t, err)
	t.Cleanup(func() { _ = featureFlags.Close(context.WithoutCancel(t.Context())) })

	pool := newPartitionedPool(t)

	mnt := NewDirectPathMount(overlay, pool, featureFlags, logger.L())

	_, err = mnt.Open(ctx)
	require.NoError(t, err)
	require.NoError(t, mnt.Close(ctx))
	parent.End()

	var events []string
	for _, s := range testSpanRecorder.Ended() {
		if s.Name() != "direct-path-mount-close" || s.SpanContext().TraceID() != traceID {
			continue
		}

		for _, e := range s.Events() {
			events = append(events, e.Name)
		}
	}
	require.NotEmpty(t, events, "the close span must record its steps")

	flush := indexOfEvent(t, events, "flushing NBD device writeback")
	descriptor := indexOfEvent(t, events, "closing NBD device descriptor")
	cancel := indexOfEvent(t, events, "canceling context")
	drain := indexOfEvent(t, events, "waiting for pending responses")
	disconnect := indexOfEvent(t, events, "disconnecting NBD")

	require.Lessf(t, flush, descriptor,
		"the flush must precede the descriptor close, and each needs its own mark since each can block on the backend: %v", events)
	require.Lessf(t, descriptor, cancel,
		"the descriptor must close before the cancel kills the data path its writeback needs, and the cancel mark bounds its interval: %v", events)
	require.Lessf(t, cancel, drain,
		"the drain must follow the cancel that unblocks the pending handlers: %v", events)
	require.Lessf(t, drain, disconnect,
		"the disconnect must wait for the drain, else it aborts responses still owed to the kernel: %v", events)
}

func indexOfEvent(t *testing.T, events []string, name string) int {
	t.Helper()

	for i, e := range events {
		if e == name {
			return i
		}
	}

	require.Failf(t, "missing close event", "%q not in %v", name, events)

	return -1
}
