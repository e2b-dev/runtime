//go:build linux

package nbd

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// hostageParkLimit bounds how long a hostage write waits for its release, so
// a hostage the test never releases still answers through the live data
// path. It sits well above everything the test waits for before Close (the
// udev settle and the foreign-holder poll, 10 s each at most), because a park
// that ends on the limit instead of the release puts the stall back before
// Close; the test asserts which one ended it. With the hostage delay it stays
// under the mount's ioTimeout.
const hostageParkLimit = 60 * time.Second

// hostageWriteDevice is the write-lane slowDevice: it parks every WriteAt
// until the test releases it, then sits on it for writeDelay, standing in for
// a stalled GCS/NFS backend holding a writeback hostage. The park pins the
// stall to Close: the kernel writes a dirtied page back whenever it chooses,
// and a delay served before Close begins leaves Close nothing to wait for.
type hostageWriteDevice struct {
	block.Device

	release    <-chan struct{}
	writeDelay time.Duration

	// parkTimedOut records a write that left its park on the limit, not on
	// the release: the stall then ran before Close and the test must say so.
	parkTimedOut atomic.Bool
}

func (h *hostageWriteDevice) WriteAt(p []byte, off int64) (int, error) {
	select {
	case <-h.release:
	case <-time.After(hostageParkLimit):
		h.parkTimedOut.Store(true)
	}

	time.Sleep(h.writeDelay)

	return h.Device.WriteAt(p, off)
}

// TestPathDirect_CloseReturnsWhileTheBackendStalls dirties the device's page
// cache and closes the mount without flushing, the shape of every teardown
// that skips the Pause path's explicit Flush (kill, expire, resume).
//
// Close flushes the dirty pages itself, and the flush has to run while the
// dispatchers can still serve it: with the handlers and sockets torn down
// first, the writeback lands on a dead connection and the close blocks until
// the kernel gives the connection up (deadconnTimeout for writeback submitted
// after the sockets died, as here; ioTimeout + deadconnTimeout for commands
// orphaned in flight -- one mechanism, two deadlines). Through a live data
// path the close returns as soon as the backend does, here after a hostage
// delay just over the watchdog threshold.
//
// Crossing the threshold also makes this test the proof that the close
// watchdog fires: the mid-stall warn, the post-stall accounting warn, and the
// slow-close counter.
func TestPathDirect_CloseReturnsWhileTheBackendStalls(t *testing.T) {
	t.Parallel()

	if os.Geteuid() != 0 {
		t.Skip("the nbd requires root privileges to run")
	}

	hostageDelay := deviceCloseWarnThreshold + time.Second

	// The watchdog logs carry this test's trace id (Close logs through its
	// span context), which is what scopes the assertions against parallel
	// tests reusing a device index.
	ctx, parent := otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd").Start(t.Context(), "test-parent")
	traceID := parent.SpanContext().TraceID().String()

	featureFlags, err := featureflags.NewClient("", "", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = featureFlags.Close(context.WithoutCancel(t.Context())) })

	overlay := setupOverlay(t, 16*1024*1024)

	// Released right before Close, so the hostage delay runs inside Close
	// whether the kernel issues the write on its own first or at Close's
	// sync.
	release := make(chan struct{})
	releaseHostage := sync.OnceFunc(func() { close(release) })
	hostage := &hostageWriteDevice{Device: overlay, release: release, writeDelay: hostageDelay}

	// deadconn is the deadline a regression stalls on; 120s keeps it far from
	// both the close budget asserted below and the hostage delay. ioTimeout
	// only has to clear the park limit plus the hostage delay so the
	// live-path writeback is never timed out.
	mnt, devicePath := setupNBDMount(t, featureFlags, hostage,
		WithIOTimeout(90*time.Second),
		WithDeadconnTimeout(120*time.Second),
	)

	// Registered after the mount's cleanup, so it runs first: a failure
	// before Close must not hand the cleanup's close a parked write.
	t.Cleanup(releaseHostage)

	idx64, err := strconv.ParseUint(strings.TrimPrefix(devicePath, "/dev/nbd"), 10, 32)
	require.NoError(t, err)
	deviceIndex := uint32(idx64)

	// A buffered write acknowledged from the page cache; its writeback is the
	// hostage, and the kernel may issue it any time from here on -- its
	// flusher, memory pressure, a sync elsewhere. The writer descriptor
	// closes again so the mount's descriptor stays the device's last opener.
	writer, err := os.OpenFile(devicePath, os.O_RDWR, 0)
	require.NoError(t, err)

	_, err = writer.WriteAt(newPattern(2*header.RootfsBlockSize), 0)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	// systemd-udevd re-probes a block device when a writable descriptor
	// closes, and a probe descriptor still open across Close would absorb
	// the kernel-side last-close work this test also exercises. Settle
	// drains the probe event, the poll waits out its descriptor; without
	// udev both return at once. A parked write does not hold the probe up:
	// the dispatcher serves each command in its own goroutine, and the
	// dirtied pages stay readable from the page cache while their writeback
	// is in flight.
	_ = exec.CommandContext(t.Context(), "udevadm", "settle", "--timeout=10").Run()
	waitForForeignHolders(t, devicePath)

	// The counter is process-wide, so the assertion below diffs it around this
	// close, filtered on the stage the hostage stalls: the writeback sync.
	slowClosesBefore := slowCloseCount(t, "sync")

	releaseHostage()
	closeStart := time.Now()
	require.NoError(t, mnt.Close(ctx))
	elapsed := time.Since(closeStart)
	parent.End()

	require.Lessf(t, elapsed, 10*time.Second,
		"Close took %s: it flushed writeback into a torn-down data path and waited for the kernel to abandon it", elapsed)
	require.False(t, hostage.parkTimedOut.Load(),
		"the hostage write left its park on the limit before Close released it, so the stall ran outside Close")

	assertCloseWatchdogFired(t, traceID, deviceIndex)
	require.GreaterOrEqual(t, slowCloseCount(t, "sync")-slowClosesBefore, int64(1),
		"the stalled close must be counted on the slow-close metric with the stage it stalled in")
}

// waitForForeignHolders blocks until no other process holds an open
// descriptor of devPath.
func waitForForeignHolders(t *testing.T, devPath string) {
	t.Helper()

	self := strconv.Itoa(os.Getpid())
	deadline := time.Now().Add(10 * time.Second)

	for {
		held := false

		fds, _ := filepath.Glob("/proc/[0-9]*/fd/*")
		for _, fd := range fds {
			if strings.Split(fd, "/")[2] == self {
				continue
			}

			if target, err := os.Readlink(fd); err == nil && target == devPath {
				held = true

				break
			}
		}

		if !held {
			return
		}

		require.Truef(t, time.Now().Before(deadline), "another process held %s open for 10s", devPath)
		time.Sleep(10 * time.Millisecond)
	}
}

// assertCloseWatchdogFired checks the watchdog's log outputs for this test's
// close, scoped by trace id and device index: the mid-stall warn and the
// accounting warn with the stall's duration.
func assertCloseWatchdogFired(t *testing.T, traceID string, deviceIndex uint32) {
	t.Helper()

	scoped := func(message string) map[string]any {
		for _, entry := range testLogObserver.FilterMessage(message).All() {
			fields := entry.ContextMap()
			if fields["trace_id"] == traceID && fields["device_index"] == deviceIndex {
				return fields
			}
		}

		return nil
	}

	require.NotNilf(t, scoped("NBD device descriptor close stalled"),
		"the watchdog must warn mid-stall for device %d", deviceIndex)

	finished := scoped("NBD device descriptor close finished after stalling")
	require.NotNilf(t, finished,
		"the watchdog must account the stall on return for device %d", deviceIndex)

	duration, ok := finished["duration"].(time.Duration)
	require.True(t, ok, "the accounting warn must carry the stall duration")
	require.Greater(t, duration, deviceCloseWarnThreshold)
}

// TestPathDirect_CloseReturnsTheWritebackError proves the teardown flush's
// sync failure is returned, not dropped: a backend that rejects writes leaves
// the page cache with a writeback error, and Close's error is the only place
// a teardown that skips the explicit Flush can learn the backend it is
// abandoning is missing acknowledged writes.
func TestPathDirect_CloseReturnsTheWritebackError(t *testing.T) {
	t.Parallel()

	if os.Geteuid() != 0 {
		t.Skip("the nbd requires root privileges to run")
	}

	ctx, parent := otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd").Start(t.Context(), "test-parent")
	traceID := parent.SpanContext().TraceID()

	featureFlags, err := featureflags.NewClient("", "", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = featureFlags.Close(context.WithoutCancel(t.Context())) })

	overlay := setupOverlay(t, 10*1024*1024)
	mnt, devicePath := setupNBDMount(t, featureFlags, &failingWriteDevice{Device: overlay})

	writer, err := os.OpenFile(devicePath, os.O_RDWR, 0)
	require.NoError(t, err)

	_, err = writer.WriteAt(newPattern(header.RootfsBlockSize), 0)
	require.NoError(t, err, "the buffered write must be acknowledged from the page cache")
	require.NoError(t, writer.Close())

	closeErr := mnt.Close(ctx)
	parent.End()

	require.ErrorContains(t, closeErr, "sync NBD device", "Close must return the writeback failure")

	// The close span carries the same error mark Flush's would.
	for _, s := range testSpanRecorder.Ended() {
		if s.Name() != "direct-path-mount-close" || s.SpanContext().TraceID() != traceID {
			continue
		}

		require.Equal(t, codes.Error, s.Status().Code, "the close span must be marked on writeback failure")

		return
	}

	require.Fail(t, "no direct-path-mount-close span recorded for this test's trace")
}

// slowCloseCount sums the slow-close counter's datapoints for one stage.
func slowCloseCount(t *testing.T, stage string) int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(t.Context(), &rm))

	var total int64
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "orchestrator.nbd.device.close.slow" {
				continue
			}

			sum, ok := m.Data.(metricdata.Sum[int64])
			require.Truef(t, ok, "orchestrator.nbd.device.close.slow must be a counter, got %T", m.Data)

			for _, dp := range sum.DataPoints {
				if v, found := dp.Attributes.Value(attribute.Key("stage")); found && v.AsString() == stage {
					total += dp.Value
				}
			}
		}
	}

	return total
}
