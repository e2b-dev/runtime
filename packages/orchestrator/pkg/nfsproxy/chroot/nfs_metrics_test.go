//go:build linux

package chroot

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/mountcache"
)

func TestRemoveOwnerUnmountCounter(t *testing.T) {
	t.Parallel()

	for _, blocked := range []bool{false, true} {
		t.Run("blocked="+strconv.FormatBool(blocked), func(t *testing.T) {
			t.Parallel()

			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
			counter, err := provider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/chroot").Int64Counter("nfs.chroot.unmounts")
			require.NoError(t, err)
			h, err := NewNFSHandler(nil, nil, nil)
			require.NoError(t, err)
			h.chrootUnmountsCounter = counter
			root := t.TempDir()
			fs, err := chrooted.Chroot(root)
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = fs.CloseAsync()
				require.NoError(t, fs.WaitClosed(context.WithoutCancel(t.Context())))
			})
			owner := mountcache.Owner{SandboxID: "unmount-counter", LifecycleID: "one"}
			h.chrootsByOwner[owner] = map[uuid.UUID]*chrooted.Chrooted{uuid.New(): fs}
			if blocked {
				blockUnmountRootOpen(t, fs, root)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			err = h.RemoveOwner(ctx, owner)
			var expected int64 = 1
			if blocked {
				require.ErrorIs(t, err, context.Canceled)
				expected = 0
			} else {
				require.NoError(t, err, "a completed root close wins over caller cancellation")
			}
			var data metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &data))
			var count int64
			for _, scope := range data.ScopeMetrics {
				for _, metric := range scope.Metrics {
					sum, ok := metric.Data.(metricdata.Sum[int64])
					require.True(t, ok)
					for _, point := range sum.DataPoints {
						count += point.Value
					}
				}
			}
			require.Equal(t, expected, count, "unfinished physical cleanup must not count as an unmount")
		})
	}
}

func blockUnmountRootOpen(t *testing.T, fs *chrooted.Chrooted, root string) {
	t.Helper()
	fifo := filepath.Join(root, "blocked-open")
	require.NoError(t, unix.Mkfifo(fifo, 0o600))
	opened := make(chan error, 1)
	thread := make(chan int, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		thread <- unix.Gettid()
		file, err := fs.Open("blocked-open")
		if err == nil {
			err = file.Close()
		}
		opened <- err
	}()
	unblock := sync.OnceFunc(func() {
		file, err := os.OpenFile(fifo, os.O_RDWR|unix.O_NONBLOCK, 0)
		require.NoError(t, err)
		require.NoError(t, <-opened)
		require.NoError(t, file.Close())
	})
	t.Cleanup(unblock)
	// Observe the actual blocked syscall, not just a goroutine-start signal.
	threadPath := filepath.Join("/proc/self/task", strconv.Itoa(<-thread), "syscall")
	require.Eventually(t, func() bool {
		state, err := os.ReadFile(threadPath)
		if err != nil {
			return false
		}
		fields := strings.Fields(string(state))

		return len(fields) > 0 && fields[0] == strconv.Itoa(unix.SYS_OPENAT2)
	}, time.Second, time.Millisecond, "the FIFO must hold an in-flight root operation")
}
