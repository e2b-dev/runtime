//go:build linux

package prefetch

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/memory"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// Only methods used by this no-fault lifecycle are implemented. Reads and Close
// delegate to a real mmap cache; unused device methods deliberately remain nil.
type lifecycleCacheSource struct {
	block.ReadonlyDevice

	cache   *block.Cache
	want    []byte
	hold    bool
	entered chan struct{}
	once    sync.Once
	calls   atomic.Uint64
	active  atomic.Int64
	badRead atomic.Bool
}

func (*lifecycleCacheSource) Header() *header.Header { return nil }
func (*lifecycleCacheSource) BlockSize() int64       { return 4096 }

func (s *lifecycleCacheSource) Slice(ctx context.Context, off, length int64) ([]byte, error) {
	s.active.Add(1)
	defer s.active.Add(-1)
	s.calls.Add(1)
	data, err := s.cache.Slice(off, length)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(data, s.want[off:off+length]) {
		s.badRead.Store(true)
	}
	if s.hold {
		s.once.Do(func() { close(s.entered) })
		<-ctx.Done()

		return nil, ctx.Err()
	}

	return data, nil
}

func (s *lifecycleCacheSource) Close() error { return s.cache.Close() }

// The pipe is an owned stand-in for the transferred UFFD descriptor. No page
// fault is generated and no memory ioctl is invoked. This drives the actual
// handshake/serve/Stop/Exit/handler.Close code, not registered guest-memory copy.
func TestCancelledStartRealBackendStopAndSourceClose(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"already-cancelled", "healthy-fetch-only", "late-cancel"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			log := logger.NewNopLogger()
			want := bytes.Repeat([]byte{0x5a}, 3*4096)
			cache, err := block.NewCache(int64(len(want)), 4096, filepath.Join(t.TempDir(), "source"), false)
			require.NoError(t, err)
			t.Cleanup(func() { _ = cache.Close() })
			n, err := cache.WriteAt(want, 0)
			require.NoError(t, err)
			require.Equal(t, len(want), n)
			source := &lifecycleCacheSource{cache: cache, want: want, hold: mode == "late-cancel", entered: make(chan struct{})}
			// Avoid the long test-name prefix in t.TempDir: Unix socket
			// addresses have a much smaller limit than regular file paths.
			socketDir, err := os.MkdirTemp("", "pf-") //nolint:usetesting // t.TempDir's test-name prefix exceeds the Unix socket path limit.
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, os.RemoveAll(socketDir)) })
			socket := filepath.Join(socketDir, "uffd.sock")
			backend := uffd.New(source, socket, log)
			// Before handler admission, cancellation must keep its error identity.
			cancelled, cancelWait := context.WithCancel(ctx)
			cancelWait()
			installed, err := backend.Prefault(cancelled, 0, want[:4096])
			require.False(t, installed)
			require.ErrorIs(t, err, context.Canceled)
			require.NoError(t, backend.Start(ctx))
			t.Cleanup(func() { _ = backend.Stop() })
			rd, wr, err := os.Pipe()
			require.NoError(t, err)
			defer rd.Close()
			defer wr.Close()
			conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: socket, Net: "unix"})
			require.NoError(t, err)
			defer conn.Close()
			regions, err := json.Marshal([]memory.Region{{BaseHostVirtAddr: 4096, Size: uintptr(len(want)), PageSize: 4096}})
			require.NoError(t, err)
			n, _, err = conn.WriteMsgUnix(regions, syscall.UnixRights(int(rd.Fd())), nil)
			require.NoError(t, err)
			require.Equal(t, len(regions), n)
			select {
			case <-backend.Ready():
			case <-ctx.Done():
				t.Fatal("actual backend did not publish Ready")
			}
			select {
			case <-backend.Exit().Done():
				t.Fatalf("Ready was an error exit, not successful admission: %v", backend.Exit().Error())
			default:
			}
			settings := ldtestdata.DataSource()
			settings.Update(settings.Flag(featureflags.MemoryPrefetchMaxFetchWorkers.Key()).ValueForAll(ldvalue.Int(1)))
			settings.Update(settings.Flag(featureflags.MemoryPrefetchMaxCopyWorkers.Key()).ValueForAll(ldvalue.Int(1)))
			settings.Update(settings.Flag(featureflags.MemoryPrefetchCoalesceMaxMB.Key()).ValueForAll(ldvalue.Int(0)))
			flags, err := featureflags.NewClientWithDatasource(settings)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, flags.Close(context.WithoutCancel(t.Context()))) })
			p := New(log, source, backend, &metadata.MemoryPrefetchMapping{Indices: []uint64{0, 1, 2}, BlockSize: 4096}, flags)
			p.Prefault = mode != "healthy-fetch-only"
			runCtx, cancelRun := context.WithCancel(ctx)
			defer cancelRun()
			if mode == "already-cancelled" {
				cancelRun()
			}
			done := make(chan error, 1)
			go func() { done <- p.Start(runCtx) }()
			if mode == "late-cancel" {
				select {
				case <-source.entered:
				case <-ctx.Done():
					t.Fatal("actual cache read was not admitted")
				}
				cancelRun()
			}
			select {
			case err := <-done:
				require.NoError(t, err, "Start preserves nil-on-cancellation")
			case <-ctx.Done():
				t.Fatal("Start failed to join before backend/source teardown")
			}
			require.Zero(t, source.active.Load(), "source callbacks outlived Start")
			require.False(t, source.badRead.Load())
			switch mode {
			case "already-cancelled":
				require.Zero(t, source.calls.Load())
			case "healthy-fetch-only":
				require.EqualValues(t, 3, source.calls.Load())
			default:
				require.Positive(t, source.calls.Load())
			}
			require.NoError(t, backend.Stop())
			select {
			case <-backend.Exit().Done():
				require.NoError(t, backend.Exit().Error())
			case <-ctx.Done():
				t.Fatal("Stop failed to settle the actual serve loop/Close")
			}
			installed, err = backend.Prefault(ctx, 0, want[:4096])
			require.False(t, installed)
			require.ErrorIs(t, err, userfaultfd.ErrClosed, "actual handler.Close was not observed")
			require.NoError(t, source.Close())
			_, err = cache.Slice(0, 4096)
			var closed *block.CacheClosedError
			require.ErrorAs(t, err, &closed, "actual source Close was not observed")
		})
	}
}
