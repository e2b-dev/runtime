//go:build linux

package uffd

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/mocks"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func startHandshakeTest(t *testing.T) *Uffd {
	t.Helper()
	// Keep the Unix path under sun_path's limit independently of test names.
	dir, err := os.MkdirTemp("", "uffd-hs-") //nolint:usetesting
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	memfile := blockmocks.NewMockReadonlyDevice(t)
	memfile.EXPECT().Header().Return(nil).Maybe()
	u := New(memfile, filepath.Join(dir, "u.sock"), logger.L())
	require.NoError(t, u.Start(t.Context()))
	t.Cleanup(func() {
		_ = u.Stop()
		// Rescue only after the assertions, so it cannot mask a missing Stop.
		_ = u.lis.Close()
		u.handshakeMu.Lock()
		conn := u.handshakeConn
		u.handshakeMu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
		defer cancel()
		select {
		case <-u.Exit().Done():
		case <-ctx.Done():
			t.Error("test cleanup could not join the handshake")
		}
	})

	return u
}

func TestStopUnblocksHandshakeListener(t *testing.T) {
	t.Parallel()
	u := startHandshakeTest(t)
	require.NoError(t, u.Stop())
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	select {
	case <-u.Exit().Done():
	case <-ctx.Done():
		t.Fatal("Stop left the pre-handshake listener running")
	}
	require.NoError(t, u.Exit().Error())
	_, err := u.handler.WaitWithContext(ctx)
	require.Error(t, err, "stopping before construction must unblock handler waiters")
	require.NoError(t, u.Stop(), "repeated stop cannot replay descriptor-close errors")
}

func TestStopUnblocksAcceptedHandshake(t *testing.T) {
	t.Parallel()
	u := startHandshakeTest(t)
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: u.socketPath, Net: "unix"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.Eventually(t, func() bool {
		u.handshakeMu.Lock()
		defer u.handshakeMu.Unlock()

		return u.handshakeConn != nil
	}, time.Second, time.Millisecond, "the handler must own the connection before Stop")

	// No mappings or rights are sent. The exit pipe cannot wake ReadMsgUnix.
	require.NoError(t, u.Stop())
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	select {
	case <-u.Exit().Done():
	case <-ctx.Done():
		t.Fatal("Stop left the accepted handshake read running")
	}
	require.NoError(t, u.Exit().Error())
	_, err = u.handler.WaitWithContext(ctx)
	require.Error(t, err, "stopping before construction must unblock handler waiters")
	require.NoError(t, u.Stop())
}

func handshakeDescriptorCopies(t *testing.T, identity unix.Stat_t) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	copies := 0
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		var stat unix.Stat_t
		if unix.Fstat(fd, &stat) == nil && stat.Dev == identity.Dev && stat.Ino == identity.Ino {
			copies++
		}
	}

	return copies
}

func TestRejectedHandshakeClosesReceivedDescriptors(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		rightsCount int
		body        string
		wantError   string
	}{
		"malformed-body":       {1, "malformed mapping", "failed parsing memory mapping data"},
		"malformed-two-rights": {2, "malformed mapping", "failed parsing memory mapping data"},
		"truncated-rights":     {3, "malformed mapping", "truncated memory handshake"},
		"empty-mapping":        {1, "[]", "memory mapping has no regions"},
		"invalid-page-size":    {2, `[{"page_size_kib":1}]`, "unsupported page size"},
		// The real wrapper consumes a zero-length ordinary file and fails mmap.
		// The handler only creates/closes a pipe here; Serve and ioctls are not reached.
		"memfd-wrap-failure": {2, `[{"size":4096,"page_size_kib":4096}]`, "failed to wrap memfd: mmap memfd"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			u := startHandshakeTest(t)
			conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: u.socketPath, Net: "unix"})
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			file, err := os.CreateTemp(t.TempDir(), "backing")
			require.NoError(t, err)
			t.Cleanup(func() { _ = file.Close() })
			var identity unix.Stat_t
			require.NoError(t, unix.Fstat(int(file.Fd()), &identity))
			require.Equal(t, 1, handshakeDescriptorCopies(t, identity))
			rights := make([]int, test.rightsCount)
			for i := range rights {
				rights[i] = int(file.Fd())
			}
			_, _, err = conn.WriteMsgUnix([]byte(test.body), syscall.UnixRights(rights...), nil)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			select {
			case <-u.Exit().Done():
			case <-ctx.Done():
				t.Fatal("rejected handshake did not finish")
			}
			require.ErrorContains(t, u.Exit().Error(), test.wantError)
			require.Equal(t, 1, handshakeDescriptorCopies(t, identity), "a rejected handshake must not retain received rights")
		})
	}
}

func TestRejectedHandshakeClosesConnection(t *testing.T) {
	t.Parallel()
	u := startHandshakeTest(t)
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: u.socketPath, Net: "unix"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = conn.Write([]byte("malformed mapping"))
	require.NoError(t, err)
	select {
	case <-u.Exit().Done():
	case <-time.After(time.Second):
		t.Fatal("rejected handshake did not finish")
	}
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(200*time.Millisecond)))
	_, err = conn.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "the owned accepted socket must close before Exit")
}

func TestHandshakePreservesOneOrTwoDescriptorOwnership(t *testing.T) {
	t.Parallel()
	for _, withMemfd := range []bool{false, true} {
		t.Run(strconv.FormatBool(withMemfd), func(t *testing.T) {
			t.Parallel()
			u := startHandshakeTest(t)
			reader, writer, err := os.Pipe()
			require.NoError(t, err)
			t.Cleanup(func() { _ = reader.Close() })
			t.Cleanup(func() { _ = writer.Close() })
			var pipeIdentity unix.Stat_t
			require.NoError(t, unix.Fstat(int(reader.Fd()), &pipeIdentity))
			require.Equal(t, 2, handshakeDescriptorCopies(t, pipeIdentity))
			file, err := os.CreateTemp(t.TempDir(), "mapped")
			require.NoError(t, err)
			t.Cleanup(func() { _ = file.Close() })
			require.NoError(t, file.Truncate(4096))
			var fileIdentity unix.Stat_t
			require.NoError(t, unix.Fstat(int(file.Fd()), &fileIdentity))
			rights := []int{int(reader.Fd())}
			if withMemfd {
				rights = append(rights, int(file.Fd()))
			}
			conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: u.socketPath, Net: "unix"})
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			// An empty ordinary pipe stands in for the event fd. No events are
			// written: Serve only polls and observes Stop, never a UFFD ioctl.
			_, _, err = conn.WriteMsgUnix([]byte(`[{"size":4096,"page_size_kib":4096}]`), syscall.UnixRights(rights...), nil)
			require.NoError(t, err)
			select {
			case <-u.Ready():
			case <-time.After(time.Second):
				t.Fatal("valid userspace handshake did not publish its handler")
			}
			_, err = u.handler.Result()
			require.NoError(t, err)
			require.Equal(t, 3, handshakeDescriptorCopies(t, pipeIdentity))
			memfd := u.Memfd(t.Context())
			if withMemfd {
				require.NotNil(t, memfd)
				t.Cleanup(func() { _ = memfd.Close() })
			} else {
				require.Nil(t, memfd)
			}
			require.Nil(t, u.Memfd(t.Context()), "transfer is single-owner")
			require.NoError(t, u.Stop())
			select {
			case <-u.Exit().Done():
			case <-time.After(time.Second):
				t.Fatal("userspace handler did not join after Stop")
			}
			require.NoError(t, u.Exit().Error())
			require.Equal(t, 2, handshakeDescriptorCopies(t, pipeIdentity), "handler must close its event descriptor")
			if withMemfd {
				require.Equal(t, 2, handshakeDescriptorCopies(t, fileIdentity), "transferred memfd must survive handler exit")
				data, err := memfd.Slice(0, 4096)
				require.NoError(t, err)
				require.Len(t, data, 4096)
			} else {
				require.Equal(t, 1, handshakeDescriptorCopies(t, fileIdentity))
			}
		})
	}
}
