//go:build linux

package sandbox

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/mocks"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func TestServeMemoryCleanupJoinsReceivedDescriptorOwner(t *testing.T) {
	t.Parallel()
	// Real Unix paths are limited independently of the long test name.
	dir, err := os.MkdirTemp("", "uffd-owner-") //nolint:usetesting
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	file, err := os.CreateTemp(t.TempDir(), "received")
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	var identity unix.Stat_t
	require.NoError(t, unix.Fstat(int(file.Fd()), &identity))

	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	memfile := blockmocks.NewMockReadonlyDevice(t)
	memfile.EXPECT().Header().Run(func() {
		close(entered)
		<-release
	}).Return(nil).Once()
	u := uffd.New(memfile, filepath.Join(dir, "u.sock"), logger.L())
	cleanup := NewCleanup()
	dependentClosed := make(chan struct{})
	cleanup.Add(t.Context(), func(context.Context) error {
		close(dependentClosed)

		return nil
	})
	require.NoError(t, serveMemory(t.Context(), cleanup, u))
	sbx := &Sandbox{Resources: &Resources{memory: u}, cleanup: cleanup}
	t.Cleanup(func() {
		unblock()
		_ = u.Stop()
		select {
		case <-u.Exit().Done():
		case <-time.After(time.Second):
			t.Error("failed to join owned handshake in test cleanup")
		}
	})
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: filepath.Join(dir, "u.sock"), Net: "unix"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	// Empty regions reach the real Header call, then fail construction before
	// Serve or any UFFD ioctl. The pause owns actual SCM_RIGHTS copies.
	_, _, err = conn.WriteMsgUnix([]byte("[]"), syscall.UnixRights(int(file.Fd())), nil)
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handshake did not acquire the descriptor before cleanup")
	}

	started, done := make(chan struct{}), make(chan error, 1)
	cleanup.AddPriority(t.Context(), func(context.Context) error {
		close(started)

		return nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	go func() { done <- sbx.Close(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not start")
	}
	select {
	case <-dependentClosed:
		t.Error("dependent cleanup ran while the memory descriptor owner was active")
	case <-time.After(200 * time.Millisecond):
	}
	var cleanupErr error
	joined := false
	select {
	case cleanupErr = <-done:
		joined = true
		t.Error("cleanup returned before the memory descriptor owner joined")
	default:
	}
	unblock()
	if !joined {
		select {
		case cleanupErr = <-done:
		case <-time.After(time.Second):
			t.Fatal("cleanup failed to join after the descriptor owner returned")
		}
	}
	require.NoError(t, cleanupErr, "the handler's operational error is not a cleanup failure")
	require.ErrorContains(t, u.Exit().Error(), "memory mapping has no regions")
	require.Equal(t, u.Exit().Error(), sbx.MemoryHandlerErr(), "joining must preserve the handler's error")
	select {
	case <-u.Exit().Done():
	default:
		t.Fatal("cleanup returned before UFFD Exit was published")
	}

	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	copies := 0
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		var st unix.Stat_t
		if unix.Fstat(fd, &st) == nil && st.Dev == identity.Dev && st.Ino == identity.Ino {
			copies++
		}
	}
	require.Equal(t, 1, copies, "cleanup must join the received-descriptor owner")
}

func TestServeMemoryCleanupPreservesCleanupFailure(t *testing.T) {
	t.Parallel()
	dir, err := os.MkdirTemp("", "uffd-close-") //nolint:usetesting
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	u := uffd.New(blockmocks.NewMockReadonlyDevice(t), filepath.Join(dir, "u.sock"), logger.L())
	cleanup := NewCleanup()
	cleanupFailure := errors.New("dependent cleanup failed")
	cleanup.Add(t.Context(), func(context.Context) error { return cleanupFailure })
	require.NoError(t, serveMemory(t.Context(), cleanup, u))
	t.Cleanup(func() {
		_ = u.Stop()
		select {
		case <-u.Exit().Done():
		case <-time.After(time.Second):
			t.Error("test cleanup failed to join rejected handshake")
		}
	})
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: filepath.Join(dir, "u.sock"), Net: "unix"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = conn.Write([]byte("malformed mapping"))
	require.NoError(t, err)
	select {
	case <-u.Exit().Done():
	case <-time.After(time.Second):
		t.Fatal("rejected handshake did not finish")
	}
	handlerFailure := u.Exit().Error()
	require.Error(t, handlerFailure)
	sbx := &Sandbox{Resources: &Resources{memory: u}, cleanup: cleanup}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err = sbx.Close(ctx)
	require.ErrorIs(t, err, cleanupFailure, "joining must not hide a real cleanup error")
	require.NotErrorIs(t, err, handlerFailure, "the handler's failure belongs to sandbox execution")
	require.Equal(t, handlerFailure, sbx.MemoryHandlerErr())
	require.ErrorIs(t, sbx.Close(ctx), cleanupFailure, "repeated Close retains the cleanup result")
}
