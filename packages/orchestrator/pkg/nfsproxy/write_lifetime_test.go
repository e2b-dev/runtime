//go:build linux

package nfsproxy

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	servernfs "github.com/willscott/go-nfs"
	clientnfs "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	nfscfg "github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

type writeFailureHandler struct {
	servernfs.Handler

	phase string
	mu    sync.Mutex
	files []*writeFailureFile
}

func (h *writeFailureHandler) FromHandle(ctx context.Context, handle []byte) (billy.Filesystem, []string, error) {
	fs, path, err := h.Handler.FromHandle(ctx, handle)
	if err != nil {
		return fs, path, err
	}

	return &writeFailureFS{Filesystem: fs, owner: h}, path, nil
}

func (h *writeFailureHandler) ToHandle(ctx context.Context, fs billy.Filesystem, path []string) []byte {
	if wrapped, ok := fs.(*writeFailureFS); ok {
		fs = wrapped.Filesystem
	}

	return h.Handler.ToHandle(ctx, fs, path)
}

type writeFailureFS struct {
	billy.Filesystem

	owner *writeFailureHandler
}

func (fs *writeFailureFS) Unwrap() billy.Filesystem { return fs.Filesystem }

func (fs *writeFailureFS) OpenFile(path string, flag int, mode os.FileMode) (billy.File, error) {
	file, err := fs.Filesystem.OpenFile(path, flag, mode)
	if err != nil || flag != os.O_RDWR {
		return file, err
	}
	wrapped := &writeFailureFile{File: file, phase: fs.owner.phase}
	fs.owner.mu.Lock()
	fs.owner.files = append(fs.owner.files, wrapped)
	fs.owner.mu.Unlock()

	return wrapped, nil
}

type writeFailureFile struct {
	billy.File

	phase  string
	closes atomic.Int64
}

func (f *writeFailureFile) Seek(offset int64, whence int) (int64, error) {
	if f.phase == "seek" {
		return 0, syscall.EIO
	}

	return f.File.Seek(offset, whence)
}

func (f *writeFailureFile) Write(data []byte) (int, error) {
	if f.phase == "write" {
		n, err := f.File.Write(data[:1])
		if err != nil {
			return n, err
		}

		return n, syscall.EIO
	}

	return f.File.Write(data)
}

func (f *writeFailureFile) Close() error {
	f.closes.Add(1)
	if err := f.File.Close(); err != nil {
		return err
	}
	// This fault reports an error after actual descriptor closure. Other
	// backends may fail Close without physically retiring their resource.
	if f.phase == "close" {
		return syscall.EIO
	}

	return nil
}

func openWriteDescriptors(t *testing.T, path string) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	var count int
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		require.NoError(t, err)
		if target == path {
			count++
		}
	}

	return count
}

func TestWriteRetiresFileOnEveryPath(t *testing.T) {
	t.Parallel()

	for _, phase := range []string{"success", "seek", "write", "close"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()

			builder := chrooted.NewBuilder(cfg.Config{PersistentVolumeMounts: map[string]string{"test": t.TempDir()}})
			teamID, volumeID := uuid.New(), uuid.New()
			root, err := builder.BuildVolumePath("test", teamID, volumeID)
			require.NoError(t, err)
			require.NoError(t, os.MkdirAll(root, 0o700))
			path := filepath.Join(root, "payload")
			require.NoError(t, os.WriteFile(path, []byte("original"), 0o600))
			// /proc reports the resolved path even when TMPDIR is a symlink.
			path, err = filepath.EvalSymlinks(path)
			require.NoError(t, err)
			probe, err := os.Open(path)
			require.NoError(t, err)
			t.Cleanup(func() { _ = probe.Close() })
			require.Equal(t, 1, openWriteDescriptors(t, path), "the physical-FD oracle must see an actual open payload")
			require.NoError(t, probe.Close())
			require.Zero(t, openWriteDescriptors(t, path))
			lifecycleID := uuid.NewString()
			sandboxes := sandbox.NewSandboxesMap()
			sandboxes.AssignNetwork(t.Context(), &sandbox.Sandbox{
				LifecycleID: lifecycleID,
				Metadata: &sandbox.Metadata{
					Config: sandbox.NewConfig(sandbox.Config{
						VolumeMounts: []sandbox.VolumeMountConfig{{ID: volumeID, Name: "volume", Path: "/volume", Type: "test"}},
					}),
					Runtime: sandboxtypes.RuntimeMetadata{SandboxID: uuid.NewString(), TeamID: teamID.String()},
				},
				Resources: &sandbox.Resources{Slot: &network.Slot{Key: "test", HostIP: net.IPv4(127, 0, 0, 1)}},
			})
			proxy, err := NewProxy(t.Context(), builder, sandboxes, nfscfg.Config{})
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, sandboxes.NetworkReleased(context.WithoutCancel(t.Context()), "127.0.0.1"))
				require.Eventually(t, func() bool { return proxy.retirement.count() == 0 }, time.Second, time.Millisecond)
			})
			faults := &writeFailureHandler{Handler: proxy.server.Handler, phase: phase}
			proxy.server.Handler = faults
			// Retain opened files so finalizers cannot hide a missed Close.
			// Rescue cleanup is registered before transport setup and runs only
			// after the ownership and physical-FD assertions below.
			t.Cleanup(func() {
				faults.mu.Lock()
				defer faults.mu.Unlock()
				for _, file := range faults.files {
					if file.closes.Load() == 0 {
						require.NoError(t, file.File.Close())
					}
				}
				require.Zero(t, openWriteDescriptors(t, path))
			})
			var lc net.ListenConfig
			listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			served := make(chan error, 1)
			go func() { served <- proxy.Serve(listener) }()
			t.Cleanup(func() { require.NoError(t, listener.Close()); require.NoError(t, <-served) })
			// Avoid the client's small privileged-source-port pool; the actual
			// proxy admits this isolated loopback connection by sandbox IP.
			client, err := dialRetry(func() (*rpc.Client, error) {
				return rpc.DialTCP("tcp", listener.Addr().String(), false)
			})
			require.NoError(t, err)
			t.Cleanup(client.Close)
			target, err := (&clientnfs.Mount{Client: client}).Mount("/"+lifecycleID+"/volume", rpc.NewAuthUnix("test", 1000, 1000).Auth())
			require.NoError(t, err)
			t.Cleanup(target.Close)
			file, err := target.OpenFile("/payload", 0o600)
			require.NoError(t, err)
			// NFS File.Close sends COMMIT, not a descriptor close. Cleanup owns
			// the client transport instead, after the WRITE request completes.
			_, err = file.Seek(1, io.SeekStart)
			require.NoError(t, err)
			_, err = file.Write([]byte("updated"))
			if phase == "success" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			faults.mu.Lock()
			opened := append([]*writeFailureFile(nil), faults.files...)
			faults.mu.Unlock()
			require.Len(t, opened, 1, "the real WRITE request must own exactly one opened file")
			assert.Equal(t, int64(1), opened[0].closes.Load(), "WRITE must call Close exactly once")
			assert.Zero(t, openWriteDescriptors(t, path), "WRITE must not retain its actual file descriptor")
			want := "oupdated"
			if phase == "seek" {
				want = "original"
			}
			if phase == "write" {
				want = "ouiginal"
			}
			actual, err := os.ReadFile(path)
			require.NoError(t, err)
			assert.Equal(t, want, string(actual), "check real successful/partial mutation, not only status")
		})
	}
}
