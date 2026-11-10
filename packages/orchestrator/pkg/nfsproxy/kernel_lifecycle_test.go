//go:build linux

package nfsproxy

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	nfscfg "github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
)

// This uses separate Linux NFS mounts, not the userspace RPC client. Explicit
// mount and namespace opt-ins keep ordinary unit runs from mounting host paths.
func TestKernelMountRetainsRootAcrossLifecycleReplacement(t *testing.T) { //nolint:paralleltest // real kernel mounts and process-wide telemetry
	if os.Getenv("NFS_TEST_KERNEL_MOUNT") != "1" {
		t.Skip("set NFS_TEST_KERNEL_MOUNT=1 with an NFS mount helper in private network/mount namespaces")
	}
	requirePrivateNativeNetwork(t)
	helper := os.Getenv("NFS_TEST_MOUNT_HELPER")
	require.NotEmpty(t, helper, "select the actual mount.nfs binary")
	f := newConcernFixture(t, nfscfg.Config{Tracing: true, RecordHandleCalls: true}, nil)
	require.NoError(t, os.WriteFile(filepath.Join(f.paths[0], "generation"), []byte("original backing root"), 0o600))
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	served := make(chan error, 1)
	go func() { served <- f.proxy.Serve(listener) }()
	t.Cleanup(func() {
		require.NoError(t, listener.Close())
		select {
		case err := <-served:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("kernel-test NFS listener did not join")
		}
	})
	port := listener.Addr().(*net.TCPAddr).Port
	options := fmt.Sprintf("vers=3,mountport=%d,mountproto=tcp,port=%d,proto=tcp,noacl,nolock,noresvport,nosharecache,soft,timeo=10,retrans=1,actimeo=0,lookupcache=none,retry=0", port, port)
	mount := func(export string, wantSuccess bool) string {
		t.Helper()
		target := t.TempDir()
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, helper, "127.0.0.1:"+export, target, "-n", "-o", options)
		output, mountErr := cmd.CombinedOutput()
		require.NoError(t, ctx.Err(), "mount helper must return rather than be killed: %s", output)
		if !wantSuccess {
			if mountErr == nil {
				require.NoError(t, unix.Unmount(target, 0), "unexpectedly accepted export must still be unmounted")
				t.Fatalf("kernel accepted stale or plain export %q", export)
			}
			require.Contains(t, strings.ToLower(string(output)), "access denied", "rejection must come from MOUNT access validation, not setup failure")

			return ""
		}
		require.NoError(t, mountErr, "kernel mount %s: %s", export, output)
		t.Cleanup(func() { require.NoError(t, unix.Unmount(target, 0), "kernel mount must physically unmount") })

		return target
	}
	readGeneration := func(target, want string) {
		t.Helper()
		data, readErr := os.ReadFile(filepath.Join(target, "generation"))
		require.NoError(t, readErr)
		require.Equal(t, want, string(data), "assert the bytes consumed through the kernel mount")
	}

	oldExport := f.mountPath("first")
	oldMount := mount(oldExport, true)
	readGeneration(oldMount, "original backing root")
	mount("/first", false)

	// Replacing the path must not retarget either the existing mount or a new
	// mount of the same lifecycle: both belong to the already-open root.
	require.NoError(t, os.Rename(f.paths[0], f.paths[0]+"-original"))
	require.NoError(t, os.Mkdir(f.paths[0], 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(f.paths[0], "generation"), []byte("replacement backing root"), 0o600))
	remount := mount(oldExport, true)
	readGeneration(oldMount, "original backing root")
	readGeneration(remount, "original backing root")
	require.NoError(t, os.WriteFile(filepath.Join(oldMount, "before-release"), []byte("owned by original root"), 0o600))
	data, err := os.ReadFile(filepath.Join(f.paths[0]+"-original", "before-release"))
	require.NoError(t, err)
	require.Equal(t, "owned by original root", string(data))
	_, err = os.Stat(filepath.Join(f.paths[0], "before-release"))
	require.ErrorIs(t, err, os.ErrNotExist)

	oldSandbox, err := f.sandboxes.GetByHostPort("127.0.0.1:1")
	require.NoError(t, err)
	require.NoError(t, f.sandboxes.NetworkReleased(t.Context(), "127.0.0.1"))
	require.Eventually(t, func() bool { return f.proxy.retirement.count() == 0 }, 5*time.Second, time.Millisecond)
	fresh := &sandbox.Sandbox{Metadata: oldSandbox.Metadata, Resources: oldSandbox.Resources, LifecycleID: uuid.NewString()}
	f.sandboxes.AssignNetwork(t.Context(), fresh)
	f.owner.LifecycleID = fresh.LifecycleID
	require.NoError(t, os.WriteFile(filepath.Join(f.paths[0], "replacement-only"), []byte("not the old mount"), 0o600))
	for _, target := range []string{oldMount, remount} {
		_, statErr := os.Stat(filepath.Join(target, "replacement-only"))
		require.ErrorIs(t, statErr, syscall.ESTALE, "old kernel root handle must be stale, not rebound to the replacement")
	}
	mount(oldExport, false)
	mount("/first", false)
	freshMount := mount(fresh.NFSMountPath("first"), true)
	readGeneration(freshMount, "replacement backing root")
	require.NoError(t, os.Rename(filepath.Join(freshMount, "generation"), filepath.Join(freshMount, "renamed")))
	data, err = os.ReadFile(filepath.Join(f.paths[0], "renamed"))
	require.NoError(t, err)
	require.Equal(t, "replacement backing root", string(data))
}
