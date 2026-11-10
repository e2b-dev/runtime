//go:build linux

package nfsproxy

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"syscall"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gonfs "github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	nfscfg "github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

type servedVolume struct {
	target    *nfs.Target
	sandboxes *sandbox.Map
	sbx       *sandbox.Sandbox
	hostPath  string

	// mount mounts the volume again over a new connection.
	mount func(*sandbox.Sandbox) (*nfs.Target, error)
}

// mountVolume serves one sandbox with one volume through a proxy built from
// config and mounts it.
func mountVolume(t *testing.T, config nfscfg.Config) servedVolume {
	t.Helper()

	if syscall.Geteuid() != 0 {
		t.Skip("skipping test as it requires root privileges")
	}

	return serveUserspaceVolume(t, config)
}

func serveUserspaceVolume(t *testing.T, config nfscfg.Config) servedVolume {
	t.Helper()

	// Chrooted confines paths with openat2 descriptors, not a process chroot.
	// Temporary directories and a userspace RPC client need no root privileges
	// or kernel NFS mount, so the handle lifecycle must also be tested rootless.

	teamID, volumeID := uuid.New(), uuid.New()
	const volumeType, volumeName = "volume-type", "volume"

	builder := chrooted.NewBuilder(cfg.Config{
		PersistentVolumeMounts: map[string]string{volumeType: t.TempDir()},
	})
	createVolumeDir(t, builder, volumeType, teamID, volumeID)
	hostPath, err := builder.BuildVolumePath(volumeType, teamID, volumeID)
	require.NoError(t, err)

	sandboxes := sandbox.NewSandboxesMap()
	sbx := &sandbox.Sandbox{
		LifecycleID: uuid.NewString(),
		Metadata: &sandbox.Metadata{
			Config: sandbox.NewConfig(sandbox.Config{
				VolumeMounts: []sandbox.VolumeMountConfig{
					{ID: volumeID, Name: volumeName, Path: "/mnt/volume", Type: volumeType},
				},
			}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: uuid.NewString(), TeamID: teamID.String()},
		},
		Resources: &sandbox.Resources{
			Slot: &network.Slot{Key: uuid.NewString(), HostIP: net.IPv4(127, 0, 0, 1)},
		},
	}
	sandboxes.AssignNetwork(t.Context(), sbx)

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	proxy, err := NewProxy(t.Context(), builder, sandboxes, config)
	if err != nil {
		require.NoError(t, listener.Close())
	}
	require.NoError(t, err)
	serveDone := make(chan struct{})
	t.Cleanup(func() {
		assert.NoError(t, listener.Close())
		<-serveDone
	})
	go func() {
		defer close(serveDone)
		assert.NoError(t, proxy.Serve(listener))
	}()

	host, portText, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portText)
	require.NoError(t, err)

	mount := func(owner *sandbox.Sandbox) (*nfs.Target, error) {
		client, err := dialRetry(func() (*rpc.Client, error) { return nfs.DialServiceAtPort(host, port) })
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		return (&nfs.Mount{Client: client}).Mount(owner.NFSMountPath(volumeName), rpc.NewAuthUnix("", 0, 0).Auth())
	}

	target, err := mount(sbx)
	require.NoError(t, err)

	return servedVolume{target: target, sandboxes: sandboxes, sbx: sbx, hostPath: hostPath, mount: mount}
}

// git holds .git/index.lock open for a whole checkout and writes it last; the
// handle must still resolve after the checkout created thousands of files.
func TestHeldFileSurvivesConfiguredNumberOfNewFiles(t *testing.T) {
	t.Parallel()

	target := mountVolume(t, nfscfg.Config{HandleCacheLimit: 4096}).target

	mkdir(t, target, "src")
	lock, err := target.OpenFile("index.lock", 0o644)
	require.NoError(t, err)

	for i := range 1500 {
		writeFile(t, target, fmt.Sprintf("src/file-%d", i), "x", 0o644)
	}

	_, err = lock.Write([]byte("index"))
	require.NoError(t, err)
	assert.NoError(t, lock.Close())
}

func TestSandboxReleaseMakesItsHandlesStale(t *testing.T) {
	t.Parallel()

	volume := mountVolume(t, nfscfg.Config{})
	held, err := volume.target.OpenFile("held", 0o644)
	require.NoError(t, err)

	require.NoError(t, volume.sandboxes.NetworkReleased(t.Context(), "127.0.0.1"))

	_, err = held.Write([]byte("late"))
	var status *nfs.Error
	require.ErrorAs(t, err, &status)
	assert.Equal(t, uint32(nfs.NFS3ErrStale), status.ErrorNum)
}

// All mounts here use the userspace NFS client over owned loopback sockets, not
// a kernel mount. A delayed old release must close old handles, never new users
// reached through the exact same slot pointer and IP.
func TestCapturedNetworkReleaseMakesOnlyOldNFSHandlesStale(t *testing.T) {
	t.Parallel()

	volume := serveUserspaceVolume(t, nfscfg.Config{})
	old := volume.sbx
	oldHeld, err := volume.target.OpenFile("old-held", 0o644)
	require.NoError(t, err)
	volume.sandboxes.TrackLifecycle(t.Context(), old)
	release := volume.sandboxes.NetworkRelease(old.Runtime.SandboxID, old.LifecycleID)
	replacement := &sandbox.Sandbox{
		LifecycleID: uuid.NewString(),
		Metadata:    old.Metadata,
		Resources:   old.Resources,
	}
	volume.sandboxes.AssignNetwork(t.Context(), replacement)
	newTarget, err := volume.mount(replacement)
	require.NoError(t, err)
	newHeld, err := newTarget.OpenFile("new-held", 0o644)
	require.NoError(t, err)

	require.NoError(t, release(t.Context(), old.Slot.HostIPString()))
	volume.sandboxes.MarkStopped(t.Context(), old)
	require.NoError(t, release(t.Context(), old.Slot.HostIPString()))
	_, err = oldHeld.Write([]byte("late old write"))
	var status *nfs.Error
	require.ErrorAs(t, err, &status, "the old lifecycle's real NFS handles must be retired")
	assert.Equal(t, uint32(nfs.NFS3ErrStale), status.ErrorNum)
	_, err = newHeld.Write([]byte("new owner write"))
	require.NoError(t, err, "old callback must not retire replacement NFS users")
	require.NoError(t, newHeld.Close())
	got, err := volume.sandboxes.GetByHostPort(net.JoinHostPort(old.Slot.HostIPString(), "80"))
	require.NoError(t, err, "old callback must not remove the replacement NFS route")
	require.Same(t, replacement, got)
	require.NoError(t, volume.sandboxes.NetworkRelease(replacement.Runtime.SandboxID, replacement.LifecycleID)(t.Context(), replacement.Slot.HostIPString()))
}

// Mounting a volume again reuses its filesystem, so its handles stay the same.
func TestRemountingAVolumeSharesItsHandles(t *testing.T) {
	t.Parallel()

	volume := mountVolume(t, nfscfg.Config{})
	writeFile(t, volume.target, "file", "x", 0o644)
	_, before, err := volume.target.Lookup("file")
	require.NoError(t, err)

	again, err := volume.mount(volume.sbx)
	require.NoError(t, err)
	_, after, err := again.Lookup("file")
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestRemountFailsWhenTheVolumeRootIsGone(t *testing.T) {
	t.Parallel()

	volume := mountVolume(t, nfscfg.Config{})
	require.NoError(t, os.RemoveAll(volume.hostPath))

	_, err := volume.mount(volume.sbx)
	assert.Error(t, err)
}

func TestNewProxyRejectsInvalidLimits(t *testing.T) {
	t.Parallel()

	for name, config := range map[string]nfscfg.Config{
		"handle cache of one":    {HandleCacheLimit: 1},
		"negative handle cache":  {HandleCacheLimit: -1},
		"negative verifier size": {DirVerifierLimit: -1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := NewProxy(t.Context(), chrooted.NewBuilder(cfg.Config{}), sandbox.NewSandboxesMap(), config)
			assert.Error(t, err)
		})
	}
}

// go-nfs only pages directory listings from the verifier cache when the
// handler it is given implements nfs.CachingHandler.
func TestOutermostHandlerCachesDirectoryListings(t *testing.T) {
	t.Parallel()

	proxy, err := NewProxy(t.Context(), chrooted.NewBuilder(cfg.Config{}), sandbox.NewSandboxesMap(), nfscfg.Config{
		Logging: true,
		Tracing: true,
		Metrics: true,
	})
	require.NoError(t, err)

	_, ok := proxy.server.Handler.(gonfs.CachingHandler)
	assert.True(t, ok, "%T does not implement nfs.CachingHandler", proxy.server.Handler)
}
