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
	hostPath  string

	// mount mounts the volume again over a new connection.
	mount func() (*nfs.Target, error)
}

// mountVolume serves one sandbox with one volume through a proxy built from
// config and mounts it.
func mountVolume(t *testing.T, config nfscfg.Config) servedVolume {
	t.Helper()

	if syscall.Geteuid() != 0 {
		t.Skip("skipping test as it requires root privileges")
	}

	teamID, volumeID := uuid.New(), uuid.New()
	const volumeType, volumeName = "volume-type", "volume"

	builder := chrooted.NewBuilder(cfg.Config{
		PersistentVolumeMounts: map[string]string{volumeType: t.TempDir()},
	})
	createVolumeDir(t, builder, volumeType, teamID, volumeID)
	hostPath, err := builder.BuildVolumePath(volumeType, teamID, volumeID)
	require.NoError(t, err)

	sandboxes := sandbox.NewSandboxesMap()
	sandboxes.AssignNetwork(t.Context(), &sandbox.Sandbox{
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
	})

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, listener.Close()) })

	proxy, err := NewProxy(t.Context(), builder, sandboxes, config)
	require.NoError(t, err)
	go func() { assert.NoError(t, proxy.Serve(listener)) }()

	host, portText, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portText)
	require.NoError(t, err)

	mount := func() (*nfs.Target, error) {
		client, err := dialRetry(func() (*rpc.Client, error) { return nfs.DialServiceAtPort(host, port) })
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		return (&nfs.Mount{Client: client}).Mount("/"+volumeName, rpc.NewAuthUnix("", 0, 0).Auth())
	}

	target, err := mount()
	require.NoError(t, err)

	return servedVolume{target: target, sandboxes: sandboxes, hostPath: hostPath, mount: mount}
}

// git holds .git/index.lock open for a whole checkout and writes it last; the
// handle must still resolve after the checkout created thousands of files.
func TestHeldFileSurvivesConfiguredNumberOfNewFiles(t *testing.T) {
	t.Parallel()

	target := mountVolume(t, nfscfg.Config{HandleCacheLimit: 4096}).target

	mkdir(t, target, "src", 0o755)
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

// Mounting a volume again reuses its filesystem, so its handles stay the same.
func TestRemountingAVolumeSharesItsHandles(t *testing.T) {
	t.Parallel()

	volume := mountVolume(t, nfscfg.Config{})
	writeFile(t, volume.target, "file", "x", 0o644)
	_, before, err := volume.target.Lookup("file")
	require.NoError(t, err)

	again, err := volume.mount()
	require.NoError(t, err)
	_, after, err := again.Lookup("file")
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestRemountFailsWhenTheVolumeRootIsGone(t *testing.T) {
	t.Parallel()

	volume := mountVolume(t, nfscfg.Config{})
	require.NoError(t, os.RemoveAll(volume.hostPath))

	_, err := volume.mount()
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
