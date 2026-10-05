//go:build linux

package nfsproxy

import (
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/zeldovich/go-rpcgen/rfc1057"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	nfscfg "github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/portmap"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

func createVolumeDir(t *testing.T, builder *chrooted.Builder, volumeType string, teamID, volumeID uuid.UUID) {
	t.Helper()

	fullVolumePath, err := builder.BuildVolumePath(volumeType, teamID, volumeID)
	require.NoError(t, err)

	t.Logf("creating volume dir: %s", fullVolumePath)
	err = os.MkdirAll(fullVolumePath, 0o755)
	require.NoError(t, err)
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	if syscall.Geteuid() != 0 {
		t.Skip("skipping test as it requires root privileges")
	}

	// setup logging
	logCfg := zap.NewDevelopmentConfig()
	logCfg.DisableStacktrace = true
	log, err := logCfg.Build(zap.AddStacktrace(zap.ErrorLevel))
	require.NoError(t, err)
	zap.ReplaceGlobals(log)

	// setup data
	sandboxID := uuid.NewString()
	teamID := uuid.New()
	volPath1 := t.TempDir()
	volType1 := "volume-type-1"
	volID1 := uuid.New()
	volName1 := "volume-1"
	volID2 := uuid.New()
	volName2 := "volume-2"
	volType2 := "volume-type-2"

	// set up paths
	config := cfg.Config{
		PersistentVolumeMounts: map[string]string{
			volType1: volPath1,
		},
	}

	builder := chrooted.NewBuilder(config)

	createVolumeDir(t, builder, volType1, teamID, volID1)
	createVolumeDir(t, builder, volType1, teamID, volID2)

	slot := &network.Slot{Key: "abc", HostIP: net.IPv4(127, 0, 0, 1)}
	require.Equal(t, "127.0.0.1", slot.HostIP.String(), "required for the test to work")

	sandboxes := sandbox.NewSandboxesMap()
	sandboxes.AssignNetwork(t.Context(), &sandbox.Sandbox{
		Metadata: &sandbox.Metadata{
			Config: sandbox.NewConfig(sandbox.Config{
				VolumeMounts: []sandbox.VolumeMountConfig{
					{ID: volID1, Name: volName1, Path: "/mnt/vol1", Type: volType1},
					{ID: volID2, Name: volName2, Path: "/mnt/vol2", Type: volType2},
				},
			}),
			Runtime: sandboxtypes.RuntimeMetadata{
				SandboxID: sandboxID,
				TeamID:    teamID.String(),
			},
		},
		Resources: &sandbox.Resources{
			Slot: slot,
		},
	})

	// setup nfs proxy server
	nfsConfig := net.ListenConfig{}
	nfsListener, err := nfsConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		err := nfsListener.Close()
		assert.NoError(t, err)
	})

	nfsProxy, err := NewProxy(t.Context(), builder, sandboxes, nfscfg.Config{})
	require.NoError(t, err)
	go func() {
		err := nfsProxy.Serve(nfsListener)
		assert.NoError(t, err)
	}()

	// get nfs server's dynamic port
	nfsAddr := nfsListener.Addr().String()
	host, portText, err := net.SplitHostPort(nfsAddr)
	require.NoError(t, err)
	port, err := strconv.Atoi(portText)
	require.NoError(t, err)

	// setup portmap server
	portmapConfig := net.ListenConfig{}
	pmListener, err := portmapConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		err := pmListener.Close()
		assert.NoError(t, err)
	})

	pm := portmap.NewPortMap(t.Context())
	pm.RegisterPort(t.Context(), uint32(port))
	go func() {
		err := pm.Serve(t.Context(), pmListener)
		assert.NoError(t, err)
	}()

	// connect via nfs client
	auth := rpc.NewAuthUnix("", 100, 101)

	// dial portmap server
	portmapperTCPClient, err := dialRetry(func() (*rpc.Client, error) {
		return rpc.DialTCP("tcp", pmListener.Addr().String(), false)
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		portmapperTCPClient.Close()
	})
	portmapperClient := &rpc.Portmapper{Client: portmapperTCPClient}
	t.Cleanup(func() {
		portmapperClient.Close()
	})

	retrievedPort, err := portmapperClient.Getport(rpc.Mapping{
		Prog: nfs.Nfs3Prog,
		Vers: nfs.Nfs3Vers,
		Prot: rfc1057.IPPROTO_TCP,
	})
	require.NoError(t, err)

	nfsClient, err := dialRetry(func() (*rpc.Client, error) {
		return nfs.DialServiceAtPort(host, retrievedPort)
	})
	require.NoError(t, err)

	// request mount
	mount := &nfs.Mount{
		Client: nfsClient,
	}
	target, err := mount.Mount("/"+volName1, auth.Auth())
	require.NoError(t, err)

	t.Run("write file", func(t *testing.T) {
		t.Parallel()

		// write a file through nfs
		const perms os.FileMode = 0o642
		fp, err := target.OpenFile("/sandbox-id.txt", perms)
		require.NoError(t, err)
		data := []byte(sandboxID)
		n, err := fp.Write(data)
		require.NoError(t, err)
		assert.Equal(t, len(data), n)
		err = fp.Close()
		require.NoError(t, err)

		// verify file contents directly
		objectName := filepath.Join(volPath1, "team-"+teamID.String(), "vol-"+volID1.String(), "sandbox-id.txt")
		object, err := os.Open(objectName)
		require.NoError(t, err)

		// verify metadata
		stat, err := object.Stat()
		require.NoError(t, err)
		assert.Equal(t, perms, stat.Mode().Perm(), "wrong permissions for %s", objectName)

		// verify contents
		data, err = io.ReadAll(object)
		require.NoError(t, err)
		assert.Equal(t, sandboxID, string(data))
	})

	t.Run("mkdir", func(t *testing.T) {
		t.Parallel()

		path := uuid.NewString()
		fh, err := target.Mkdir(path, 0o755)
		require.NoError(t, err)
		assert.NotNil(t, fh)
	})

	t.Run("list file in nfs", func(t *testing.T) {
		t.Parallel()

		// setup root dir, to prevent collisions
		path := uuid.NewString()
		mkdir(t, target, path, 0o755)

		// write files
		writeFile(t, target, filepath.Join(path, "file.txt"), "file.txt contents", 0o644)
		writeFile(t, target, filepath.Join(path, "file2.txt"), "file2.txt contents", 0o755)

		// ensure files can be listed
		items, err := target.ReadDirPlus(path)
		require.NoError(t, err)
		require.Len(t, items, 2)

		// normalize the order
		slices.SortFunc(items, func(a, b *nfs.EntryPlus) int {
			return strings.Compare(a.Name(), b.Name())
		})

		assert.Equal(t, "file.txt", items[0].Name())
		assert.Equal(t, os.FileMode(0o644), items[0].Mode())
		assert.Equal(t, "file2.txt", items[1].Name())
		assert.Equal(t, os.FileMode(0o755), items[1].Mode())
	})

	t.Run("access", func(t *testing.T) {
		t.Parallel()

		path := uuid.NewString()
		mkdir(t, target, path, 0o755)
		writeFile(t, target, filepath.Join(path, "file.txt"), "file.txt contents", 0o644)
		mode, err := target.Access(filepath.Join(path, "file.txt"), 0o644)
		require.NoError(t, err)
		assert.Equal(t, uint32(0o644), mode)
	})

	t.Run("lookup missing file", func(t *testing.T) {
		t.Parallel()

		// verify that file can be read with getattr
		path := uuid.NewString()
		stat1, fh1, err := target.Lookup(path)
		require.ErrorIs(t, err, os.ErrNotExist)
		assert.Nil(t, fh1)
		assert.Nil(t, stat1)
	})
}

// dialRetry retries on EADDRINUSE: the nfs client library binds a random
// ephemeral local port and doesn't retry collisions for non-privileged dials.
func dialRetry[T any](dial func() (T, error)) (ret T, err error) {
	for range 5 {
		ret, err = dial()
		if !errors.Is(err, syscall.EADDRINUSE) {
			return ret, err
		}
	}

	return ret, err
}

func writeFile(t *testing.T, target *nfs.Target, path string, content string, perm os.FileMode) {
	t.Helper()

	fp, err := target.OpenFile(path, perm)
	require.NoError(t, err)

	n, err := fp.Write([]byte(content))
	require.NoError(t, err)
	assert.Equal(t, len(content), n, "wrong number of bytes written")

	err = fp.Close()
	require.NoError(t, err)
}

func mkdir(t *testing.T, target *nfs.Target, path string, perm os.FileMode) []byte {
	t.Helper()

	fh, err := target.Mkdir(path, perm)
	require.NoError(t, err)

	return fh
}
