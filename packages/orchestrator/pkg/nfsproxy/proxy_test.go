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
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	servernfs "github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/zeldovich/go-rpcgen/rfc1057"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	nfscfg "github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/mountcache"
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

type remoteAddrConn struct {
	net.Conn

	remote net.Addr
}

func (c remoteAddrConn) RemoteAddr() net.Addr {
	return c.remote
}

type blockingChrootBuilder struct {
	mu sync.Mutex

	inner   *chrooted.Builder
	entered chan *chrooted.Chrooted
	proceed chan struct{}
}

func (b *blockingChrootBuilder) arm() (<-chan *chrooted.Chrooted, chan struct{}) {
	entered := make(chan *chrooted.Chrooted, 1)
	proceed := make(chan struct{})

	b.mu.Lock()
	if b.entered != nil || b.proceed != nil {
		b.mu.Unlock()
		panic("chroot builder barrier already armed")
	}
	b.entered = entered
	b.proceed = proceed
	b.mu.Unlock()

	return entered, proceed
}

func (b *blockingChrootBuilder) Chroot(volumeType string, teamID, volumeID uuid.UUID) (*chrooted.Chrooted, error) {
	fs, err := b.inner.Chroot(volumeType, teamID, volumeID)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	entered := b.entered
	proceed := b.proceed
	b.entered = nil
	b.proceed = nil
	b.mu.Unlock()

	if entered != nil {
		entered <- fs
		<-proceed
	}

	return fs, nil
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
	lifecycleID := uuid.NewString()
	sandboxes.AssignNetwork(t.Context(), &sandbox.Sandbox{
		LifecycleID: lifecycleID,
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
	target, err := mount.Mount("/"+lifecycleID+"/"+volName1, auth.Auth())
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
		mkdir(t, target, path)

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
		mkdir(t, target, path)
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

func TestHandleCacheIsolationAcrossVolumes(t *testing.T) { //nolint:paralleltest // exercises a live loopback NFS server
	if syscall.Geteuid() != 0 {
		t.Skip("skipping test as it requires root privileges")
	}

	teamID := uuid.New()
	volumeType := "volume-type-cache-isolation"
	volumeA := uuid.New()
	volumeB := uuid.New()
	volumeAName := "volume-a"
	volumeBName := "volume-b"

	config := cfg.Config{PersistentVolumeMounts: map[string]string{volumeType: t.TempDir()}} //nolint:exhaustruct_v5 // focused test needs only volume roots
	builder := chrooted.NewBuilder(config)
	createVolumeDir(t, builder, volumeType, teamID, volumeA)
	createVolumeDir(t, builder, volumeType, teamID, volumeB)

	sandboxes := sandbox.NewSandboxesMap()
	lifecycleID := uuid.NewString()
	sandboxes.AssignNetwork(t.Context(), &sandbox.Sandbox{ //nolint:exhaustruct_v5 // focused lifecycle fixture
		LifecycleID: lifecycleID,
		Metadata: &sandbox.Metadata{
			Config: sandbox.NewConfig(sandbox.Config{ //nolint:exhaustruct_v5 // focused mount fixture
				VolumeMounts: []sandbox.VolumeMountConfig{
					{ID: volumeA, Name: volumeAName, Path: "/mnt/a", Type: volumeType},
					{ID: volumeB, Name: volumeBName, Path: "/mnt/b", Type: volumeType},
				}}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: uuid.NewString(), TeamID: teamID.String()}, //nolint:exhaustruct_v5 // only routing identity is required
		},
		Resources: &sandbox.Resources{Slot: &network.Slot{Key: "cache-isolation", HostIP: net.IPv4(127, 0, 0, 1)}}, //nolint:exhaustruct_v5 // only the assigned host IP is required
	})

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0") //nolint:exhaustruct_v5 // default listen behavior
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, listener.Close()) })

	proxy, err := newProxy(t.Context(), builder, sandboxes, nfscfg.Config{}, 2) //nolint:exhaustruct_v5 // middleware is intentionally disabled
	require.NoError(t, err)
	go func() { assert.NoError(t, proxy.Serve(listener)) }()

	host, portText, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portText)
	require.NoError(t, err)
	auth := rpc.NewAuthUnix("", 100, 101)

	mountVolume := func(name string) *nfs.Target {
		client, err := dialRetry(func() (*rpc.Client, error) {
			return nfs.DialServiceAtPort(host, port)
		})
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		target, err := (&nfs.Mount{Client: client}).Mount("/"+lifecycleID+"/"+name, auth.Auth()) //nolint:exhaustruct_v5 // direct-port client has no Addr
		require.NoError(t, err)

		return target
	}

	targetA := mountVolume(volumeAName)
	targetB := mountVolume(volumeBName)
	handleA := mkdir(t, targetA, "keep")

	// More than B's ordinary-handle capacity must not consume A's shard.
	for _, name := range []string{"one", "two", "three", "four"} {
		mkdir(t, targetB, name)
	}

	_, err = targetA.GetAttr(handleA)
	require.NoError(t, err, "mount B churn must not stale mount A's original raw handle")

	require.NoError(t, sandboxes.NetworkReleased(t.Context(), "127.0.0.1"))
	require.NoError(t, sandboxes.NetworkReleased(t.Context(), "127.0.0.1"), "repeated release is idempotent")
	require.Eventually(t, func() bool { return proxy.retirement.count() == 0 }, time.Second, time.Millisecond)
	_, _, err = proxy.cache.FromHandle(t.Context(), handleA)
	var stale *servernfs.NFSStatusError
	require.ErrorAs(t, err, &stale)
	require.Equal(t, servernfs.NFSStatusStale, stale.NFSStatus)
}

func TestNetworkReleaseClosesAdmissionBeforePhysicalDrain(t *testing.T) { //nolint:paralleltest // event-controls one shared lifecycle
	if syscall.Geteuid() != 0 {
		t.Skip("skipping test as it requires root privileges")
	}

	teamID := uuid.New()
	sandboxID := uuid.NewString()
	oldLifecycleID := uuid.NewString()
	newLifecycleID := uuid.NewString()
	volumeType := "volume-type-lifecycle-admission"
	volumeID := uuid.New()
	volumeName := "volume"
	secondVolumeID := uuid.New()
	secondVolumeName := "second-volume"
	ip := net.IPv4(127, 0, 0, 1)

	config := cfg.Config{PersistentVolumeMounts: map[string]string{volumeType: t.TempDir()}} //nolint:exhaustruct_v5 // focused test needs only volume roots
	rawBuilder := chrooted.NewBuilder(config)
	createVolumeDir(t, rawBuilder, volumeType, teamID, volumeID)
	createVolumeDir(t, rawBuilder, volumeType, teamID, secondVolumeID)
	builder := &blockingChrootBuilder{
		mu:      sync.Mutex{},
		inner:   rawBuilder,
		entered: nil,
		proceed: nil,
	}

	newSandbox := func(lifecycleID string) *sandbox.Sandbox {
		return &sandbox.Sandbox{ //nolint:exhaustruct_v5 // focused lifecycle fixture
			LifecycleID: lifecycleID,
			Metadata: &sandbox.Metadata{
				Config: sandbox.NewConfig(sandbox.Config{ //nolint:exhaustruct_v5 // focused mount fixture
					VolumeMounts: []sandbox.VolumeMountConfig{{ID: volumeID, Name: volumeName, Path: "/mnt/volume", Type: volumeType}, {ID: secondVolumeID, Name: secondVolumeName, Path: "/mnt/second", Type: volumeType}},
				}),
				Runtime: sandboxtypes.RuntimeMetadata{SandboxID: sandboxID, TeamID: teamID.String()}, //nolint:exhaustruct_v5 // only routing identity is required
			},
			Resources: &sandbox.Resources{Slot: &network.Slot{Key: lifecycleID, HostIP: ip}}, //nolint:exhaustruct_v5 // only the assigned host IP is required
		}
	}

	sandboxes := sandbox.NewSandboxesMap()
	sandboxes.AssignNetwork(t.Context(), newSandbox(oldLifecycleID))
	proxy, err := newProxy(t.Context(), builder, sandboxes, nfscfg.Config{}, 2) //nolint:exhaustruct_v5 // middleware is intentionally disabled
	require.NoError(t, err)

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		require.NoError(t, serverConn.Close())
		require.NoError(t, clientConn.Close())
	})
	conn := remoteAddrConn{Conn: serverConn, remote: &net.TCPAddr{IP: ip, Port: 2049, Zone: ""}}
	request := servernfs.MountRequest{Dirpath: []byte("/" + oldLifecycleID + "/" + volumeName)} //nolint:exhaustruct_v5 // only the requested path is required

	status, oldFilesystem, _ := proxy.cache.Mount(t.Context(), conn, request)
	require.Equal(t, servernfs.MountStatusOk, status)
	oldHandle := proxy.cache.ToHandle(t.Context(), oldFilesystem, nil)
	require.NotEmpty(t, oldHandle)

	entered, proceed := builder.arm()
	type mountResult struct {
		status     servernfs.MountStatus
		filesystem billy.Filesystem
	}
	mountDone := make(chan mountResult, 1)
	go func() {
		secondRequest := servernfs.MountRequest{Dirpath: []byte("/" + oldLifecycleID + "/" + secondVolumeName)} //nolint:exhaustruct_v5 // exercise the first actual allocation of the second volume
		mountStatus, filesystem, _ := proxy.cache.Mount(t.Context(), conn, secondRequest)
		mountDone <- mountResult{status: mountStatus, filesystem: filesystem}
	}()

	allocated := <-entered
	owner := mountcache.Owner{SandboxID: sandboxID, LifecycleID: oldLifecycleID}
	closed, ok := proxy.gate.OwnerClosed(owner)
	require.True(t, ok)
	releaseDone := make(chan error, 1)
	go func() { releaseDone <- sandboxes.NetworkReleased(t.Context(), ip.String()) }()
	<-closed
	require.NoError(t, <-releaseDone, "closed admission and stale handles permit network release before the old MOUNT drains")

	close(proceed)
	late := <-mountDone
	require.Equal(t, servernfs.MountStatusErrAcces, late.status)
	require.Eventually(t, func() bool { return proxy.retirement.count() == 0 }, time.Second, time.Millisecond)
	assert.Empty(t, proxy.cache.ToHandle(t.Context(), late.filesystem, nil), "closed lifecycle must yield no usable mount handle")
	_, err = allocated.Stat("after-release")
	require.Error(t, err, "the late allocation must be closed and discarded")

	shards, owners := proxy.cache.Counts()
	assert.Zero(t, shards)
	assert.Zero(t, owners)
	assert.Zero(t, proxy.chroots.Count())
	_, _, err = proxy.cache.FromHandle(t.Context(), oldHandle)
	var stale *servernfs.NFSStatusError
	require.ErrorAs(t, err, &stale)
	require.Equal(t, servernfs.NFSStatusStale, stale.NFSStatus)
	require.NoError(t, sandboxes.NetworkReleased(t.Context(), ip.String()), "repeated release must remain idempotent")

	sandboxes.AssignNetwork(t.Context(), newSandbox(newLifecycleID))
	status, _, _ = proxy.cache.Mount(t.Context(), conn, request)
	require.Equal(t, servernfs.MountStatusErrAcces, status, "the old captured export cannot mount a replacement")
	replacementConn := remoteAddrConn{remote: &net.TCPAddr{IP: ip, Port: 32769}}
	replacementRequest := servernfs.MountRequest{Dirpath: []byte("/" + newLifecycleID + "/" + volumeName)}
	status, replacementFilesystem, _ := proxy.cache.Mount(t.Context(), replacementConn, replacementRequest)
	require.Equal(t, servernfs.MountStatusOk, status)
	replacementHandle := proxy.cache.ToHandle(t.Context(), replacementFilesystem, nil)
	require.NotEmpty(t, replacementHandle)
	resolved, _, err := proxy.cache.FromHandle(t.Context(), replacementHandle)
	require.NoError(t, err)
	require.Same(t, replacementFilesystem, resolved)
	shards, owners = proxy.cache.Counts()
	assert.Equal(t, 1, shards)
	assert.Equal(t, 1, owners)
	assert.Equal(t, 1, proxy.chroots.Count())
	require.NoError(t, sandboxes.NetworkReleased(t.Context(), ip.String()))
	require.Eventually(t, func() bool { return proxy.retirement.count() == 0 }, time.Second, time.Millisecond)
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

func mkdir(t *testing.T, target *nfs.Target, path string) []byte {
	t.Helper()

	fh, err := target.Mkdir(path, 0o755)
	require.NoError(t, err)

	return fh
}
