//go:build linux

package nfsproxy

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	servernfs "github.com/willscott/go-nfs"
	clientnfs "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	nfscfg "github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/chroot"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/mountcache"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

type concernFixture struct {
	proxy     *Proxy
	sandboxes *sandbox.Map
	builder   *chrooted.Builder
	teamID    uuid.UUID
	ids       [2]uuid.UUID
	paths     [2]string
	owner     mountcache.Owner
	conn      remoteAddrConn
}

func newConcernFixture(t *testing.T, config nfscfg.Config, wrap func(*chrooted.Builder) chroot.Builder) *concernFixture {
	t.Helper()
	require.Zero(t, os.Geteuid(), "actual chroot proof must run as root, never skip")

	return buildConcernFixture(t, config, wrap)
}

// buildConcernFixture also supports selected unprivileged NFS wire checks.
func buildConcernFixture(t *testing.T, config nfscfg.Config, wrap func(*chrooted.Builder) chroot.Builder) *concernFixture {
	t.Helper()
	conn, peer := net.Pipe()
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
		require.NoError(t, peer.Close())
	})
	f := &concernFixture{teamID: uuid.New(), ids: [2]uuid.UUID{uuid.New(), uuid.New()}, owner: mountcache.Owner{SandboxID: uuid.NewString(), LifecycleID: uuid.NewString()}, conn: remoteAddrConn{Conn: conn, remote: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 32768}}}
	f.builder = chrooted.NewBuilder(cfg.Config{PersistentVolumeMounts: map[string]string{"proof": t.TempDir()}})
	for i, id := range f.ids {
		p, err := f.builder.BuildVolumePath("proof", f.teamID, id)
		require.NoError(t, err)
		f.paths[i] = p
		require.NoError(t, os.MkdirAll(p, 0o755))
	}
	f.sandboxes = sandbox.NewSandboxesMap()
	f.sandboxes.AssignNetwork(t.Context(), &sandbox.Sandbox{LifecycleID: f.owner.LifecycleID, Metadata: &sandbox.Metadata{Config: sandbox.NewConfig(sandbox.Config{VolumeMounts: []sandbox.VolumeMountConfig{{ID: f.ids[0], Name: "first", Path: "/first", Type: "proof"}, {ID: f.ids[1], Name: "second", Path: "/second", Type: "proof"}}}), Runtime: sandboxtypes.RuntimeMetadata{SandboxID: f.owner.SandboxID, TeamID: f.teamID.String()}}, Resources: &sandbox.Resources{Slot: &network.Slot{Key: "proof", HostIP: net.IPv4(127, 0, 0, 1)}}})
	var b chroot.Builder = f.builder
	if wrap != nil {
		b = wrap(f.builder)
	}
	var err error
	f.proxy, err = newProxy(t.Context(), b, f.sandboxes, config, 2)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, f.sandboxes.NetworkReleased(context.WithoutCancel(t.Context()), "127.0.0.1"))
		require.Eventually(t, func() bool { return f.proxy.retirement.count() == 0 }, time.Second, time.Millisecond,
			"the fixture must join physical descriptor cleanup separately from logical network release")
	})

	return f
}

func (f *concernFixture) mountPath(name string) string {
	return "/" + f.owner.LifecycleID + "/" + name
}

func (f *concernFixture) mount(t *testing.T) billy.Filesystem {
	t.Helper()
	status, fs, _ := f.proxy.server.Handler.Mount(t.Context(), f.conn, servernfs.MountRequest{Dirpath: []byte(f.mountPath("first"))})
	require.Equal(t, servernfs.MountStatusOk, status)
	require.NotNil(t, fs)

	return fs
}

func concernFilesystemIdentity(fs billy.Filesystem) billy.Filesystem {
	for {
		wrapper, ok := fs.(interface{ Unwrap() billy.Filesystem })
		if !ok {
			return fs
		}
		fs = wrapper.Unwrap()
	}
}

func readConcernFile(t *testing.T, fs billy.Filesystem, name string) string {
	t.Helper()
	file, err := fs.Open(name)
	require.NoError(t, err)
	contents, err := io.ReadAll(file)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	return string(contents)
}

var (
	concernMeterOnce   sync.Once
	concernMeterReader *sdkmetric.ManualReader
	concernTracerOnce  sync.Once
	concernSpans       *tracetest.InMemoryExporter
)

func concernOpenCount(t *testing.T) int64 {
	t.Helper()
	var data metricdata.ResourceMetrics
	require.NoError(t, concernMeterReader.Collect(t.Context(), &data))
	var opens int64
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "orchestrator.nfsproxy.calls.total" {
				continue
			}
			sums, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, p := range sums.DataPoints {
				if v, ok := p.Attributes.Value(attribute.Key("operation")); ok && v.AsString() == "FS.Open" {
					opens += p.Value
				}
			}
		}
	}

	return opens
}

func TestRootHandlePreservesRealMiddlewareAndMetrics(t *testing.T) { //nolint:paralleltest // process-wide telemetry and actual root-FD barriers
	concernMeterOnce.Do(func() {
		concernMeterReader = sdkmetric.NewManualReader()
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(concernMeterReader)))
	})
	for _, recordHandles := range []bool{false, true} { //nolint:paralleltest // shared telemetry
		t.Run(strconv.FormatBool(recordHandles), func(t *testing.T) {
			before := concernOpenCount(t)
			f := newConcernFixture(t, nfscfg.Config{Metrics: true, Tracing: true, Logging: true, RecordHandleCalls: recordHandles}, nil)
			require.NoError(t, os.WriteFile(filepath.Join(f.paths[0], "payload"), []byte("real data"), 0o644))
			require.NotNil(t, f.conn.LocalAddr(), "real logging middleware requires both connection addresses")
			fs := f.mount(t)
			handle := f.proxy.server.Handler.ToHandle(t.Context(), fs, nil)
			resolved, path, err := f.proxy.server.Handler.FromHandle(t.Context(), handle)
			require.NoError(t, err)
			require.Empty(t, path)
			assert.Same(t, concernFilesystemIdentity(fs), concernFilesystemIdentity(resolved), "middleware must wrap the canonical mount for the current request")
			require.Equal(t, "real data", readConcernFile(t, resolved, "payload"))
			assert.Equal(t, int64(1), concernOpenCount(t)-before, "cached root read must emit actual filesystem operation telemetry")
		})
	}
}

func TestRenameAcrossRootAndSubdirectoryWithTracing(t *testing.T) { //nolint:paralleltest // actual chroot and process-wide telemetry
	// No-op spans compare equal even when request-local wrappers are different.
	// Keep one synchronous provider alive for repeated runs in this test process.
	concernTracerOnce.Do(func() {
		concernSpans = tracetest.NewInMemoryExporter()
		otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(concernSpans)))
	})
	for _, recordHandles := range []bool{false, true} { //nolint:paralleltest // shared telemetry
		t.Run(strconv.FormatBool(recordHandles), func(t *testing.T) {
			concernSpans.Reset()
			f := buildConcernFixture(t, nfscfg.Config{Metrics: true, Tracing: true, RecordHandleCalls: recordHandles}, nil)
			require.NoError(t, os.Mkdir(filepath.Join(f.paths[0], "sub"), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(f.paths[0], "file"), []byte("payload"), 0o644))

			listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			served := make(chan error, 1)
			go func() { served <- f.proxy.Serve(listener) }()
			t.Cleanup(func() {
				require.NoError(t, listener.Close())
				require.NoError(t, <-served)
			})

			_, portText, err := net.SplitHostPort(listener.Addr().String())
			require.NoError(t, err)
			port, err := strconv.Atoi(portText)
			require.NoError(t, err)
			client, err := clientnfs.DialServiceAtPort("127.0.0.1", port)
			require.NoError(t, err)
			t.Cleanup(func() { client.Close() })
			target, err := (&clientnfs.Mount{Client: client}).Mount(f.mountPath("first"), rpc.NewAuthUnix("", 100, 101).Auth())
			require.NoError(t, err)

			// RENAME asks the real server to compare the root directory handle with
			// the subdirectory handle. Both must resolve through the same mounted FS.
			require.NoError(t, target.Rename("file", "sub/file"))
			require.NotEmpty(t, concernSpans.GetSpans(), "exercise real request spans, not the default no-op tracer")
			contents, err := os.ReadFile(filepath.Join(f.paths[0], "sub", "file"))
			require.NoError(t, err)
			require.Equal(t, "payload", string(contents))
		})
	}
}

func TestRemountRetainsCanonicalRootAndOrdinaryCapability(t *testing.T) { //nolint:paralleltest // serialize process-wide telemetry and actual root-FD barriers
	f := newConcernFixture(t, nfscfg.Config{}, nil)
	require.NoError(t, os.WriteFile(filepath.Join(f.paths[0], "payload"), []byte("original directory"), 0o644))
	first := f.mount(t)
	rootHandle := f.proxy.server.Handler.ToHandle(t.Context(), first, nil)
	childHandle := f.proxy.server.Handler.ToHandle(t.Context(), first, []string{"payload"})
	require.NoError(t, os.Rename(f.paths[0], f.paths[0]+".retired"))
	require.NoError(t, os.MkdirAll(f.paths[0], 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(f.paths[0], "payload"), []byte("replacement directory"), 0o644))
	second := f.mount(t)
	assert.Equal(t, 1, f.proxy.chroots.Count(), "remount must not accumulate new unused root descriptors")
	require.Equal(t, rootHandle, f.proxy.server.Handler.ToHandle(t.Context(), second, nil), "same logical mount retains its opaque root handle")
	root, _, err := f.proxy.server.Handler.FromHandle(t.Context(), rootHandle)
	require.NoError(t, err)
	ordinary, path, err := f.proxy.server.Handler.FromHandle(t.Context(), childHandle)
	require.NoError(t, err)
	require.Equal(t, []string{"payload"}, path)
	fromRoot := readConcernFile(t, root, "payload")
	fromChild := readConcernFile(t, ordinary, path[0])
	assert.Equal(t, "original directory", fromRoot, "real root FD must remain the canonical capability across remount")
	assert.Equal(t, "original directory", fromChild)
	assert.Equal(t, fromChild, fromRoot, "root and ordinary handles cannot resolve different root descriptors")
	assert.Equal(t, 1, concernRootFDCount(t, f.paths[0]+".retired"), "the original root must remain the sole canonical descriptor")
	assert.Zero(t, concernRootFDCount(t, f.paths[0]), "remount must not open an unused replacement-directory descriptor")
	require.NoError(t, f.sandboxes.NetworkReleased(t.Context(), "127.0.0.1"))
	require.Eventually(t, func() bool { return f.proxy.retirement.count() == 0 }, time.Second, time.Millisecond)
	assert.Zero(t, concernRootFDCount(t, f.paths[0]+".retired"), "release must retire the actual canonical root, not only the current registry entry")
	assert.Zero(t, concernRootFDCount(t, f.paths[0]))
}

func concernRootFDCount(t *testing.T, root string) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	var count int
	for _, entry := range entries {
		target, linkErr := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if errors.Is(linkErr, os.ErrNotExist) {
			continue
		}
		require.NoError(t, linkErr)
		if target == root {
			count++
		}
	}

	return count
}

type allocatedConcernBuilder struct {
	inner   *chrooted.Builder
	entered chan *chrooted.Chrooted
	proceed chan struct{}
}

func (b *allocatedConcernBuilder) Chroot(typ string, team, id uuid.UUID) (*chrooted.Chrooted, error) {
	fs, err := b.inner.Chroot(typ, team, id)
	if err != nil {
		return nil, err
	}
	b.entered <- fs // both actual root FDs exist before either can register
	<-b.proceed

	return fs, nil
}

func TestConcurrentFirstMountKeepsOneCanonicalRootAndClosesLoser(t *testing.T) { //nolint:paralleltest // serialize process-wide telemetry and actual root-FD barriers
	entered := make(chan *chrooted.Chrooted, 2)
	proceed := make(chan struct{})
	f := newConcernFixture(t, nfscfg.Config{}, func(inner *chrooted.Builder) chroot.Builder {
		return &allocatedConcernBuilder{inner: inner, entered: entered, proceed: proceed}
	})
	var unblock sync.Once
	unblockMounts := func() { unblock.Do(func() { close(proceed) }) }
	t.Cleanup(unblockMounts)
	type result struct {
		status servernfs.MountStatus
		fs     billy.Filesystem
	}
	finished := make(chan result, 2)
	for range 2 {
		go func() {
			status, fs, _ := f.proxy.server.Handler.Mount(t.Context(), f.conn, servernfs.MountRequest{Dirpath: []byte(f.mountPath("first"))})
			finished <- result{status: status, fs: fs}
		}()
	}
	allocated := make([]*chrooted.Chrooted, 0, 2)
	for range 2 {
		select {
		case fs := <-entered:
			allocated = append(allocated, fs)
		case <-time.After(time.Second):
			t.Fatal("both first-MOUNT allocations must reach the actual FD barrier")
		}
	}
	require.Equal(t, 2, concernRootFDCount(t, f.paths[0]))
	unblockMounts()
	var rootHandle []byte
	for range 2 {
		select {
		case r := <-finished:
			require.Equal(t, servernfs.MountStatusOk, r.status)
			handle := f.proxy.server.Handler.ToHandle(t.Context(), r.fs, nil)
			require.NotEmpty(t, handle)
			if rootHandle == nil {
				rootHandle = handle
			} else {
				assert.Equal(t, rootHandle, handle)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent first MOUNT never finished")
		}
	}
	assert.Equal(t, 1, f.proxy.chroots.Count())
	assert.Equal(t, 1, concernRootFDCount(t, f.paths[0]), "redundant actual allocation must close, not merely disappear from the gauge")
	var closed int
	for _, fs := range allocated {
		_, err := fs.Stat(".")
		if errors.Is(err, chrooted.ErrClosed) {
			closed++
		} else {
			require.NoError(t, err)
		}
	}
	assert.Equal(t, 1, closed, "exactly one real allocation remains canonical")
	require.NoError(t, f.sandboxes.NetworkReleased(t.Context(), "127.0.0.1"))
	assert.Eventually(t, func() bool { return concernRootFDCount(t, f.paths[0]) == 0 }, time.Second, time.Millisecond,
		"the independently owned cleanup must retire the remaining actual root FD")
}

type preOpenConcernBuilder struct {
	inner     *chrooted.Builder
	blocked   uuid.UUID
	entered   chan struct{}
	proceed   chan struct{}
	allocated chan *chrooted.Chrooted
	once      sync.Once
}

func (b *preOpenConcernBuilder) Chroot(typ string, team, id uuid.UUID) (*chrooted.Chrooted, error) {
	if id == b.blocked {
		b.once.Do(func() { close(b.entered) })
		<-b.proceed
	}
	fs, err := b.inner.Chroot(typ, team, id)
	if id == b.blocked && err == nil {
		b.allocated <- fs
	}

	return fs, err
}

func TestNetworkReleaseCancellationRetainsCleanup(t *testing.T) { //nolint:paralleltest // serialize process-wide telemetry and actual root-FD barriers
	entered := make(chan struct{})
	proceed := make(chan struct{})
	var b *preOpenConcernBuilder
	f := newConcernFixture(t, nfscfg.Config{}, func(inner *chrooted.Builder) chroot.Builder {
		b = &preOpenConcernBuilder{inner: inner, entered: entered, proceed: proceed, allocated: make(chan *chrooted.Chrooted, 1)}

		return b
	})
	b.blocked = f.ids[1]
	var unblock sync.Once
	unblockMount := func() { unblock.Do(func() { close(proceed) }) }
	t.Cleanup(unblockMount) // run before fixture release even on an assertion failure
	first := f.mount(t)
	rootHandle := f.proxy.server.Handler.ToHandle(t.Context(), first, nil)
	mountDone := make(chan servernfs.MountStatus, 1)
	go func() {
		s, _, _ := f.proxy.server.Handler.Mount(t.Context(), f.conn, servernfs.MountRequest{Dirpath: []byte(f.mountPath("second"))})
		mountDone <- s
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("real second-volume builder was never reached")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	releaseDone := make(chan error, 1)
	go func() { releaseDone <- f.sandboxes.NetworkReleased(ctx, "127.0.0.1") }()
	closed, ok := f.proxy.gate.OwnerClosed(f.owner)
	require.True(t, ok)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("release did not close real owner admission")
	}
	cancel()
	select {
	case releaseErr := <-releaseDone:
		require.NoError(t, releaseErr)
	case <-time.After(time.Second):
		t.Fatal("logical release must finish without joining a blocked allocation")
	}
	require.Equal(t, 1, f.proxy.retirement.count(), "the original cleanup must remain owned after the callback returns")
	_, _, liveErr := f.proxy.server.Handler.FromHandle(t.Context(), rootHandle)
	require.ErrorAs(t, liveErr, new(*servernfs.NFSStatusError), "handles must be invalidated before logical release succeeds")
	_, liveErr = first.Stat(".")
	require.NoError(t, liveErr, "caller cancellation must not terminate the still-admitted initializer")
	assert.Equal(t, 1, concernRootFDCount(t, f.paths[0]), "the warmed FD remains owned until the real admission joins")
	unblockMount()
	select {
	case status := <-mountDone:
		assert.Equal(t, servernfs.MountStatusErrAcces, status, "late real allocation must refuse closed admission and close its FD")
	case <-time.After(time.Second):
		t.Fatal("late actual allocation never completed")
	}
	late := <-b.allocated
	_, lateErr := late.Stat(".")
	require.ErrorIs(t, lateErr, chrooted.ErrClosed, "closed admission must physically discard the late actual allocation")
	assert.Eventually(t, func() bool { return f.proxy.retirement.count() == 0 }, time.Second, time.Millisecond,
		"successful original cleanup must remove its retirement entry")
	_, _, err := f.proxy.server.Handler.FromHandle(t.Context(), rootHandle)
	require.ErrorAs(t, err, new(*servernfs.NFSStatusError), "released owner's handle must be stale")
	assert.Eventually(t, func() bool {
		_, closedErr := first.Stat(".")

		return errors.Is(closedErr, chrooted.ErrClosed) && f.proxy.chroots.Count() == 0
	}, time.Second, time.Millisecond, "owned continuation must join and close the actual warmed chroot")
	assert.Eventually(t, func() bool { return concernRootFDCount(t, f.paths[0]) == 0 && concernRootFDCount(t, f.paths[1]) == 0 }, time.Second, time.Millisecond, "both warmed and late actual root descriptors must be retired")
}

type concernSlotStorage struct{ released atomic.Int64 }

func (*concernSlotStorage) Acquire(context.Context) (*network.Slot, error) {
	return nil, context.Canceled
}

func (s *concernSlotStorage) Release(*network.Slot) error {
	s.released.Add(1)

	return nil
}

func newEmptyConcernPool(t *testing.T) (*network.Pool, *concernSlotStorage) {
	t.Helper()
	storage := &concernSlotStorage{}
	pool := network.NewPool(2, 2, storage, network.Config{}) //nolint:exhaustruct_v5 // no network is created in this retained-error proof
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	pool.Populate(canceled) // closes its empty producer; Acquire/CreateNetwork are never called

	return pool, storage
}

func TestNetworkPoolCloseDoesNotWaitForBlockedNFSAdmission(t *testing.T) { //nolint:paralleltest // real FD ownership with one isolated virtual clock
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		proceed := make(chan struct{})
		var builder *preOpenConcernBuilder
		f := newConcernFixture(t, nfscfg.Config{}, func(inner *chrooted.Builder) chroot.Builder {
			builder = &preOpenConcernBuilder{inner: inner, entered: entered, proceed: proceed, allocated: make(chan *chrooted.Chrooted, 1)}

			return builder
		})
		builder.blocked = f.ids[1]
		var unblock sync.Once
		unblockMount := func() { unblock.Do(func() { close(proceed) }) }
		t.Cleanup(unblockMount)
		first := f.mount(t)
		rootHandle := f.proxy.server.Handler.ToHandle(t.Context(), first, nil)
		mountDone := make(chan servernfs.MountStatus, 1)
		go func() {
			status, _, _ := f.proxy.server.Handler.Mount(t.Context(), f.conn, servernfs.MountRequest{Dirpath: []byte(f.mountPath("second"))})
			mountDone <- status
		}()
		<-entered
		pool, storage := newEmptyConcernPool(t)
		notified := make(chan error, 1)
		slot, err := network.NewSlot("nfs-concern", 1, network.Config{}, network.NewNoopEgressProxy())
		require.NoError(t, err)
		slot.HostIP = net.IPv4(127, 0, 0, 1)
		release := f.sandboxes.NetworkRelease(f.owner.SandboxID, f.owner.LifecycleID)
		require.NoError(t, pool.ReturnAsync(t.Context(), slot, func(ctx context.Context, ip string) error {
			err := release(ctx, ip)
			notified <- err

			return err
		}, 0))
		closeDone := make(chan error, 1)
		go func() { closeDone <- pool.Close(t.Context()) }()
		synctest.Wait()
		require.NoError(t, <-notified, "logical release must not wait for the old initializer")
		// No native network is created in this test. Native tests separately
		// require actual veth/netns absence; here cleanup must reach storage.
		require.NotErrorIs(t, <-closeDone, network.ErrSlotRetained)
		require.Equal(t, int64(1), storage.released.Load())
		require.Equal(t, 1, f.proxy.retirement.count())
		require.Equal(t, 1, concernRootFDCount(t, f.paths[0]))
		_, _, staleErr := f.proxy.server.Handler.FromHandle(t.Context(), rootHandle)
		require.ErrorAs(t, staleErr, new(*servernfs.NFSStatusError))
		require.NoError(t, release(t.Context(), slot.HostIPString()), "repeated logical release must not start another physical close")
		require.Equal(t, 1, f.proxy.retirement.count())
		unblockMount()
		require.Equal(t, servernfs.MountStatusErrAcces, <-mountDone)
		late := <-builder.allocated
		_, lateErr := late.Stat(".")
		require.ErrorIs(t, lateErr, chrooted.ErrClosed)
		synctest.Wait()
		_, closedErr := first.Stat(".")
		require.ErrorIs(t, closedErr, chrooted.ErrClosed)
		require.Zero(t, concernRootFDCount(t, f.paths[0]))
		require.Zero(t, concernRootFDCount(t, f.paths[1]))
		require.Zero(t, f.proxy.retirement.count())
		require.Equal(t, int64(1), storage.released.Load(), "late descriptor cleanup must not release storage twice")
	})
}
