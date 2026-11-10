//go:build linux

package nfsproxy

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netlink"
	servernfs "github.com/willscott/go-nfs"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	nfscfg "github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/chroot"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	networkv2 "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network/v2"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

type singleNativeSlotStorage struct {
	slot      *network.Slot
	allocated atomic.Bool
	released  atomic.Int64
}

func (s *singleNativeSlotStorage) Acquire(ctx context.Context) (*network.Slot, error) {
	if s.allocated.CompareAndSwap(false, true) {
		return s.slot, nil
	}
	<-ctx.Done()

	return nil, ctx.Err()
}

func (s *singleNativeSlotStorage) Release(*network.Slot) error {
	s.released.Add(1)

	return nil
}

func requirePrivateNativeNetwork(t *testing.T) {
	t.Helper()
	if os.Getenv("NFS_TEST_NATIVE_NETWORK") != "1" {
		t.Skip("set NFS_TEST_NATIVE_NETWORK=1 inside private network and mount namespaces")
	}
	require.Zero(t, os.Geteuid(), "native network lifecycle requires root")
	for _, namespace := range []string{"net", "mnt"} {
		current, err := os.Readlink("/proc/self/ns/" + namespace)
		require.NoError(t, err)
		initial, err := os.Readlink("/proc/1/ns/" + namespace)
		require.NoError(t, err)
		require.NotEqual(t, initial, current, "native network lifecycle requires a private %s namespace", namespace)
	}
	// Namespace IDs alone do not isolate directory writes or shared mount
	// propagation. Keep the named-network anchors on test-owned memory.
	require.NoError(t, unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""))
	require.NoError(t, unix.Mount("nfs-native-test", "/run", "tmpfs", unix.MS_NODEV|unix.MS_NOSUID, "size=1m,mode=0755"))
	t.Cleanup(func() { require.NoError(t, unix.Unmount("/run", 0)) })
}

func TestNetworkPoolNativeNFSReleaseBeforeMountDrain(t *testing.T) { //nolint:paralleltest // creates native slots and the v2 singleton firewall
	requirePrivateNativeNetwork(t)
	for _, version := range []int{1, 2} { //nolint:paralleltest // native slots and singleton firewall must be joined before the next case
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			entered := make(chan struct{})
			proceed := make(chan struct{})
			var builder *preOpenConcernBuilder
			f := newConcernFixture(t, nfscfg.Config{}, func(inner *chrooted.Builder) chroot.Builder {
				builder = &preOpenConcernBuilder{inner: inner, entered: entered, proceed: proceed, allocated: make(chan *chrooted.Chrooted, 1)}

				return builder
			})
			builder.blocked = f.ids[1]
			var unblock sync.Once
			releaseMount := func() { unblock.Do(func() { close(proceed) }) }
			t.Cleanup(releaseMount)
			config := network.Config{OrchestratorInSandboxIPAddress: "192.0.2.1", HyperloopProxyPort: 5010, NFSProxyPort: 5011, PortmapperPort: 5012, SandboxTCPFirewallHTTPPort: 5016, SandboxTCPFirewallTLSPort: 5017, SandboxTCPFirewallOtherPort: 5018, NetworkVersion: version}
			slot, err := network.NewSlot("nfs-native-retirement", 32000+version, config, network.NewNoopEgressProxy())
			require.NoError(t, err)
			storage := &singleNativeSlotStorage{slot: slot}
			release := f.sandboxes.NetworkRelease(f.owner.SandboxID, f.owner.LifecycleID)
			returned := false
			poolClosed := false
			var pool network.PoolInterface
			var populate func(context.Context)
			if version == 1 {
				p := network.NewPool(1, 1, storage, config)
				pool, populate = p, p.Populate
			} else {
				firewall, err := networkv2.NewHostFirewall(network.DefaultGateway(), config)
				require.NoError(t, err)
				p := networkv2.NewV2Pool(storage, config, firewall, nil)
				pool, populate = p, p.Populate
			}
			populateCtx, cancelPopulate := context.WithCancel(t.Context())
			populated := make(chan struct{})
			go func() { defer close(populated); populate(populateCtx) }()
			t.Cleanup(func() {
				releaseMount()
				cancelPopulate()
				<-populated
				if !returned && storage.allocated.Load() {
					require.NoError(t, pool.ReturnAsync(context.WithoutCancel(t.Context()), slot, release, 0))
				}
				if !poolClosed {
					require.NoError(t, pool.Close(context.WithoutCancel(t.Context())))
				}
			})
			getCtx, cancelGet := context.WithTimeout(t.Context(), 10*time.Second)
			got, err := pool.Get(getCtx, nil, sandboxtypes.EgressClassSandbox)
			cancelGet()
			cancelPopulate()
			<-populated
			require.NoError(t, err)
			require.Same(t, slot, got)
			_, err = netlink.LinkByName(slot.VethName())
			require.NoError(t, err, "real slot must exist before its NFS users are released")
			// Restore the test's original route with this real pool-owned slot.
			sbx, err := f.sandboxes.GetByHostPort("127.0.0.1:1")
			require.NoError(t, err)
			sbx.Resources.Slot = slot
			f.sandboxes.AssignNetwork(t.Context(), sbx)
			f.conn.remote = &net.TCPAddr{IP: slot.HostIP, Port: 32768}
			first := f.mount(t)
			rootHandle := f.proxy.server.Handler.ToHandle(t.Context(), first, nil)
			releaseOpen := blockConcernRootOpen(t, first, f.paths[0])
			mountDone := make(chan servernfs.MountStatus, 1)
			go func() {
				status, _, _ := f.proxy.server.Handler.Mount(t.Context(), f.conn, servernfs.MountRequest{Dirpath: []byte(f.mountPath("second"))})
				mountDone <- status
			}()
			<-entered
			notified := make(chan error, 3)
			require.NoError(t, pool.ReturnAsync(t.Context(), slot, func(ctx context.Context, ip string) error {
				err := release(ctx, ip)
				notified <- err

				return err
			}, 0))
			returned = true
			select {
			case err := <-notified:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("network release waited for the old mount to drain")
			}
			require.Zero(t, storage.released.Load())
			_, err = netlink.LinkByName(slot.VethName())
			require.NoError(t, err, "active slot reuse must keep the actual network")
			recovered := make(chan *network.Slot, 1)
			// Populate has finished, so wait until the asynchronously returned
			// slot wins the real pool fast path instead of its closed producer.
			require.Eventually(t, func() bool {
				ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
				defer cancel()
				got, err := pool.Get(ctx, nil, sandboxtypes.EgressClassSandbox)
				if err != nil {
					return false
				}
				recovered <- got

				return true
			}, time.Second, time.Millisecond)
			require.Same(t, slot, <-recovered)
			returned = false
			require.Equal(t, 1, f.proxy.retirement.count(), "physical old-owner cleanup is independent of slot reuse")
			require.Equal(t, 1, concernRootFDCount(t, f.paths[0]))
			_, _, err = f.proxy.server.Handler.FromHandle(t.Context(), rootHandle)
			var stale *servernfs.NFSStatusError
			require.ErrorAs(t, err, &stale)
			require.Equal(t, servernfs.NFSStatusStale, stale.NFSStatus)
			releaseMount()
			require.Equal(t, servernfs.MountStatusErrAcces, <-mountDone)
			require.Equal(t, 1, f.proxy.retirement.count(), "the actual blocked root operation still owns physical cleanup")
			require.Equal(t, 1, concernRootFDCount(t, f.paths[0]))

			// Reassign the real reused slot while the old root syscall is still
			// blocked. Delayed old-owner release must not detach this new route.
			replacement := &sandbox.Sandbox{
				Metadata: sbx.Metadata, Resources: sbx.Resources, LifecycleID: sbx.LifecycleID + "-replacement",
			}
			f.sandboxes.AssignNetwork(t.Context(), replacement)
			oldRelease := release
			release = f.sandboxes.NetworkRelease(replacement.Runtime.SandboxID, replacement.LifecycleID)
			oldConn := f.conn
			oldRequest := servernfs.MountRequest{Dirpath: []byte(f.mountPath("first"))}
			status, _, _ := f.proxy.server.Handler.Mount(t.Context(), oldConn, oldRequest)
			require.Equal(t, servernfs.MountStatusErrAcces, status, "old captured export must remain denied on slot reuse")
			f.owner.LifecycleID = replacement.LifecycleID
			f.conn = remoteAddrConn{remote: &net.TCPAddr{IP: slot.HostIP, Port: 32769}}
			newFS := f.mount(t)
			newHandle := f.proxy.server.Handler.ToHandle(t.Context(), newFS, nil)
			require.Equal(t, 2, concernRootFDCount(t, f.paths[0]))
			require.NoError(t, oldRelease(t.Context(), slot.HostIPString()))
			resolved, _, err := f.proxy.server.Handler.FromHandle(t.Context(), newHandle)
			require.NoError(t, err)
			require.Same(t, concernFilesystemIdentity(newFS), concernFilesystemIdentity(resolved))
			current, err := f.sandboxes.GetByHostPort(net.JoinHostPort(slot.HostIPString(), "1"))
			require.NoError(t, err)
			require.Equal(t, replacement.LifecycleID, current.LifecycleID)
			releaseOpen()
			require.Eventually(t, func() bool { return f.proxy.retirement.count() == 0 }, time.Second, time.Millisecond)
			require.Equal(t, 1, concernRootFDCount(t, f.paths[0]), "only the replacement root may remain")
			require.Zero(t, concernRootFDCount(t, f.paths[1]))
			require.NoError(t, pool.ReturnAsync(t.Context(), slot, release, 0))
			returned = true
			poolClosed = true
			require.NoError(t, pool.Close(t.Context()))
			require.Eventually(t, func() bool { return f.proxy.retirement.count() == 0 }, time.Second, time.Millisecond)
			require.Zero(t, concernRootFDCount(t, f.paths[0]))
			require.Equal(t, int64(1), storage.released.Load(), "the original native slot must retire exactly once")
			_, err = netlink.LinkByName(slot.VethName())
			var absent netlink.LinkNotFoundError
			require.ErrorAs(t, err, &absent)
			_, err = os.Stat(filepath.Join(network.NetNamespacesDir, slot.NamespaceID()))
			require.ErrorIs(t, err, os.ErrNotExist, "slot namespace must be physically removed")
		})
	}
}

type releaseFuncSubscriber func(context.Context, *sandbox.Sandbox) error

func (releaseFuncSubscriber) OnInsert(context.Context, *sandbox.Sandbox)   {}
func (releaseFuncSubscriber) OnStopping(context.Context, *sandbox.Sandbox) {}
func (s releaseFuncSubscriber) OnNetworkRelease(ctx context.Context, sbx *sandbox.Sandbox) error {
	return s(ctx, sbx)
}

func TestNetworkPoolNativePendingShutdown(t *testing.T) { //nolint:paralleltest // native networks and process-wide firewall state
	requirePrivateNativeNetwork(t)
	for _, version := range []int{1, 2} { //nolint:paralleltest // native slots and singleton firewall must be joined before the next case
		for _, releaseFails := range []bool{false, true} {
			t.Run(strconv.Itoa(version)+"/release_fails="+strconv.FormatBool(releaseFails), func(t *testing.T) {
				f := newConcernFixture(t, nfscfg.Config{}, nil)
				f.proxy.server.Handler.ToHandle(t.Context(), f.mount(t), nil)
				releaseEntered, releaseContinue := make(chan struct{}), make(chan struct{})
				unblockRelease := sync.OnceFunc(func() { close(releaseContinue) })
				var calls atomic.Int64
				f.sandboxes.Subscribe(releaseFuncSubscriber(func(ctx context.Context, _ *sandbox.Sandbox) error {
					// Fixture cleanup can notify again after the pool is closed and checked.
					if calls.Add(1) != 1 {
						return nil
					}
					close(releaseEntered)
					<-releaseContinue
					if err := ctx.Err(); err != nil {
						return err
					}
					if releaseFails {
						return context.DeadlineExceeded
					}

					return nil
				}))
				config := network.Config{OrchestratorInSandboxIPAddress: "192.0.2.1", HyperloopProxyPort: 5010, NFSProxyPort: 5011, PortmapperPort: 5012, SandboxTCPFirewallHTTPPort: 5016, SandboxTCPFirewallTLSPort: 5017, SandboxTCPFirewallOtherPort: 5018, NetworkVersion: version}
				slot, err := network.NewSlot("nfs-native-shutdown", 32100+version, config, network.NewNoopEgressProxy())
				require.NoError(t, err)
				storage := &singleNativeSlotStorage{slot: slot}
				var pool network.PoolInterface
				var populate func(context.Context)
				if version == 1 {
					p := network.NewPool(1, 1, storage, config)
					pool, populate = p, p.Populate
				} else {
					firewall, err := networkv2.NewHostFirewall(network.DefaultGateway(), config)
					require.NoError(t, err)
					p := networkv2.NewV2Pool(storage, config, firewall, nil)
					pool, populate = p, p.Populate
				}
				populateCtx, cancelPopulate := context.WithCancel(t.Context())
				populated := make(chan struct{})
				go func() { defer close(populated); populate(populateCtx) }()
				var closeOnce sync.Once
				closed := make(chan struct{})
				var closeErr error
				startClose := func() {
					closeOnce.Do(func() {
						go func() { defer close(closed); closeErr = pool.Close(context.WithoutCancel(t.Context())) }()
					})
				}
				t.Cleanup(func() {
					unblockRelease()
					cancelPopulate()
					<-populated
					startClose()
					<-closed
				})
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				got, err := pool.Get(ctx, nil, sandboxtypes.EgressClassSandbox)
				require.NoError(t, err)
				require.Same(t, slot, got)
				sbx, err := f.sandboxes.GetByHostPort("127.0.0.1:1")
				require.NoError(t, err)
				sbx.Resources.Slot = slot
				f.sandboxes.AssignNetwork(t.Context(), sbx)
				release := f.sandboxes.NetworkRelease(f.owner.SandboxID, f.owner.LifecycleID)
				returnCtx, cancelReturn := context.WithCancel(t.Context())
				defer cancelReturn()
				require.NoError(t, pool.ReturnAsync(returnCtx, slot, release, 0))
				<-releaseEntered // the return worker is inside its lifecycle-bound callback
				cancelReturn()
				startClose()
				require.Eventually(t, func() bool {
					ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
					defer cancel()
					got, err := pool.Get(ctx, nil, sandboxtypes.EgressClassSandbox)

					return got == nil && errors.Is(err, network.ErrClosed)
				}, time.Second, time.Millisecond, "Close must shut pool admission before the release continues")
				unblockRelease()
				// Populate remains blocked in Acquire, so Close cannot drain the
				// reuse queue and hide an incorrect post-shutdown enqueue.
				require.Eventually(t, func() bool { return storage.released.Load() == 1 }, time.Second, time.Millisecond,
					"the actual return must clean the slot instead of retaining or enqueueing it after shutdown")
				cancelPopulate()
				<-populated
				<-closed
				if releaseFails {
					require.ErrorIs(t, closeErr, network.ErrClosed)
					require.ErrorIs(t, closeErr, context.DeadlineExceeded)
					require.NotErrorIs(t, closeErr, network.ErrSlotRetained)
				} else {
					require.NoError(t, closeErr)
				}
				require.Equal(t, int64(1), calls.Load())
				require.Equal(t, int64(1), storage.released.Load())
				got, err = pool.Get(t.Context(), nil, sandboxtypes.EgressClassSandbox)
				require.Nil(t, got)
				require.ErrorIs(t, err, network.ErrClosed)
				_, err = netlink.LinkByName(slot.VethName())
				var absent netlink.LinkNotFoundError
				require.ErrorAs(t, err, &absent)
				_, err = os.Stat(filepath.Join(network.NetNamespacesDir, slot.NamespaceID()))
				require.ErrorIs(t, err, os.ErrNotExist)
			})
		}
	}
}

func TestNetworkPoolNativeHandledRecycleFailure(t *testing.T) { //nolint:paralleltest // owns real networks and the v2 singleton firewall
	requirePrivateNativeNetwork(t)
	for _, version := range []int{1, 2} { //nolint:paralleltest // joins the previous native pool before the next
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			config := network.Config{OrchestratorInSandboxIPAddress: "192.0.2.1", HyperloopProxyPort: 5010, NFSProxyPort: 5011, PortmapperPort: 5012, SandboxTCPFirewallHTTPPort: 5016, SandboxTCPFirewallTLSPort: 5017, SandboxTCPFirewallOtherPort: 5018, NetworkVersion: version}
			slot, err := network.NewSlot("handled-recycle", 32010+version, config, network.NewNoopEgressProxy())
			require.NoError(t, err)
			storage := &singleNativeSlotStorage{slot: slot}
			var pool network.PoolInterface
			var populate func(context.Context)
			if version == 1 {
				p := network.NewPool(1, 1, storage, config)
				pool, populate = p, p.Populate
			} else {
				firewall, err := networkv2.NewHostFirewall(network.DefaultGateway(), config)
				require.NoError(t, err)
				p := networkv2.NewV2Pool(storage, config, firewall, nil)
				pool, populate = p, p.Populate
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			go func() { defer close(done); populate(ctx) }()
			t.Cleanup(func() {
				cancel()
				<-done
				_ = pool.Close(context.WithoutCancel(t.Context()))
			})
			getCtx, cancelGet := context.WithTimeout(t.Context(), 10*time.Second)
			got, err := pool.Get(getCtx, nil, sandboxtypes.EgressClassSandbox)
			cancelGet()
			cancel()
			<-done
			require.NoError(t, err)
			require.Same(t, slot, got)
			// Retiring the actual firewall makes ResetInternet fail, while the
			// remaining network can still be torn down successfully.
			require.NoError(t, slot.CloseFirewall(t.Context()))
			require.Error(t, slot.ResetInternet(t.Context()))
			require.NoError(t, pool.ReturnAsync(t.Context(), slot, func(context.Context, string) error { return nil }, 0))
			require.Eventually(t, func() bool { return storage.released.Load() == 1 }, time.Second, time.Millisecond)
			require.NoError(t, pool.Close(t.Context()), "a handled recycle failure must not poison later shutdown")
			_, err = netlink.LinkByName(slot.VethName())
			var absent netlink.LinkNotFoundError
			require.ErrorAs(t, err, &absent)
			_, err = os.Stat(filepath.Join("/run/netns", slot.NamespaceID()))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}
