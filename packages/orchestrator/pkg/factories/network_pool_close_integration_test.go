//go:build linux

package factories

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/nftables"
	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netlink"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	networkv2 "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network/v2"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// This decorates real local storage, not slot creation or kernel teardown.
// Pausing the first queued slot's release makes the required cleanup join
// observable after its actual namespace and veth have been removed.
type shutdownKernelStorage struct {
	network.Storage

	mu       sync.Mutex
	acquired int
	released int
	errors   []error
	entered  chan struct{}
	resume   chan struct{}
}

func (s *shutdownKernelStorage) Acquire(ctx context.Context) (*network.Slot, error) {
	slot, err := s.Storage.Acquire(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.errors = append(s.errors, err)
	} else {
		s.acquired++
	}

	return slot, err
}

func (s *shutdownKernelStorage) Release(slot *network.Slot) error {
	_, nsErr := os.Stat(filepath.Join(network.NetNamespacesDir, slot.NamespaceID()))
	if !errors.Is(nsErr, os.ErrNotExist) {
		return fmt.Errorf("namespace still present or inaccessible at release: %s: %w", slot.NamespaceID(), nsErr)
	}
	_, linkErr := netlink.LinkByName(slot.VethName())
	if _, notFound := errors.AsType[netlink.LinkNotFoundError](linkErr); !notFound {
		return fmt.Errorf("veth still present or inaccessible at release: %s: %w", slot.VethName(), linkErr)
	}
	if slot.Idx == 1 {
		close(s.entered)
		<-s.resume
	}
	err := s.Storage.Release(slot)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released++
	if err != nil {
		s.errors = append(s.errors, err)
	}

	return err
}

func (s *shutdownKernelStorage) counts() (int, int, []error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.acquired, s.released, append([]error(nil), s.errors...)
}

// Run explicitly inside a fresh network AND mount namespace with private tmpfs
// /run/netns and a default route. These cases share the singleton firewall and
// namespace names, so neither this test nor its subtests may run in parallel.
func TestNetworkPoolCloserDrainsFullKernelPool(t *testing.T) { //nolint:paralleltest // uses singleton firewall and named network namespaces
	if os.Getenv("E2B_TEST_ISOLATED_NETWORK") != "1" {
		t.Skip("requires explicitly selected isolated kernel networking test")
	}
	require.Zero(t, os.Geteuid())
	for _, kind := range []string{"net", "mnt"} {
		self, err := os.Readlink("/proc/self/ns/" + kind)
		require.NoError(t, err)
		host, err := os.Readlink("/proc/1/ns/" + kind)
		require.NoError(t, err)
		require.NotEqual(t, host, self, "must not mutate the host namespace")
	}
	var fs unix.Statfs_t
	require.NoError(t, unix.Statfs(network.NetNamespacesDir, &fs))
	require.EqualValues(t, unix.TMPFS_MAGIC, fs.Type)

	for _, version := range []int{1, 2} { //nolint:paralleltest // singleton firewall and namespace names require serial cases
		for _, forced := range []bool{false, true} {
			mode := "graceful"
			if forced {
				mode = "force-stop"
			}
			t.Run(fmt.Sprintf("v%d/%s", version, mode), func(t *testing.T) {
				cfg, err := network.ParseConfig()
				require.NoError(t, err)
				cfg.NetworkVersion = version
				local, err := network.NewStorageLocal(t.Context(), cfg, network.NewNoopEgressProxy())
				require.NoError(t, err)
				storage := &shutdownKernelStorage{Storage: local, entered: make(chan struct{}), resume: make(chan struct{})}
				var pool networkPoolProducer
				populateName := "network.(*Pool).Populate("
				if version == 1 {
					pool = network.NewPool(network.NewSlotsPoolSize, network.ReusedSlotsPoolSize, storage, cfg)
				} else {
					firewall, err := networkv2.NewHostFirewall(network.DefaultGateway(), cfg)
					require.NoError(t, err)
					observer, err := networkv2.NewVethObserver()
					require.NoError(t, err)
					pool = networkv2.NewV2Pool(storage, cfg, firewall, observer)
					populateName = "v2.(*V2Pool).Populate("
				}
				producerCtx, cancelProducer := context.WithCancel(context.WithoutCancel(t.Context()))
				var services errgroup.Group
				services.Go(func() error {
					pool.Populate(producerCtx)

					return nil
				})
				var resumeOnce sync.Once
				resume := func() { resumeOnce.Do(func() { close(storage.resume) }) }
				settled := false
				t.Cleanup(func() {
					// Failure rescue is separate from the assertions below. It must
					// not turn producer cancellation into evidence for shutdown.
					cancelProducer()
					resume()
					if !settled {
						require.NoError(t, pool.Close(context.WithoutCancel(t.Context())))
						require.NoError(t, services.Wait())
					}
				})

				// Read-only channel length plus a blocked Populate stack proves the
				// actual full send, rather than guessing readiness from a delay.
				queue := reflect.ValueOf(pool).Elem().FieldByName("newSlots")
				require.Equal(t, network.NewSlotsPoolSize-1, queue.Cap())
				require.Eventually(t, func() bool {
					a, r, errs := storage.counts()
					if queue.Len() != queue.Cap() || a != network.NewSlotsPoolSize || r != 0 || len(errs) != 0 {
						return false
					}
					buf := make([]byte, 1<<20)
					stacks := string(buf[:runtime.Stack(buf, true)])
					for stack := range strings.SplitSeq(stacks, "\n\n") {
						lines := strings.Split(stack, "\n")
						if len(lines) > 1 && strings.Contains(lines[0], "[select]") && strings.Contains(lines[1], populateName) {
							return true
						}
					}

					return false
				}, 30*time.Second, 5*time.Millisecond, "Populate must block with 31 queued and one fully created slot")
				indices, err := network.ListSlotNamespaces(network.NetNamespacesDir)
				require.NoError(t, err)
				require.Len(t, indices, network.NewSlotsPoolSize)
				t.Logf("FULL_KERNEL_POOL version=%d queued=%d namespaces=%d producer=blocked-select", version, queue.Len(), len(indices))

				closeCtx, cancelClose := context.WithCancel(context.WithoutCancel(t.Context()))
				defer cancelClose()
				if forced {
					cancelClose()
				}
				closed := make(chan error, 1)
				joined := make(chan error, 1)
				go func() {
					closed <- networkPoolCloser(pool).close(closeCtx)
					joined <- services.Wait()
				}()
				select {
				case <-storage.entered:
				case err := <-closed:
					t.Fatalf("mandatory closer returned before kernel drain: %v", err)
				case <-time.After(15 * time.Second):
					t.Fatal("closer did not reach real kernel teardown")
				}
				require.Empty(t, closed, "Close must join storage release after kernel removal")
				require.Empty(t, joined, "shutdown must not finish before mandatory Close")
				slot, err := pool.Get(producerCtx, new(orchestrator.SandboxNetworkConfig), sandboxtypes.EgressClassSandbox)
				require.Nil(t, slot)
				require.ErrorIs(t, err, network.ErrClosed)
				resume()
				select {
				case err := <-closed:
					require.NoError(t, err)
				case <-time.After(15 * time.Second):
					t.Fatal("mandatory Close failed to join")
				}
				select {
				case err := <-joined:
					require.NoError(t, err)
				case <-time.After(15 * time.Second):
					t.Fatal("service group failed to join")
				}
				settled = true
				require.NoError(t, producerCtx.Err(), "producer cancellation must not supply completion")
				a, r, errs := storage.counts()
				require.Empty(t, errs)
				require.Equal(t, network.NewSlotsPoolSize, a)
				require.Equal(t, a, r)
				require.Zero(t, queue.Len())
				indices, err = network.ListSlotNamespaces(network.NetNamespacesDir)
				require.NoError(t, err)
				require.Empty(t, indices)
				links, err := netlink.LinkList()
				require.NoError(t, err)
				for _, link := range links {
					require.False(t, strings.HasPrefix(link.Attrs().Name, network.DefaultVethPrefix) || strings.HasPrefix(link.Attrs().Name, network.LegacyVethPrefix), "orphan veth: %s", link.Attrs().Name)
				}
				rules, err := exec.CommandContext(t.Context(), "iptables-save").CombinedOutput()
				require.NoError(t, err, "%s", rules)
				require.NotContains(t, string(rules), "-A ", "orphan iptables rule")
				tables, err := (&nftables.Conn{}).ListTables()
				require.NoError(t, err)
				for _, table := range tables {
					require.NotEqual(t, "v2-host-firewall", table.Name, "orphan host firewall")
				}
				t.Logf("JOINED_KERNEL_POOL version=%d forced=%t acquired=%d released=%d producer_context=%v namespaces=0 veths=0 slot_rules=0", version, forced, a, r, producerCtx.Err())
			})
		}
	}
}
