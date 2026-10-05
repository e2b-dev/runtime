//go:build linux

package v2

import (
	"context"
	"net"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/containernetworking/plugins/pkg/ns"
	"github.com/google/nftables"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// dscpConfig builds the host-firewall test config with the DSCP fields parsed
// from the real env path, so the nil-inherits semantics of
// BUILD_SANDBOX_EGRESS_DSCP are exercised end to end. A set-but-empty build
// value behaves as unset (the env parser skips it), so "" drives the
// inherits-sandbox path.
func dscpConfig(t *testing.T, sandboxDSCP, buildDSCP string) network.Config {
	t.Helper()

	t.Setenv("SANDBOX_EGRESS_DSCP", sandboxDSCP)
	t.Setenv("BUILD_SANDBOX_EGRESS_DSCP", buildDSCP)

	parsed, err := network.ParseConfig()
	require.NoError(t, err)

	config := testConfig()
	config.SandboxEgressDSCP = parsed.SandboxEgressDSCP
	config.BuildSandboxEgressDSCP = parsed.BuildSandboxEgressDSCP

	return config
}

// makeDSCPTestNS creates the slot's named namespace with the slot-firewall
// table and the seeded DSCP chain, as CreateNetworkV2 does, without the rest
// of the datapath.
func makeDSCPTestNS(t *testing.T, slot *network.Slot, config network.Config) {
	t.Helper()

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	host, err := netns.Get()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, netns.Set(host))
		_ = host.Close()
	}()

	nsHandle, err := netns.NewNamed(slot.NamespaceID())
	require.NoError(t, err)
	defer nsHandle.Close()
	t.Cleanup(func() { _ = netns.DeleteNamed(slot.NamespaceID()) })

	conn, err := nftables.New(nftables.AsLasting())
	require.NoError(t, err)
	defer conn.CloseLasting()

	table := conn.AddTable(&nftables.Table{Name: "slot-firewall", Family: nftables.TableFamilyINet})
	require.NoError(t, SetupEgressDSCP(conn, table, slot.VpeerName(), config.EgressDSCP(sandboxtypes.EgressClassSandbox)))
}

// requireDSCPByte asserts the slot's in-namespace mangle rule stamps wantTOS —
// the TOS byte v1's EgressTOS resolution produces. wantTOS 0 asserts no rule.
func requireDSCPByte(t *testing.T, slot *network.Slot, wantTOS int) {
	t.Helper()

	nsHandle, err := ns.GetNS(filepath.Join(network.NetNamespacesDir, slot.NamespaceID()))
	require.NoError(t, err)
	defer nsHandle.Close()

	var got []uint8
	require.NoError(t, nsHandle.Do(func(_ ns.NetNS) error {
		conn, err := nftables.New(nftables.AsLasting())
		if err != nil {
			return err
		}
		defer conn.CloseLasting()

		table := &nftables.Table{Name: "slot-firewall", Family: nftables.TableFamilyINet}
		chain := &nftables.Chain{Name: nsMangleChainName, Table: table}
		rules, err := conn.GetRules(table, chain)
		if err != nil {
			return err
		}
		for _, r := range rules {
			if dscp, ok := dscpRuleValue(r, slot.VpeerName()); ok {
				got = append(got, dscp)
			}
		}

		return nil
	}))

	if wantTOS == 0 {
		require.Emptyf(t, got, "DSCP 0 must leave no mangle rule in %s", slot.NamespaceID())

		return
	}

	require.Lenf(t, got, 1, "want exactly one DSCP rule in %s, got %v", slot.NamespaceID(), got)
	require.Equal(t, wantTOS, int(got[0])<<2)
}

// The stamped byte must equal what v1's EgressDSCP/EgressTOS resolution
// produces for the same env, class by class: seed, build override,
// nil-inherits, and 0-disables all included.
func TestEgressDSCP_V1Parity(t *testing.T) { //nolint:paralleltest // mutates host netns state (named netns per case); t.Setenv
	skipIfNotLinuxRoot(t)

	tests := []struct {
		name       string
		sandboxEnv string
		buildEnv   string
	}{
		{"unset build value falls back to the sandbox value", "8", ""},
		{"build value overrides for builds only", "8", "16"},
		{"explicit zero disables marking for builds only", "8", "0"},
		{"builds can be marked while sandboxes are not", "0", "16"},
		{"both disabled", "0", ""},
		{"max DSCP maps to the top of the TOS byte", "8", "63"},
	}

	for _, tt := range tests { //nolint:paralleltest // named netns + t.Setenv per case
		t.Run(tt.name, func(t *testing.T) {
			config := dscpConfig(t, tt.sandboxEnv, tt.buildEnv)

			slot := makeTestSlot(t, reserveNSTestIdx(t))
			makeDSCPTestNS(t, slot, config)

			// Creation seeds the untenanted class, as v1's CreateNetwork does.
			requireDSCPByte(t, slot, config.EgressTOS().For(sandboxtypes.EgressClassSandbox))

			for _, class := range []sandboxtypes.EgressClass{sandboxtypes.EgressClassBuild, sandboxtypes.EgressClassSandbox} {
				require.NoError(t, ApplyEgressDSCP(slot, config.EgressDSCP(class)))
				requireDSCPByte(t, slot, config.EgressTOS().For(class))
			}
		})
	}
}

func TestEgressDSCP_RestampSemantics(t *testing.T) { //nolint:paralleltest // mutates host netns state (named netns); t.Setenv
	skipIfNotLinuxRoot(t)

	config := dscpConfig(t, "8", "16")

	slot := makeTestSlot(t, reserveNSTestIdx(t))
	makeDSCPTestNS(t, slot, config)
	requireDSCPByte(t, slot, 8<<2)

	// Re-applying the same class must not duplicate the rule.
	require.NoError(t, ApplyEgressDSCP(slot, 8))
	requireDSCPByte(t, slot, 8<<2)

	// Out of the 6-bit range is rejected before any chain mutation, so the
	// previous class stays installed.
	require.Error(t, ApplyEgressDSCP(slot, 200))
	requireDSCPByte(t, slot, 8<<2)

	// 0 removes the rule outright; a second 0 is a clean no-op.
	require.NoError(t, ApplyEgressDSCP(slot, 0))
	requireDSCPByte(t, slot, 0)
	require.NoError(t, ApplyEgressDSCP(slot, 0))
	requireDSCPByte(t, slot, 0)
}

type fakeStorage struct{}

func (f *fakeStorage) Acquire(context.Context) (*network.Slot, error) {
	return nil, context.Canceled
}

func (f *fakeStorage) Release(*network.Slot) error { return nil }

// The pool seam that protects tenants, through the same path production
// takes: Get stamps the tenant's class, returnSlot restores the untenanted
// one before the slot becomes reusable.
func TestV2Pool_GetStampsAndRecycleRestoresDSCP(t *testing.T) { //nolint:paralleltest // mutates host netns state: singleton nftables table "v2-host-firewall", named netns, veth links; t.Setenv
	skipIfNotLinuxRoot(t)

	ctx := t.Context()
	config := dscpConfig(t, "8", "16")

	hf := newTestHostFirewall(t, config)

	observer, err := NewVethObserver()
	require.NoError(t, err)

	pool := NewV2Pool(&fakeStorage{}, config, hf, observer)
	t.Cleanup(func() { _ = pool.Close(context.WithoutCancel(t.Context())) })

	slot := makeTestSlot(t, reserveNSTestIdx(t))
	sv2 := NewSlotV2(slot)
	pool.registry.Store(sv2)
	require.NoError(t, CreateNetworkV2(ctx, slot, sv2, hf, observer))
	t.Cleanup(func() { _ = RemoveNetworkV2(context.WithoutCancel(t.Context()), slot, sv2, hf, observer) })

	// Queue and close before any assertion can fail the test: Close ranges
	// over newSlots, so leaving it open would deadlock the cleanup.
	pool.newSlots <- slot
	close(pool.newSlots) // Populate never runs here.

	requireDSCPByte(t, slot, 8<<2)

	got, err := pool.Get(ctx, nil, sandboxtypes.EgressClassBuild)
	require.NoError(t, err)
	require.Equal(t, slot.Idx, got.Idx)
	requireDSCPByte(t, got, 16<<2)

	require.NoError(t, pool.returnSlot(ctx, got, func(context.Context, string) error { return nil }, 0))
	requireDSCPByte(t, got, 8<<2)

	select {
	case reused := <-pool.reusedSlots:
		require.Equal(t, slot.Idx, reused.Idx)
	default:
		t.Fatal("recycled slot must go back to the reuse queue, not be torn down")
	}
}

// The wire-level claim: the TOS byte on the veth link equals v1's EgressTOS
// resolution, and 0 leaves the byte untouched.
func TestEgressDSCP_WireTOS(t *testing.T) { //nolint:paralleltest // mutates host netns state: singleton nftables table "v2-host-firewall", named netns, veth links; t.Setenv
	skipIfNotLinuxRoot(t)

	ctx := t.Context()
	config := dscpConfig(t, "8", "16")

	hf := newTestHostFirewall(t, config)

	slot := makeTestSlot(t, reserveNSTestIdx(t))
	sv2 := NewSlotV2(slot)
	require.NoError(t, CreateNetworkV2(ctx, slot, sv2, hf, nil))
	t.Cleanup(func() { _ = RemoveNetworkV2(context.WithoutCancel(t.Context()), slot, sv2, hf, nil) })

	assert.Equal(t, config.EgressTOS().For(sandboxtypes.EgressClassSandbox), observeEgressTOS(t, slot))

	require.NoError(t, ApplyEgressDSCP(slot, config.EgressDSCP(sandboxtypes.EgressClassBuild)))
	assert.Equal(t, config.EgressTOS().For(sandboxtypes.EgressClassBuild), observeEgressTOS(t, slot))

	require.NoError(t, ApplyEgressDSCP(slot, 0))
	assert.Equal(t, 0, observeEgressTOS(t, slot))
}

const wireProbePort = 48765

// observeEgressTOS sends a UDP packet from inside the slot's namespace toward
// the host and returns the IPv4 TOS byte it carries on the veth link — the
// same wire bytes v1's in-namespace mangle rule produces. The AF_PACKET
// capture sees the frame as it leaves the namespace, before any host
// netfilter hook — so it also verifies the header checksum the stamp has to
// keep valid: an L2 capture accepts a packet the next hop would drop.
func observeEgressTOS(t *testing.T, slot *network.Slot) int {
	t.Helper()

	link, err := netlink.LinkByName(slot.VethName())
	require.NoError(t, err)

	fd, err := unix.Socket(unix.AF_PACKET, unix.SOCK_DGRAM, int(htons(unix.ETH_P_IP)))
	require.NoError(t, err)
	defer unix.Close(fd)

	require.NoError(t, unix.Bind(fd, &unix.SockaddrLinklayer{
		Protocol: htons(unix.ETH_P_IP),
		Ifindex:  link.Attrs().Index,
	}))
	require.NoError(t, unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &unix.Timeval{Sec: 10}))

	sendFromNamespace(t, slot, slot.VethIP(), wireProbePort)

	buf := make([]byte, 256)
	for {
		n, _, err := unix.Recvfrom(fd, buf, 0)
		require.NoError(t, err, "no probe packet captured on %s", slot.VethName())
		if n < 20 || buf[0]>>4 != 4 {
			continue
		}
		ihl := int(buf[0]&0x0f) * 4
		if buf[9] != unix.IPPROTO_UDP || n < ihl+8 {
			continue
		}
		if int(buf[ihl+2])<<8|int(buf[ihl+3]) != wireProbePort {
			continue
		}

		require.Zerof(t, ipHeaderChecksum(buf[:ihl]), "stamped packet carries a corrupt IPv4 header checksum: % x", buf[:ihl])

		return int(buf[1])
	}
}

func htons(v uint16) uint16 { return v<<8 | v>>8 }

// ipHeaderChecksum folds the one's-complement sum over a whole IPv4 header,
// including its checksum field: a valid header sums to zero.
func ipHeaderChecksum(header []byte) uint16 {
	var sum uint32
	for i := 0; i+1 < len(header); i += 2 {
		sum += uint32(header[i])<<8 | uint32(header[i+1])
	}
	for sum>>16 != 0 {
		sum = sum&0xffff + sum>>16
	}

	return ^uint16(sum)
}

func sendFromNamespace(t *testing.T, slot *network.Slot, ip net.IP, port int) {
	t.Helper()

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	host, err := netns.Get()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, netns.Set(host))
		_ = host.Close()
	}()

	target, err := netns.GetFromName(slot.NamespaceID())
	require.NoError(t, err)
	defer target.Close()
	require.NoError(t, netns.Set(target))

	conn, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: ip, Port: port})
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte("probe"))
	require.NoError(t, err)
}
