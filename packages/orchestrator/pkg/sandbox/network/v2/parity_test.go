//go:build linux

package v2

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/containernetworking/plugins/pkg/ns"
	"github.com/google/nftables"
	"github.com/google/nftables/expr"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

// Resumed guests keep a cached ARP entry for the gateway, so the tap MAC must
// be the same fixed address v1 stamps.
func TestCreateNetworkV2_TapUsesFixedHostMAC(t *testing.T) { //nolint:paralleltest // mutates host netns state: singleton nftables table "v2-host-firewall", named netns, veth links
	skipIfNotLinuxRoot(t)

	ctx := t.Context()

	hf := newTestHostFirewall(t, testConfig())

	slot := makeTestSlot(t, reserveNSTestIdx(t))
	sv2 := NewSlotV2(slot)
	require.NoError(t, CreateNetworkV2(ctx, slot, sv2, hf, nil))
	t.Cleanup(func() { _ = RemoveNetworkV2(context.WithoutCancel(t.Context()), slot, sv2, hf, nil) })

	nsHandle, err := ns.GetNS(filepath.Join(network.NetNamespacesDir, slot.NamespaceID()))
	require.NoError(t, err)
	defer nsHandle.Close()

	var got string
	require.NoError(t, nsHandle.Do(func(_ ns.NetNS) error {
		tap, err := netlink.LinkByName(slot.TapName())
		if err != nil {
			return err
		}
		got = tap.Attrs().HardwareAddr.String()

		return nil
	}))

	assert.Equal(t, network.TapHostHardwareAddr().String(), got)
}

// v1's host FORWARD rules only ever append ACCEPTs, so the v2 table must not
// drop anything: no default-drop policy, and the gw→veth ACCEPT is
// unconditional (a ct-state match would reject new inbound flows v1 allows).
func TestHostFirewall_ForwardChainMatchesV1(t *testing.T) { //nolint:paralleltest // creates/deletes the singleton nftables table "v2-host-firewall" shared by all HostFirewall tests
	skipIfNotLinuxRoot(t)

	hf := newTestHostFirewall(t, testConfig())

	chains, err := hf.conn.ListChainsOfTableFamily(nftables.TableFamilyINet)
	require.NoError(t, err)

	var fwd *nftables.Chain
	for _, c := range chains {
		if c.Table.Name == hostFwTableName && c.Name == "forward" {
			fwd = c

			break
		}
	}
	require.NotNil(t, fwd, "forward chain must exist")
	require.NotNil(t, fwd.Policy)
	assert.Equal(t, nftables.ChainPolicyAccept, *fwd.Policy, "v2 must not install a default-drop forward hook")

	rules, err := hf.conn.GetRules(hf.table, fwd)
	require.NoError(t, err)
	require.Len(t, rules, 2, "forward chain holds exactly v1's two ACCEPTs")

	for _, r := range rules {
		accepts := false
		for _, e := range r.Exprs {
			if v, ok := e.(*expr.Verdict); ok && v.Kind == expr.VerdictAccept {
				accepts = true
			}
			_, isCt := e.(*expr.Ct)
			assert.False(t, isCt, "forward ACCEPTs must be unconditional, as v1's are")
		}
		assert.True(t, accepts, "every forward rule must be an ACCEPT")
	}
}

// A slot left in the sets by a crashed run must not block its index: reclaim
// tears the slot down, then reconcile drops what it left behind.
func TestHostFirewall_ReconcileClearsStaleSlots(t *testing.T) { //nolint:paralleltest // creates/deletes the singleton nftables table "v2-host-firewall" shared by all HostFirewall tests
	skipIfNotLinuxRoot(t)

	ctx := t.Context()

	hf := newTestHostFirewall(t, testConfig())

	slot := makeTestSlot(t, reserveNSTestIdx(t))
	sv2 := NewSlotV2(slot)
	require.NoError(t, hf.AddSlot(ctx, sv2))

	require.NoError(t, hf.ReconcileSlots(ctx, nil))

	elements, err := hf.conn.GetSetElements(hf.vethSet)
	require.NoError(t, err)
	assert.Empty(t, elements, "reconcile must drop set elements no live slot owns")

	// A live slot is left alone: reconcile must recognise it as already
	// present, not delete and re-add it. Comparing set membership by a
	// mis-trimmed key classifies every live veth as both stale and missing,
	// which nets out to the same element count — so count the delete instead.
	require.NoError(t, hf.AddSlot(ctx, sv2))
	require.NoError(t, hf.ReconcileSlots(ctx, []*SlotV2{sv2}))
	require.NoError(t, vethSetHas(hf, slot.VethName()))

	require.NoError(t, hf.RemoveSlot(ctx, sv2))
}

// Sandboxes outlive the orchestrator process: closing the pool while a slot is
// still registered must leave its rules in place.
func TestHostFirewall_ClosePreservesLiveSlots(t *testing.T) { //nolint:paralleltest // creates/deletes the singleton nftables table "v2-host-firewall" shared by all HostFirewall tests
	skipIfNotLinuxRoot(t)

	ctx := t.Context()

	hf, err := NewHostFirewall("lo", testConfig())
	require.NoError(t, err)

	slot := makeTestSlot(t, reserveNSTestIdx(t))
	sv2 := NewSlotV2(slot)
	require.NoError(t, hf.AddSlot(ctx, sv2))
	require.NoError(t, hf.Close())

	reopened := newTestHostFirewall(t, testConfig())
	require.NoError(t, vethSetHas(reopened, slot.VethName()), "a live slot's rules must survive Close")

	require.NoError(t, reopened.RemoveSlot(ctx, sv2))

	// With no slot left, Close does tear the table down.
	require.NoError(t, reopened.Close())
	after := newTestHostFirewall(t, testConfig())
	elements, err := after.conn.GetSetElements(after.vethSet)
	require.NoError(t, err)
	assert.Empty(t, elements, "Close must delete the table once no slot is left")
}

// A live slot's padded set key must match its name: the classification is
// what decides whether reconcile deletes a live sandbox's veth, and comparing
// the raw 16-byte key against the name silently makes every live slot look
// both stale and missing.
func TestClassifyVeths(t *testing.T) {
	t.Parallel()

	current := []nftables.SetElement{
		{Key: ifnameBytes("veth-1")},
		{Key: ifnameBytes("veth-99")},
	}
	desired := map[string]bool{"veth-1": true, "veth-2": true}

	stale, missing := classifyVeths(current, desired)

	require.Len(t, stale, 1, "only the veth no live slot owns is stale")
	require.Equal(t, ifnameBytes("veth-99"), stale[0].Key)
	require.Equal(t, []string{"veth-2"}, missing)
}

// vethSetHas reports whether the veth set holds the named interface.
func vethSetHas(hf *HostFirewall, veth string) error {
	elements, err := hf.conn.GetSetElements(hf.vethSet)
	if err != nil {
		return err
	}

	want := ifnameBytes(veth)
	for _, e := range elements {
		if bytes.Equal(e.Key, want) {
			return nil
		}
	}

	return fmt.Errorf("veth %s not in set (%d elements)", veth, len(elements))
}

// Teardown is idempotent: repeated calls, and calls against a slot that was
// never created, must not error — that is what makes create-failure cleanup
// and startup reclaim safe.
func TestRemoveNetworkV2_Idempotent(t *testing.T) { //nolint:paralleltest // mutates host netns state: singleton nftables table "v2-host-firewall", named netns, veth links
	skipIfNotLinuxRoot(t)

	ctx := t.Context()

	hf := newTestHostFirewall(t, testConfig())

	slot := makeTestSlot(t, reserveNSTestIdx(t))
	sv2 := NewSlotV2(slot)

	// Never created.
	require.NoError(t, RemoveNetworkV2(ctx, slot, sv2, hf, nil))

	require.NoError(t, CreateNetworkV2(ctx, slot, sv2, hf, nil))
	require.NoError(t, RemoveNetworkV2(ctx, slot, sv2, hf, nil))
	require.NoError(t, RemoveNetworkV2(ctx, slot, sv2, hf, nil))
}

// A stale namespace from a failed teardown is the reclaim anchor: create must
// reclaim it rather than fail, so the index stays usable.
func TestCreateNetworkV2_ReclaimsStaleNamespace(t *testing.T) { //nolint:paralleltest // mutates host netns state: singleton nftables table "v2-host-firewall", named netns, veth links
	skipIfNotLinuxRoot(t)

	ctx := t.Context()

	hf := newTestHostFirewall(t, testConfig())

	slot := makeTestSlot(t, reserveNSTestIdx(t))
	sv2 := NewSlotV2(slot)

	require.NoError(t, CreateNetworkV2(ctx, slot, sv2, hf, nil))

	// Leave the namespace and the set elements behind, as a crashed run would.
	require.NoError(t, slot.CloseFirewall(ctx))

	require.NoError(t, CreateNetworkV2(ctx, slot, sv2, hf, nil), "a stale anchor must be reclaimed, not fatal")
	t.Cleanup(func() { _ = RemoveNetworkV2(context.WithoutCancel(t.Context()), slot, sv2, hf, nil) })

	_, err := os.Stat(filepath.Join(network.NetNamespacesDir, slot.NamespaceID()))
	require.NoError(t, err, "the rebuilt slot must have its namespace")
}

// fwmark policy routing is not part of this datapath, so its sysctl must not
// gate startup.
func TestValidateV2Prerequisites_IgnoresSrcValidMark(t *testing.T) {
	t.Parallel()

	// Both reads gate on the host's actual sysctls: the assertion only means
	// something where src_valid_mark is off, and ip_forward would fail the
	// check for an unrelated reason.
	if sysctlValue(t, "/proc/sys/net/ipv4/conf/all/src_valid_mark") != "0" {
		t.Skip("src_valid_mark is already 1; this box cannot show that the check is gone")
	}
	if sysctlValue(t, "/proc/sys/net/ipv4/ip_forward") != "1" {
		t.Skip("ip_forward is off; the prerequisite check fails here for an unrelated reason")
	}
	if _, err := exec.LookPath("iptables"); err != nil {
		t.Skip("iptables binary not on PATH; the prerequisite check fails here for an unrelated reason")
	}

	require.NoError(t, ValidateV2Prerequisites())
}

func sysctlValue(t *testing.T, path string) string {
	t.Helper()

	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	return strings.TrimSpace(string(raw))
}

// slotStorage hands out one prepared slot so a pool test can drive
// createNetworkSlot without the local storage's namespace scan.
type slotStorage struct{ slot *network.Slot }

func (s *slotStorage) Acquire(context.Context) (*network.Slot, error) { return s.slot, nil }
func (s *slotStorage) Release(*network.Slot) error                    { return nil }

// nonDNSUDPFlagClient serves the flag at value from an offline data source the
// test updates at will, through a client built like the orchestrator's.
func nonDNSUDPFlagClient(t *testing.T, value network.NonDNSUDPMode) (*featureflags.Client, *ldtestdata.TestDataSource) {
	t.Helper()

	td := ldtestdata.DataSource()
	setNonDNSUDPFlag(td, value)
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return ff, td
}

func setNonDNSUDPFlag(td *ldtestdata.TestDataSource, value network.NonDNSUDPMode) {
	td.Update(td.Flag(featureflags.SandboxFirewallNonDNSUDPFlag.Key()).ValueForAll(ldvalue.String(string(value))))
}

// slotFilterChainRules lists the slot firewall's filter chain from inside the
// slot's namespace.
func slotFilterChainRules(t *testing.T, slot *network.Slot) []*nftables.Rule {
	t.Helper()

	nsHandle, err := ns.GetNS(filepath.Join(network.NetNamespacesDir, slot.NamespaceID()))
	require.NoError(t, err)
	defer nsHandle.Close()

	var rules []*nftables.Rule
	require.NoError(t, nsHandle.Do(func(_ ns.NetNS) error {
		conn, err := nftables.New()
		if err != nil {
			return err
		}
		table := &nftables.Table{Name: "slot-firewall", Family: nftables.TableFamilyINet}
		rules, err = conn.GetRules(table, &nftables.Chain{Name: "PREROUTE_FILTER", Table: table})

		return err
	}))

	return rules
}

// findNonDNSUDPRule reports whether the chain carries the counted UDP rule
// and whether that rule drops.
func findNonDNSUDPRule(rules []*nftables.Rule) (counted, dropped bool) {
	for _, r := range rules {
		var udp, counter, drop bool
		for _, e := range r.Exprs {
			switch x := e.(type) {
			case *expr.Cmp:
				udp = udp || x.Op == expr.CmpOpEq && bytes.Equal(x.Data, []byte{unix.IPPROTO_UDP})
			case *expr.Objref:
				counter = x.Type == int(nftables.ObjTypeCounter)
			case *expr.Verdict:
				drop = x.Kind == expr.VerdictDrop
			}
		}
		if udp && counter {
			return true, drop
		}
	}

	return false, false
}

// The v2 datapath builds its in-namespace chain through the same firewall, so
// a v2 slot carries the non-DNS UDP rule its pool's flag selects.
func TestCreateNetworkV2_SlotFirewallHasNonDNSUDPRule(t *testing.T) { //nolint:paralleltest // mutates host netns state: singleton nftables table "v2-host-firewall", named netns, veth links
	skipIfNotLinuxRoot(t)

	ctx := t.Context()

	hf := newTestHostFirewall(t, testConfig())

	ff, _ := nonDNSUDPFlagClient(t, network.NonDNSUDPModeDrop)
	slot := makeTestSlot(t, reserveNSTestIdx(t))
	pool := NewV2Pool(&slotStorage{slot: slot}, testConfig(), hf, nil, WithNonDNSUDPFlag(ff))
	got, err := pool.createNetworkSlot(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.cleanup(context.WithoutCancel(t.Context()), got) })

	require.Equal(t, network.NonDNSUDPModeDrop, got.Firewall.NonDNSUDPMode())

	rules := slotFilterChainRules(t, got)
	counted, dropped := findNonDNSUDPRule(rules)
	require.True(t, counted && dropped, "the v2 slot chain must carry the counted UDP drop rule, got %d rules", len(rules))
}
