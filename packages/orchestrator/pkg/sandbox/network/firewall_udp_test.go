//go:build linux

package network

import (
	"bytes"
	"context"
	"os"
	"sync/atomic"
	"testing"

	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// ruleShape is the match and verdict shape of one filter chain rule, precise
// enough to tell the chain's rules apart and to pin their order.
type ruleShape struct {
	ct bool
	// ctDir is the conntrack direction comparison following a ct direction
	// load: "reply" or "" when the rule does not match on it.
	ctDir  string
	lookup bool
	// notInSets is the number of inverted set lookups.
	notInSets int
	// counter is a reference to the table's named counter.
	counter bool
	// l4proto is the protocol comparison following a meta l4proto load:
	// "!=tcp", "==udp", "!=udp", or "" when the rule does not match on it.
	l4proto string
	// nfproto is the family comparison following a meta nfproto load:
	// "ipv4" or "" when the rule applies to both families.
	nfproto string
	// dport is the UDP destination port comparison following a transport
	// header load: "!=53" or "" when the rule does not match on it.
	dport string
	// verdict is "accept", "drop" or "" when the rule only counts.
	verdict string
}

func shapeOf(r *nftables.Rule) ruleShape {
	var s ruleShape
	pending := ""
	for _, e := range r.Exprs {
		switch x := e.(type) {
		case *expr.Ct:
			s.ct = true
			if x.Key == expr.CtKeyDIRECTION {
				pending = "ctdir"
			}
		case *expr.Lookup:
			if x.Invert {
				s.notInSets++
			} else {
				s.lookup = true
			}
		case *expr.Objref:
			s.counter = x.Type == int(nftables.ObjTypeCounter)
		case *expr.Meta:
			switch x.Key {
			case expr.MetaKeyL4PROTO:
				pending = "l4proto"
			case expr.MetaKeyNFPROTO:
				pending = "nfproto"
			}
		case *expr.Payload:
			if x.Base == expr.PayloadBaseTransportHeader && x.Offset == 2 && x.Len == 2 {
				pending = "dport"
			}
		case *expr.Cmp:
			switch {
			case pending == "l4proto" && x.Op == expr.CmpOpNeq && bytes.Equal(x.Data, []byte{unix.IPPROTO_TCP}):
				s.l4proto = "!=tcp"
			case pending == "l4proto" && x.Op == expr.CmpOpEq && bytes.Equal(x.Data, []byte{unix.IPPROTO_UDP}):
				s.l4proto = "==udp"
			case pending == "l4proto" && x.Op == expr.CmpOpNeq && bytes.Equal(x.Data, []byte{unix.IPPROTO_UDP}):
				s.l4proto = "!=udp"
			case pending == "ctdir" && x.Op == expr.CmpOpEq && bytes.Equal(x.Data, []byte{ctDirReply}):
				s.ctDir = "reply"
			case pending == "dport" && x.Op == expr.CmpOpNeq && bytes.Equal(x.Data, binaryutil.BigEndian.PutUint16(53)):
				s.dport = "!=53"
			case pending == "nfproto" && x.Op == expr.CmpOpEq && bytes.Equal(x.Data, []byte{unix.NFPROTO_IPV4}):
				s.nfproto = "ipv4"
			}
			pending = ""
		case *expr.Verdict:
			switch x.Kind {
			case expr.VerdictAccept:
				s.verdict = "accept"
			case expr.VerdictDrop:
				s.verdict = "drop"
			}
		}
	}

	return s
}

func filterChainShapes(t *testing.T, nsName string) []ruleShape {
	t.Helper()

	rules := filterChainRules(t, nsName)
	shapes := make([]ruleShape, 0, len(rules))
	for _, r := range rules {
		shapes = append(shapes, shapeOf(r))
	}

	return shapes
}

var (
	establishedNonUDPAccept = ruleShape{ct: true, l4proto: "!=udp", verdict: "accept"}
	establishedReplyAccept  = ruleShape{ct: true, ctDir: "reply", verdict: "accept"}
	allowSetAccept          = ruleShape{lookup: true, verdict: "accept"}
	denySetDrop             = ruleShape{lookup: true, verdict: "drop"}
	nonTCPDenySetDrop       = ruleShape{lookup: true, l4proto: "!=tcp", verdict: "drop"}
	nonTCPUserAllow         = ruleShape{lookup: true, l4proto: "!=tcp", verdict: "accept"}
	nonTCPUserDeny          = ruleShape{lookup: true, l4proto: "!=tcp", verdict: "drop"}
	nonDNSUDPCount          = ruleShape{counter: true, l4proto: "==udp", dport: "!=53", nfproto: "ipv4", notInSets: 2}
	nonDNSUDPDrop           = ruleShape{counter: true, l4proto: "==udp", dport: "!=53", nfproto: "ipv4", notInSets: 2, verdict: "drop"}
)

// defaultChain is the chain every slot has carried so far: the off mode must
// keep producing exactly this.
var defaultChain = []ruleShape{establishedNonUDPAccept, establishedReplyAccept, allowSetAccept, denySetDrop, nonTCPUserAllow, nonTCPUserDeny}

func newFirewallTestSlot(t *testing.T, key string) *Slot {
	t.Helper()

	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)

	slot, err := NewSlot(key, reserveNSTestIdx(t), config, NewNoopEgressProxy())
	require.NoError(t, err)

	require.NoError(t, slot.CreateNetwork(t.Context()))
	t.Cleanup(func() { _ = slot.RemoveNetwork(context.WithoutCancel(t.Context())) })

	return slot
}

// The non-DNS UDP rule heads the chain, ahead of the established accept,
// matches IPv4 only, exempts both allow sets, counts in both active modes,
// drops only in drop mode, and is absent in off mode, which leaves the chain
// exactly as it has always been.
func TestFirewall_NonDNSUDP_RuleShape(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "fw-non-dns-udp-shape")

	require.Equal(t, defaultChain, filterChainShapes(t, slot.NamespaceID()), "a fresh slot carries the default chain")

	for _, tc := range []struct {
		mode NonDNSUDPMode
		want []ruleShape
	}{
		{NonDNSUDPModeCount, []ruleShape{nonDNSUDPCount, establishedNonUDPAccept, establishedReplyAccept, allowSetAccept, denySetDrop, nonTCPUserAllow, nonTCPUserDeny}},
		{NonDNSUDPModeDrop, []ruleShape{nonDNSUDPDrop, establishedNonUDPAccept, establishedReplyAccept, allowSetAccept, denySetDrop, nonTCPUserAllow, nonTCPUserDeny}},
		{NonDNSUDPModeOff, defaultChain},
	} {
		require.NoError(t, slot.Firewall.ApplyRules(t.Context(), false, tc.mode, nil, nil))
		require.Equal(t, tc.want, filterChainShapes(t, slot.NamespaceID()), "mode %s", tc.mode)
	}
}

// ParseNonDNSUDPMode accepts only the three modes and falls back to off, so a
// mistyped flag value can never drop traffic.
func TestParseNonDNSUDPMode(t *testing.T) {
	t.Parallel()

	require.Equal(t, NonDNSUDPModeOff, ParseNonDNSUDPMode("off"))
	require.Equal(t, NonDNSUDPModeCount, ParseNonDNSUDPMode("count"))
	require.Equal(t, NonDNSUDPModeDrop, ParseNonDNSUDPMode("drop"))
	require.Equal(t, NonDNSUDPModeOff, ParseNonDNSUDPMode(""))
	require.Equal(t, NonDNSUDPModeOff, ParseNonDNSUDPMode("DROP"))
	require.Equal(t, NonDNSUDPModeOff, ParseNonDNSUDPMode("block"))
}

// With a BYOP proxy the predefined deny rule stays non-TCP-only so TCP reaches
// the proxy, and the non-DNS UDP rule is the only difference between the
// off and drop chains.
func TestFirewall_NonDNSUDP_BYOPKeepsTCPPath(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "fw-non-dns-udp-byop")

	require.NoError(t, slot.Firewall.ApplyRules(t.Context(), true, NonDNSUDPModeOff, nil, nil))
	byopChain := []ruleShape{establishedNonUDPAccept, establishedReplyAccept, allowSetAccept, nonTCPDenySetDrop, nonTCPUserAllow, nonTCPUserDeny}
	require.Equal(t, byopChain, filterChainShapes(t, slot.NamespaceID()))

	require.NoError(t, slot.Firewall.ApplyRules(t.Context(), true, NonDNSUDPModeDrop, nil, nil))
	got := filterChainShapes(t, slot.NamespaceID())
	require.Equal(t, []ruleShape{nonDNSUDPDrop, establishedNonUDPAccept, establishedReplyAccept, allowSetAccept, nonTCPDenySetDrop, nonTCPUserAllow, nonTCPUserDeny}, got)

	for _, shape := range got {
		require.NotEqual(t, "==tcp", shape.l4proto, "no rule may match TCP: the proxy owns it")
	}
}

// singleSlotStorage hands out one prepared slot so a pool test can drive
// createNetworkSlot without the local storage's namespace scan.
type singleSlotStorage struct{ slot *Slot }

func (s *singleSlotStorage) Acquire(context.Context) (*Slot, error) { return s.slot, nil }
func (s *singleSlotStorage) Release(*Slot) error                    { return nil }

// A pool's flag reaches the slot before its first chain build, so a fresh
// slot already carries the configured rule.
func TestPool_NonDNSUDPModeReachesSlot(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)

	slot, err := NewSlot("fw-non-dns-udp-pool", reserveNSTestIdx(t), config, NewNoopEgressProxy())
	require.NoError(t, err)

	ff, _ := nonDNSUDPFlagSource(t, NonDNSUDPModeDrop)
	pool := NewPool(2, 1, &singleSlotStorage{slot: slot}, config, WithNonDNSUDPFlag(ff))
	got, err := pool.createNetworkSlot(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = got.RemoveNetwork(context.WithoutCancel(t.Context())) })

	require.Equal(t, NonDNSUDPModeDrop, got.Firewall.NonDNSUDPMode())
	require.Equal(t, []ruleShape{nonDNSUDPDrop, establishedNonUDPAccept, establishedReplyAccept, allowSetAccept, denySetDrop, nonTCPUserAllow, nonTCPUserDeny}, filterChainShapes(t, got.NamespaceID()))
}

// ResetInternet rebuilds only a chain the tenant shaped, with the mode the
// provider reports at that moment; a default-posture chain is left alone on
// recycle whatever the mode now reads, since the next hand-out applies the
// incoming tenant's mode, and a slot that is off and stays off keeps the
// default chain.
func TestFirewall_NonDNSUDP_ResetRestoresMode(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "fw-non-dns-udp-reset")

	var mode atomic.Value
	mode.Store("off")
	slot.UseNonDNSUDPMode(func(context.Context) string { return mode.Load().(string) })

	countChain := []ruleShape{nonDNSUDPCount, establishedNonUDPAccept, establishedReplyAccept, allowSetAccept, denySetDrop, nonTCPUserAllow, nonTCPUserDeny}
	dropChain := []ruleShape{nonDNSUDPDrop, establishedNonUDPAccept, establishedReplyAccept, allowSetAccept, denySetDrop, nonTCPUserAllow, nonTCPUserDeny}
	noRules := &orchestrator.SandboxNetworkConfig{}

	require.NoError(t, slot.ResetInternet(t.Context()))
	require.Equal(t, defaultChain, filterChainShapes(t, slot.NamespaceID()), "off stays off")
	require.Empty(t, slot.Firewall.TakeNonDNSUDPTally(), "no tally survives a release")

	mode.Store("count")
	handles := ruleHandles(filterChainRules(t, slot.NamespaceID()))
	require.NoError(t, slot.ResetInternet(t.Context()))
	require.Equal(t, handles, ruleHandles(filterChainRules(t, slot.NamespaceID())), "a mode change alone does not rebuild a default-posture chain on recycle")
	require.Empty(t, slot.Firewall.TakeNonDNSUDPTally(), "no tally survives a release")

	require.NoError(t, slot.ConfigureInternet(t.Context(), noRules))
	require.Equal(t, countChain, filterChainShapes(t, slot.NamespaceID()), "the next hand-out applies the tenant's mode")

	handles = ruleHandles(filterChainRules(t, slot.NamespaceID()))
	require.NoError(t, slot.ResetInternet(t.Context()))
	require.Equal(t, handles, ruleHandles(filterChainRules(t, slot.NamespaceID())), "a recycle in the same mode harvests without rebuilding the chain")
	require.Empty(t, slot.Firewall.TakeNonDNSUDPTally(), "no tally survives a release")

	mode.Store("drop")
	netCfg := &orchestrator.SandboxNetworkConfig{
		Egress: &orchestrator.SandboxNetworkEgressConfig{DeniedCidrs: []string{"10.0.0.0/8"}},
	}
	require.NoError(t, slot.ConfigureInternet(t.Context(), netCfg))
	require.Equal(t, dropChain, filterChainShapes(t, slot.NamespaceID()), "a tenant's rules build with the current mode")

	reader := swapNonDNSUDPCounter(t)
	slot.AssignSandbox("sandbox-reset", "team-reset")

	mode.Store("count")
	require.NoError(t, slot.ResetInternet(t.Context()))
	require.Equal(t, countChain, filterChainShapes(t, slot.NamespaceID()), "recycle after a tenant rebuilds with the current mode")
	require.False(t, slot.firewallCustomRules.Load())
	require.Empty(t, slot.sandboxID, "the tenant is forgotten on recycle")

	// Neither chain the tenant ran under counted a datagram, so the release
	// adds no point at all rather than zero-valued ones.
	require.Empty(t, nonDNSUDPPointsByChain(t, reader), "a chain that counted nothing adds no point")
	require.Empty(t, slot.Firewall.TakeNonDNSUDPTally(), "no tally survives a release")

	mode.Store("off")
	require.NoError(t, slot.ResetInternet(t.Context()))
	require.Equal(t, countChain, filterChainShapes(t, slot.NamespaceID()), "off alone does not rebuild on recycle")
	require.Empty(t, slot.Firewall.TakeNonDNSUDPTally(), "no tally survives a release")
	require.NoError(t, slot.ConfigureInternet(t.Context(), noRules))
	require.Equal(t, defaultChain, filterChainShapes(t, slot.NamespaceID()), "off removes the rule at the next hand-out")
}

func ruleHandles(rules []*nftables.Rule) []uint64 {
	handles := make([]uint64, 0, len(rules))
	for _, r := range rules {
		handles = append(handles, r.Handle)
	}

	return handles
}

// DenyEgress replaces the chain descriptor as one value: the isolated chain
// carries no rule, so neither the departing mode nor its posture may survive.
func TestFirewall_DenyEgressReplacesInstalledChain(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "fw-deny-egress-descriptor")

	require.NoError(t, slot.Firewall.ApplyRules(t.Context(), false, NonDNSUDPModeDrop, nil, []string{"10.0.0.0/8"}))
	require.Equal(t, ChainDescriptor{Mode: NonDNSUDPModeDrop, Posture: postureRules}, slot.Firewall.Installed())

	require.NoError(t, slot.DenyEgress(t.Context()))
	require.Equal(t, ChainDescriptor{Mode: NonDNSUDPModeOff, Posture: postureIsolated}, slot.Firewall.Installed())
}

// A harvest taken while another goroutine replaces the chain reads the
// counter, mode and posture of one chain generation: the firewall lock covers
// all three, which the race detector checks.
func TestFirewall_HarvestSnapshotIsOneChainGeneration(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "fw-harvest-snapshot")
	fw := slot.Firewall
	ctx := t.Context()

	require.NoError(t, fw.ApplyRules(ctx, false, NonDNSUDPModeCount, nil, nil))

	const rounds = 50
	done := make(chan error, 1)
	go func() {
		for i := range rounds {
			var denied []string
			if i%2 == 1 {
				denied = []string{"10.0.0.0/8"}
			}
			if err := fw.ApplyRules(ctx, false, NonDNSUDPModeCount, nil, denied); err != nil {
				done <- err

				return
			}
		}
		done <- nil
	}()

	for range rounds {
		snapshot, err := fw.Harvest()
		require.NoError(t, err)
		require.Equal(t, NonDNSUDPModeCount, snapshot.Mode)
		require.Contains(t, []string{postureDefault, postureRules}, snapshot.Posture)
	}
	require.NoError(t, <-done)
}

// A slot built or recycled under one mode serves its next tenant under that
// tenant's mode: ConfigureInternet consults the mode even when the tenant has
// no egress rules, so a flag flipped to off reaches pre-built slots too, and
// a slot already in the tenant's mode keeps its chain.
func TestFirewall_NonDNSUDP_ConfigureInternetAppliesTenantMode(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)

	slot, err := NewSlot("fw-non-dns-udp-tenant-mode", reserveNSTestIdx(t), config, NewNoopEgressProxy())
	require.NoError(t, err)

	var mode atomic.Value
	mode.Store("drop")
	slot.UseNonDNSUDPMode(func(context.Context) string { return mode.Load().(string) })

	require.NoError(t, slot.CreateNetwork(t.Context()))
	t.Cleanup(func() { _ = slot.RemoveNetwork(context.WithoutCancel(t.Context())) })

	dropChain := []ruleShape{nonDNSUDPDrop, establishedNonUDPAccept, establishedReplyAccept, allowSetAccept, denySetDrop, nonTCPUserAllow, nonTCPUserDeny}
	countChain := []ruleShape{nonDNSUDPCount, establishedNonUDPAccept, establishedReplyAccept, allowSetAccept, denySetDrop, nonTCPUserAllow, nonTCPUserDeny}
	require.Equal(t, dropChain, filterChainShapes(t, slot.NamespaceID()), "the slot was built under drop")

	noRules := &orchestrator.SandboxNetworkConfig{}

	mode.Store("off")
	require.NoError(t, slot.ConfigureInternet(t.Context(), noRules))
	require.Equal(t, defaultChain, filterChainShapes(t, slot.NamespaceID()), "a tenant under off gets no rule from a slot built under drop")
	require.False(t, slot.firewallCustomRules.Load(), "a default-posture rebuild is not a tenant rule set")

	handles := ruleHandles(filterChainRules(t, slot.NamespaceID()))
	require.NoError(t, slot.ConfigureInternet(t.Context(), noRules))
	require.Equal(t, handles, ruleHandles(filterChainRules(t, slot.NamespaceID())), "a slot already in the tenant's mode keeps its chain")

	mode.Store("count")
	require.NoError(t, slot.ConfigureInternet(t.Context(), noRules))
	require.Equal(t, countChain, filterChainShapes(t, slot.NamespaceID()), "a tenant under count gets the rule from a slot built under off")
}

// A slot without a firewall refuses every firewall operation with the same
// error, a tenant without egress rules and the release included, so a pool
// never hands such a slot out or reuses it; nothing of the tenant's state is
// touched by a refused operation.
func TestSlot_WithoutFirewallRefusesEveryOperation(t *testing.T) {
	t.Parallel()

	slot, err := NewSlot("fw-no-firewall", 1, Config{}, NewNoopEgressProxy())
	require.NoError(t, err)
	slot.AssignSandbox("sandbox-no-firewall", "team-no-firewall")

	withRules := &orchestrator.SandboxNetworkConfig{
		Egress: &orchestrator.SandboxNetworkEgressConfig{DeniedCidrs: []string{"10.0.0.0/8"}},
	}
	for name, op := range map[string]func() error{
		"configure with rules":    func() error { return slot.ConfigureInternet(t.Context(), withRules) },
		"configure without rules": func() error { return slot.ConfigureInternet(t.Context(), &orchestrator.SandboxNetworkConfig{}) },
		"update":                  func() error { return slot.UpdateInternet(t.Context(), withRules.GetEgress()) },
		"deny egress":             func() error { return slot.DenyEgress(t.Context()) },
		"reset":                   func() error { return slot.ResetInternet(t.Context()) },
	} {
		require.ErrorContains(t, op(), "no firewall attached", name)
	}
	require.False(t, slot.firewallCustomRules.Load(), "nothing was configured, so nothing needs cleaning up")
	require.Equal(t, "sandbox-no-firewall", slot.sandboxID, "a refused release keeps the tenant")
	require.NoError(t, slot.CloseFirewall(t.Context()), "closing a slot without a firewall is a no-op")
}
