//go:build linux

package network

import (
	"context"
	"os"
	"runtime"
	"testing"

	"github.com/google/nftables"
	"github.com/google/nftables/expr"
	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netns"
)

// TestDenyEgress_InstallsAllProtocolDrop verifies that DenyEgress reshapes the
// slot firewall so EVERY guest-originated packet is dropped — including TCP,
// which the normal user deny-set deliberately leaves to the egress proxy. This
// is the safety guarantee for the pause-resume prefetch harvest: its throwaway
// sandbox must not be able to reach the network at all, not even the
// orchestrator's own in-sandbox IP (which fronts the NFS proxy/portmapper/
// hyperloop). The ONLY accepts are the ESTABLISHED/RELATED pair ApplyRules
// installs too, so the orchestrator can still drive the resume by connecting
// INTO the guest; there must be no destination/set accept that would let the
// guest open a connection outward.
//
// It builds a real slot netns + nftables ruleset, so it needs root; it skips
// otherwise — same gating as the other privileged integration tests in this
// package (see TestCreateNetwork_TagsEgressWithDSCP).
func TestDenyEgress_InstallsAllProtocolDrop(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)

	slot, err := NewSlot("deny-egress-test", reserveNSTestIdx(t), config, NewNoopEgressProxy())
	require.NoError(t, err)

	require.NoError(t, slot.CreateNetwork(t.Context()))
	t.Cleanup(func() { _ = slot.RemoveNetwork(context.WithoutCancel(t.Context())) })

	// Baseline: the default ruleset must NOT unconditionally drop guest egress
	// (internet is allowed by default and TCP is handed to the proxy).
	base := summarizeFilterChain(t, slot.NamespaceID())
	require.Falsef(t, base.hasUnconditionalDrop,
		"default ruleset must not drop all egress, got %+v", base)

	require.NoError(t, slot.DenyEgress(t.Context()))

	got := summarizeFilterChain(t, slot.NamespaceID())

	require.Truef(t, got.hasUnconditionalDrop,
		"DenyEgress must install an all-protocol drop for guest-originated traffic, got %+v", got)
	require.Truef(t, got.dropIsLast,
		"the all-protocol drop must be the LAST rule (the catch-all) so every packet not matched by an earlier accept is dropped, got %+v", got)
	require.Falsef(t, got.hasProtocolQualifiedRule,
		"DenyEgress must leave no protocol-qualified rule — TCP must be dropped too, not proxied; got %+v", got)
	require.Truef(t, got.hasEstablishedAccept,
		"DenyEgress must still accept ESTABLISHED/RELATED so the control path replies, got %+v", got)
	require.Falsef(t, got.hasAllowSetAccept,
		"DenyEgress must NOT accept any allow-set/destination — all guest-initiated egress is dropped, including to the orchestrator IP; got %+v", got)
	require.Equal(t, []ruleShape{establishedNonUDPAccept, establishedReplyAccept, {verdict: "drop"}}, filterChainShapes(t, slot.NamespaceID()),
		"DenyEgress accepts only the ESTABLISHED/RELATED pair ahead of the drop")
}

// headFilterProxy is an EgressProxy whose head rule accepts everything from
// the tap.
type headFilterProxy struct{ NoopEgressProxy }

func (headFilterProxy) HeadFilterRules(s *Slot) [][]expr.Any {
	return [][]expr.Any{{
		&expr.Meta{Key: expr.MetaKeyIIFNAME, Register: 1},
		&expr.Cmp{Register: 1, Op: expr.CmpOpEq, Data: append([]byte(s.TapName()), 0)},
		&expr.Verdict{Kind: expr.VerdictAccept},
	}}
}

// TestHeadFilterRules_LeadEveryPolicy verifies that the egress proxy's head
// rules come first after a policy replacement and are absent after DenyEgress.
// Needs root; skips otherwise.
func TestHeadFilterRules_LeadEveryPolicy(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)

	slot, err := NewSlot("head-filter-test", reserveNSTestIdx(t), config, headFilterProxy{})
	require.NoError(t, err)

	require.NoError(t, slot.CreateNetwork(t.Context()))
	t.Cleanup(func() { _ = slot.RemoveNetwork(context.WithoutCancel(t.Context())) })

	require.True(t, summarizeFilterChain(t, slot.NamespaceID()).plainAcceptFirst, "the head rule leads the initial rules")

	require.NoError(t, slot.Firewall.ApplyRules(t.Context(), true, NonDNSUDPModeCount, []string{"192.0.2.0/24"}, []string{"0.0.0.0/0"}, false))
	require.True(t, summarizeFilterChain(t, slot.NamespaceID()).plainAcceptFirst, "the head rule leads a replaced policy, the non-DNS UDP rule included")

	require.NoError(t, slot.DenyEgress(t.Context()))
	require.False(t, summarizeFilterChain(t, slot.NamespaceID()).hasPlainAccept, "DenyEgress installs no head rule")
}

// TestDenyEgress_NilFirewallErrors verifies DenyEgress fails fast — returning an
// error rather than panicking, and without marking custom rules — when the slot's
// firewall is not initialized (so a later ResetInternet during cleanup can't hit
// a nil firewall either). Root-free: the guard returns before entering the netns.
func TestDenyEgress_NilFirewallErrors(t *testing.T) {
	t.Parallel()

	slot, err := NewSlot("deny-egress-nil-fw", 1, Config{}, NewNoopEgressProxy())
	require.NoError(t, err)

	// No InitializeFirewall, so slot.Firewall is nil.
	require.Error(t, slot.DenyEgress(t.Context()))
}

// filterChainSummary classifies the rules in the slot firewall's filter chain by
// the shape that matters for egress isolation.
type filterChainSummary struct {
	// hasUnconditionalDrop is a drop with no protocol/set/conntrack qualifier —
	// i.e. it drops every protocol to any destination from the tap.
	hasUnconditionalDrop bool
	// hasProtocolQualifiedRule is any rule without a conntrack match that
	// matches on the L4 protocol (the non-TCP-only variant that would let TCP
	// escape to the proxy).
	hasProtocolQualifiedRule bool
	// hasEstablishedAccept is a conntrack-state accept (return traffic).
	hasEstablishedAccept bool
	// hasAllowSetAccept is a set-lookup accept (the predefined allow set).
	hasAllowSetAccept bool
	// dropIsLast is true when the final rule in the chain is the unconditional
	// drop — i.e. it is the catch-all that every unaccepted packet falls through to.
	dropIsLast bool
	// hasPlainAccept is an accept with no protocol/set/conntrack qualifier,
	// which only an egress proxy's head rule installs; plainAcceptFirst when
	// the chain starts with one.
	hasPlainAccept, plainAcceptFirst bool
}

// filterChainRules enters the slot's network namespace and lists the rules in
// the firewall filter chain.
func filterChainRules(t *testing.T, nsName string) []*nftables.Rule {
	t.Helper()

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	host, err := netns.Get()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, netns.Set(host))
		_ = host.Close()
	}()

	target, err := netns.GetFromName(nsName)
	require.NoError(t, err)
	defer func() { _ = target.Close() }()
	require.NoError(t, netns.Set(target))

	conn, err := nftables.New()
	require.NoError(t, err)

	table := &nftables.Table{Name: tableName, Family: nftables.TableFamilyINet}
	chain := &nftables.Chain{Name: "PREROUTE_FILTER", Table: table}
	rules, err := conn.GetRules(table, chain)
	require.NoError(t, err)
	require.NotEmpty(t, rules, "filter chain should have rules")

	return rules
}

// summarizeFilterChain classifies the rules in the slot firewall's filter chain.
func summarizeFilterChain(t *testing.T, nsName string) filterChainSummary {
	t.Helper()

	rules := filterChainRules(t, nsName)

	var s filterChainSummary
	for i, r := range rules {
		var drop, accept, l4proto, setLookup, ct bool
		for _, e := range r.Exprs {
			switch x := e.(type) {
			case *expr.Meta:
				if x.Key == expr.MetaKeyL4PROTO {
					l4proto = true
				}
			case *expr.Verdict:
				switch x.Kind {
				case expr.VerdictDrop:
					drop = true
				case expr.VerdictAccept:
					accept = true
				}
			case *expr.Lookup:
				setLookup = true
			case *expr.Ct:
				ct = true
			}
		}

		isUnconditionalDrop := drop && !l4proto && !setLookup && !ct
		plainAccept := accept && !l4proto && !setLookup && !ct
		if plainAccept {
			s.hasPlainAccept = true
		}
		if i == 0 {
			s.plainAcceptFirst = plainAccept
		}
		if l4proto && !ct {
			s.hasProtocolQualifiedRule = true
		}
		if isUnconditionalDrop {
			s.hasUnconditionalDrop = true
		}
		if accept && ct {
			s.hasEstablishedAccept = true
		}
		if accept && setLookup {
			s.hasAllowSetAccept = true
		}
		if i == len(rules)-1 {
			s.dropIsLast = isUnconditionalDrop
		}
	}

	return s
}
