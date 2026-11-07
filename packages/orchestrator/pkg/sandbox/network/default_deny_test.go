package network

import (
	"context"
	"os"
	"testing"

	"github.com/google/nftables/expr"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestApplyRules_DefaultDeny verifies the opt-in default-deny posture:
//   - the chain is recorded as posture "deny" with the non-DNS UDP mode forced
//     to Drop (so the per-slot UDP tally is kept while UDP is dropped);
//   - the chain ends in a terminal non-TCP drop, so any non-TCP the operator
//     did not put in an allow set is dropped rather than hitting the default
//     ACCEPT policy. TCP is never matched by it (TCP leaves via the REDIRECT to
//     the userspace proxy, which enforces default-deny there).
//
// Needs root; skips otherwise.
func TestApplyRules_DefaultDeny(t *testing.T) { //nolint:paralleltest // netns + nftables
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)

	slot, err := NewSlot("default-deny-test", reserveNSTestIdx(t), config, NewNoopEgressProxy())
	require.NoError(t, err)

	require.NoError(t, slot.CreateNetwork(t.Context()))
	t.Cleanup(func() { _ = slot.RemoveNetwork(context.WithoutCancel(t.Context())) })

	// default-deny with one allowed CIDR; mode Off requested — must be forced to Drop.
	require.NoError(t, slot.Firewall.ApplyRules(t.Context(), false, NonDNSUDPModeOff, []string{"203.0.113.0/24"}, nil, true))

	require.Equal(t, ChainDescriptor{Mode: NonDNSUDPModeDrop, Posture: postureDeny}, slot.Firewall.Installed(),
		"default-deny must record posture=deny and force the non-DNS UDP mode to Drop")

	rules := filterChainRules(t, slot.NamespaceID())
	last := rules[len(rules)-1]

	var hasL4ProtoNeqTCP, dropVerdict, ctMatch, setLookup bool
	for _, e := range last.Exprs {
		switch x := e.(type) {
		case *expr.Meta:
			if x.Key == expr.MetaKeyL4PROTO {
				hasL4ProtoNeqTCP = true
			}
		case *expr.Cmp:
			if x.Op == expr.CmpOpNeq && len(x.Data) == 1 && x.Data[0] == unix.IPPROTO_TCP {
				hasL4ProtoNeqTCP = true
			}
		case *expr.Verdict:
			if x.Kind == expr.VerdictDrop {
				dropVerdict = true
			}
		case *expr.Ct:
			ctMatch = true
		case *expr.Lookup:
			setLookup = true
		}
	}

	require.True(t, hasL4ProtoNeqTCP && dropVerdict,
		"the last rule under default-deny must be a non-TCP drop")
	require.False(t, ctMatch || setLookup,
		"the terminal non-TCP drop must be unconditional on destination/state (only protocol-qualified)")
}
