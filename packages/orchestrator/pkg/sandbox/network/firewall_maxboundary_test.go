//go:build linux

package network

import (
	"net/netip"
	"os"
	"strings"
	"testing"

	"github.com/google/nftables"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ipv4SetProbe struct {
	address netip.Addr
	want    bool
}

func intervalSetContainsIPv4(t *testing.T, elements []nftables.SetElement, address netip.Addr) bool {
	t.Helper()

	// Interval lookup uses the closest boundary at or below the address.
	nearest := netip.Addr{}
	contains := false
	for _, element := range elements {
		boundary, ok := netip.AddrFromSlice(element.Key)
		require.Truef(t, ok && boundary.Is4(), "invalid IPv4 set boundary %v", element.Key)
		if address.Less(boundary) || nearest.IsValid() && !nearest.Less(boundary) {
			continue
		}
		nearest = boundary
		contains = !element.IntervalEnd
	}

	return nearest.IsValid() && contains
}

func assertIPv4SetMembership(t *testing.T, elements []nftables.SetElement, probes []ipv4SetProbe) {
	t.Helper()

	for _, probe := range probes {
		assert.Equalf(t, probe.want, intervalSetContainsIPv4(t, elements, probe.address),
			"set membership for %s", probe.address)
	}
}

// TestApplyRules_MaxBoundaryCIDRs is the max-boundary regression: egress CIDRs
// whose range ends at 255.255.255.255 used to make ApplyRules fail. Builds a real
// slot netns + nftables ruleset, so it needs root and skips otherwise.
func TestApplyRules_MaxBoundaryCIDRs(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)

	slot, err := NewSlot("fw-maxboundary-test", reserveNSTestIdx(t), config, NewNoopEgressProxy())
	require.NoError(t, err)

	require.NoError(t, slot.CreateNetwork(t.Context()))
	t.Cleanup(func() { _ = slot.RemoveNetwork() })

	// Boundary cases that regressed, then passing neighbours as controls.
	cidrs := []string{
		"240.0.0.0/4", "248.0.0.0/5", "255.0.0.0/8", "224.0.0.0/3", "128.0.0.0/1",
		"255.255.255.254/31", "255.255.255.255/32", "0.0.0.0/0",
		"240.0.0.0/5", "224.0.0.0/4", "255.255.255.254/32", "10.0.0.0/8",
	}

	for _, cidr := range cidrs {
		t.Run("deny/"+cidr, func(t *testing.T) {
			require.NoErrorf(t, slot.Firewall.ApplyRules(t.Context(), false, nil, []string{cidr}),
				"denyOut %s must apply without error", cidr)
		})
		t.Run("allow/"+cidr, func(t *testing.T) {
			require.NoErrorf(t, slot.Firewall.ApplyRules(t.Context(), false, []string{cidr}, nil),
				"allowOut %s must apply without error", cidr)
		})
	}

	// Overlapping ranges must collapse before insertion. Redundant boundaries
	// can be rejected or silently truncate max-ending intervals.
	batches := [][]string{
		{"10.0.0.0/8", "240.0.0.0/4"},
		{"0.0.0.0/0", "10.0.0.0/8"},
		{"0.0.0.0/0", "240.0.0.0/4"},
		{"128.0.0.0/1", "240.0.0.0/4"},
		{"224.0.0.0/3", "240.0.0.0/4", "248.0.0.0/5"},
		{"255.255.255.255/32", "10.0.0.0/8"},
		{"128.0.0.0/1", "200.0.0.0/8"}, // ordinary nested inside a non-zero boundary
		{"128.0.0.0/1", "10.0.0.0/8"},  // ordinary below the boundary, kept
	}
	for _, b := range batches {
		t.Run("deny-batch/"+strings.Join(b, ","), func(t *testing.T) {
			require.NoErrorf(t, slot.Firewall.ApplyRules(t.Context(), false, nil, b),
				"denyOut %v must apply without error", b)
		})
		t.Run("allow-batch/"+strings.Join(b, ","), func(t *testing.T) {
			require.NoErrorf(t, slot.Firewall.ApplyRules(t.Context(), false, b, nil),
				"allowOut %v must apply without error", b)
		})
	}

	overlaps := []struct {
		cidrs  []string
		probes []ipv4SetProbe
	}{
		{
			[]string{"128.0.0.0/1", "200.0.0.0/8"},
			[]ipv4SetProbe{
				{netip.MustParseAddr("127.255.255.255"), false},
				{netip.MustParseAddr("128.0.0.0"), true},
				{netip.MustParseAddr("200.255.255.255"), true},
				{netip.MustParseAddr("201.0.0.0"), true},
				{maxIPv4, true},
			},
		},
		{
			[]string{"224.0.0.0/3", "240.0.0.1"},
			[]ipv4SetProbe{
				{netip.MustParseAddr("223.255.255.255"), false},
				{netip.MustParseAddr("224.0.0.0"), true},
				{netip.MustParseAddr("240.0.0.1"), true},
				{netip.MustParseAddr("240.0.0.2"), true},
				{maxIPv4, true},
			},
		},
	}
	for _, tc := range overlaps { //nolint:paralleltest // shares the test's single netns/nftables slot, cannot run in parallel
		name := strings.Join(tc.cidrs, ",")
		t.Run("deny-overlap/"+name, func(t *testing.T) {
			require.NoError(t, slot.Firewall.ApplyRules(t.Context(), false, nil, tc.cidrs))
			elements, err := slot.Firewall.conn.GetSetElements(slot.Firewall.userDenySet.Set())
			require.NoError(t, err)
			assertIPv4SetMembership(t, elements, tc.probes)
		})
		t.Run("allow-overlap/"+name, func(t *testing.T) {
			require.NoError(t, slot.Firewall.ApplyRules(t.Context(), false, tc.cidrs, nil))
			elements, err := slot.Firewall.conn.GetSetElements(slot.Firewall.userAllowSet.Set())
			require.NoError(t, err)
			assertIPv4SetMembership(t, elements, tc.probes)
		})
	}
}

// TestIPv4RangeEndingAtMax pins the classification the fix turns on: exactly the
// inputs whose range ends at 255.255.255.255 take the hand-built path.
func TestIPv4RangeEndingAtMax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in        string
		wantMax   bool
		wantStart string // only checked when wantMax
	}{
		{"0.0.0.0/0", true, "0.0.0.0"},
		{"128.0.0.0/1", true, "128.0.0.0"},
		{"224.0.0.0/3", true, "224.0.0.0"},
		{"240.0.0.0/4", true, "240.0.0.0"},
		{"248.0.0.0/5", true, "248.0.0.0"},
		{"255.0.0.0/8", true, "255.0.0.0"},
		{"255.255.255.254/31", true, "255.255.255.254"},
		{"255.255.255.255/32", true, "255.255.255.255"},
		{"255.255.255.255", true, "255.255.255.255"}, // bare address
		// ends below the ceiling → toolkit path
		{"240.0.0.0/5", false, ""},
		{"224.0.0.0/4", false, ""},
		{"255.255.255.254/32", false, ""},
		{"255.255.255.254", false, ""},
		{"10.0.0.0/8", false, ""},
		{"0.0.0.0/1", false, ""},
		// IPv6 is out of scope (the toolkit path handles / rejects it)
		{"::/0", false, ""},
		{"2001:db8::/32", false, ""},
		// unparseable → not a boundary; toolkit path surfaces the error
		{"not-a-cidr", false, ""},
	}
	for _, tc := range tests {
		start, end, ok := ipv4Range(tc.in)
		isMax := ok && end == maxIPv4
		require.Equalf(t, tc.wantMax, isMax, "ipv4Range(%q) ends at max", tc.in)
		if tc.wantMax {
			require.Equalf(t, netip.MustParseAddr(tc.wantStart), start, "ipv4Range(%q) start", tc.in)
		}
	}
}

// TestSplitEgressCIDRs pins the partition + subsumption the fix turns on. The
// nested case is the silent-truncation bug: an ordinary CIDR inside a non-zero
// boundary interval must be dropped, or the non-merge flush corrupts the range.
func TestSplitEgressCIDRs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		in          []string
		wantToolkit []string
		wantStart   string // "" ⇒ no boundary
	}{
		{"no boundary", []string{"10.0.0.0/8", "192.168.0.0/16"}, []string{"10.0.0.0/8", "192.168.0.0/16"}, ""},
		{"single boundary", []string{"240.0.0.0/4"}, []string{}, "240.0.0.0"},
		{"disjoint ordinary + boundary", []string{"10.0.0.0/8", "240.0.0.0/4"}, []string{"10.0.0.0/8"}, "240.0.0.0"},
		{"0.0.0.0/0 subsumes all", []string{"0.0.0.0/0", "10.0.0.0/8"}, []string{}, "0.0.0.0"},
		{"nested ordinary dropped", []string{"128.0.0.0/1", "200.0.0.0/8"}, []string{}, "128.0.0.0"},
		{"two boundary collapse", []string{"128.0.0.0/1", "240.0.0.0/4"}, []string{}, "128.0.0.0"},
		{"below-boundary ordinary kept", []string{"128.0.0.0/1", "10.0.0.0/8"}, []string{"10.0.0.0/8"}, "128.0.0.0"},
		{"mixed keep-below + drop-nested", []string{"128.0.0.0/1", "200.0.0.0/8", "10.0.0.0/8"}, []string{"10.0.0.0/8"}, "128.0.0.0"},
		{"broadcast only", []string{"255.255.255.255/32"}, []string{}, "255.255.255.255"},
	}
	for _, tc := range tests {
		toolkit, start := splitEgressCIDRs(tc.in)
		if tc.wantStart == "" {
			require.Falsef(t, start.IsValid(), "%s: boundaryStart valid", tc.name)
		} else {
			require.Equalf(t, netip.MustParseAddr(tc.wantStart), start, "%s: boundaryStart", tc.name)
		}
		require.Equalf(t, tc.wantToolkit, toolkit, "%s: toolkit set", tc.name)
	}
}
