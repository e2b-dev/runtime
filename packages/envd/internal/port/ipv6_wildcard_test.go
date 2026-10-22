package port

import (
	"testing"

	gopsnet "github.com/shirou/gopsutil/v4/net"

	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/upgrade"
)

// Bug A: the "::" IPv6 wildcard listener must match the port-forwarder filter.
func TestScannerFilter_MatchesIPv6Wildcard(t *testing.T) {
	sf := &ScannerFilter{
		IPs:   []string{"127.0.0.1", "localhost", "::1", "::"},
		State: "LISTEN",
	}
	cases := []struct {
		ip, state string
		want      bool
	}{
		{"::", "LISTEN", true}, // IPv6 wildcard — the bug this fixes
		{"127.0.0.1", "LISTEN", true},
		{"::1", "LISTEN", true},
		{"::", "ESTABLISHED", false},  // wrong state
		{"10.0.0.1", "LISTEN", false}, // not a loopback/wildcard
	}
	for _, c := range cases {
		proc := &gopsnet.ConnectionStat{Status: c.state, Laddr: gopsnet.Addr{IP: c.ip, Port: 8080}}
		if got := sf.Match(proc); got != c.want {
			t.Errorf("Match(ip=%q,state=%q) = %v, want %v", c.ip, c.state, got, c.want)
		}
	}
}

// Bug C: socat backend must use literal loopback addresses, not "localhost".
func TestBackendAddr(t *testing.T) {
	if got := backendAddr(4); got != "127.0.0.1" {
		t.Errorf("backendAddr(4) = %q, want 127.0.0.1", got)
	}
	if got := backendAddr(6); got != "[::1]" {
		t.Errorf("backendAddr(6) = %q, want [::1]", got)
	}
}

// Handover: old single-dash keys are migrated to the new pid-port-ip format so a
// live upgrade does not spawn a duplicate socat for the same listener.
func TestNormalizeForwardKey(t *testing.T) {
	cases := []struct {
		key    string
		family uint32
		want   string
	}{
		{"42-8080", 4, "42-8080-127.0.0.1"},           // old format, IPv4 inferred
		{"42-8080", 6, "42-8080-::1"},                 // old format, IPv6 inferred
		{"42-8080-127.0.0.1", 4, "42-8080-127.0.0.1"}, // new format, unchanged
		{"42-8080-::1", 6, "42-8080-::1"},             // new format, unchanged
	}
	for _, c := range cases {
		fp := &upgrade.ForwardedPort{Key: c.key, Family: c.family}
		if got := normalizeForwardKey(fp); got != c.want {
			t.Errorf("normalizeForwardKey(%q,fam=%d) = %q, want %q", c.key, c.family, got, c.want)
		}
	}
}
