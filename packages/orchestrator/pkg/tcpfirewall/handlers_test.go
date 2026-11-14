//go:build linux

package tcpfirewall

import (
	"fmt"
	"net"
	"syscall"
	"testing"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	sandbox_network "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-network"
)

func TestMatchDomain(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		hostname string
		pattern  string
		want     bool
	}{
		// Exact matches
		{
			name:     "exact match",
			hostname: "example.com",
			pattern:  "example.com",
			want:     true,
		},
		{
			name:     "exact match case insensitive",
			hostname: "Example.COM",
			pattern:  "example.com",
			want:     true,
		},
		{
			name:     "exact match pattern uppercase",
			hostname: "example.com",
			pattern:  "EXAMPLE.COM",
			want:     true,
		},
		{
			name:     "no match different domain",
			hostname: "example.com",
			pattern:  "other.com",
			want:     false,
		},

		// Wildcard *
		{
			name:     "wildcard matches any hostname",
			hostname: "anything.example.com",
			pattern:  "*",
			want:     true,
		},
		{
			name:     "wildcard matches simple hostname",
			hostname: "localhost",
			pattern:  "*",
			want:     true,
		},

		// Suffix wildcards *.domain
		{
			name:     "suffix wildcard matches subdomain",
			hostname: "api.example.com",
			pattern:  "*.example.com",
			want:     true,
		},
		{
			name:     "suffix wildcard matches nested subdomain",
			hostname: "deep.nested.example.com",
			pattern:  "*.example.com",
			want:     true,
		},
		{
			name:     "suffix wildcard case insensitive",
			hostname: "API.EXAMPLE.COM",
			pattern:  "*.example.com",
			want:     true,
		},
		{
			name:     "suffix wildcard does not match exact domain",
			hostname: "example.com",
			pattern:  "*.example.com",
			want:     false,
		},
		{
			name:     "suffix wildcard does not match different domain",
			hostname: "api.other.com",
			pattern:  "*.example.com",
			want:     false,
		},
		{
			name:     "suffix wildcard does not match partial suffix",
			hostname: "notexample.com",
			pattern:  "*.example.com",
			want:     false,
		},

		// Edge cases
		{
			name:     "empty hostname",
			hostname: "",
			pattern:  "example.com",
			want:     false,
		},
		{
			name:     "empty pattern",
			hostname: "example.com",
			pattern:  "",
			want:     false,
		},
		{
			name:     "both empty - empty pattern never matches",
			hostname: "",
			pattern:  "",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := matchDomain(tt.hostname, tt.pattern)
			if got != tt.want {
				t.Errorf("matchDomain(%q, %q) = %v, want %v", tt.hostname, tt.pattern, got, tt.want)
			}
		})
	}
}

func TestIsEgressAllowed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		network   *orchestrator.SandboxNetworkConfig
		hostname  string
		ip        net.IP
		want      bool
		wantError bool
	}{
		// ---------------------------------------------------------------------
		// Default Allow Behavior
		// Traffic is allowed unless explicitly blocked by denied CIDRs.
		// ---------------------------------------------------------------------
		{
			name:     "nil network config allows all",
			network:  nil,
			hostname: "example.com",
			ip:       net.ParseIP("1.2.3.4"),
			want:     true,
		},
		{
			name:     "nil egress config allows all",
			network:  &orchestrator.SandboxNetworkConfig{},
			hostname: "example.com",
			ip:       net.ParseIP("1.2.3.4"),
			want:     true,
		},
		{
			name: "empty egress config allows all",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{},
			},
			hostname: "example.com",
			ip:       net.ParseIP("1.2.3.4"),
			want:     true,
		},

		// ---------------------------------------------------------------------
		// Denied CIDRs (The Only Blocking Mechanism)
		// This is the ONLY way to block traffic. Everything else is allowed.
		// ---------------------------------------------------------------------
		{
			name: "denied CIDR blocks traffic",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					DeniedCidrs: []string{"10.0.0.0/8"},
				},
			},
			hostname: "",
			ip:       net.ParseIP("10.1.2.3"),
			want:     false,
		},
		{
			name: "denied CIDR exact IP blocks",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					DeniedCidrs: []string{"1.2.3.4/32"},
				},
			},
			hostname: "",
			ip:       net.ParseIP("1.2.3.4"),
			want:     false,
		},
		{
			name: "IP not in denied CIDR allows",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					DeniedCidrs: []string{"10.0.0.0/8"},
				},
			},
			hostname: "",
			ip:       net.ParseIP("192.168.1.1"),
			want:     true,
		},

		// ---------------------------------------------------------------------
		// Whitelist Mode: Deny All + Bypass Exceptions
		// ---------------------------------------------------------------------
		{
			name: "whitelist mode: deny all with domain bypass",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					AllowedDomains: []string{"example.com"},
					DeniedCidrs:    []string{"0.0.0.0/0"}, // Required to block by default
				},
			},
			hostname: "example.com",
			ip:       net.ParseIP("1.2.3.4"),
			want:     true, // Domain bypass checked before denied CIDRs
		},
		{
			name: "whitelist mode: deny all with CIDR bypass",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					AllowedCidrs: []string{"10.0.0.0/8"},
					DeniedCidrs:  []string{"0.0.0.0/0"}, // Required to block by default
				},
			},
			hostname: "",
			ip:       net.ParseIP("10.1.2.3"),
			want:     true, // CIDR bypass checked before denied CIDRs
		},
		{
			name: "whitelist mode: traffic blocked when no bypass matches",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					AllowedDomains: []string{"example.com"},
					DeniedCidrs:    []string{"0.0.0.0/0"},
				},
			},
			hostname: "other.com", // Domain doesn't match bypass
			ip:       net.ParseIP("1.2.3.4"),
			want:     false, // Blocked by denied CIDR (0.0.0.0/0)
		},

		// ---------------------------------------------------------------------
		// Bypass Rules Always Win Over Deny
		// ---------------------------------------------------------------------
		{
			name: "bypass: broad allowed CIDR bypasses specific denied CIDR",
			// Warning: A broad allow will bypass a more specific deny.
			// IP 10.1.1.1 matches allowed 10.0.0.0/8 -> bypass, never checks denied.
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					AllowedCidrs: []string{"10.0.0.0/8"},
					DeniedCidrs:  []string{"10.1.1.1/32"},
				},
			},
			hostname: "",
			ip:       net.ParseIP("10.1.1.1"),
			want:     true, // Bypass matched, deny never checked
		},
		{
			name: "bypass: specific allowed CIDR bypasses broad denied CIDR",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					AllowedCidrs: []string{"10.1.1.1/32"},
					DeniedCidrs:  []string{"10.0.0.0/8"},
				},
			},
			hostname: "",
			ip:       net.ParseIP("10.1.1.1"),
			want:     true, // Bypass matched
		},
		{
			name: "bypass: domain bypass skips denied CIDR check entirely",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					AllowedDomains: []string{"example.com"},
					DeniedCidrs:    []string{sandbox_network.AllInternetTrafficCIDR},
				},
			},
			hostname: "example.com",
			ip:       net.ParseIP("1.2.3.4"),
			want:     true,
		},
		{
			name: "no bypass match: denied CIDR blocks traffic",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					AllowedDomains: []string{"allowed.com"},
					AllowedCidrs:   []string{"192.168.0.0/16"},
					DeniedCidrs:    []string{sandbox_network.AllInternetTrafficCIDR},
				},
			},
			hostname: "other.com",
			ip:       net.ParseIP("10.1.2.3"),
			want:     false,
		},

		// ---------------------------------------------------------------------
		// Multiple Rules
		// ---------------------------------------------------------------------
		{
			name: "multiple allowed domains second matches",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					AllowedDomains: []string{"first.com", "second.com", "third.com"},
					DeniedCidrs:    []string{sandbox_network.AllInternetTrafficCIDR},
				},
			},
			hostname: "second.com",
			ip:       net.ParseIP("1.2.3.4"),
			want:     true,
		},
		{
			name: "multiple allowed CIDRs second matches",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					AllowedCidrs: []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"},
					DeniedCidrs:  []string{sandbox_network.AllInternetTrafficCIDR},
				},
			},
			hostname: "",
			ip:       net.ParseIP("172.20.1.1"),
			want:     true,
		},
		{
			name: "multiple denied CIDRs second matches",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					DeniedCidrs: []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"},
				},
			},
			hostname: "",
			ip:       net.ParseIP("172.20.1.1"),
			want:     false,
		},

		// ---------------------------------------------------------------------
		// Error Handling
		// ---------------------------------------------------------------------
		{
			name: "invalid allowed CIDR returns error",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					AllowedCidrs: []string{"invalid-cidr"},
				},
			},
			hostname:  "",
			ip:        net.ParseIP("1.2.3.4"),
			want:      false,
			wantError: true,
		},
		{
			name: "invalid denied CIDR returns error",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					DeniedCidrs: []string{"not-a-cidr"},
				},
			},
			hostname:  "",
			ip:        net.ParseIP("1.2.3.4"),
			want:      false,
			wantError: true,
		},
		{
			name: "allowed CIDR checked before invalid denied CIDR",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					AllowedCidrs: []string{"1.2.3.0/24"},
					DeniedCidrs:  []string{"invalid"},
				},
			},
			hostname:  "",
			ip:        net.ParseIP("1.2.3.4"),
			want:      true,
			wantError: false,
		},
		{
			name: "default_deny drops traffic matching no allow rule",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{DefaultDeny: true},
			},
			hostname: "example.com",
			ip:       net.ParseIP("1.2.3.4"),
			want:     false,
		},
		{
			name: "default_deny still allows an explicitly allowed domain",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					DefaultDeny:    true,
					AllowedDomains: []string{"example.com"},
				},
			},
			hostname: "example.com",
			ip:       net.ParseIP("1.2.3.4"),
			want:     true,
		},
		{
			name: "default_deny still allows an explicitly allowed CIDR",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					DefaultDeny:  true,
					AllowedCidrs: []string{"1.2.3.0/24"},
				},
			},
			hostname: "",
			ip:       net.ParseIP("1.2.3.4"),
			want:     true,
		},
		{
			name: "default_deny drops a no-hostname (no-SNI) connection not in an allow CIDR",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{DefaultDeny: true},
			},
			hostname: "",
			ip:       net.ParseIP("9.9.9.9"),
			want:     false,
		},
		{
			name: "denied_domains blocks a domain even under default-allow",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					DeniedDomains: []string{"*.evil.com"},
				},
			},
			hostname: "c2.evil.com",
			ip:       net.ParseIP("1.2.3.4"),
			want:     false,
		},
		{
			name: "allowed_domains beats denied_domains (allow has priority)",
			network: &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					AllowedDomains: []string{"safe.evil.com"},
					DeniedDomains:  []string{"*.evil.com"},
				},
			},
			hostname: "safe.evil.com",
			ip:       net.ParseIP("1.2.3.4"),
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sbx := &sandbox.Sandbox{
				Metadata: &sandbox.Metadata{
					Config: sandbox.NewConfig(sandbox.Config{Network: tt.network}),
				},
			}

			got, _, err := isEgressAllowed(sbx, tt.hostname, tt.ip)

			if tt.wantError {
				if err == nil {
					t.Errorf("isEgressAllowed() expected error, got nil")
				}

				return
			}

			if err != nil {
				t.Errorf("isEgressAllowed() unexpected error: %v", err)

				return
			}

			if got != tt.want {
				t.Errorf("isEgressAllowed() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAlwaysDeniedCIDRs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		ip   string
		want bool
	}{
		// IPs in denied CIDRs (internal/private ranges)
		{"10.0.0.1 is denied", "10.0.0.1", true},
		{"10.255.255.255 is denied", "10.255.255.255", true},
		{"192.168.1.1 is denied", "192.168.1.1", true},
		{"172.16.0.1 is denied", "172.16.0.1", true},
		{"172.31.255.255 is denied", "172.31.255.255", true},
		{"169.254.1.1 is denied (link-local)", "169.254.1.1", true},
		{"127.0.0.1 is denied (loopback)", "127.0.0.1", true},

		// IPs NOT in denied CIDRs (public IPs)
		{"8.8.8.8 is allowed (Google DNS)", "8.8.8.8", false},
		{"1.1.1.1 is allowed (Cloudflare)", "1.1.1.1", false},
		{"142.250.80.46 is allowed (Google)", "142.250.80.46", false},

		// IPv6 denied ranges
		{"::1 is denied (IPv6 loopback)", "::1", true},
		{"fc00::1 is denied (IPv6 unique local)", "fc00::1", true},
		{"fe80::1 is denied (IPv6 link-local)", "fe80::1", true},

		// IPv6 allowed (public)
		{"2001:4860:4860::8888 is allowed (Google IPv6 DNS)", "2001:4860:4860::8888", false},

		// Edge cases around CIDR boundaries
		{"172.15.255.255 is allowed (just before 172.16.0.0/12)", "172.15.255.255", false},
		{"172.32.0.0 is allowed (just after 172.16.0.0/12)", "172.32.0.0", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ip := net.ParseIP(tt.ip)
			if ip == nil {
				t.Fatalf("Failed to parse IP: %s", tt.ip)
			}

			got := isIPInAlwaysDeniedCIDRs(ip)
			if got != tt.want {
				t.Errorf("isIPInDeniedCIDRs(%s) = %v, want %v", tt.ip, got, tt.want)
			}
		})
	}
}

// Reads IP_TOS back off a real socket, so the shift is checked against the kernel.
func TestMarkDSCP_SetsSocketTOS(t *testing.T) {
	t.Parallel()

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()

	for _, tos := range []int{0, 8 << 2, 16 << 2, 63 << 2} {
		t.Run(fmt.Sprintf("tos_%#x", tos), func(t *testing.T) {
			t.Parallel()

			dialer := &net.Dialer{
				Control: func(_, _ string, c syscall.RawConn) error { return markDSCP(c, tos) },
			}

			conn, err := dialer.DialContext(t.Context(), "tcp4", ln.Addr().String())
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			t.Cleanup(func() { _ = conn.Close() })

			raw, err := conn.(*net.TCPConn).SyscallConn()
			if err != nil {
				t.Fatalf("syscall conn: %v", err)
			}

			var got int
			var sockErr error
			if err := raw.Control(func(fd uintptr) {
				got, sockErr = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_TOS)
			}); err != nil {
				t.Fatalf("control: %v", err)
			}
			if sockErr != nil {
				t.Fatalf("getsockopt IP_TOS: %v", sockErr)
			}

			if got != tos {
				t.Errorf("IP_TOS = %#x, want %#x", got, tos)
			}
		})
	}
}
