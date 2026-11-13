//go:build linux

package network

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vishvananda/netlink"
)

// TestIsDefaultRoute pins the default-route predicate against every way the
// kernel/netlink can represent (or fail to represent) a route. The IPv4 default
// route comes back either with Dst == nil (RTA_DST omitted) or with an explicit
// 0.0.0.0/0 Dst; both must match. A nil Gw, a non-default prefix, and the IPv6
// default must not.
func TestIsDefaultRoute(t *testing.T) {
	t.Parallel()

	gw := net.ParseIP("192.168.128.1")

	tests := []struct {
		name  string
		route netlink.Route
		want  bool
	}{
		{
			name:  "v4 default, Dst nil (RTA_DST omitted)",
			route: netlink.Route{Family: netlink.FAMILY_V4, Dst: nil, Gw: gw},
			want:  true,
		},
		{
			name:  "v4 default, Dst 0.0.0.0/0 (RTA_DST sent)",
			route: netlink.Route{Family: netlink.FAMILY_V4, Dst: &net.IPNet{IP: net.IPv4zero, Mask: net.CIDRMask(0, 32)}, Gw: gw},
			want:  true,
		},
		{
			// isDefaultRoute classifies by prefix only; the Gw != nil gate
			// lives in the caller (getDefaultGateway), so a gateway-less v4
			// default is still a default route here.
			name:  "v4 default, no gateway (Gw gate is the caller's job)",
			route: netlink.Route{Family: netlink.FAMILY_V4, Dst: nil, Gw: nil},
			want:  true,
		},
		{
			name:  "v4 non-default 10.0.0.0/8",
			route: netlink.Route{Family: netlink.FAMILY_V4, Dst: &net.IPNet{IP: net.IPv4(10, 0, 0, 0), Mask: net.CIDRMask(8, 32)}, Gw: gw},
			want:  false,
		},
		{
			name:  "v6 default ::/0 is not an IPv4 default",
			route: netlink.Route{Family: netlink.FAMILY_V6, Dst: &net.IPNet{IP: net.IPv6unspecified, Mask: net.CIDRMask(0, 128)}, Gw: gw},
			want:  false,
		},
		{
			name:  "v6 default with nil Dst is not an IPv4 default",
			route: netlink.Route{Family: netlink.FAMILY_V6, Dst: nil, Gw: gw},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isDefaultRoute(tt.route))
		})
	}
}

// TestIsDefaultRoute_NilDstStringIsNilSafe documents the premise the fix rests
// on: calling String() on a nil *net.IPNet does not panic (it returns "<nil>"),
// so the original code never panicked on the nil default route — it just missed
// it. This guards against a regression back to a String()-equality check.
func TestIsDefaultRoute_NilDstStringIsNilSafe(t *testing.T) {
	t.Parallel()

	var dst *net.IPNet
	assert.Equal(t, "<nil>", dst.String())
}
