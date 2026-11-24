//go:build linux

package network

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// Nothing accepts on the listener: the kernel completes the handshake from
// the backlog.
func listenTCP(t *testing.T, h *udpHarness, ip string, port string) {
	t.Helper()

	var listener net.Listener
	h.in(t, h.sinkNS, func() error {
		var lc net.ListenConfig
		var err error
		listener, err = lc.Listen(t.Context(), "tcp4", net.JoinHostPort(ip, port))

		return err
	})
	t.Cleanup(func() { _ = listener.Close() })
}

// Only a dial timeout counts as unreachable. Any other error fails the test,
// so a broken harness is not read as a drop.
func tcpConnects(t *testing.T, h *udpHarness, addr string) bool {
	t.Helper()

	connected := false
	h.in(t, h.guestNS, func() error {
		dialer := net.Dialer{Timeout: time.Second}
		conn, err := dialer.DialContext(t.Context(), "tcp4", addr)
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				return nil
			}

			return err
		}
		connected = true

		return conn.Close()
	})

	return connected
}

// Azure's host endpoint is a public address, so no private or link-local
// range covers it: only its own deny entry keeps the guest off it.
func TestFirewall_AzureHostEndpointStaysDenied(t *testing.T) { //nolint:paralleltest // builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	const (
		hostEndpoint   = "168.63.129.16"
		wireServerPort = "80"
		hostPluginPort = "32526"
	)

	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	ipCmd(t, "-n", h.sinkNS, "addr", "add", hostEndpoint+"/32", "dev", "lo")
	slot := newPolicySlot(t, h, idx, NonDNSUDPModeOff)

	listenTCP(t, h, hostEndpoint, wireServerPort)
	listenTCP(t, h, hostEndpoint, hostPluginPort)
	deniedDNS := echoServer(t, listenUDP(t, h, hostEndpoint, "53"))
	listenTCP(t, h, harnessPublicIP, "80")
	echoServer(t, listenUDP(t, h, harnessPublicIP, "53"))

	policies := []struct {
		name string
		// nil keeps the chain the slot was built with.
		egress *orchestrator.SandboxNetworkEgressConfig
	}{
		{"default policy", nil},
		{"allow list names the endpoint", &orchestrator.SandboxNetworkEgressConfig{AllowedCidrs: []string{hostEndpoint + "/32"}}},
		{"allow list covers every address", &orchestrator.SandboxNetworkEgressConfig{AllowedCidrs: []string{"0.0.0.0/0"}}},
	}
	for _, policy := range policies { //nolint:paralleltest // builds netns state; cannot run in parallel
		t.Run(policy.name, func(t *testing.T) {
			if policy.egress != nil {
				require.NoError(t, slot.UpdateInternet(t.Context(), policy.egress))
			}

			require.True(t, tcpConnects(t, h, net.JoinHostPort(harnessPublicIP, "80")), "TCP to a public address is forwarded")
			require.True(t, sendAndExpectReply(t, dialIn(t, h, h.guestNS, "udp4", net.JoinHostPort(harnessPublicIP, "53")), "public"), "UDP to a public address is forwarded")

			assert.False(t, tcpConnects(t, h, net.JoinHostPort(hostEndpoint, wireServerPort)), "TCP to WireServer is dropped")
			assert.False(t, tcpConnects(t, h, net.JoinHostPort(hostEndpoint, hostPluginPort)), "TCP to the host plugin is dropped")
			assert.False(t, sendAndExpectReply(t, dialIn(t, h, h.guestNS, "udp4", net.JoinHostPort(hostEndpoint, "53")), "denied"), "UDP 53 to the host endpoint is dropped")
			assert.Zero(t, deniedDNS.Load(), "no datagram reaches the host endpoint")
		})
	}
}
