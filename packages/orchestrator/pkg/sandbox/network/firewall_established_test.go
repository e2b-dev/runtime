//go:build linux

package network

import (
	"context"
	"io"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// newPolicySlot builds a firewall in the harness namespace under the given
// non-DNS UDP mode and wraps it in a slot that reads that mode.
func newPolicySlot(t *testing.T, h *udpHarness, idx int, mode NonDNSUDPMode) *Slot {
	t.Helper()

	config, err := ParseConfig()
	require.NoError(t, err)

	fw, err := NewFirewall(h.fwNSPath(), h.tap, config.OrchestratorInSandboxIPAddress, nil, nil, mode)
	require.NoError(t, err)
	slot := &Slot{Idx: idx, Firewall: fw, config: config, sandboxID: "sandbox-established"}
	slot.UseNonDNSUDPMode(func(context.Context) string { return string(mode) })
	t.Cleanup(func() { _ = slot.CloseFirewall(context.WithoutCancel(t.Context())) })

	return slot
}

// dialIn opens a connection from the named harness namespace.
func dialIn(t *testing.T, h *udpHarness, nsName, network, addr string) net.Conn {
	t.Helper()

	var conn net.Conn
	h.in(t, nsName, func() error {
		var dialer net.Dialer
		var err error
		conn, err = dialer.DialContext(t.Context(), network, addr)

		return err
	})
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

// A network update that denies a destination stops a UDP flow the guest
// opened to it while the policy still allowed it, in every non-DNS UDP mode
// and on DNS and non-DNS ports alike, while a flow to a destination the new
// policy still allows keeps working.
func TestFirewall_UpdateStopsEstablishedDeniedUDP(t *testing.T) { //nolint:paralleltest // builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	cases := []struct {
		port string
		mode NonDNSUDPMode
	}{
		{"53", NonDNSUDPModeOff},
		{"53", NonDNSUDPModeCount},
		{"53", NonDNSUDPModeDrop},
		{"5000", NonDNSUDPModeOff},
	}
	for _, tc := range cases { //nolint:paralleltest // builds netns state; cannot run in parallel
		t.Run(tc.port+"/"+string(tc.mode), func(t *testing.T) {
			idx := reserveNSTestIdx(t)
			h := newUDPHarness(t, idx)
			slot := newPolicySlot(t, h, idx, tc.mode)

			deniedEcho := echoServer(t, listenUDP(t, h, harnessTenantIP, tc.port))
			echoServer(t, listenUDP(t, h, harnessPlatformIP, tc.port))

			require.NoError(t, slot.ConfigureInternet(t.Context(), &orchestrator.SandboxNetworkConfig{
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					AllowedCidrs: []string{harnessTenantIP + "/32", harnessPlatformIP + "/32"},
				},
			}))

			denied := dialIn(t, h, h.guestNS, "udp4", net.JoinHostPort(harnessTenantIP, tc.port))
			allowed := dialIn(t, h, h.guestNS, "udp4", net.JoinHostPort(harnessPlatformIP, tc.port))
			require.True(t, sendAndExpectReply(t, denied, "before"), "the flow to the soon-denied address is open")
			require.True(t, sendAndExpectReply(t, allowed, "before"), "the flow to the still-allowed address is open")

			require.NoError(t, slot.UpdateInternet(t.Context(), &orchestrator.SandboxNetworkEgressConfig{
				AllowedCidrs: []string{harnessPlatformIP + "/32"},
				DeniedCidrs:  []string{"0.0.0.0/0"},
			}))
			receivedBefore := deniedEcho.Load()

			assert.False(t, sendAndExpectReply(t, denied, "after"), "the open flow to the denied address is cut")
			assert.Equal(t, receivedBefore, deniedEcho.Load(), "no datagram of the cut flow reaches the denied address")
			fresh := dialIn(t, h, h.guestNS, "udp4", net.JoinHostPort(harnessTenantIP, tc.port))
			assert.False(t, sendAndExpectReply(t, fresh, "fresh"), "a new flow to the denied address is dropped")
			assert.True(t, sendAndExpectReply(t, allowed, "after"), "the open flow to the still-allowed address keeps working")
		})
	}
}

// Connections opened into the guest, UDP and TCP, keep their replies under a
// deny-all policy and under DenyEgress: the guest answers to an address in
// the predefined deny set, as it does to the orchestrator's control path.
func TestFirewall_InboundFlowsSurviveDeny(t *testing.T) { //nolint:paralleltest // builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	slot := newPolicySlot(t, h, idx, NonDNSUDPModeOff)
	require.NoError(t, slot.ConfigureInternet(t.Context(), &orchestrator.SandboxNetworkConfig{}))

	var udpListener net.PacketConn
	var tcpListener net.Listener
	h.in(t, h.guestNS, func() error {
		var lc net.ListenConfig
		var err error
		udpListener, err = lc.ListenPacket(t.Context(), "udp4", net.JoinHostPort(harnessGuestIP, "7000"))
		if err != nil {
			return err
		}
		tcpListener, err = lc.Listen(t.Context(), "tcp4", net.JoinHostPort(harnessGuestIP, "7001"))

		return err
	})
	t.Cleanup(func() {
		_ = udpListener.Close()
		_ = tcpListener.Close()
	})
	echoServer(t, udpListener)
	go func() {
		for {
			conn, err := tcpListener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	udpAddr := net.JoinHostPort(harnessGuestIP, "7000")
	tcpAddr := net.JoinHostPort(harnessGuestIP, "7001")
	udpFlow := dialIn(t, h, h.sinkNS, "udp4", udpAddr)
	tcpFlow := dialIn(t, h, h.sinkNS, "tcp4", tcpAddr)
	require.True(t, sendAndExpectReply(t, udpFlow, "udp-default"), "the guest answers inbound UDP under the default policy")
	require.True(t, sendAndExpectReply(t, tcpFlow, "tcp-default"), "the guest answers inbound TCP under the default policy")

	for _, step := range []struct {
		name  string
		apply func() error
	}{
		{"deny-all", func() error {
			return slot.UpdateInternet(t.Context(), &orchestrator.SandboxNetworkEgressConfig{DeniedCidrs: []string{"0.0.0.0/0"}})
		}},
		{"deny-egress", func() error { return slot.DenyEgress(t.Context()) }},
	} {
		require.NoError(t, step.apply(), step.name)

		assert.True(t, sendAndExpectReply(t, udpFlow, step.name), "%s: an open inbound UDP flow keeps its replies", step.name)
		assert.True(t, sendAndExpectReply(t, tcpFlow, step.name), "%s: an open inbound TCP connection keeps its replies", step.name)
		assert.True(t, sendAndExpectReply(t, dialIn(t, h, h.sinkNS, "udp4", udpAddr), step.name), "%s: a new inbound UDP flow gets replies", step.name)
		assert.True(t, sendAndExpectReply(t, dialIn(t, h, h.sinkNS, "tcp4", tcpAddr), step.name), "%s: a new inbound TCP connection gets replies", step.name)
	}
}
