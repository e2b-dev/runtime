//go:build linux

package network

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containernetworking/plugins/pkg/ns"
	"github.com/google/nftables"
	"github.com/google/nftables/expr"
	"github.com/mdlayher/netlink"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// udpHarness joins three scratch namespaces with two veth pairs: the guest's
// uplink enters the firewall namespace under the tap name the firewall
// matches, and the sink owns 1.1.1.1 so every forwarded packet lands on a
// socket there. The firewall namespace carries the slot's name for its index,
// so the slot's own methods find it.
type udpHarness struct {
	fwNS, guestNS, sinkNS string
	tap                   string
}

const (
	harnessGuestIP  = "10.200.0.2"
	harnessFwGuest  = "10.200.0.1"
	harnessFwSink   = "10.200.0.5"
	harnessSinkIP   = "10.200.0.6"
	harnessPublicIP = "1.1.1.1"
	// harnessTenantIP and harnessPlatformIP are the sink's addresses a test
	// may list in the user and the predefined allow set.
	harnessTenantIP   = "8.8.8.8"
	harnessPlatformIP = "9.9.9.9"
	// The guest link's IPv6 addresses. Bytes 8 to 11 of the guest's address
	// are 198.51.100.1, so a rule reading them as an IPv4 destination finds
	// an address in neither allow set.
	harnessFwGuestIP6 = "fd00::c633:6401:0:1"
	harnessGuestIP6   = "fd00::c633:6401:0:2"
)

func ipCmd(t *testing.T, args ...string) {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "ip", args...).CombinedOutput()
	require.NoErrorf(t, err, "ip %s: %s", strings.Join(args, " "), out)
}

func newUDPHarness(t *testing.T, idx int) *udpHarness {
	t.Helper()

	h := &udpHarness{
		fwNS:    fmt.Sprintf("ns-%d", idx),
		guestNS: fmt.Sprintf("udpgst-%d", idx),
		sinkNS:  fmt.Sprintf("udpsnk-%d", idx),
		tap:     fmt.Sprintf("tap%d", idx),
	}
	guest := fmt.Sprintf("gst%d", idx)
	uplink := fmt.Sprintf("upl%d", idx)
	sink := fmt.Sprintf("snk%d", idx)

	// Deleting a namespace takes its links with it; the host-side deletes only
	// matter when setup failed before a link was moved.
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		for _, name := range []string{h.fwNS, h.guestNS, h.sinkNS} {
			_ = exec.CommandContext(ctx, "ip", "netns", "del", name).Run()
		}
		for _, link := range []string{h.tap, uplink} {
			_ = exec.CommandContext(ctx, "ip", "link", "del", link).Run()
		}
	})

	for _, name := range []string{h.fwNS, h.guestNS, h.sinkNS} {
		ipCmd(t, "netns", "add", name)
	}
	ipCmd(t, "link", "add", h.tap, "type", "veth", "peer", "name", guest)
	ipCmd(t, "link", "add", uplink, "type", "veth", "peer", "name", sink)
	ipCmd(t, "link", "set", h.tap, "netns", h.fwNS)
	ipCmd(t, "link", "set", guest, "netns", h.guestNS)
	ipCmd(t, "link", "set", uplink, "netns", h.fwNS)
	ipCmd(t, "link", "set", sink, "netns", h.sinkNS)

	ipCmd(t, "-n", h.fwNS, "addr", "add", harnessFwGuest+"/30", "dev", h.tap)
	ipCmd(t, "-n", h.fwNS, "addr", "add", harnessFwGuestIP6+"/64", "dev", h.tap, "nodad")
	ipCmd(t, "-n", h.fwNS, "addr", "add", harnessFwSink+"/30", "dev", uplink)
	ipCmd(t, "-n", h.fwNS, "link", "set", h.tap, "up")
	ipCmd(t, "-n", h.fwNS, "link", "set", uplink, "up")
	ipCmd(t, "-n", h.fwNS, "route", "add", "default", "via", harnessSinkIP)
	ipCmd(t, "netns", "exec", h.fwNS, "sysctl", "-qw", "net.ipv4.ip_forward=1")

	ipCmd(t, "-n", h.guestNS, "addr", "add", harnessGuestIP+"/30", "dev", guest)
	ipCmd(t, "-n", h.guestNS, "addr", "add", harnessGuestIP6+"/64", "dev", guest, "nodad")
	ipCmd(t, "-n", h.guestNS, "link", "set", guest, "up")
	ipCmd(t, "-n", h.guestNS, "route", "add", "default", "via", harnessFwGuest)

	ipCmd(t, "-n", h.sinkNS, "addr", "add", harnessSinkIP+"/30", "dev", sink)
	for _, ip := range []string{harnessPublicIP, harnessTenantIP, harnessPlatformIP} {
		ipCmd(t, "-n", h.sinkNS, "addr", "add", ip+"/32", "dev", sink)
	}
	ipCmd(t, "-n", h.sinkNS, "link", "set", sink, "up")
	ipCmd(t, "-n", h.sinkNS, "route", "add", "default", "via", harnessFwSink)

	return h
}

// fwNSPath is where the firewall namespace is bound, what a slot's firewall
// is built from.
func (h *udpHarness) fwNSPath() string {
	return filepath.Join(NetNamespacesDir, h.fwNS)
}

// in runs fn with the calling thread inside the named namespace; sockets and
// netlink connections it opens stay bound to that namespace afterwards.
func (h *udpHarness) in(t *testing.T, nsName string, fn func() error) {
	t.Helper()

	handle, err := ns.GetNS(filepath.Join(NetNamespacesDir, nsName))
	require.NoError(t, err)
	defer handle.Close()

	require.NoError(t, handle.Do(func(ns.NetNS) error { return fn() }))
}

// countDatagrams reads until no datagram has arrived for a second.
func countDatagrams(t *testing.T, conn net.PacketConn) int {
	t.Helper()

	buf := make([]byte, 64)
	count := 0
	for {
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
		_, _, err := conn.ReadFrom(buf)
		if err != nil {
			var netErr net.Error
			require.Truef(t, errors.As(err, &netErr) && netErr.Timeout(), "unexpected read error: %v", err)

			return count
		}
		count++
	}
}

// nonDNSUDPPointsByChain collects the metric and keys every point by the
// chain it was reported under; every point must carry this datapath's
// network version.
func nonDNSUDPPointsByChain(t *testing.T, reader *sdkmetric.ManualReader) map[ChainDescriptor]int64 {
	t.Helper()

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.WithoutCancel(t.Context()), &collected))
	points := map[ChainDescriptor]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != nonDNSUDPMetricName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, point := range sum.DataPoints {
				mode, _ := point.Attributes.Value("mode")
				posture, _ := point.Attributes.Value("posture")
				version, _ := point.Attributes.Value("network_version")
				require.Equal(t, "1", version.AsString())
				points[ChainDescriptor{Mode: NonDNSUDPMode(mode.AsString()), Posture: posture.AsString()}] = point.Value
			}
		}
	}

	return points
}

// swapNonDNSUDPCounter points the package counter at a manual reader for the
// test's duration.
func swapNonDNSUDPCounter(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()

	original := nonDNSUDPCounter
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		nonDNSUDPCounter = original
		_ = provider.Shutdown(context.WithoutCancel(t.Context()))
	})

	counter, err := provider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network").Int64Counter(nonDNSUDPMetricName)
	require.NoError(t, err)
	nonDNSUDPCounter = counter

	return reader
}

// echoServer answers every datagram with its payload and counts what it
// received; a reply is what turns the guest's flow into an established one.
func echoServer(t *testing.T, conn net.PacketConn) *atomic.Int32 {
	t.Helper()

	var received atomic.Int32
	go func() {
		buf := make([]byte, 64)
		for {
			n, addr, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			received.Add(1)
			_, _ = conn.WriteTo(buf[:n], addr)
		}
	}()

	return &received
}

// listenUDP opens a UDP socket in the sink namespace on the given address.
func listenUDP(t *testing.T, h *udpHarness, ip string, port string) net.PacketConn {
	t.Helper()

	var conn net.PacketConn
	h.in(t, h.sinkNS, func() error {
		var lc net.ListenConfig
		var err error
		conn, err = lc.ListenPacket(t.Context(), "udp4", net.JoinHostPort(ip, port))

		return err
	})
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

// sendAndExpectReply writes a datagram on the guest's flow and waits for the
// sink's echo; it returns whether the echo arrived.
func sendAndExpectReply(t *testing.T, flow net.Conn, payload string) bool {
	t.Helper()

	_, err := flow.Write([]byte(payload))
	require.NoError(t, err)
	require.NoError(t, flow.SetReadDeadline(time.Now().Add(time.Second)))
	buf := make([]byte, 64)
	n, err := flow.Read(buf)
	if err != nil {
		var netErr net.Error
		require.Truef(t, errors.As(err, &netErr) && netErr.Timeout(), "unexpected read error: %v", err)

		return false
	}
	require.Equal(t, payload, string(buf[:n]))

	return true
}

// sendSideTraffic sends what the rule must leave alone: one DNS datagram,
// one TCP connection, and one datagram each to the tenant-allowed and the
// platform-allowed address.
func sendSideTraffic(t *testing.T, h *udpHarness) {
	t.Helper()

	ctx := t.Context()
	h.in(t, h.guestNS, func() error {
		if err := sendDatagram(ctx, harnessPublicIP+":53", "dns"); err != nil {
			return err
		}
		dialer := net.Dialer{Timeout: 3 * time.Second}
		conn, err := dialer.DialContext(ctx, "tcp4", net.JoinHostPort(harnessPublicIP, "443"))
		if err != nil {
			return fmt.Errorf("tcp must stay untouched: %w", err)
		}
		if err := conn.Close(); err != nil {
			return err
		}
		if err := sendDatagram(ctx, harnessTenantIP+":443", "tenant"); err != nil {
			return err
		}

		return sendDatagram(ctx, harnessPlatformIP+":443", "platform")
	})
}

// A guest flow to UDP 443 that gets a reply keeps being counted on every
// later datagram, and a flip to drop mode drops that established flow too;
// DNS, TCP and datagrams to an address in either allow set are neither
// counted nor dropped in both modes, and the close reports each chain's
// tally under its own mode and posture.
func TestFirewall_NonDNSUDP_PacketPath(t *testing.T) { //nolint:paralleltest // replaces the package counter and builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	ctx := t.Context()
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	reader := swapNonDNSUDPCounter(t)
	tenantAllowed := []string{harnessTenantIP + "/32"}

	fw, err := NewFirewall(h.fwNSPath(), h.tap, config.OrchestratorInSandboxIPAddress, []string{harnessPlatformIP + "/32"}, nil, NonDNSUDPModeCount)
	require.NoError(t, err)
	require.NoError(t, fw.ApplyRules(ctx, false, NonDNSUDPModeCount, tenantAllowed, nil, false))
	slot := &Slot{Idx: idx, Firewall: fw, config: config, sandboxID: "sandbox-packet-path"}
	t.Cleanup(func() { _ = slot.CloseFirewall(context.WithoutCancel(t.Context())) })

	received := echoServer(t, listenUDP(t, h, harnessPublicIP, "443"))
	udp53 := listenUDP(t, h, harnessPublicIP, "53")
	udpTenant := listenUDP(t, h, harnessTenantIP, "443")
	udpPlatform := listenUDP(t, h, harnessPlatformIP, "443")
	var tcp443 net.Listener
	h.in(t, h.sinkNS, func() error {
		var lc net.ListenConfig
		var err error
		tcp443, err = lc.Listen(ctx, "tcp4", net.JoinHostPort(harnessPublicIP, "443"))

		return err
	})
	defer tcp443.Close()

	var flow net.Conn
	h.in(t, h.guestNS, func() error {
		var dialer net.Dialer
		var err error
		flow, err = dialer.DialContext(ctx, "udp4", net.JoinHostPort(harnessPublicIP, "443"))

		return err
	})
	defer flow.Close()

	// Count mode: the first datagram gets its reply, which establishes the
	// flow; the next two must still be counted.
	require.True(t, sendAndExpectReply(t, flow, "flow-0"), "count mode forwards the first datagram")
	require.True(t, sendAndExpectReply(t, flow, "flow-1"), "count mode forwards an established flow")
	require.True(t, sendAndExpectReply(t, flow, "flow-2"))
	sendSideTraffic(t, h)
	require.Equal(t, 1, countDatagrams(t, udp53), "DNS is forwarded")
	require.Equal(t, 1, countDatagrams(t, udpTenant), "a tenant-allowed address is forwarded")
	require.Equal(t, 1, countDatagrams(t, udpPlatform), "a platform-allowed address is forwarded")
	require.Equal(t, int32(3), received.Load())

	snapshot, err := fw.Harvest()
	require.NoError(t, err)
	require.Equal(t, ChainDescriptor{Mode: NonDNSUDPModeCount, Posture: postureRules}, snapshot.ChainDescriptor)
	require.Equal(t, uint64(3), snapshot.Packets, "every datagram of the flow is counted, established or not")
	require.Positive(t, snapshot.Bytes)

	// Drop mode on the live flow: its conntrack entry must not keep it open.
	require.NoError(t, fw.ApplyRules(ctx, false, NonDNSUDPModeDrop, tenantAllowed, nil, false))
	require.False(t, sendAndExpectReply(t, flow, "flow-3"), "drop mode drops an established flow")
	require.False(t, sendAndExpectReply(t, flow, "flow-4"))
	require.False(t, sendAndExpectReply(t, flow, "flow-5"))
	sendSideTraffic(t, h)
	require.Equal(t, 1, countDatagrams(t, udp53), "DNS is forwarded in drop mode")
	require.Equal(t, 1, countDatagrams(t, udpTenant), "a tenant-allowed address is forwarded in drop mode")
	require.Equal(t, 1, countDatagrams(t, udpPlatform), "a platform-allowed address is forwarded in drop mode")
	require.Equal(t, int32(3), received.Load(), "drop mode forwards none of the flow's datagrams")

	snapshot, err = fw.Harvest()
	require.NoError(t, err)
	require.Equal(t, ChainDescriptor{Mode: NonDNSUDPModeDrop, Posture: postureRules}, snapshot.ChainDescriptor)
	require.Equal(t, uint64(3), snapshot.Packets, "dropped datagrams are counted")

	require.NoError(t, slot.CloseFirewall(t.Context()))
	require.Equal(t, map[ChainDescriptor]int64{
		{Mode: NonDNSUDPModeCount, Posture: postureRules}: 3,
		{Mode: NonDNSUDPModeDrop, Posture: postureRules}:  3,
	}, nonDNSUDPPointsByChain(t, reader), "the pre-tenant chain counted nothing and adds no point")
}

// newCountingSlot builds a count-mode firewall in the harness namespace and
// wraps it in a slot whose namespace name the harness owns.
func newCountingSlot(t *testing.T, h *udpHarness, idx int, config Config) *Slot {
	t.Helper()

	fw, err := NewFirewall(h.fwNSPath(), h.tap, config.OrchestratorInSandboxIPAddress, nil, nil, NonDNSUDPModeCount)
	require.NoError(t, err)
	slot := &Slot{Idx: idx, Firewall: fw, config: config, sandboxID: "sandbox-tally"}
	slot.UseNonDNSUDPMode(func(context.Context) string { return string(NonDNSUDPModeCount) })
	t.Cleanup(func() { _ = slot.CloseFirewall(context.WithoutCancel(t.Context())) })

	return slot
}

func sendNonDNSUDP(t *testing.T, h *udpHarness, n int) {
	t.Helper()

	require.NoError(t, h.sendNonDNSUDP(t.Context(), n))
}

// sendNonDNSUDP sends n datagrams from the guest to UDP 443 on the public
// address; it reports errors instead of failing, so a firewall hook can run it
// off the test goroutine.
func (h *udpHarness) sendNonDNSUDP(ctx context.Context, n int) error {
	handle, err := ns.GetNS(filepath.Join(NetNamespacesDir, h.guestNS))
	if err != nil {
		return err
	}
	defer handle.Close()

	return handle.Do(func(ns.NetNS) error {
		for i := range n {
			if err := sendDatagram(ctx, harnessPublicIP+":443", fmt.Sprintf("quic-%d", i)); err != nil {
				return err
			}
		}

		return nil
	})
}

// A live egress update replaces the chain but not the tally: the packets
// counted before the update are reported under the chain that counted them,
// the ones after under the new chain.
func TestFirewall_NonDNSUDP_TallySurvivesUpdate(t *testing.T) { //nolint:paralleltest // replaces the package counter and builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	reader := swapNonDNSUDPCounter(t)
	slot := newCountingSlot(t, h, idx, config)

	sendNonDNSUDP(t, h, 3)
	require.NoError(t, slot.UpdateInternet(t.Context(), &orchestrator.SandboxNetworkEgressConfig{DeniedCidrs: []string{"8.8.8.8/32"}}))
	sendNonDNSUDP(t, h, 2)

	require.NoError(t, slot.CloseFirewall(t.Context()))
	require.Equal(t, map[ChainDescriptor]int64{
		{Mode: NonDNSUDPModeCount, Posture: postureDefault}: 3,
		{Mode: NonDNSUDPModeCount, Posture: postureRules}:   2,
	}, nonDNSUDPPointsByChain(t, reader))
}

// Datagrams the retired chain counts after its replacement is buffered and
// before it is committed are credited to the retired chain, and a recycle
// that switches to off reports them with the departing tenant: each chain
// counts on its own named counter, read once the switch is done.
func TestFirewall_NonDNSUDP_ReplacementCreditsPreFlushDatagramsToRetiredChain(t *testing.T) { //nolint:paralleltest // replaces the package counter and builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	ctx := t.Context()
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	reader := swapNonDNSUDPCounter(t)
	slot := newCountingSlot(t, h, idx, config)

	var hookErr error
	inFlushWindow := func(n int) {
		beforeFlush = func() { hookErr = errors.Join(hookErr, h.sendNonDNSUDP(ctx, n)) }
	}
	t.Cleanup(func() { beforeFlush = nil })

	sendNonDNSUDP(t, h, 3)
	inFlushWindow(2)
	require.NoError(t, slot.UpdateInternet(ctx, &orchestrator.SandboxNetworkEgressConfig{DeniedCidrs: []string{"8.8.8.8/32"}}))
	require.NoError(t, hookErr)

	sendNonDNSUDP(t, h, 1)
	inFlushWindow(4)
	slot.UseNonDNSUDPMode(func(context.Context) string { return string(NonDNSUDPModeOff) })
	require.NoError(t, slot.ResetInternet(ctx))
	require.NoError(t, hookErr)
	beforeFlush = nil

	require.Equal(t, map[ChainDescriptor]int64{
		{Mode: NonDNSUDPModeCount, Posture: postureDefault}: 5,
		{Mode: NonDNSUDPModeCount, Posture: postureRules}:   5,
	}, nonDNSUDPPointsByChain(t, reader), "the recycle to off reports everything the tenant's chains counted")
}

// A tenant admitted under off onto a slot built under count had its chain
// rebuilt, which harvested the earlier chain's tally; when that tenant leaves
// with the mode still off, the release path with nothing to rebuild still
// exports and clears that tally instead of handing it to the next tenant.
func TestFirewall_NonDNSUDP_ResetUnderOffExportsTally(t *testing.T) { //nolint:paralleltest // replaces the package counter and builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	ctx := t.Context()
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	reader := swapNonDNSUDPCounter(t)
	slot := newCountingSlot(t, h, idx, config)

	sendNonDNSUDP(t, h, 3)

	slot.UseNonDNSUDPMode(func(context.Context) string { return string(NonDNSUDPModeOff) })
	require.NoError(t, slot.ConfigureInternet(ctx, &orchestrator.SandboxNetworkConfig{}))
	require.Equal(t, NonDNSUDPModeOff, slot.Firewall.NonDNSUDPMode(), "the tenant's chain is off")
	require.Empty(t, nonDNSUDPPointsByChain(t, reader), "nothing is exported before the tenant leaves")

	require.NoError(t, slot.ResetInternet(ctx))
	require.Equal(t, map[ChainDescriptor]int64{{Mode: NonDNSUDPModeCount, Posture: postureDefault}: 3}, nonDNSUDPPointsByChain(t, reader))
	require.Empty(t, slot.Firewall.TakeNonDNSUDPTally(), "the release leaves no tally behind")
}

// A reset whose rebuild fails leaves the old chain in place; the packets it
// counted are exported once, by the close that follows, not twice.
func TestFirewall_NonDNSUDP_FailedResetExportsOnce(t *testing.T) { //nolint:paralleltest // replaces the package counter and builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	reader := swapNonDNSUDPCounter(t)
	slot := newCountingSlot(t, h, idx, config)
	slot.firewallCustomRules.Store(true)

	sendNonDNSUDP(t, h, 3)

	// A rule for a table that does not exist fails the whole batch, so the
	// kernel keeps the counting chain.
	missing := &nftables.Table{Name: "missing", Family: nftables.TableFamilyINet}
	slot.Firewall.conn.AddRule(&nftables.Rule{Table: missing, Chain: &nftables.Chain{Name: "missing", Table: missing}, Exprs: accept()})
	require.Error(t, slot.ResetInternet(t.Context()))
	require.Equal(t, ChainDescriptor{Mode: NonDNSUDPModeCount, Posture: postureDefault}, slot.Firewall.Installed())

	require.NoError(t, slot.CloseFirewall(t.Context()))
	require.Equal(t, map[ChainDescriptor]int64{{Mode: NonDNSUDPModeCount, Posture: postureDefault}: 3}, nonDNSUDPPointsByChain(t, reader))
}

func sendDatagram(ctx context.Context, addr, payload string) error {
	return sendDatagramOver(ctx, "udp4", addr, payload)
}

func sendDatagramOver(ctx context.Context, network, addr, payload string) error {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, network, addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Write([]byte(payload))

	return err
}

// The rule reads an IPv4 destination, so it must not see IPv6: an IPv6 UDP
// datagram from the tap is neither counted nor dropped in drop mode, while
// an IPv4 one on the same chain is both.
func TestFirewall_NonDNSUDP_IPv6LeftAlone(t *testing.T) { //nolint:paralleltest // builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	ctx := t.Context()
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)

	fw, err := NewFirewall(h.fwNSPath(), h.tap, config.OrchestratorInSandboxIPAddress, nil, nil, NonDNSUDPModeDrop)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fw.Close(context.WithoutCancel(t.Context())) })
	var udp6 net.PacketConn
	h.in(t, h.fwNS, func() error {
		var lc net.ListenConfig
		var err error
		udp6, err = lc.ListenPacket(ctx, "udp6", net.JoinHostPort(harnessFwGuestIP6, "443"))

		return err
	})
	t.Cleanup(func() { _ = udp6.Close() })

	h.in(t, h.guestNS, func() error {
		return sendDatagramOver(ctx, "udp6", net.JoinHostPort(harnessFwGuestIP6, "443"), "v6")
	})
	require.Equal(t, 1, countDatagrams(t, udp6), "an IPv6 datagram passes a drop-mode chain")

	snapshot, err := fw.Harvest()
	require.NoError(t, err)
	require.Zero(t, snapshot.Packets, "an IPv6 datagram is not counted")

	sendNonDNSUDP(t, h, 1)
	snapshot, err = fw.Harvest()
	require.NoError(t, err)
	require.Equal(t, uint64(1), snapshot.Packets, "the same chain counts and drops IPv4")
}

// hostTableExists reports whether a table of the firewall's name is in the
// calling thread's namespace, the host's for a test goroutine.
func hostTableExists(t *testing.T) bool {
	t.Helper()

	conn, err := nftables.New()
	require.NoError(t, err)
	tables, err := conn.ListTablesOfFamily(nftables.TableFamilyINet)
	require.NoError(t, err)
	for _, table := range tables {
		if table.Name == tableName {
			return true
		}
	}

	return false
}

// The firewall is built, updated, read and closed from the host thread,
// never from inside the slot namespace, and everything lands in the slot
// namespace: the connection dials through the namespace descriptor the
// firewall holds. The host namespace never sees the table.
func TestFirewall_HostThreadOperatesOnSlotNamespace(t *testing.T) { //nolint:paralleltest // builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	ctx := t.Context()
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	require.False(t, hostTableExists(t), "the host namespace has no table of that name before the test")

	fw, err := NewFirewall(h.fwNSPath(), h.tap, config.OrchestratorInSandboxIPAddress, nil, nil, NonDNSUDPModeOff)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fw.Close(context.WithoutCancel(t.Context())) })
	require.Equal(t, defaultChain, filterChainShapes(t, h.fwNS), "the default chain is in the slot namespace")
	require.False(t, hostTableExists(t), "building the firewall adds nothing to the host namespace")

	require.NoError(t, fw.ApplyRules(ctx, false, NonDNSUDPModeCount, nil, []string{"8.8.8.8/32"}, false))
	require.Equal(t, []ruleShape{nonDNSUDPCount, establishedNonUDPAccept, establishedReplyAccept, allowSetAccept, denySetDrop, nonTCPUserAllow, nonTCPUserDeny},
		filterChainShapes(t, h.fwNS), "the update replaced the chain in the slot namespace")

	sendNonDNSUDP(t, h, 3)
	// A harvest from a goroutine of its own, as the pool's return path runs.
	snapshots := make(chan NonDNSUDPSnapshot, 1)
	errs := make(chan error, 1)
	go func() {
		snapshot, err := fw.Harvest()
		snapshots <- snapshot
		errs <- err
	}()
	require.NoError(t, <-errs)
	snapshot := <-snapshots
	require.Equal(t, ChainDescriptor{Mode: NonDNSUDPModeCount, Posture: postureRules}, snapshot.ChainDescriptor)
	require.Equal(t, uint64(3), snapshot.Packets, "the read reaches the slot namespace's counter")

	require.NoError(t, fw.Close(ctx))
	require.False(t, slotTableExists(t, h), "the close deleted the table from the slot namespace")
	require.False(t, hostTableExists(t))
}

// slotTableExists reports whether the slot's table is in the harness's
// firewall namespace.
func slotTableExists(t *testing.T, h *udpHarness) bool {
	t.Helper()

	exists := false
	h.in(t, h.fwNS, func() error {
		conn, err := nftables.New()
		if err != nil {
			return err
		}
		tables, err := conn.ListTablesOfFamily(nftables.TableFamilyINet)
		if err != nil {
			return err
		}
		for _, table := range tables {
			exists = exists || table.Name == tableName
		}

		return nil
	})

	return exists
}

// lastingSocketClosed reports whether the connection's lasting socket is
// gone: a request on a connection without one dials anew, which the fake
// dial observes, while one with the socket still open goes through it.
func lastingSocketClosed(fw *Firewall) bool {
	dialed := false
	fw.conn.TestDial = func([]netlink.Message) ([]netlink.Message, error) {
		dialed = true

		return nil, errInjectedNetlink
	}
	defer func() { fw.conn.TestDial = nil }()
	_, _ = fw.conn.ListTables()

	return dialed
}

// probeSlotTable opens a lasting connection inside the harness's firewall
// namespace and returns a check for the slot's table through it: the socket
// keeps reaching that namespace once its name is unlinked.
func probeSlotTable(t *testing.T, h *udpHarness) func() bool {
	t.Helper()

	var probe *nftables.Conn
	h.in(t, h.fwNS, func() error {
		var err error
		probe, err = nftables.New(nftables.AsLasting())

		return err
	})
	t.Cleanup(func() { _ = probe.CloseLasting() })

	return func() bool {
		tables, err := probe.ListTablesOfFamily(nftables.TableFamilyINet)
		require.NoError(t, err)
		for _, table := range tables {
			if table.Name == tableName {
				return true
			}
		}

		return false
	}
}

// Unlinking the namespace's name does not take the counters with it: the
// descriptor the firewall holds keeps the namespace alive. A close after
// the name is gone still reads the counters, deletes the table and closes
// the socket, so packets never read before are reported rather than lost.
func TestFirewall_NonDNSUDP_CloseWithNamespaceGoneExportsTally(t *testing.T) { //nolint:paralleltest // replaces the package counter and builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	reader := swapNonDNSUDPCounter(t)
	slot := newCountingSlot(t, h, idx, config)
	fw := slot.Firewall
	tableExists := probeSlotTable(t, h)

	// Nothing reads the three before the name goes.
	sendNonDNSUDP(t, h, 3)
	ipCmd(t, "netns", "del", h.fwNS)

	require.NoError(t, slot.CloseFirewall(t.Context()))
	require.Nil(t, slot.Firewall)
	require.Equal(t, map[ChainDescriptor]int64{{Mode: NonDNSUDPModeCount, Posture: postureDefault}: 3}, nonDNSUDPPointsByChain(t, reader), "the counters are read through the lasting socket")
	require.False(t, tableExists(), "the table is deleted through the lasting socket")
	require.True(t, lastingSocketClosed(fw), "the socket that kept the namespace alive is closed")
}

// A close whose table delete fails still reports what it read and detaches
// the firewall, so the slot's teardown neither loses the tally nor reads the
// counters again on a retry.
func TestFirewall_NonDNSUDP_CloseErrorStillExportsTally(t *testing.T) { //nolint:paralleltest // replaces the package counter and builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	ctx := t.Context()
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	reader := swapNonDNSUDPCounter(t)
	slot := newCountingSlot(t, h, idx, config)
	fw := slot.Firewall

	sendNonDNSUDP(t, h, 3)

	// A rule with a verdict the kernel does not know fails the batch that
	// deletes the table; the read before it is a request of its own.
	fw.conn.AddRule(&nftables.Rule{Table: fw.table, Chain: fw.filterChain, Exprs: []expr.Any{&expr.Verdict{Kind: expr.VerdictKind(1 << 20)}}})

	err = slot.CloseFirewall(ctx)
	require.Error(t, err)
	require.NotErrorIs(t, err, unix.ENOENT, "the delete failed for a reason other than the table being gone")
	require.Nil(t, slot.Firewall, "a retry must not read the counters again")
	require.Equal(t, map[ChainDescriptor]int64{{Mode: NonDNSUDPModeCount, Posture: postureDefault}: 3}, nonDNSUDPPointsByChain(t, reader))
	require.True(t, slotTableExists(t, h), "the failed delete left the table for the namespace's removal")
}

// After a failed flush the firewall reopens its connection; when that reopen
// fails too, the kept connection dials per request through the namespace
// descriptor the firewall holds, so a close from the host thread still
// reads the tally and deletes the slot's table.
func TestFirewall_NonDNSUDP_CloseAfterFailedReopenExportsTally(t *testing.T) { //nolint:paralleltest // replaces the package counter and builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	ctx := t.Context()
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	reader := swapNonDNSUDPCounter(t)
	slot := newCountingSlot(t, h, idx, config)
	fw := slot.Firewall

	sendNonDNSUDP(t, h, 3)

	// A rule for a table that does not exist fails the flush; with no file
	// descriptor to spare, the reopen that follows fails too.
	missing := &nftables.Table{Name: "missing", Family: nftables.TableFamilyINet}
	fw.conn.AddRule(&nftables.Rule{Table: missing, Chain: &nftables.Chain{Name: "missing", Table: missing}, Exprs: accept()})
	restore, err := forbidNewFileDescriptors()
	require.NoError(t, err)
	applyErr := fw.ApplyRules(ctx, false, NonDNSUDPModeCount, nil, []string{"8.8.8.8/32"}, false)
	restore()
	require.ErrorIs(t, applyErr, unix.EMFILE, "the reopen must have been the step that failed")

	require.NoError(t, slot.CloseFirewall(ctx))
	require.Nil(t, slot.Firewall)
	require.Equal(t, map[ChainDescriptor]int64{{Mode: NonDNSUDPModeCount, Posture: postureDefault}: 3}, nonDNSUDPPointsByChain(t, reader))
	require.False(t, slotTableExists(t, h), "the slot's table is gone from its namespace")
}

// A failed flush reopens the lasting connection; when that reopen cannot
// open a socket, the firewall keeps the connection it had instead of a
// transient substitute, so there is never a second connection to leak.
func TestFirewall_FailedConnReopenKeepsOneConn(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "fw-conn-reopen")
	fw := slot.Firewall
	before := fw.conn
	ctx := t.Context()

	// A rule for a table that does not exist fails the flush that follows.
	missing := &nftables.Table{Name: "missing", Family: nftables.TableFamilyINet}
	fw.conn.AddRule(&nftables.Rule{Table: missing, Chain: &nftables.Chain{Name: "missing", Table: missing}, Exprs: accept()})

	restore, err := forbidNewFileDescriptors()
	require.NoError(t, err)
	applyErr := fw.ApplyRules(ctx, false, NonDNSUDPModeOff, nil, []string{"10.0.0.0/8"}, false)
	restore()
	require.ErrorIs(t, applyErr, unix.EMFILE, "the reopen must have been the step that failed")
	require.Same(t, before, fw.conn, "the firewall keeps its one connection")
	require.Equal(t, ChainDescriptor{Mode: NonDNSUDPModeOff, Posture: postureDefault}, fw.Installed(), "the kernel kept the previous chain")
}

// The namespace-pinned dial opens the calling thread's namespace file, enters
// the slot namespace and only then creates the netlink socket. With one
// descriptor to spare the socket is the step that fails, and the dial has to
// report it: a connection to no socket would crash the first request on it.
func TestFirewall_DialReportsSocketFailure(t *testing.T) { //nolint:paralleltest // lowers the process's open-file limit and builds netns state; cannot run in parallel
	slot := newFirewallTestSlot(t, "fw-dial-no-socket")

	restore, err := allowOneNewFileDescriptor()
	require.NoError(t, err)
	conn, err := dialNetNS(slot.Firewall.netNS)
	restore()
	if conn != nil {
		_ = conn.CloseLasting()
	}
	require.ErrorIs(t, err, unix.EMFILE)
	sysErr, ok := errors.AsType[*os.SyscallError](err)
	require.True(t, ok, "the failure is reported from a system call: %v", err)
	require.Equal(t, "socket", sysErr.Syscall, "the socket, not the namespace file, was the step that failed")
	require.Nil(t, conn)
}

// After Close the connection is gone and the descriptor number the firewall
// held may belong to another file, so a second Close, a read or a rebuild is
// refused before it reaches either. With no file descriptor to spare, a
// refusal that still sent a netlink request would come back as EMFILE
// instead of the closed error.
func TestFirewall_OperationsAfterCloseAreRefused(t *testing.T) { //nolint:paralleltest // lowers the process's open-file limit and builds netns state; cannot run in parallel
	slot := newFirewallTestSlot(t, "fw-closed")
	fw := slot.Firewall
	ctx := t.Context()
	require.NoError(t, slot.CloseFirewall(ctx))

	restore, err := forbidNewFileDescriptors()
	require.NoError(t, err)
	defer restore()

	require.Equal(t, errFirewallClosed, fw.Close(ctx), "a second close")
	_, err = fw.Harvest()
	require.Equal(t, errFirewallClosed, err, "a read")
	require.Equal(t, errFirewallClosed, fw.ApplyRules(ctx, false, NonDNSUDPModeCount, nil, nil, false), "a rebuild")
	require.Equal(t, errFirewallClosed, fw.DenyEgress(ctx), "an isolation")
}

// forbidNewFileDescriptors drops the soft open-file limit to zero so every
// socket() in this process fails with EMFILE until restore runs; files
// already open keep working.
func forbidNewFileDescriptors() (restore func(), err error) {
	return capNewFileDescriptors(0)
}

// allowOneNewFileDescriptor lets exactly one more file be opened in this
// process until restore runs; the one after it fails with EMFILE.
func allowOneNewFileDescriptor() (restore func(), err error) {
	lowest, err := unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	if err := unix.Close(lowest); err != nil {
		return nil, err
	}

	return capNewFileDescriptors(uint64(lowest) + 1) //nolint:gosec // a descriptor number is never negative
}

// capNewFileDescriptors sets the soft open-file limit, the bound on the
// number a new descriptor may take, until restore runs.
func capNewFileDescriptors(cur uint64) (restore func(), err error) {
	var limit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
		return nil, err
	}
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &unix.Rlimit{Cur: cur, Max: limit.Max}); err != nil {
		return nil, err
	}

	return func() { _ = unix.Setrlimit(unix.RLIMIT_NOFILE, &limit) }, nil
}

// retiredCounterName names the counter the installed chain does not
// reference: the one the chain before it counted on.
func retiredCounterName(fw *Firewall) string {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	return fw.counters[fw.spareCounterIndex()].obj.Name
}

// addLatePackets puts n datagrams on the named counter after its chain was
// flushed, the way a CPU still running the retired rule does: a chain ahead
// of the firewall's counts them on that counter and drops them, so the
// installed chain never sees them.
func (h *udpHarness) addLatePackets(t *testing.T, counterName string, n int) {
	t.Helper()

	table := &nftables.Table{Name: tableName, Family: nftables.TableFamilyINet}
	policy := nftables.ChainPolicyAccept
	var conn *nftables.Conn
	var chain *nftables.Chain
	h.in(t, h.fwNS, func() error {
		var err error
		conn, err = nftables.New(nftables.AsLasting())
		if err != nil {
			return err
		}
		chain = conn.AddChain(&nftables.Chain{
			Name:     "late",
			Table:    table,
			Type:     nftables.ChainTypeFilter,
			Hooknum:  nftables.ChainHookPrerouting,
			Priority: nftables.ChainPriorityRef(-200),
			Policy:   &policy,
		})
		conn.AddRule(&nftables.Rule{Table: table, Chain: chain, Exprs: []expr.Any{
			&expr.Meta{Key: expr.MetaKeyIIFNAME, Register: 1},
			&expr.Cmp{Register: 1, Op: expr.CmpOpEq, Data: append([]byte(h.tap), 0)},
			&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
			&expr.Cmp{Register: 1, Op: expr.CmpOpEq, Data: []byte{unix.IPPROTO_UDP}},
			&expr.Objref{Type: int(nftables.ObjTypeCounter), Name: counterName},
			&expr.Verdict{Kind: expr.VerdictDrop},
		}})

		return conn.Flush()
	})
	defer func() {
		conn.FlushChain(chain)
		conn.DelChain(chain)
		require.NoError(t, conn.Flush())
		require.NoError(t, conn.CloseLasting())
	}()

	sendNonDNSUDP(t, h, n)
}

// A rule's retirement is acknowledged before every CPU has stopped running
// it, so its counter can still grow after the replacement has read it. The
// retired counter keeps its chain until its next read, at the replacement
// that reassigns it or at a release, and the late packets are reported
// under the chain that counted them.
func TestFirewall_NonDNSUDP_LatePacketsOnRetiredCounterKeepTheirChain(t *testing.T) { //nolint:paralleltest // replaces the package counter and builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	ctx := t.Context()
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	reader := swapNonDNSUDPCounter(t)
	slot := newCountingSlot(t, h, idx, config)
	fw := slot.Firewall

	var mode atomic.Value
	mode.Store("count")
	slot.UseNonDNSUDPMode(func(context.Context) string { return mode.Load().(string) })

	// The retired counter is read again by the replacement that reassigns it.
	sendNonDNSUDP(t, h, 3)
	require.NoError(t, slot.UpdateInternet(ctx, &orchestrator.SandboxNetworkEgressConfig{DeniedCidrs: []string{"8.8.8.8/32"}}))
	h.addLatePackets(t, retiredCounterName(fw), 2)
	sendNonDNSUDP(t, h, 1)
	mode.Store("drop")
	require.NoError(t, slot.ResetInternet(ctx))
	require.Equal(t, map[ChainDescriptor]int64{
		{Mode: NonDNSUDPModeCount, Posture: postureDefault}: 5,
		{Mode: NonDNSUDPModeCount, Posture: postureRules}:   1,
	}, nonDNSUDPPointsByChain(t, reader), "late packets go to the chain that counted them, not to the one taking over its counter")

	// The retired counter is read again by a release that rebuilds nothing.
	mode.Store("count")
	require.NoError(t, slot.ConfigureInternet(ctx, &orchestrator.SandboxNetworkConfig{}))
	h.addLatePackets(t, retiredCounterName(fw), 2)
	sendNonDNSUDP(t, h, 1)
	require.NoError(t, slot.ResetInternet(ctx))
	require.Equal(t, map[ChainDescriptor]int64{
		{Mode: NonDNSUDPModeCount, Posture: postureDefault}: 6,
		{Mode: NonDNSUDPModeCount, Posture: postureRules}:   1,
		{Mode: NonDNSUDPModeDrop, Posture: postureDefault}:  2,
	}, nonDNSUDPPointsByChain(t, reader), "a release reads the retired counter under its own chain")
}

var errInjectedNetlink = errors.New("injected netlink failure")

// failNetlinkReads makes every request the firewall's connection sends fail
// until restore is called: the lasting socket is closed so each request dials
// anew, and the dial is routed to a fake that answers with an error. Packets
// stay in the kernel's counters throughout.
func failNetlinkReads(t *testing.T, fw *Firewall) (dials *atomic.Int32, restore func()) {
	t.Helper()

	require.NoError(t, fw.conn.CloseLasting())
	dials = &atomic.Int32{}
	fw.conn.TestDial = func([]netlink.Message) ([]netlink.Message, error) {
		dials.Add(1)

		return nil, errInjectedNetlink
	}

	return dials, func() { fw.conn.TestDial = nil }
}

// A release whose counter read fails once reads again, and the retry
// exports the tenant's tally.
func TestFirewall_NonDNSUDP_ReleaseRetriesAFailedRead(t *testing.T) { //nolint:paralleltest // replaces the package counter and builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	ctx := t.Context()
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	reader := swapNonDNSUDPCounter(t)
	slot := newCountingSlot(t, h, idx, config)
	fw := slot.Firewall

	sendNonDNSUDP(t, h, 3)

	dials, restore := failNetlinkReads(t, fw)
	t.Cleanup(restore)
	// The fault clears itself after the first request.
	fw.conn.TestDial = func([]netlink.Message) ([]netlink.Message, error) {
		dials.Add(1)
		restore()

		return nil, errInjectedNetlink
	}

	require.NoError(t, slot.ResetInternet(ctx))
	require.Equal(t, int32(1), dials.Load(), "exactly one read failed")
	require.Equal(t, map[ChainDescriptor]int64{{Mode: NonDNSUDPModeCount, Posture: postureDefault}: 3}, nonDNSUDPPointsByChain(t, reader))
	require.Empty(t, slot.sandboxID, "the retry completed the release")
}

// A release that rebuilds the chain first reads the counter the rebuild
// reuses; when that read fails once it is read again, so the release
// succeeds and the retry's packets are credited to the chain that counted
// them, as a release without a rebuild already does.
func TestFirewall_NonDNSUDP_RebuildRetriesAFailedSpareRead(t *testing.T) { //nolint:paralleltest // replaces the package counter and builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	ctx := t.Context()
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	reader := swapNonDNSUDPCounter(t)
	slot := newCountingSlot(t, h, idx, config)
	fw := slot.Firewall

	// The tenant's rules retire the counting chain's counter, which then
	// receives late packets: the release's rebuild reads it before reusing it.
	sendNonDNSUDP(t, h, 3)
	require.NoError(t, slot.UpdateInternet(ctx, &orchestrator.SandboxNetworkEgressConfig{DeniedCidrs: []string{"8.8.8.8/32"}}))
	h.addLatePackets(t, retiredCounterName(fw), 1)
	sendNonDNSUDP(t, h, 1)

	dials, restore := failNetlinkReads(t, fw)
	t.Cleanup(restore)
	// The fault clears itself after the first request, the rebuild's read.
	fw.conn.TestDial = func([]netlink.Message) ([]netlink.Message, error) {
		dials.Add(1)
		restore()

		return nil, errInjectedNetlink
	}

	require.NoError(t, slot.ResetInternet(ctx))
	require.Equal(t, int32(1), dials.Load(), "exactly one read failed")
	require.Equal(t, ChainDescriptor{Mode: NonDNSUDPModeCount, Posture: postureDefault}, fw.Installed(), "the rebuild went through")
	require.Equal(t, map[ChainDescriptor]int64{
		{Mode: NonDNSUDPModeCount, Posture: postureDefault}: 4,
		{Mode: NonDNSUDPModeCount, Posture: postureRules}:   1,
	}, nonDNSUDPPointsByChain(t, reader), "the retry credits the late packet to the retired chain")
	require.Empty(t, slot.sandboxID, "the release completed")
}

// A release whose counter read keeps failing fails, keeps the tenant, and
// exports nothing: the pool then destroys the slot, whose close reads the
// counters with the departing tenant, so no later tenant can inherit them.
// The release reads the counters under off too, where there is no chain to
// rebuild.
func TestFirewall_NonDNSUDP_ReleaseFailsWhenTheReadKeepsFailing(t *testing.T) { //nolint:paralleltest // replaces the package counter and builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	ctx := t.Context()
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	reader := swapNonDNSUDPCounter(t)
	slot := newCountingSlot(t, h, idx, config)
	fw := slot.Firewall

	sendNonDNSUDP(t, h, 3)

	// The tenant runs under off: the switch retired the counting chain, whose
	// counter then receives late packets.
	slot.UseNonDNSUDPMode(func(context.Context) string { return string(NonDNSUDPModeOff) })
	require.NoError(t, slot.ConfigureInternet(ctx, &orchestrator.SandboxNetworkConfig{}))
	require.Equal(t, NonDNSUDPModeOff, fw.NonDNSUDPMode())
	h.addLatePackets(t, retiredCounterName(fw), 2)

	dials, restore := failNetlinkReads(t, fw)
	t.Cleanup(restore)

	// The library formats the read error into its message, so the text, not
	// the chain, carries the injected failure.
	require.ErrorContains(t, slot.ResetInternet(ctx), errInjectedNetlink.Error())
	require.Equal(t, int32(2), dials.Load(), "the read is attempted twice")
	require.Equal(t, "sandbox-tally", slot.sandboxID, "a failed release keeps the tenant")
	require.Empty(t, nonDNSUDPPointsByChain(t, reader), "a failed release exports nothing")

	restore()
	require.NoError(t, slot.CloseFirewall(ctx))
	require.Equal(t, map[ChainDescriptor]int64{{Mode: NonDNSUDPModeCount, Posture: postureDefault}: 5}, nonDNSUDPPointsByChain(t, reader), "the close reads everything with the departing tenant")
}

// probeNamespaceDescriptor returns a check that reports whether the
// descriptor the firewall holds on its namespace is closed: the link the
// process's descriptor table shows for its number is gone or names
// something other than the namespace's bind mount; the link resolves the
// mount's path, so only the name is compared.
func probeNamespaceDescriptor(t *testing.T, h *udpHarness, fw *Firewall) func() bool {
	t.Helper()

	link := fmt.Sprintf("/proc/self/fd/%d", fw.netNS.Fd())
	target, err := os.Readlink(link)
	require.NoError(t, err)
	require.Equal(t, h.fwNS, filepath.Base(target), "the descriptor names the firewall's namespace")

	return func() bool {
		now, err := os.Readlink(link)

		return err != nil || now != target
	}
}

// A close whose counter read fails once reads again. A slot returned during
// the pool's shutdown skips the release, so the close is its tenant's only
// read: the retry exports the tally and the close reports success.
func TestFirewall_NonDNSUDP_CloseRetriesAFailedRead(t *testing.T) { //nolint:paralleltest // replaces the package counter and builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	ctx := t.Context()
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	reader := swapNonDNSUDPCounter(t)
	slot := newCountingSlot(t, h, idx, config)
	fw := slot.Firewall

	sendNonDNSUDP(t, h, 3)

	dials, restore := failNetlinkReads(t, fw)
	t.Cleanup(restore)
	// The fault clears itself after the first request.
	fw.conn.TestDial = func([]netlink.Message) ([]netlink.Message, error) {
		dials.Add(1)
		restore()

		return nil, errInjectedNetlink
	}

	require.NoError(t, slot.CloseFirewall(ctx))
	require.Equal(t, int32(1), dials.Load(), "exactly one read failed")
	require.Nil(t, slot.Firewall)
	require.Equal(t, map[ChainDescriptor]int64{{Mode: NonDNSUDPModeCount, Posture: postureDefault}: 3}, nonDNSUDPPointsByChain(t, reader), "the retry exports the tenant's tally")
	require.False(t, slotTableExists(t, h), "the table is deleted after the retry")
}

// A close whose counter read keeps failing still deletes the table and
// closes the connection and the namespace descriptor, so nothing leaks, and
// exports what was read before; it returns the failure, so the slot's
// cleanup reports the unread packets lost rather than success.
func TestFirewall_NonDNSUDP_CloseReportsAReadThatKeepsFailing(t *testing.T) { //nolint:paralleltest // replaces the package counter and builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	ctx := t.Context()
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	reader := swapNonDNSUDPCounter(t)
	slot := newCountingSlot(t, h, idx, config)
	fw := slot.Firewall
	tableExists := probeSlotTable(t, h)
	descriptorClosed := probeNamespaceDescriptor(t, h, fw)

	// Three are read into the tally before the fault; two stay in the kernel.
	sendNonDNSUDP(t, h, 3)
	_, err = fw.Harvest()
	require.NoError(t, err)
	sendNonDNSUDP(t, h, 2)

	dials, restore := failNetlinkReads(t, fw)
	t.Cleanup(restore)
	// Both reads fail; the fault clears for the table's deletion.
	fw.conn.TestDial = func([]netlink.Message) ([]netlink.Message, error) {
		if dials.Add(1) == 2 {
			restore()
		}

		return nil, errInjectedNetlink
	}

	// The library formats the read error into its message, so the text, not
	// the chain, carries the injected failure.
	err = slot.CloseFirewall(ctx)
	require.ErrorContains(t, err, errInjectedNetlink.Error())
	require.Equal(t, int32(2), dials.Load(), "the read is attempted twice")
	require.Nil(t, slot.Firewall, "a retry must not read the counters again")
	require.Equal(t, map[ChainDescriptor]int64{{Mode: NonDNSUDPModeCount, Posture: postureDefault}: 3}, nonDNSUDPPointsByChain(t, reader), "what was read before the fault is exported")
	require.False(t, tableExists(), "the table is deleted despite the failed read")
	require.True(t, descriptorClosed(), "the descriptor that kept the namespace alive is closed")
}

// A read attempts every counter: when the retired counter's read fails, the
// live counter is still read and credited, and the error names the failure.
func TestFirewall_HarvestReadsEveryCounterDespiteOneFailure(t *testing.T) { //nolint:paralleltest // builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	ctx := t.Context()
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	slot := newCountingSlot(t, h, idx, config)
	fw := slot.Firewall

	sendNonDNSUDP(t, h, 3)
	require.NoError(t, slot.UpdateInternet(ctx, &orchestrator.SandboxNetworkEgressConfig{DeniedCidrs: []string{"8.8.8.8/32"}}))

	// The retired counter is the first one read. No chain references it any
	// more, so the kernel lets it go; its read then fails with not-exists.
	retired := retiredCounterName(fw)
	require.Equal(t, nonDNSUDPCounterNameA, retired)
	h.in(t, h.fwNS, func() error {
		conn, err := nftables.New()
		if err != nil {
			return err
		}
		conn.DeleteObject(&nftables.CounterObj{Table: fw.table, Name: retired})

		return conn.Flush()
	})

	sendNonDNSUDP(t, h, 2)
	snapshot, err := fw.Harvest()
	require.ErrorContains(t, err, retired)
	require.ErrorContains(t, err, unix.ENOENT.Error())
	require.Equal(t, uint64(2), snapshot.Packets, "the live counter is read despite the retired one failing")
	require.Equal(t, uint64(2), fw.TakeNonDNSUDPTally()[ChainDescriptor{Mode: NonDNSUDPModeCount, Posture: postureRules}].Packets)
}

// An egress update and the firewall's close are serialized by the slot, in
// both orders, so neither reaches a detached firewall. Update first: the
// update holds the slot with its replacement buffered when the close arrives;
// the close waits, the update's rebuild reads the datagram counted before it
// into the firewall's tally, and the close then exports it. Close first: the
// close reads and exports the datagram, and an update after it is refused as
// having no firewall rather than failed. The race detector covers the first
// order, where the two run on separate goroutines.
func TestSlot_UpdateAndCloseSerializeInBothOrders(t *testing.T) { //nolint:paralleltest // replaces the package counter and builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	ctx := t.Context()
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	reader := swapNonDNSUDPCounter(t)
	rules := &orchestrator.SandboxNetworkEgressConfig{DeniedCidrs: []string{"8.8.8.8/32"}}
	preUpdateChain := ChainDescriptor{Mode: NonDNSUDPModeCount, Posture: postureDefault}

	// Update first.
	slot := newCountingSlot(t, h, idx, config)
	fw := slot.Firewall
	sendNonDNSUDP(t, h, 1)

	closeHoldsSlot := make(chan struct{})
	closeMayRun := make(chan struct{})
	closeDone := make(chan error, 1)
	// A failed assertion must not leave the close holding the slot, or the
	// harness cleanup would wait on it forever.
	letCloseRun := sync.OnceFunc(func() { close(closeMayRun) })
	t.Cleanup(letCloseRun)
	beforeFlush = func() {
		// The update holds the slot, its replacement buffered and not yet
		// committed. The close arriving now waits for the slot the way
		// CloseFirewall does, holds it once the update is done so the
		// firewall's memory can be inspected in between, and then closes.
		go func() {
			slot.firewallMu.Lock()
			close(closeHoldsSlot)
			<-closeMayRun
			slot.firewallMu.Unlock()
			closeDone <- slot.CloseFirewall(ctx)
		}()
	}
	t.Cleanup(func() { beforeFlush = nil })
	require.NoError(t, slot.UpdateInternet(ctx, rules), "the update completes against the firewall it captured")
	beforeFlush = nil

	<-closeHoldsSlot
	require.Equal(t, ChainDescriptor{Mode: NonDNSUDPModeCount, Posture: postureRules}, fw.Installed(), "the update's chain is in place before the close runs")
	fw.mu.Lock()
	read := fw.tally[preUpdateChain]
	fw.mu.Unlock()
	require.Equal(t, uint64(1), read.Packets, "the update's rebuild read the datagram counted before it")
	require.Empty(t, nonDNSUDPPointsByChain(t, reader), "nothing is exported before the close")

	letCloseRun()
	require.NoError(t, <-closeDone, "the close ran once the update released the slot")
	require.Nil(t, slot.Firewall)
	require.Equal(t, map[ChainDescriptor]int64{preUpdateChain: 1}, nonDNSUDPPointsByChain(t, reader), "the close exported what the rebuild read")
	require.ErrorContains(t, slot.UpdateInternet(ctx, rules), "no firewall attached", "a closed slot refuses every later update")

	// Close first.
	slot = newCountingSlot(t, h, idx, config)
	sendNonDNSUDP(t, h, 1)
	require.NoError(t, slot.CloseFirewall(ctx))
	require.ErrorContains(t, slot.UpdateInternet(ctx, rules), "no firewall attached", "an update after the close is refused, not failed")
	require.Nil(t, slot.Firewall)
	require.Equal(t, map[ChainDescriptor]int64{preUpdateChain: 2}, nonDNSUDPPointsByChain(t, reader), "the close read and exported the datagram")
}
