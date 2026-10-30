//go:build linux

package network

import (
	"context"
	"fmt"
	"log"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/containernetworking/plugins/pkg/ns"
	"github.com/coreos/go-iptables/iptables"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	netutils "k8s.io/utils/net"

	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network")

const (
	defaultHostNetworkCIDR = "10.11.0.0/16"
	defaultVrtNetworkCIDR  = "10.12.0.0/16"

	hostMask          = 32
	vrtMask           = 31                  // 2 usable ips per block (vpeer and veth)
	vrtAddressPerSlot = 1 << (32 - vrtMask) // vrt addresses per slot (vpeer and veth)

	tapMask          = 30
	tapInterfaceName = "tap0"
	tapIp            = "169.254.0.22"
	tapMAC           = "02:FC:00:00:00:05"
	tapHostMAC       = "02:FC:00:00:00:06"
)

var (
	hostNetworkCIDR     = getHostNetworkCIDR()
	vrtNetworkCIDR      = getVrtNetworkCIDR()
	vrtSlotsSize        = GetVrtSlotsSize()
	tapHostHardwareAddr = getTapHostHardwareAddr()
)

// Slot network allocation
//
// For each slot, we allocate three IP addresses:
// Host IP - used to access the sandbox from the host machine
// Vpeer and Veth IPs - used by the sandbox to communicate with the host
//
// Host default namespace creates a /16 CIDR block for the host IPs.
// Slot with Idx 1 will receive 10.11.0.1 and so on. Its allocated incrementally by slot Idx.
// Host mask is /32 because we only use one IP per slot.
//
// Vrt addresses (vpeer and veth) are allocated from a /31 CIDR block so we can use CIDR for network link routing.
// By default, they are using 10.12.0.0/16 CIDR block, that can be configured via environment variable.
// Vpeer receives the first IP in the block, and Veth receives the second IP. Block is calculated as (slot index * addresses per slot allocation).
// Vrt address per slot is always 2, so we can allocate /31 CIDR block for each slot.
type Slot struct {
	Key string
	Idx int

	// Firewall is read by tests; every operation captures it under
	// firewallMu and uses that value, never the field.
	Firewall *Firewall

	// firewallMu serializes every firewall operation with CloseFirewall, so
	// a close can neither detach the firewall under an operation in flight
	// nor read the counters while one rebuilds the chain.
	firewallMu sync.Mutex

	// firewallCustomRules is used to track if custom firewall rules are set for the slot and need a cleanup.
	firewallCustomRules atomic.Bool

	// egressDSCP is the class the slot's rule currently stamps (0 = no rule).
	// Not a synchronization point: callers need exclusive slot ownership,
	// which the pool guarantees between Get and the recycle give-back.
	egressDSCP atomic.Uint32

	vPeerIp net.IP
	vEthIp  net.IP
	vrtMask net.IPMask

	tapIp   net.IP
	tapMask net.IPMask

	// HostIP is IP address for the sandbox from the host machine.
	// You can use it to make requests to the sandbox.
	HostIP   net.IP
	hostNet  *net.IPNet
	hostCIDR string

	hyperloopPort string

	egressProxy EgressProxy
	config      Config

	// nonDNSUDPMode is consulted at every firewall chain build; nil means off.
	nonDNSUDPMode NonDNSUDPModeProvider

	// sandboxID names the tenant for the release log line. Not a
	// synchronization point: the owning sandbox sets it and the pool's
	// return path, which runs after, reads and clears it.
	sandboxID string

	// handedOut is set by the pool from the take to the return, so a mode
	// re-application leaves a slot on its way back to its release. Written
	// without firewallMu: the return path must not wait on a rebuild in
	// flight, and a mark that drops mid-rebuild changes nothing, because the
	// release rebuilds the chain again under the mutex.
	handedOut atomic.Bool

	// networkCreated tracks whether the network for this slot was cleanly
	// created by the current process. When true, teardown can skip legacy-name
	// cleanup.
	networkCreated atomic.Bool

	// tenantMode evaluates the mode with the flag contexts of the hand-out or
	// the last egress update, so a flag change keeps the tenant's targeting;
	// tenantEgress is what the chain was last built from. All under firewallMu.
	tenantMode   func() NonDNSUDPMode
	tenantEgress *orchestrator.SandboxNetworkEgressConfig
}

// NonDNSUDPModeProvider returns the configured non-DNS UDP mode value
// ("off", "count" or "drop") for the next firewall chain build.
type NonDNSUDPModeProvider func(ctx context.Context) string

func NewSlot(key string, idx int, config Config, egressProxy EgressProxy) (*Slot, error) {
	if idx < 1 || idx > vrtSlotsSize {
		return nil, fmt.Errorf("slot index %d is out of range [1, %d]", idx, vrtSlotsSize)
	}

	if config.VethPrefix == "" {
		config.VethPrefix = DefaultVethPrefix
	}
	if err := ValidateVethPrefix(config.VethPrefix); err != nil {
		return nil, err
	}

	vEthIp, err := netutils.GetIndexedIP(vrtNetworkCIDR, idx*vrtAddressPerSlot)
	if err != nil {
		return nil, fmt.Errorf("failed to get veth indexed IP: %w", err)
	}

	vPeerIp, err := netutils.GetIndexedIP(vrtNetworkCIDR, idx*vrtAddressPerSlot+1)
	if err != nil {
		return nil, fmt.Errorf("failed to get vpeer indexed IP: %w", err)
	}

	vrtCIDR := fmt.Sprintf("%s/%d", vPeerIp.String(), vrtMask)
	_, vrtNet, err := net.ParseCIDR(vrtCIDR)
	if err != nil {
		return nil, fmt.Errorf("failed to parse vrt CIDR: %w", err)
	}

	hostIp, err := netutils.GetIndexedIP(hostNetworkCIDR, idx)
	if err != nil {
		return nil, fmt.Errorf("failed to get host IP: %w", err)
	}

	hostCIDR := fmt.Sprintf("%s/%d", hostIp.String(), hostMask)
	_, hostNet, err := net.ParseCIDR(hostCIDR)
	if err != nil {
		return nil, fmt.Errorf("failed to parse host CIDR: %w", err)
	}

	tapCIDR := fmt.Sprintf("%s/%d", tapIp, tapMask)
	tapIp, tapNet, err := net.ParseCIDR(tapCIDR)
	if err != nil {
		return nil, fmt.Errorf("failed to parse tap CIDR: %w", err)
	}

	slot := &Slot{
		Key: key,
		Idx: idx,

		vPeerIp: vPeerIp,
		vEthIp:  vEthIp,
		vrtMask: vrtNet.Mask,

		tapIp:   tapIp,
		tapMask: tapNet.Mask,

		HostIP:   hostIp,
		hostNet:  hostNet,
		hostCIDR: hostCIDR,

		hyperloopPort: strconv.FormatUint(uint64(config.HyperloopProxyPort), 10),

		config:      config,
		egressProxy: egressProxy,
	}

	return slot, nil
}

func (s *Slot) VpeerName() string {
	return "eth0"
}

func (s *Slot) VpeerIP() net.IP {
	return s.vPeerIp
}

func (s *Slot) VethIP() net.IP {
	return s.vEthIp
}

func (s *Slot) VethPrefix() string {
	if s.config.VethPrefix != "" {
		return s.config.VethPrefix
	}

	return DefaultVethPrefix
}

func (s *Slot) VethName() string {
	return fmt.Sprintf("%s%d", s.VethPrefix(), s.Idx)
}

// MarkNetworkCreated sets whether the network was cleanly created by this process.
func (s *Slot) MarkNetworkCreated(ok bool) {
	s.networkCreated.Store(ok)
}

// TeardownVethNames returns the veth device names to clean up during teardown.
// If the slot network was cleanly created by this process, only the configured
// name is returned. Otherwise, if the configured prefix differs from the legacy
// prefix, it returns [configured, legacy]; when equal, [configured].
func (s *Slot) TeardownVethNames() []string {
	configured := s.VethName()
	if s.networkCreated.Load() {
		return []string{configured}
	}

	legacy := fmt.Sprintf("%s%d", LegacyVethPrefix, s.Idx)
	if configured == legacy {
		return []string{configured}
	}

	return []string{configured, legacy}
}

func (s *Slot) VrtMask() net.IPMask {
	return s.vrtMask
}

func (s *Slot) HostIPString() string {
	return s.HostIP.String()
}

func (s *Slot) HostMask() net.IPMask {
	return s.hostNet.Mask
}

func (s *Slot) HostNet() *net.IPNet {
	return s.hostNet
}

func (s *Slot) HostCIDR() string {
	return s.hostCIDR
}

func (s *Slot) NamespaceIP() string {
	return "169.254.0.21"
}

func (s *Slot) NamespaceID() string {
	return fmt.Sprintf("ns-%d", s.Idx)
}

func (s *Slot) TapName() string {
	return tapInterfaceName
}

func (s *Slot) TapIP() net.IP {
	return s.tapIp
}

func (s *Slot) TapIPString() string {
	return s.tapIp.String()
}

func (s *Slot) TapMask() int {
	return tapMask
}

func (s *Slot) TapMaskString() string {
	mask := net.CIDRMask(s.TapMask(), 32)

	return net.IP(mask).String()
}

func (s *Slot) TapCIDR() net.IPMask {
	return s.tapMask
}

func (s *Slot) TapMAC() string {
	return tapMAC
}

// UseNonDNSUDPMode sets the provider every later chain build consults.
func (s *Slot) UseNonDNSUDPMode(provider NonDNSUDPModeProvider) {
	s.nonDNSUDPMode = provider
}

// AssignSandbox names the tenant the slot was handed to.
func (s *Slot) AssignSandbox(sandboxID string) {
	s.sandboxID = sandboxID
}

// exportNonDNSUDP reports every non-empty tally the firewall harvested for
// the departing tenant and forgets them all; a chain that counted nothing
// adds no point.
func (s *Slot) exportNonDNSUDP(ctx context.Context, fw *Firewall) {
	for chain, tally := range fw.TakeNonDNSUDPTally() {
		if tally.Packets == 0 {
			continue
		}

		recordNonDNSUDP(ctx, tally.Packets, chain, s.config.NetworkVersion)
		logger.L().Info(ctx, "slot firewall saw non-DNS UDP from the guest",
			logger.WithSandboxID(s.sandboxID),
			zap.Int("slot_index", s.Idx),
			zap.String("mode", string(chain.Mode)),
			zap.String("posture", chain.Posture),
			zap.Uint64("packets", tally.Packets),
			zap.Uint64("bytes", tally.Bytes),
		)
	}
}

// netNSPath is where the slot's network namespace is bound.
func (s *Slot) netNSPath() string {
	return filepath.Join(NetNamespacesDir, s.NamespaceID())
}

// errNoFirewall is what every firewall operation returns for a slot without
// a firewall: NewSlot does not attach one, and CloseFirewall detaches it.
func (s *Slot) errNoFirewall() error {
	return fmt.Errorf("no firewall attached to slot namespace '%s'", s.NamespaceID())
}

// attachedFirewall returns the firewall an operation runs against, or the
// no-firewall error. Caller holds firewallMu.
func (s *Slot) attachedFirewall() (*Firewall, error) {
	if s.Firewall == nil {
		return nil, s.errNoFirewall()
	}

	return s.Firewall, nil
}

func (s *Slot) currentNonDNSUDPMode(ctx context.Context) NonDNSUDPMode {
	if s.nonDNSUDPMode == nil {
		return NonDNSUDPModeOff
	}

	return ParseNonDNSUDPMode(s.nonDNSUDPMode(ctx))
}

func (s *Slot) setHandedOut(handedOut bool) {
	s.handedOut.Store(handedOut)
}

// rememberTenant keeps what a rebuild in another mode needs: the request's
// flag contexts, detached from the request, which would otherwise keep its
// span and every value up its chain reachable for the sandbox's lifetime.
// Called once the chain is built from the egress, never before: a failed
// apply leaves the kernel on the previous chain, so the memory stays what
// that chain was built from and a later rebuild cannot install an egress
// the caller was told failed. Caller holds firewallMu.
func (s *Slot) rememberTenant(ctx context.Context, egress *orchestrator.SandboxNetworkEgressConfig) {
	flagCtx := featureflags.DetachContext(ctx)
	s.tenantMode = func() NonDNSUDPMode { return s.currentNonDNSUDPMode(flagCtx) }
	s.tenantEgress = egress
}

// reapplyNonDNSUDPMode rebuilds the chain when the tenant's mode now differs
// from the installed one; the rebuild keeps the egress rules and the tally. A
// slot on its way back is left to its release, and an isolated chain carries
// no rule whatever the mode. Reports whether the chain was rebuilt.
func (s *Slot) reapplyNonDNSUDPMode(ctx context.Context) (bool, error) {
	s.firewallMu.Lock()
	defer s.firewallMu.Unlock()

	if !s.handedOut.Load() || s.tenantMode == nil {
		return false, nil
	}

	fw, err := s.attachedFirewall()
	if err != nil {
		return false, err
	}

	installed := fw.Installed()
	if installed.Posture == postureIsolated {
		return false, nil
	}

	mode := s.tenantMode()
	if mode == installed.Mode {
		return false, nil
	}

	egress := s.tenantEgress
	if err := fw.ApplyRules(ctx, egress.GetEgressProxyAddress() != "", mode, egress.GetAllowedCidrs(), egress.GetDeniedCidrs(), egress.GetDefaultDeny()); err != nil {
		return false, err
	}

	return true, nil
}

func (s *Slot) InitializeFirewall(ctx context.Context) error {
	s.firewallMu.Lock()
	defer s.firewallMu.Unlock()

	if s.Firewall != nil {
		return fmt.Errorf("firewall is already initialized for slot %s", s.Key)
	}

	fw, err := NewFirewall(s.netNSPath(), s.TapName(), s.config.OrchestratorInSandboxIPAddress, s.config.AllowSandboxInternalCIDRs, s.egressProxy.HeadFilterRules(s), s.currentNonDNSUDPMode(ctx))
	if err != nil {
		return fmt.Errorf("error initializing firewall: %w", err)
	}
	s.Firewall = fw

	return nil
}

// CloseFirewall reads the counters a last time, deletes the slot's table and
// closes the connection and the namespace descriptor the firewall holds; the
// descriptor keeps the namespace alive, counters included, even once its
// name is unlinked, so the close needs nothing of the calling thread. The
// tally is exported whatever the outcome and the firewall detached, so a
// failed close loses nothing it read and a retry neither reads nor reports
// twice; a read that failed twice is in the error.
// Every firewall operation waits for a close in progress and is refused once
// the firewall is detached.
func (s *Slot) CloseFirewall(ctx context.Context) error {
	s.firewallMu.Lock()
	defer s.firewallMu.Unlock()

	fw := s.Firewall
	if fw == nil {
		return nil
	}

	err := fw.Close(ctx)
	s.exportNonDNSUDP(ctx, fw)
	s.Firewall = nil
	if err != nil {
		return fmt.Errorf("error closing firewall: %w", err)
	}

	return nil
}

// dscpMangleRuleArgs builds the mangle/POSTROUTING rule that stamps the given
// DSCP class on everything leaving the sandbox netns through the vpeer uplink.
func (s *Slot) dscpMangleRuleArgs(dscp uint8) []string {
	return []string{"-o", s.VpeerName(), "-j", "DSCP", "--set-dscp", strconv.Itoa(int(dscp))}
}

// Without a timeout go-iptables emits a bare --wait, which blocks forever on
// the legacy backend's host-wide lock.
const iptablesLockTimeout = 5 // seconds

// One handle for the package: New() execs `iptables --version` per call, and
// commands run in the calling thread's netns regardless of where New() ran.
// Cached on success only — a transient New() failure (fork EAGAIN/ENOMEM)
// must not poison the process. A racing duplicate New() is harmless.
var cachedIptables atomic.Pointer[iptables.IPTables]

// IptablesHandle returns the package's shared iptables handle.
func IptablesHandle() (*iptables.IPTables, error) {
	return iptablesHandle()
}

func iptablesHandle() (*iptables.IPTables, error) {
	if t := cachedIptables.Load(); t != nil {
		return t, nil
	}

	t, err := iptables.New(iptables.Timeout(iptablesLockTimeout))
	if err != nil {
		return nil, err
	}
	cachedIptables.Store(t)

	return t, nil
}

// applyEgressDSCP re-stamps the slot's egress rule so a build can be marked
// differently from a regular sandbox. 0 removes the rule; returns without
// entering the netns when the class already matches.
func (s *Slot) applyEgressDSCP(ctx context.Context, dscp uint8) error {
	current := uint8(s.egressDSCP.Load())
	if current == dscp {
		return nil
	}

	_, span := tracer.Start(ctx, "slot-egress-dscp-apply", trace.WithAttributes(
		attribute.String("namespace_id", s.NamespaceID()),
		attribute.Int("dscp", int(dscp)),
	))
	defer span.End()

	n, err := ns.GetNS(s.netNSPath())
	if err != nil {
		return fmt.Errorf("failed to get slot network namespace '%s': %w", s.NamespaceID(), err)
	}
	defer n.Close()

	err = n.Do(func(_ ns.NetNS) error {
		tables, err := iptablesHandle()
		if err != nil {
			return fmt.Errorf("error initializing iptables: %w", err)
		}

		// Append before delete: DSCP is non-terminal, so the appended (last)
		// entry wins while both rules exist and egress is never unmarked.
		// After a partial failure AppendUnique may keep an earlier duplicate
		// in place instead of moving it last, but every such path errors into
		// cleanupWith, so no tenant runs on that slot.
		if dscp > 0 {
			if err := tables.AppendUnique("mangle", "POSTROUTING", s.dscpMangleRuleArgs(dscp)...); err != nil {
				return fmt.Errorf("error creating DSCP %d mangle rule on vpeer: %w", dscp, err)
			}

			s.egressDSCP.Store(uint32(dscp))
		}

		if current > 0 {
			if err := tables.DeleteIfExists("mangle", "POSTROUTING", s.dscpMangleRuleArgs(current)...); err != nil {
				return fmt.Errorf("error removing DSCP %d mangle rule on vpeer: %w", current, err)
			}
		}

		if dscp == 0 {
			s.egressDSCP.Store(0)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed execution in network namespace '%s': %w", s.NamespaceID(), err)
	}

	return nil
}

func (s *Slot) ConfigureInternet(ctx context.Context, network *orchestrator.SandboxNetworkConfig) (e error) {
	ctx, span := tracer.Start(ctx, "slot-internet-configure", trace.WithAttributes(
		attribute.String("namespace_id", s.NamespaceID()),
	))
	defer span.End()

	egress := network.GetEgress()
	hasUserRules := len(egress.GetAllowedCidrs()) != 0 ||
		len(egress.GetDeniedCidrs()) != 0 ||
		len(egress.GetAllowedDomains()) != 0 ||
		len(egress.GetDeniedDomains()) != 0 ||
		egress.GetDefaultDeny()
	hasBYOP := egress.GetEgressProxyAddress() != ""

	s.firewallMu.Lock()
	defer s.firewallMu.Unlock()

	fw, err := s.attachedFirewall()
	if err != nil {
		return err
	}

	// The incoming tenant's mode decides, not the one the slot was built or
	// recycled under; a default-posture slot already in that mode is left
	// alone, as internet access is allowed by default.
	mode := s.currentNonDNSUDPMode(ctx)
	if !hasUserRules && !hasBYOP {
		if fw.NonDNSUDPMode() == mode {
			s.rememberTenant(ctx, egress)

			return nil
		}
	} else {
		s.firewallCustomRules.Store(true)
	}

	if err := fw.ApplyRules(ctx, hasBYOP, mode, egress.GetAllowedCidrs(), egress.GetDeniedCidrs(), egress.GetDefaultDeny()); err != nil {
		return err
	}
	s.rememberTenant(ctx, egress)

	return nil
}

// UpdateInternet replaces all user firewall rules atomically in a single nftables flush.
func (s *Slot) UpdateInternet(ctx context.Context, egress *orchestrator.SandboxNetworkEgressConfig) error {
	ctx, span := tracer.Start(ctx, "slot-internet-update", trace.WithAttributes(
		attribute.String("namespace_id", s.NamespaceID()),
	))
	defer span.End()

	s.firewallMu.Lock()
	defer s.firewallMu.Unlock()

	fw, err := s.attachedFirewall()
	if err != nil {
		return err
	}

	allowedCIDRs := egress.GetAllowedCidrs()
	deniedCIDRs := egress.GetDeniedCidrs()
	hasBYOP := egress.GetEgressProxyAddress() != ""

	// Set before mutating: a partial failure must still trigger cleanup.
	s.firewallCustomRules.Store(true)

	if err := fw.ApplyRules(ctx, hasBYOP, s.currentNonDNSUDPMode(ctx), allowedCIDRs, deniedCIDRs, egress.GetDefaultDeny()); err != nil {
		return err
	}
	s.rememberTenant(ctx, egress)

	return nil
}

// DenyEgress drops all guest-originated traffic except the orchestrator control
// path, isolating a throwaway sandbox (the pause-resume prefetch harvest) so its
// envd init cannot reach the network. Like UpdateInternet it marks custom rules
// so the slot's cleanup resets the firewall before reuse.
func (s *Slot) DenyEgress(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "slot-internet-deny-egress", trace.WithAttributes(
		attribute.String("namespace_id", s.NamespaceID()),
	))
	defer span.End()

	s.firewallMu.Lock()
	defer s.firewallMu.Unlock()

	// Guard against a missing firewall before marking custom rules: a stored
	// flag with a nil firewall would also panic the cleanup (ResetInternet).
	fw, err := s.attachedFirewall()
	if err != nil {
		return err
	}

	// Set before mutating: a partial failure must still trigger cleanup.
	s.firewallCustomRules.Store(true)

	return fw.DenyEgress(ctx)
}

// ResetInternet releases the slot's tenant: a chain the tenant shaped is
// rebuilt as a default-posture one in the current non-DNS UDP mode, the
// counters are read and the tally exported. A default-posture chain is left
// alone whatever the mode now reads, because ConfigureInternet applies the
// incoming tenant's mode on every hand-out; a rebuild here would only add a
// flush per slot cycle.
func (s *Slot) ResetInternet(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "slot-internet-reset", trace.WithAttributes(
		attribute.String("namespace_id", s.NamespaceID()),
	))
	defer span.End()

	s.firewallMu.Lock()
	defer s.firewallMu.Unlock()

	fw, err := s.attachedFirewall()
	if err != nil {
		return err
	}

	if s.firewallCustomRules.Load() {
		// Revert BYOP so the next tenant can't inherit non-TCP-only deny rules.
		if err := fw.ApplyRules(ctx, false, s.currentNonDNSUDPMode(ctx), nil, nil, false); err != nil {
			return err
		}
	}

	// Whatever the mode, every counter is read before the slot can carry
	// another tenant: a packet left in the kernel would be reported with
	// the next one. A read that fails twice fails the release, and the
	// pool destroys the slot instead of reusing it.
	if err := fw.harvestWithRetry(ctx, "at release", func() error {
		_, err := fw.Harvest()

		return err
	}); err != nil {
		return err
	}

	s.release(ctx, fw)

	return nil
}

// release exports the departing tenant's tally and forgets the tenant.
func (s *Slot) release(ctx context.Context, fw *Firewall) {
	s.exportNonDNSUDP(ctx, fw)
	s.firewallCustomRules.Store(false)
	s.sandboxID = ""
	s.tenantMode = nil
	s.tenantEgress = nil
}

func getHostNetworkCIDR() *net.IPNet {
	cidr := env.GetEnv("SANDBOXES_HOST_NETWORK_CIDR", defaultHostNetworkCIDR)

	_, subnet, err := net.ParseCIDR(cidr)
	if err != nil {
		log.Fatalf("Failed to parse network CIDR %s: %v", cidr, err)
	}

	log.Println("Using host network cidr", "cidr", cidr)

	return subnet
}

// TapHostHardwareAddr returns the fixed host-side tap MAC.
func TapHostHardwareAddr() net.HardwareAddr {
	return slices.Clone(tapHostHardwareAddr)
}

// getTapHostHardwareAddr parses the fixed tapHostMAC constant once at package
// init, so CreateNetwork doesn't reparse a value that can never change or
// fail at runtime.
func getTapHostHardwareAddr() net.HardwareAddr {
	addr, err := net.ParseMAC(tapHostMAC)
	if err != nil {
		log.Fatalf("Failed to parse tap host MAC address %s: %v", tapHostMAC, err)
	}

	return addr
}

func getVrtNetworkCIDR() *net.IPNet {
	cidr := env.GetEnv("SANDBOXES_VRT_NETWORK_CIDR", defaultVrtNetworkCIDR)

	_, subnet, err := net.ParseCIDR(cidr)
	if err != nil {
		log.Fatalf("Failed to parse network CIDR %s: %v", cidr, err)
	}

	log.Printf("Using vrt network cidr %s", cidr)

	return subnet
}

func GetVrtSlotsSize() int {
	ones, _ := getVrtNetworkCIDR().Mask.Size()

	// total IPs in the CIDR block
	totalIPs := 1 << (32 - ones)

	// total slots that we can allocate
	// we need to divide total IPs by number of addresses per slot (vpeer and veth)
	// then we subtract the number of addresses so it will not overflow, because we are adding them incrementally by slot index
	totalSlots := (totalIPs / vrtAddressPerSlot) - vrtAddressPerSlot

	log.Printf("Using network slot size: %d", totalSlots)

	return totalSlots
}
