//go:build linux

package v2

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"

	"github.com/google/nftables"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// slotForwardAccepts builds the slot's iptables FORWARD accept pair,
// matching v1's two rules.
func slotForwardAccepts(slot *network.Slot, gw string) [][]string {
	return [][]string{
		{"-i", slot.VethName(), "-o", gw, "-j", "ACCEPT"},
		{"-i", gw, "-o", slot.VethName(), "-j", "ACCEPT"},
	}
}

// CreateNetworkV2 sets up the full network stack for a v2 slot.
// It reuses the same netlink setup pattern as v1 (namespace, veth, tap, routes)
// but moves the datapath to nftables.
//
// Compared to v1:
//   - In-namespace NAT: nftables SetupNamespaceNAT() replaces 2 iptables rules
//   - Host firewall: hf.AddSlot() replaces the host iptables rules with 2 set
//     element adds; only v1's two filter-FORWARD accepts stay in iptables
//   - eBPF: observer.Attach() adds optional per-veth counters (best-effort)
func CreateNetworkV2(ctx context.Context, slot *network.Slot, slotV2 *SlotV2,
	hf *HostFirewall, observer *VethObserver,
) error {
	// Run on a goroutine of its own so the thread it pins is this call's
	// alone. createNetworkV2 keeps that thread locked if it cannot get back to
	// the host namespace, and the runtime destroys a still-locked thread when
	// its goroutine exits — a caller's long-lived goroutine would instead keep
	// building every later slot inside the namespace it got stuck in.
	done := make(chan error, 1)
	go func() {
		done <- createNetworkV2(ctx, slot, slotV2, hf, observer)
	}()

	return <-done
}

func createNetworkV2(ctx context.Context, slot *network.Slot, slotV2 *SlotV2,
	hf *HostFirewall, observer *VethObserver,
) (retErr error) {
	// Prevent thread changes so we can safely manipulate namespaces
	runtime.LockOSThread()

	// Save the original (host) namespace
	hostNS, err := netns.Get()
	if err != nil {
		runtime.UnlockOSThread()

		return fmt.Errorf("cannot get current (host) namespace: %w", err)
	}

	cleanupNeeded := false
	defer func() {
		restoreErr := netns.Set(hostNS)
		if restoreErr != nil {
			// Deliberately no UnlockOSThread: the thread is still inside the
			// sandbox namespace, so it must die with this goroutine rather
			// than be reused. The slot fails with it — its host-side state
			// cannot be trusted, and cleanup would run in the wrong namespace.
			retErr = errors.Join(retErr, fmt.Errorf("error resetting network namespace back to host: %w", restoreErr))
			logger.L().Error(ctx, "error resetting network namespace back to host", zap.Error(restoreErr))
		} else {
			runtime.UnlockOSThread()

			if retErr != nil && cleanupNeeded {
				if cleanupErr := RemoveNetworkV2(ctx, slot, slotV2, hf, observer); cleanupErr != nil {
					retErr = errors.Join(retErr, fmt.Errorf("error cleaning up partially created v2 network: %w", cleanupErr))
				}
			}
		}

		hostNS.Close()
	}()

	// An existing namespace for this index is a stale reclaim anchor from a
	// failed teardown whose rules, routes and veth may still exist. Run a full
	// (idempotent) teardown to reclaim them; deleting only the namespace would
	// orphan that state.
	available, err := network.IsNamespaceAvailable(network.NetNamespacesDir, slot.NamespaceID())
	if err != nil {
		return fmt.Errorf("cannot check for stale namespace: %w", err)
	}
	if !available {
		if err := RemoveNetworkV2(ctx, slot, slotV2, hf, observer); err != nil {
			return fmt.Errorf("cannot reclaim stale v2 network slot: %w", err)
		}
	}

	// --- Create namespace ---
	ns, err := netns.NewNamed(slot.NamespaceID())
	if err != nil {
		return fmt.Errorf("cannot create new namespace: %w", err)
	}
	cleanupNeeded = true

	defer ns.Close()

	// --- Create veth pair (inside new namespace, then move veth to host) ---
	vethAttrs := netlink.NewLinkAttrs()
	vethAttrs.Name = slot.VethName()
	veth := &netlink.Veth{
		LinkAttrs: vethAttrs,
		PeerName:  slot.VpeerName(),
	}

	if err := netlink.LinkAdd(veth); err != nil {
		return fmt.Errorf("error creating veth device: %w", err)
	}

	vpeer, err := netlink.LinkByName(slot.VpeerName())
	if err != nil {
		return fmt.Errorf("error finding vpeer: %w", err)
	}

	if err := netlink.LinkSetUp(vpeer); err != nil {
		return fmt.Errorf("error setting vpeer device up: %w", err)
	}

	if err := netlink.AddrAdd(vpeer, &netlink.Addr{
		IPNet: &net.IPNet{IP: slot.VpeerIP(), Mask: slot.VrtMask()},
	}); err != nil {
		return fmt.Errorf("error adding vpeer device address: %w", err)
	}

	// Move veth to host namespace
	if err := netlink.LinkSetNsFd(veth, int(hostNS)); err != nil {
		return fmt.Errorf("error moving veth device to host namespace: %w", err)
	}

	if err := netns.Set(hostNS); err != nil {
		return fmt.Errorf("error setting network namespace: %w", err)
	}

	vethInHost, err := netlink.LinkByName(slot.VethName())
	if err != nil {
		return fmt.Errorf("error finding veth: %w", err)
	}

	if err := netlink.LinkSetUp(vethInHost); err != nil {
		return fmt.Errorf("error setting veth device up: %w", err)
	}

	if err := netlink.AddrAdd(vethInHost, &netlink.Addr{
		IPNet: &net.IPNet{IP: slot.VethIP(), Mask: slot.VrtMask()},
	}); err != nil {
		return fmt.Errorf("error adding veth device address: %w", err)
	}

	// Switch back to sandbox namespace for tap setup
	if err := netns.Set(ns); err != nil {
		return fmt.Errorf("error setting network namespace to %s: %w", ns.String(), err)
	}

	// --- Create TAP device for Firecracker ---
	tapAttrs := netlink.NewLinkAttrs()
	tapAttrs.Name = slot.TapName()
	tapAttrs.Namespace = ns
	tap := &netlink.Tuntap{
		Mode:      netlink.TUNTAP_MODE_TAP,
		LinkAttrs: tapAttrs,
	}

	if err := netlink.LinkAdd(tap); err != nil {
		return fmt.Errorf("error creating tap device: %w", err)
	}

	// Keep resumed guests' cached gateway ARP entries valid. Tap devices are
	// isolated in per-sandbox network namespaces, so sharing this MAC is safe.
	if err := netlink.LinkSetHardwareAddr(tap, network.TapHostHardwareAddr()); err != nil {
		return fmt.Errorf("error setting tap device hardware address: %w", err)
	}

	if err := netlink.LinkSetUp(tap); err != nil {
		return fmt.Errorf("error setting tap device up: %w", err)
	}

	if err := netlink.AddrAdd(tap, &netlink.Addr{
		IPNet: &net.IPNet{IP: slot.TapIP(), Mask: slot.TapCIDR()},
	}); err != nil {
		return fmt.Errorf("error setting address of the tap device: %w", err)
	}

	// --- Loopback ---
	lo, err := netlink.LinkByName("lo")
	if err != nil {
		return fmt.Errorf("error finding lo: %w", err)
	}
	if err := netlink.LinkSetUp(lo); err != nil {
		return fmt.Errorf("error setting lo device up: %w", err)
	}

	// --- Default route inside namespace ---
	if err := netlink.RouteAdd(&netlink.Route{
		Scope: netlink.SCOPE_UNIVERSE,
		Gw:    slot.VethIP(),
	}); err != nil {
		return fmt.Errorf("error adding default NS route: %w", err)
	}

	// --- In-namespace NAT (replaces 2 iptables rules) ---
	// We need a fresh nftables conn inside this namespace context
	nsConn, err := nftables.New(nftables.AsLasting())
	if err != nil {
		return fmt.Errorf("nftables conn in namespace: %w", err)
	}

	// The v1 Firewall creates a table named "slot-firewall" — reuse it
	nsTable := nsConn.AddTable(&nftables.Table{
		Name:   "slot-firewall",
		Family: nftables.TableFamilyINet,
	})

	if err := SetupNamespaceNAT(nsConn, nsTable, slot.VpeerName(), slot.HostIPString(), slot.NamespaceIP()); err != nil {
		nsConn.CloseLasting()

		return fmt.Errorf("setup namespace NAT: %w", err)
	}

	// Slots are pooled: seed the untenanted (regular-sandbox) class; the pool
	// re-stamps per tenant on Get and restores it on recycle.
	if err := SetupEgressDSCP(nsConn, nsTable, slot.VpeerName(), hf.config.EgressDSCP(sandboxtypes.EgressClassSandbox)); err != nil {
		nsConn.CloseLasting()

		return fmt.Errorf("setup egress DSCP: %w", err)
	}
	nsConn.CloseLasting()

	// --- Initialize in-namespace firewall (reuse v1 Firewall) ---
	if err := slot.InitializeFirewall(); err != nil {
		return fmt.Errorf("error initializing slot firewall: %w", err)
	}

	// --- Switch back to host namespace ---
	if err := netns.Set(hostNS); err != nil {
		return fmt.Errorf("error setting network namespace to %s: %w", hostNS.String(), err)
	}

	// --- Host route to namespace ---
	if err := netlink.RouteAdd(&netlink.Route{
		Gw:  slot.VpeerIP(),
		Dst: slot.HostNet(),
	}); err != nil {
		return fmt.Errorf("error adding route from host to FC: %w", err)
	}

	// --- Host forward accepts ---
	// Kept in iptables: netfilter runs every forward hook and the most
	// restrictive verdict wins, so an accept in the v2 nftables table cannot
	// beat an iptables `-P FORWARD DROP` policy (Docker's default) — a rule
	// appended to that chain itself can.
	tables, err := network.IptablesHandle()
	if err != nil {
		return fmt.Errorf("error initializing iptables: %w", err)
	}

	for _, rule := range slotForwardAccepts(slot, hf.defaultGw) {
		if err := tables.Append("filter", "FORWARD", rule...); err != nil {
			return fmt.Errorf("error creating forwarding rule: %w", err)
		}
	}

	// --- Host firewall: add slot to sets (replaces v1's remaining host rules) ---
	if err := hf.AddSlot(ctx, slotV2); err != nil {
		return fmt.Errorf("error adding slot to host firewall: %w", err)
	}

	// --- eBPF observability (best-effort) ---
	if observer != nil {
		if err := observer.Attach(slot.VethName()); err != nil {
			logger.L().Warn(ctx, "failed to attach eBPF observer (non-fatal)", zap.Error(err))
		}
	}

	return nil
}

// RemoveNetworkV2 tears down the network for a v2 slot. It is idempotent:
// state that is already gone is not an error, so it can reclaim a partially
// created or previously half-torn-down slot.
func RemoveNetworkV2(ctx context.Context, slot *network.Slot, slotV2 *SlotV2,
	hf *HostFirewall, observer *VethObserver,
) error {
	var errs []error

	// Close in-namespace firewall
	if err := slot.CloseFirewall(); err != nil {
		errs = append(errs, fmt.Errorf("error closing firewall: %w", err))
	}

	// Remove from host firewall sets
	if err := hf.RemoveSlot(ctx, slotV2); err != nil {
		errs = append(errs, fmt.Errorf("error removing slot from host firewall: %w", err))
	}

	// Delete host forward accepts
	tables, err := network.IptablesHandle()
	if err != nil {
		errs = append(errs, fmt.Errorf("error initializing iptables: %w", err))
	} else {
		for _, rule := range slotForwardAccepts(slot, hf.defaultGw) {
			appendUnlessAbsent(&errs, tables.Delete("filter", "FORWARD", rule...), isIPTablesNotExist, "error deleting forwarding rule: %w")
		}
	}

	// Detach eBPF observer
	if observer != nil {
		if err := observer.Detach(slot.VethName()); err != nil {
			errs = append(errs, fmt.Errorf("error detaching eBPF observer: %w", err))
		}
	}

	// Delete host route
	err = netlink.RouteDel(&netlink.Route{
		Gw:  slot.VpeerIP(),
		Dst: slot.HostNet(),
	})
	appendUnlessAbsent(&errs, err, isRouteNotExist, "error deleting route from host to FC: %w")

	// Delete veth device explicitly (prevents race on reuse)
	vethLink, err := netlink.LinkByName(slot.VethName())
	if err != nil {
		appendUnlessAbsent(&errs, err, isLinkNotExist, "error finding veth: %w")
	} else {
		appendUnlessAbsent(&errs, netlink.LinkDel(vethLink), isLinkNotExist, "error deleting veth device: %w")
	}

	// The /run/netns entry is the anchor startup reclaim uses to rediscover a
	// leaked slot, and every host-side item above is keyed by the slot index:
	// deleting it after a failed teardown would orphan that state with no way
	// to retry it. CreateNetworkV2 removes a stale anchor before reusing the
	// slot.
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	err = netns.DeleteNamed(slot.NamespaceID())
	appendUnlessAbsent(&errs, err, isNamespaceNotExist, "error deleting namespace: %w")

	return errors.Join(errs...)
}
