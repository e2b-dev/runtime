//go:build linux

package network

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/coreos/go-iptables/iptables"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type notExistError interface {
	IsNotExist() bool
}

type multiUnwrapError interface {
	Unwrap() []error
}

func ignoreExpectedAbsent(err error, isExpected func(error) bool) bool {
	if err == nil {
		return true
	}

	var joined multiUnwrapError
	if errors.As(err, &joined) {
		for _, child := range joined.Unwrap() {
			if !ignoreExpectedAbsent(child, isExpected) {
				return false
			}
		}

		return true
	}

	return isExpected(err)
}

func isIPTablesNotExist(err error) bool {
	var notExist notExistError

	return errors.As(err, &notExist) && notExist.IsNotExist()
}

func isRouteNotExist(err error) bool {
	return errors.Is(err, unix.ESRCH) || errors.Is(err, unix.ENOENT)
}

func isLinkNotExist(err error) bool {
	var linkNotFound netlink.LinkNotFoundError

	return errors.As(err, &linkNotFound) || errors.Is(err, unix.ENODEV) || errors.Is(err, unix.ENOENT)
}

func isNamespaceNotExist(err error) bool {
	return os.IsNotExist(err) || errors.Is(err, unix.ENOENT)
}

// DeleteNamespace is netns.DeleteNamed, except that an anchor whose mount is
// already gone (EINVAL, not a mount point, as after a restart into a new mount
// namespace) is removed instead of failing every later teardown.
func DeleteNamespace(name string) error {
	err := netns.DeleteNamed(name)
	if errors.Is(err, unix.EINVAL) {
		return os.Remove(filepath.Join(NetNamespacesDir, name))
	}

	return err
}

func appendUnlessExpectedAbsentf(errs *[]error, err error, isExpected func(error) bool, format string) {
	if ignoreExpectedAbsent(err, isExpected) {
		return
	}

	*errs = append(*errs, fmt.Errorf(format, err))
}

func (s *Slot) CreateNetwork(ctx context.Context) (retErr error) {
	s.networkCreated.Store(false)

	// Prevent thread changes so we can safely manipulate with namespaces
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Save the original (host) namespace and restore it upon function exit
	hostNS, err := netns.Get()
	if err != nil {
		return fmt.Errorf("cannot get current (host) namespace: %w", err)
	}

	cleanupNeeded := false
	defer func() {
		restoreErr := netns.Set(hostNS)
		if restoreErr != nil {
			logger.L().Error(ctx, "error resetting network namespace back to the host namespace", zap.Error(restoreErr))
		}

		if retErr != nil && cleanupNeeded {
			if restoreErr != nil {
				retErr = errors.Join(retErr, fmt.Errorf("error resetting network namespace back to the host namespace before cleanup: %w", restoreErr))
			} else if cleanupErr := s.RemoveNetwork(ctx); cleanupErr != nil {
				retErr = errors.Join(retErr, fmt.Errorf("error cleaning up partially created network: %w", cleanupErr))
			}
		}

		err = hostNS.Close()
		if err != nil {
			logger.L().Error(ctx, "error closing host network namespace", zap.Error(err))
		}
	}()

	// An existing namespace for this index is a stale reclaim anchor from a
	// failed teardown whose iptables rules, routes and veth may still exist. Run
	// a full (idempotent) RemoveNetwork to reclaim them; deleting only the
	// namespace would orphan those rules. On failure the anchor is kept and we
	// abort so the slot is retried later instead of leaking.
	available, err := isNamespaceAvailable(NetNamespacesDir, s.NamespaceID())
	if err != nil {
		return fmt.Errorf("cannot check for stale namespace: %w", err)
	}
	if !available {
		if err = s.RemoveNetwork(ctx); err != nil {
			return fmt.Errorf("cannot reclaim stale network slot: %w", err)
		}
	}

	// Create NS for the sandbox
	ns, err := netns.NewNamed(s.NamespaceID())
	if err != nil {
		return fmt.Errorf("cannot create new namespace: %w", err)
	}
	cleanupNeeded = true

	defer ns.Close()

	// Create the Veth and Vpeer
	vethAttrs := netlink.NewLinkAttrs()
	vethAttrs.Name = s.VethName()
	veth := &netlink.Veth{
		LinkAttrs: vethAttrs,
		PeerName:  s.VpeerName(),
	}

	err = netlink.LinkAdd(veth)
	if err != nil {
		return fmt.Errorf("error creating veth device: %w", err)
	}

	vpeer, err := netlink.LinkByName(s.VpeerName())
	if err != nil {
		return fmt.Errorf("error finding vpeer: %w", err)
	}

	err = netlink.LinkSetUp(vpeer)
	if err != nil {
		return fmt.Errorf("error setting vpeer device up: %w", err)
	}

	err = netlink.AddrAdd(vpeer, &netlink.Addr{
		IPNet: &net.IPNet{
			IP:   s.VpeerIP(),
			Mask: s.VrtMask(),
		},
	})
	if err != nil {
		return fmt.Errorf("error adding vpeer device address: %w", err)
	}

	// Move Veth device to the host NS
	err = netlink.LinkSetNsFd(veth, int(hostNS))
	if err != nil {
		return fmt.Errorf("error moving veth device to the host namespace: %w", err)
	}

	err = netns.Set(hostNS)
	if err != nil {
		return fmt.Errorf("error setting network namespace: %w", err)
	}

	vethInHost, err := netlink.LinkByName(s.VethName())
	if err != nil {
		return fmt.Errorf("error finding veth: %w", err)
	}

	err = netlink.LinkSetUp(vethInHost)
	if err != nil {
		return fmt.Errorf("error setting veth device up: %w", err)
	}

	err = netlink.AddrAdd(vethInHost, &netlink.Addr{
		IPNet: &net.IPNet{
			IP:   s.VethIP(),
			Mask: s.VrtMask(),
		},
	})
	if err != nil {
		return fmt.Errorf("error adding veth device address: %w", err)
	}

	err = netns.Set(ns)
	if err != nil {
		return fmt.Errorf("error setting network namespace to %s: %w", ns.String(), err)
	}

	// Create Tap device for FC in NS
	tapAttrs := netlink.NewLinkAttrs()
	tapAttrs.Name = s.TapName()
	tapAttrs.Namespace = ns
	tap := &netlink.Tuntap{
		Mode:      netlink.TUNTAP_MODE_TAP,
		LinkAttrs: tapAttrs,
	}

	err = netlink.LinkAdd(tap)
	if err != nil {
		return fmt.Errorf("error creating tap device: %w", err)
	}

	// Keep resumed guests' cached gateway ARP entries valid. Tap devices are
	// isolated in per-sandbox network namespaces, so sharing this MAC is safe.
	err = netlink.LinkSetHardwareAddr(tap, tapHostHardwareAddr)
	if err != nil {
		return fmt.Errorf("error setting tap device hardware address: %w", err)
	}

	err = netlink.LinkSetUp(tap)
	if err != nil {
		return fmt.Errorf("error setting tap device up: %w", err)
	}

	err = netlink.AddrAdd(tap, &netlink.Addr{
		IPNet: &net.IPNet{
			IP:   s.TapIP(),
			Mask: s.TapCIDR(),
		},
	})
	if err != nil {
		return fmt.Errorf("error setting address of the tap device: %w", err)
	}

	// Set NS lo device up
	lo, err := netlink.LinkByName(loopbackInterface)
	if err != nil {
		return fmt.Errorf("error finding lo: %w", err)
	}

	err = netlink.LinkSetUp(lo)
	if err != nil {
		return fmt.Errorf("error setting lo device up: %w", err)
	}

	// Add NS default route
	err = netlink.RouteAdd(&netlink.Route{
		Scope: netlink.SCOPE_UNIVERSE,
		Gw:    s.VethIP(),
	})
	if err != nil {
		return fmt.Errorf("error adding default NS route: %w", err)
	}

	tables, err := iptables.New()
	if err != nil {
		return fmt.Errorf("error initializing iptables: %w", err)
	}

	// Add NAT routing rules to NS
	err = tables.Append("nat", "POSTROUTING", "-o", s.VpeerName(), "-s", s.NamespaceIP(), "-j", "SNAT", "--to", s.HostIPString())
	if err != nil {
		return fmt.Errorf("error creating postrouting rule to vpeer: %w", err)
	}

	err = tables.Append("nat", "PREROUTING", "-i", s.VpeerName(), "-d", s.HostIPString(), "-j", "DNAT", "--to", s.NamespaceIP())
	if err != nil {
		return fmt.Errorf("error creating postrouting rule from vpeer: %w", err)
	}

	// Marker for downstream L3 firewalls. 0 disables; see SANDBOX_EGRESS_DSCP.
	// Slots are pooled: this seeds the untenanted class, Pool.Get re-stamps
	// for builds, recycle restores it.
	dscp := s.config.untenantedDSCP()
	if dscp > 0 {
		err = tables.Append("mangle", "POSTROUTING", s.dscpMangleRuleArgs(dscp)...)
		if err != nil {
			return fmt.Errorf("error creating DSCP mangle rule on vpeer: %w", err)
		}
	}
	s.egressDSCP.Store(uint32(dscp))

	err = s.InitializeFirewall(ctx)
	if err != nil {
		return fmt.Errorf("error initializing slot firewall: %w", err)
	}

	// Go back to original namespace
	err = netns.Set(hostNS)
	if err != nil {
		return fmt.Errorf("error setting network namespace to %s: %w", hostNS.String(), err)
	}

	// Add routing from host to FC namespace
	err = netlink.RouteAdd(&netlink.Route{
		Gw:  s.VpeerIP(),
		Dst: s.HostNet(),
	})
	if err != nil {
		return fmt.Errorf("error adding route from host to FC: %w", err)
	}

	// Add host forwarding rules
	err = tables.Append("filter", "FORWARD", "-i", s.VethName(), "-o", defaultGateway, "-j", "ACCEPT")
	if err != nil {
		return fmt.Errorf("error creating forwarding rule to default gateway: %w", err)
	}

	err = tables.Append("filter", "FORWARD", "-i", defaultGateway, "-o", s.VethName(), "-j", "ACCEPT")
	if err != nil {
		return fmt.Errorf("error creating forwarding rule from default gateway: %w", err)
	}

	// Add host postrouting rules
	err = tables.Append("nat", "POSTROUTING", "-s", s.HostCIDR(), "-o", defaultGateway, "-j", "MASQUERADE")
	if err != nil {
		return fmt.Errorf("error creating postrouting rule: %w", err)
	}

	// Redirect traffic destined for hyperloop proxy
	err = tables.Append(
		"nat", "PREROUTING", "-i", s.VethName(),
		"-p", "tcp", "-d", s.config.OrchestratorInSandboxIPAddress, "--dport", "80",
		"-j", "REDIRECT", "--to-port", s.hyperloopPort,
	)
	if err != nil {
		return fmt.Errorf("error creating HTTP redirect rule to sandbox hyperloop proxy server: %w", err)
	}

	// Redirect traffic destined for portmapper
	err = tables.Append("nat", "PREROUTING",
		"--in-interface", s.VethName(), "--protocol", "tcp",
		"--destination", s.config.OrchestratorInSandboxIPAddress, "--dport", "111",
		"--jump", "REDIRECT", "--to-port", fmt.Sprintf("%d", s.config.PortmapperPort),
	)
	if err != nil {
		return fmt.Errorf("error creating NFS redirect rule to sandbox portmapper server: %w", err)
	}

	// Redirect traffic destined for NFS proxy
	err = tables.Append("nat", "PREROUTING",
		"--in-interface", s.VethName(), "--protocol", "tcp",
		"--destination", s.config.OrchestratorInSandboxIPAddress, "--dport", "2049",
		"--jump", "REDIRECT", "--to-port", fmt.Sprintf("%d", s.config.NFSProxyPort),
	)
	if err != nil {
		return fmt.Errorf("error creating NFS redirect rule to sandbox NFS proxy server: %w", err)
	}

	// Create rules needed by egress proxy
	err = s.egressProxy.OnSlotCreate(s, tables)
	if err != nil {
		return err
	}

	s.networkCreated.Store(true)

	return nil
}

func (s *Slot) RemoveNetwork(ctx context.Context) error {
	var errs []error

	err := s.CloseFirewall(ctx)
	if err != nil {
		errs = append(errs, fmt.Errorf("error closing firewall: %w", err))
	}

	tables, err := iptables.New()
	if err != nil {
		errs = append(errs, fmt.Errorf("error initializing iptables: %w", err))
	} else {
		for _, vethName := range s.TeardownVethNames() {
			// Delete host forwarding rules
			err = tables.Delete("filter", "FORWARD", "-i", vethName, "-o", defaultGateway, "-j", "ACCEPT")
			appendUnlessExpectedAbsentf(&errs, err, isIPTablesNotExist, "error deleting host forwarding rule to default gateway: %w")

			err = tables.Delete("filter", "FORWARD", "-i", defaultGateway, "-o", vethName, "-j", "ACCEPT")
			appendUnlessExpectedAbsentf(&errs, err, isIPTablesNotExist, "error deleting host forwarding rule from default gateway: %w")

			// Delete hyperloop proxy redirect rule
			err = tables.Delete(
				"nat", "PREROUTING", "-i", vethName,
				"-p", "tcp", "-d", s.config.OrchestratorInSandboxIPAddress, "--dport", "80",
				"-j", "REDIRECT", "--to-port", s.hyperloopPort,
			)
			appendUnlessExpectedAbsentf(&errs, err, isIPTablesNotExist, "error deleting sandbox hyperloop proxy redirect rule: %w")

			// Delete NFS proxy redirect rule
			err = tables.Delete("nat", "PREROUTING",
				"--in-interface", vethName, "--protocol", "tcp",
				"--destination", s.config.OrchestratorInSandboxIPAddress, "--dport", "2049",
				"--jump", "REDIRECT", "--to-port", strconv.Itoa(int(s.config.NFSProxyPort)),
			)
			appendUnlessExpectedAbsentf(&errs, err, isIPTablesNotExist, "error deleting sandbox NFS proxy redirect rule: %w")

			// Delete portmapper redirect rule
			err = tables.Delete("nat", "PREROUTING",
				"--in-interface", vethName, "--protocol", "tcp",
				"--destination", s.config.OrchestratorInSandboxIPAddress, "--dport", "111",
				"--jump", "REDIRECT", "--to-port", strconv.Itoa(int(s.config.PortmapperPort)),
			)
			appendUnlessExpectedAbsentf(&errs, err, isIPTablesNotExist, "error deleting sandbox portmapper redirect rule: %w")
		}

		// Delete host postrouting rules
		err = tables.Delete("nat", "POSTROUTING", "-s", s.HostCIDR(), "-o", defaultGateway, "-j", "MASQUERADE")
		appendUnlessExpectedAbsentf(&errs, err, isIPTablesNotExist, "error deleting host postrouting rule: %w")

		// Delete changes made by egress proxy
		err = s.egressProxy.OnSlotDelete(s, tables)
		appendUnlessExpectedAbsentf(&errs, err, isIPTablesNotExist, "%w")
	}

	// Delete routing from host to FC namespace
	err = netlink.RouteDel(&netlink.Route{
		Gw:  s.VpeerIP(),
		Dst: s.HostNet(),
	})
	appendUnlessExpectedAbsentf(&errs, err, isRouteNotExist, "error deleting route from host to FC: %w")

	// Delete veth device
	// We explicitly delete the veth device from the host namespace because even though deleting
	// is deleting the device there may be a race condition when creating a new veth device with
	// the same name immediately after deleting the namespace.
	for _, vethName := range s.TeardownVethNames() {
		veth, err := netlink.LinkByName(vethName)
		if err != nil {
			appendUnlessExpectedAbsentf(&errs, err, isLinkNotExist, "error finding veth: %w")
		} else {
			err = netlink.LinkDel(veth)
			appendUnlessExpectedAbsentf(&errs, err, isLinkNotExist, "error deleting veth device: %w")
		}
	}

	// Delete the named namespace only after every host-side (root namespace)
	// teardown above has succeeded. The /run/netns entry is the anchor that
	// startup reclaim uses to rediscover a leaked slot, and the host-side
	// iptables/route/veth state removed above is keyed by the slot index. If any
	// of that teardown failed, deleting the namespace now would orphan the
	// remaining state with no way to rediscover and retry it. Preserving the
	// anchor lets the next teardown attempt (including startup reclaim) finish
	// the job. CreateNetwork removes a stale anchor before reusing the slot.
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	err = DeleteNamespace(s.NamespaceID())
	appendUnlessExpectedAbsentf(&errs, err, isNamespaceNotExist, "error deleting namespace: %w")

	return errors.Join(errs...)
}
