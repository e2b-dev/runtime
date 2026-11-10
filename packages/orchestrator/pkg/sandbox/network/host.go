//go:build linux

package network

import (
	"context"
	"errors"
	"fmt"

	"github.com/vishvananda/netlink"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// Host loopback interface name
const loopbackInterface = "lo"

// Host default gateway name
var defaultGateway = utils.Must(getDefaultGateway(context.Background()))

// DefaultGateway returns the detected host default gateway interface name.
func DefaultGateway() string { return defaultGateway }

//	func getDefaultGateway() (string, error) {
//		route, err := exec.Command(
//			"sh",
//			"-c",
//			"ip route show default | awk '{print $5}'",
//		).Output()
//		if err != nil {
//			return "", fmt.Errorf("error fetching default gateway: %w", err)
//		}
//
//		return string(route), nil
//	}
func getDefaultGateway(ctx context.Context) (string, error) {
	routes, err := netlink.RouteList(nil, netlink.FAMILY_ALL)
	if err != nil {
		return "", fmt.Errorf("error fetching routes: %w", err)
	}

	for _, route := range routes {
		if route.Gw == nil || !isDefaultRoute(route) {
			continue
		}

		logger.L().Info(ctx, "default gateway", zap.String("gateway", route.Gw.String()))

		link, linkErr := netlink.LinkByIndex(route.LinkIndex)

		if linkErr != nil {
			return "", fmt.Errorf("error fetching interface for default gateway: %w", linkErr)
		}

		return link.Attrs().Name, nil
	}

	return "", errors.New("cannot find default gateway")
}

// isDefaultRoute reports whether route is an IPv4 default route (0.0.0.0/0).
//
// The kernel represents it in one of two ways depending on kernel/netlink
// version: with Dst == nil (RTA_DST omitted) or with an explicit 0.0.0.0/0 Dst.
// Matching on the zero-length prefix accepts both. (net.IPNet.String() is
// nil-safe and returns "<nil>", so the previous route.Dst.String() check never
// panicked — it silently failed to match the nil form.)
//
// Restricted to IPv4 to preserve the original behaviour; route.Family is set
// even when Dst is nil, so it is the reliable family discriminator here.
func isDefaultRoute(route netlink.Route) bool {
	if route.Family != netlink.FAMILY_V4 {
		return false
	}
	if route.Dst == nil {
		return true
	}
	ones, _ := route.Dst.Mask.Size()
	return ones == 0
}
