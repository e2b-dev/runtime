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
		// Default route has a nil Dst (representing 0.0.0.0/0).
		if route.Dst == nil && route.Gw != nil {
			logger.L().Info(ctx, "default gateway", zap.String("gateway", route.Gw.String()))

			link, linkErr := netlink.LinkByIndex(route.LinkIndex)

			if linkErr != nil {
				return "", fmt.Errorf("error fetching interface for default gateway: %w", linkErr)
			}

			return link.Attrs().Name, nil
		}
	}

	return "", errors.New("cannot find default gateway")
}
