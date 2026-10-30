//go:build linux

package tcpfirewall

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/inetaf/tcpproxy"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	sandbox_network "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-network"
)

const (
	// upstreamDialTimeout is the maximum time to wait for upstream connections.
	// This prevents goroutine leaks from slow/unresponsive DNS or connections.
	upstreamDialTimeout = 30 * time.Second

	noHostnameValue = ""
)

// Sets both IP_TOS and IPV6_TCLASS because Happy Eyeballs may pick either family.
// Tolerates ENOPROTOOPT from the non-matching one.
func markDSCP(c syscall.RawConn, tos int) error {
	if tos == 0 {
		return nil
	}

	var sockErr error

	err := c.Control(func(fd uintptr) {
		v4Err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_TOS, tos)
		v6Err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, syscall.IPV6_TCLASS, tos)
		if v4Err != nil && v6Err != nil {
			sockErr = fmt.Errorf("setsockopt IP_TOS / IPV6_TCLASS both failed: %w", errors.Join(v4Err, v6Err))
		}
	})
	if err != nil {
		return err
	}

	return sockErr
}

// domainHandler handles connections with hostname information (HTTP Host header or TLS SNI).
func domainHandler(ctx context.Context, c egressConn) {
	// Get hostname from tcpproxy's wrapped connection (HTTP Host or TLS SNI).
	// Hostname can be empty, e.g. for https://1.1.1.1 like requests.
	var hostname string
	if tc, ok := c.conn.(*tcpproxy.Conn); ok {
		hostname = tc.HostName
	}

	allowed, matchType, err := isEgressAllowed(c.sbx, hostname, c.dstIP)
	if err != nil {
		c.logger.Error(ctx, "Egress check failed", zap.Error(err))
		c.metrics.RecordError(ctx, ErrorTypeEgressCheck, c.protocol)
		c.conn.Close()

		return
	}

	if !allowed {
		c.metrics.RecordDecision(ctx, DecisionBlocked, c.protocol, matchType)
		c.conn.Close()

		return
	}

	c.metrics.RecordDecision(ctx, DecisionAllowed, c.protocol, matchType)

	// When allowed by domain match, dial the hostname directly (not the sandbox's resolved IP).
	// This prevents DNS spoofing attacks where the sandbox modifies /etc/hosts to redirect
	// an allowed domain to an arbitrary IP. We use Go's net.Dialer which provides built-in
	// Happy Eyeballs (RFC 8305) for multi-IP fallback when some IPs are unreachable.
	// After connecting, we verify the connected IP is not internal/private.
	if matchType == MatchTypeDomain {
		proxyWithIPVerification(ctx, c, net.JoinHostPort(hostname, fmt.Sprintf("%d", c.dstPort)))

		return
	}

	// For non-domain matches, use the original destination IP
	proxy(ctx, c, c.upstreamAddr())
}

// cidrOnlyHandler handles connections without hostname information.
func cidrOnlyHandler(ctx context.Context, c egressConn) {
	// No hostname available for CIDR-only handler
	allowed, matchType, err := isEgressAllowed(c.sbx, noHostnameValue, c.dstIP)
	if err != nil {
		c.logger.Error(ctx, "Egress check failed", zap.Error(err))
		c.metrics.RecordError(ctx, ErrorTypeEgressCheck, c.protocol)
		c.conn.Close()

		return
	}

	if !allowed {
		c.metrics.RecordDecision(ctx, DecisionBlocked, c.protocol, matchType)
		c.conn.Close()

		return
	}

	c.metrics.RecordDecision(ctx, DecisionAllowed, c.protocol, matchType)

	proxy(ctx, c, c.upstreamAddr())
}

// proxy proxies the connection to the upstream address.
func proxy(ctx context.Context, c egressConn, upstreamAddr string) {
	proxyWithDial(ctx, c, upstreamAddr, upstreamDialer(ctx, c, upstreamAddr, false).DialContext)
}

// proxyWithIPVerification preserves Happy Eyeballs while rejecting resolved
// internal addresses before any TCP handshake, independently of guest DNS.
func proxyWithIPVerification(ctx context.Context, c egressConn, upstreamAddr string) {
	proxyWithDial(ctx, c, upstreamAddr, upstreamDialer(ctx, c, upstreamAddr, true).DialContext)
}

func upstreamDialer(ctx context.Context, c egressConn, upstreamAddr string, verifyIP bool) *net.Dialer {
	dialer := &net.Dialer{Timeout: upstreamDialTimeout}
	if !verifyIP {
		dialer.Control = func(_, _ string, rawConn syscall.RawConn) error {
			return markDSCP(rawConn, c.tos)
		}

		return dialer
	}
	dialer.ControlContext = func(_ context.Context, _, address string, rawConn syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("failed to parse resolved address %q: %w", address, err)
		}
		resolvedIP := net.ParseIP(host)
		if resolvedIP == nil {
			return fmt.Errorf("failed to parse IP from resolved address %q", host)
		}
		if isIPInAlwaysDeniedCIDRs(resolvedIP) {
			c.logger.Warn(ctx, "Blocked connection to internal IP via hostname",
				zap.String("upstream_addr", upstreamAddr),
				zap.String("resolved_ip", resolvedIP.String()))
			c.metrics.RecordError(ctx, ErrorTypeResolvedIPBlocked, c.protocol)

			return fmt.Errorf("hostname resolved to internal IP %s", resolvedIP)
		}

		return markDSCP(rawConn, c.tos)
	}

	return dialer
}

// proxyWithDial owns both copy directions for either raw-socket dial policy.
// Policy and DSCP retain their raw-socket dial ordering.
func proxyWithDial(ctx context.Context, c egressConn, upstreamAddr string, dial func(context.Context, string, string) (net.Conn, error)) {
	tracker := c.metrics.TrackConnection(c.protocol)
	defer tracker.Close(ctx)

	dp := &tcpproxy.DialProxy{
		Addr:        upstreamAddr,
		DialTimeout: upstreamDialTimeout,
		DialContext: dial,
	}
	dp.HandleConn(c.conn)
}

// isEgressAllowed checks if egress is allowed based on domain and CIDR rules.
// Returns the allowed status and the match type for metrics.
// Priority order:
//  1. Allow domain / Allow CIDR (if either matches → allow)
//  2. Deny domain / Deny CIDR (if either matches → deny)
//  3. Default: allow, or deny when the egress config opts into default_deny
func isEgressAllowed(sbx *sandbox.Sandbox, hostname string, ip net.IP) (bool, MatchType, error) {
	egress := sbx.Config.GetNetworkEgress()
	if egress == nil {
		// No egress configuration, allow all traffic.
		return true, MatchTypeNone, nil
	}

	// Priority 1: Check allowed domains
	if hostname != noHostnameValue {
		for _, domain := range egress.GetAllowedDomains() {
			if matchDomain(hostname, domain) {
				return true, MatchTypeDomain, nil // Explicitly allowed by domain
			}
		}
	}

	// Priority 1: Check allowed CIDRs
	for _, cidr := range egress.GetAllowedCidrs() {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			return false, MatchTypeNone, fmt.Errorf("invalid allowed CIDR %q: %w", cidr, err)
		}

		if ipNet.Contains(ip) {
			return true, MatchTypeCIDR, nil // Explicitly allowed by CIDR
		}
	}

	// Priority 2: Check denied domains
	if hostname != noHostnameValue {
		for _, domain := range egress.GetDeniedDomains() {
			if matchDomain(hostname, domain) {
				return false, MatchTypeDomain, nil // Explicitly denied by domain
			}
		}
	}

	// Priority 2: Check denied CIDRs
	for _, cidr := range egress.GetDeniedCidrs() {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			return false, MatchTypeNone, fmt.Errorf("invalid denied CIDR %q: %w", cidr, err)
		}

		if ipNet.Contains(ip) {
			return false, MatchTypeCIDR, nil // Blocked by CIDR
		}
	}

	// Priority 3: default posture. Under opt-in default-deny, anything not
	// explicitly allowed above is dropped — this also closes the no-SNI / no-Host
	// blind pass (cidrOnlyHandler reaches here with no hostname, so only an
	// allowed CIDR can let it through). Default-allow is the historical behaviour.
	if egress.GetDefaultDeny() {
		return false, MatchTypeNone, nil
	}

	return true, MatchTypeNone, nil
}

// matchDomain checks if a hostname matches a domain pattern.
// Patterns can be exact matches, wildcards (*), or suffix wildcards (*.example.com).
func matchDomain(hostname, pattern string) bool {
	switch {
	case pattern == "":
		// Empty pattern should never match
		return false
	case strings.EqualFold(pattern, hostname):
		return true
	case strings.EqualFold(pattern, "*"):
		return true
	case strings.HasPrefix(pattern, "*."):
		suffix := pattern[1:]
		if strings.HasSuffix(strings.ToLower(hostname), strings.ToLower(suffix)) {
			return true
		}
	}

	return false
}

// isIPInAlwaysDeniedCIDRs checks if an IP is within the denied sandbox CIDRs (internal/private ranges).
func isIPInAlwaysDeniedCIDRs(ip net.IP) bool {
	for _, cidr := range sandbox_network.DeniedSandboxCIDRs {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			continue
		}

		if ipNet.Contains(ip) {
			return true
		}
	}

	return false
}
