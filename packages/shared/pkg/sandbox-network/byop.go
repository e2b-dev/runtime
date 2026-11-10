package sandbox_network

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/e2b-dev/infra/packages/shared/pkg/env"
)

// DevAllowedProxyEndpointCIDRs is a dev-only allowlist that overrides
// DeniedSandboxCIDRs at BYOP validation time. Populated from
// BYOP_DEV_ALLOWED_PROXY_CIDRS (comma-separated CIDRs) only when running in
// a development build; ignored in production so a stray env var cannot
// widen the BYOP endpoint allowlist.
var DevAllowedProxyEndpointCIDRs = func() []string {
	if !env.IsDevelopment() {
		return nil
	}

	return parseCIDRsEnv("BYOP_DEV_ALLOWED_PROXY_CIDRS")
}()

// parsedDevAllowedProxyEndpointCIDRs is DevAllowedProxyEndpointCIDRs pre-parsed
// for IsIPDevAllowedAsProxyEndpoint.
var parsedDevAllowedProxyEndpointCIDRs = func() []*net.IPNet {
	out := make([]*net.IPNet, 0, len(DevAllowedProxyEndpointCIDRs))
	for _, c := range DevAllowedProxyEndpointCIDRs {
		_, ipNet, err := net.ParseCIDR(c)
		if err != nil {
			continue
		}
		out = append(out, ipNet)
	}

	return out
}()

func parseCIDRsEnv(name string) []string {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, _, err := net.ParseCIDR(p); err != nil {
			panic(fmt.Sprintf("sandbox_network: invalid CIDR in env var %s: %q: %v", name, p, err))
		}
		out = append(out, p)
	}

	return out
}

// IsIPDevAllowedAsProxyEndpoint reports whether ip falls into any CIDR in
// DevAllowedProxyEndpointCIDRs.
func IsIPDevAllowedAsProxyEndpoint(ip net.IP) bool {
	for _, ipNet := range parsedDevAllowedProxyEndpointCIDRs {
		if ipNet.Contains(ip) {
			return true
		}
	}

	return false
}

// EgressProxyConfig is a transport-agnostic view of a BYOP SOCKS5 proxy
// configuration. Mirrors EgressProxy{Address,Username,Password,TLS} on
// db/pkg/types.SandboxNetworkEgressConfig.
type EgressProxyConfig struct {
	Address  string
	Username string
	Password string
	TLS      *EgressProxyTLSConfig
}

// EgressProxyTLSConfig configures TLS on the hop to the proxy. CACert carries
// PEM inline: a certificate is public material.
type EgressProxyTLSConfig struct {
	Enabled    bool
	ServerName string
	CACert     string
}

// maxSOCKS5CredentialLen is the maximum byte length of a SOCKS5
// username or password, see RFC 1929.
const maxSOCKS5CredentialLen = 255

// maxServerNameLen is the maximum length of a DNS name (RFC 1035).
const maxServerNameLen = 253

// maxCACertLen bounds the inline PEM bundle, which rides on every sandbox
// create and is stored with the paused sandbox. 8 KiB holds a chain several
// certificates deep.
const maxCACertLen = 8192

// ErrInvalidCACertificates is returned when a CA bundle is not one or more PEM
// certificates that all parse.
var ErrInvalidCACertificates = errors.New("egress proxy caCert must be one or more PEM certificates")

// ParseCACertPool builds a verification pool from a PEM bundle. Text outside
// the blocks is allowed, as CA bundles carry comments between certificates.
// Like x509.CertPool.AppendCertsFromPEM, it skips a block pem.Decode cannot
// read, so a truncated or corrupted certificate fails at the handshake rather
// than here. The admission check and the dialer share it, so a bundle
// accepted at create time is one the dialer can use.
func ParseCACertPool(caCert string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	certs := 0

	for rest := []byte(caCert); ; {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}

		if block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("%w: found a %q block", ErrInvalidCACertificates, block.Type)
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("%w: certificate %d: %w", ErrInvalidCACertificates, certs+1, err)
		}

		pool.AddCert(cert)
		certs++
	}

	if certs == 0 {
		return nil, fmt.Errorf("%w: no certificate found", ErrInvalidCACertificates)
	}

	return pool, nil
}

// validateEgressProxyTLS checks the TLS block and returns a canonical copy.
func validateEgressProxyTLS(cfg *EgressProxyTLSConfig) (*EgressProxyTLSConfig, error) {
	if cfg == nil {
		return nil, nil
	}

	serverName := strings.TrimSpace(strings.ToLower(cfg.ServerName))
	caCert := strings.TrimSpace(cfg.CACert)

	// A disabled block means the same thing as no block.
	if !cfg.Enabled {
		if serverName != "" || caCert != "" {
			return nil, errors.New("egress proxy tls serverName and caCert must be empty when tls is not enabled")
		}

		return nil, nil
	}

	if len(serverName) > maxServerNameLen {
		return nil, fmt.Errorf("egress proxy tls serverName must not exceed %d bytes", maxServerNameLen)
	}
	if len(caCert) > maxCACertLen {
		return nil, fmt.Errorf("egress proxy tls caCert must not exceed %d bytes", maxCACertLen)
	}
	if caCert != "" {
		if _, err := ParseCACertPool(caCert); err != nil {
			return nil, err
		}
	}

	return &EgressProxyTLSConfig{
		Enabled:    true,
		ServerName: serverName,
		CACert:     caCert,
	}, nil
}

// ErrEgressProxyInternalEndpoint is returned when a configured BYOP endpoint
// resolves to an IP in DeniedSandboxCIDRs.
var ErrEgressProxyInternalEndpoint = errors.New("egress proxy endpoint resolves to an internal / denied IP range")

// HostResolver resolves a hostname to one or more IPs. IP literals are
// returned without touching DNS. Implementations must honor ctx.
type HostResolver func(ctx context.Context, host string) ([]net.IP, error)

// DefaultHostResolver uses net.DefaultResolver.LookupIPAddr and honors ctx.
func DefaultHostResolver(ctx context.Context, host string) ([]net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}, nil
	}
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no A/AAAA records for %q", host)
	}
	ips := make([]net.IP, len(addrs))
	for i, a := range addrs {
		ips[i] = a.IP
	}

	return ips, nil
}

// ValidateEgressProxy checks a BYOP SOCKS5 config and rejects configurations
// that would expose E2B infrastructure or are malformed. On success it returns
// a canonical copy (host lower-cased, whitespace trimmed); the input is not
// mutated. cfg == nil returns (nil, nil). resolve defaults to DefaultHostResolver.
//
// Rules:
//   - Address must parse as "host:port" with a non-zero port.
//   - Host must be an IP literal or resolve via the provided resolver.
//   - Every resolved A/AAAA record must NOT be in DeniedSandboxCIDRs.
//   - If Username == "" then Password must also be "" (no orphan password).
//   - Username and Password are each capped at 255 bytes (RFC 1929).
//   - A TLS block that is not enabled carries no other field, and an enabled
//     one has a parseable CA bundle within the size caps.
func ValidateEgressProxy(ctx context.Context, cfg *EgressProxyConfig, resolve HostResolver) (*EgressProxyConfig, error) {
	if cfg == nil {
		return nil, nil
	}
	if resolve == nil {
		resolve = DefaultHostResolver
	}

	address := strings.TrimSpace(cfg.Address)
	if address == "" {
		return nil, errors.New("egress proxy address must not be empty")
	}

	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("egress proxy address must be in host:port form: %w", err)
	}
	host = strings.TrimSpace(strings.ToLower(host))
	if host == "" {
		return nil, errors.New("egress proxy host must not be empty")
	}
	portNum, err := strconv.Atoi(port)
	if err != nil || portNum <= 0 || portNum > 65535 {
		return nil, fmt.Errorf("egress proxy port %q is not a valid 1-65535 value", port)
	}

	// Resolve and ensure no resolved IP falls in the denied set.
	ips, err := resolve(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve egress proxy host %q: %w", host, err)
	}
	for _, ip := range ips {
		if IsIPInDeniedSandboxCIDRs(ip) && !IsIPDevAllowedAsProxyEndpoint(ip) {
			return nil, fmt.Errorf("%w: %s resolves to %s", ErrEgressProxyInternalEndpoint, host, ip)
		}
	}

	username := cfg.Username
	password := cfg.Password
	if username == "" && password != "" {
		return nil, errors.New("egress proxy password must be empty when username is empty")
	}
	if len(username) > maxSOCKS5CredentialLen {
		return nil, fmt.Errorf("egress proxy username must not exceed %d bytes", maxSOCKS5CredentialLen)
	}
	if len(password) > maxSOCKS5CredentialLen {
		return nil, fmt.Errorf("egress proxy password must not exceed %d bytes", maxSOCKS5CredentialLen)
	}

	tlsConfig, err := validateEgressProxyTLS(cfg.TLS)
	if err != nil {
		return nil, err
	}

	return &EgressProxyConfig{
		Address:  net.JoinHostPort(host, strconv.Itoa(portNum)),
		Username: username,
		Password: password,
		TLS:      tlsConfig,
	}, nil
}

// thisNetworkCIDR is the IPv4 "this network" block (RFC 1122, 0.0.0.0/8).
var thisNetworkCIDR = func() *net.IPNet {
	_, ipNet, err := net.ParseCIDR("0.0.0.0/8")
	if err != nil {
		panic(fmt.Sprintf("sandbox_network: invalid CIDR in thisNetworkCIDR: %v", err))
	}

	return ipNet
}()

// IsIPInDeniedSandboxCIDRs reports whether ip must be denied as a BYOP egress
// proxy endpoint: the unspecified addresses, "this network" block
// (0.0.0.0/8) or any IP in DeniedSandboxCIDRs. 0.0.0.0/8 is checked here
// because it cannot be encoded into the kernel nftables denylist.
func IsIPInDeniedSandboxCIDRs(ip net.IP) bool {
	if ip == nil || ip.IsUnspecified() || thisNetworkCIDR.Contains(ip) {
		return true
	}

	for _, ipNet := range parsedDeniedSandboxCIDRs {
		if ipNet.Contains(ip) {
			return true
		}
	}

	return false
}
