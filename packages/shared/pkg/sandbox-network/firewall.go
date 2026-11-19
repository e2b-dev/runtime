package sandbox_network

import (
	"fmt"
	"net"
	"strings"
	"unicode/utf8"

	"github.com/miekg/dns"
	"github.com/ngrok/firewall_toolkit/pkg/set"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	AllInternetTrafficCIDR = "0.0.0.0/0"

	DefaultNameserver = "8.8.8.8"
)

var DeniedSandboxCIDRs = []string{
	// IPv4 private/local ranges
	"10.0.0.0/8",     // RFC 1918 private.
	"100.64.0.0/10",  // RFC 6598 CGNAT / shared address space; used by some cloud providers for internal services.
	"127.0.0.0/8",    // RFC 1122 loopback.
	"169.254.0.0/16", // RFC 3927 link-local (incl. cloud metadata 169.254.169.254).
	"172.16.0.0/12",  // RFC 1918 private.
	"192.168.0.0/16", // RFC 1918 private.

	"168.63.129.16/32", // Azure host endpoint: VM agent (WireServer, host plugin), platform DNS, health probes.

	// IPv6 local ranges
	"::1/128",   // RFC 4291 loopback.
	"fc00::/7",  // RFC 4193 unique local.
	"fe80::/10", // RFC 4291 link-local.
}

var DeniedSandboxSetData = utils.Must(set.AddressStringsToSetData(DeniedSandboxCIDRs))

// parsedDeniedSandboxCIDRs is DeniedSandboxCIDRs pre-parsed for
// IsIPInDeniedSandboxCIDRs.
var parsedDeniedSandboxCIDRs = func() []*net.IPNet {
	out := make([]*net.IPNet, 0, len(DeniedSandboxCIDRs))
	for _, c := range DeniedSandboxCIDRs {
		_, ipNet, err := net.ParseCIDR(c)
		if err != nil {
			panic(fmt.Sprintf("sandbox_network: invalid CIDR in DeniedSandboxCIDRs: %q: %v", c, err))
		}
		out = append(out, ipNet)
	}

	return out
}()

// AddressStringToCIDR converts a string address to the CIDR format.
// Supports only IPv4 addresses.
func AddressStringToCIDR(addressString string) string {
	if !strings.Contains(addressString, "/") {
		addressString += "/32"
	}

	return addressString
}

// AddressStringsToCIDRs converts a list of string addresses to the CIDR format.
// Supports only IPv4 addresses.
func AddressStringsToCIDRs(addressStrings []string) []string {
	data := make([]string, 0, len(addressStrings))

	for _, addressString := range addressStrings {
		data = append(data, AddressStringToCIDR(addressString))
	}

	return data
}

// IsIPOrCIDR checks if a string is a valid IP address or CIDR notation.
func IsIPOrCIDR(s string) bool {
	// Check if it's a valid IP address
	if ip := net.ParseIP(s); ip != nil {
		return true
	}

	// Check if it's a valid CIDR
	_, _, err := net.ParseCIDR(s)

	return err == nil
}

// IsSpecifiedIPOrCIDR checks if a string is a valid IP address or CIDR notation
// with a specified (non-zero) IP. It rejects unspecified addresses like 0.0.0.0
// or :: (which cause errors in nftables), but allows 0.0.0.0/0 as a special case.
func IsSpecifiedIPOrCIDR(s string) bool {
	if !IsIPOrCIDR(s) {
		return false
	}

	// Allow the special all-traffic CIDR
	if s == AllInternetTrafficCIDR {
		return true
	}

	// Extract the IP portion
	if ip := net.ParseIP(s); ip != nil {
		return !ip.IsUnspecified()
	}

	ip, _, err := net.ParseCIDR(s)
	if err != nil {
		return false
	}

	return !ip.IsUnspecified()
}

// ParseAddressesAndDomains separates a list of strings into IP addresses/CIDRs and domain names.
func ParseAddressesAndDomains(entries []string) (addresses []string, domains []string) {
	for _, entry := range entries {
		if IsIPOrCIDR(entry) {
			addresses = append(addresses, entry)
		} else {
			domains = append(domains, entry)
		}
	}

	return addresses, domains
}

// MatchDomainPattern reports whether hostname matches an exact domain, the
// all-domains wildcard, or a leading-label wildcard such as *.example.com.
func MatchDomainPattern(hostname, pattern string) bool {
	if hostname == "" || pattern == "" || strings.HasPrefix(hostname, ".") {
		return false
	}

	switch {
	case strings.EqualFold(pattern, hostname):
		return true
	case pattern == "*":
		return true
	case !strings.HasPrefix(pattern, "*."):
		return false
	}

	suffix := strings.TrimPrefix(pattern, "*.")
	if suffix == "" || strings.HasPrefix(suffix, ".") {
		return false
	}
	if strings.HasSuffix(hostname, ".") != strings.HasSuffix(suffix, ".") {
		return false
	}
	if _, ok := dns.IsDomainName(suffix); !ok {
		return false
	}
	if _, ok := dns.IsDomainName(hostname); !ok {
		return false
	}

	return !strings.EqualFold(hostname, suffix) && dns.IsSubDomain(suffix, hostname)
}

// IsValidWildcardDomainPattern reports whether pattern contains exactly one
// leading wildcard label followed by a DNS-1123 subdomain.
func IsValidWildcardDomainPattern(pattern string) bool {
	if len(pattern) > validation.DNS1123SubdomainMaxLength {
		return false
	}
	for i := range len(pattern) {
		if pattern[i] >= utf8.RuneSelf {
			return false
		}
	}

	suffix, ok := strings.CutPrefix(pattern, "*.")

	return ok && len(validation.IsDNS1123Subdomain(strings.ToLower(suffix))) == 0
}
