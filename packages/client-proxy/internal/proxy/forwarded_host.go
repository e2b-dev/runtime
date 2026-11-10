package proxy

import (
	"net/http"
	"slices"
	"strings"

	reverseproxy "github.com/e2b-dev/infra/packages/shared/pkg/proxy"
)

const headerForwardedHost = "X-Forwarded-Host"

// restoreForwardedHost lets a proxy in front of this one address the sandbox
// by this plane's host while the sandbox still sees the host its client used:
// when X-Forwarded-Host is a single host under one of domains that names the
// same sandbox and port as the request, it becomes the request's Host and the
// header is dropped. Anything else is left as it came. The header comes from
// the caller, so the same-sandbox check is what keeps it from setting an
// arbitrary Host; at most it picks another name of the same sandbox.
func restoreForwardedHost(r *http.Request, sandboxID string, port uint64, domains []string) {
	if len(domains) == 0 {
		return
	}

	// net/http canonicalizes header names, so the map is read directly.
	values := r.Header[headerForwardedHost]
	if len(values) != 1 || strings.Contains(values[0], ",") {
		return
	}
	forwarded := strings.TrimSpace(values[0])
	// Sandbox hosts are served on 443 only; any other port would only let a
	// caller put a made-up port in the Host the sandbox sees.
	if i := strings.LastIndexByte(forwarded, ':'); i >= 0 && forwarded[i+1:] != "443" {
		return
	}

	domain, ok := reverseproxy.MatchSandboxHost(forwarded, r.Header, sandboxID, port)
	if !ok {
		return
	}
	domain = strings.TrimSuffix(domain, ".")
	if !slices.ContainsFunc(domains, func(allowed string) bool { return strings.EqualFold(domain, allowed) }) {
		return
	}

	r.Host = forwarded
	delete(r.Header, headerForwardedHost)
}

// normalizeForwardedHostDomains drops empty entries, surrounding spaces and
// trailing dots, so they compare with a host's domain.
func normalizeForwardedHostDomains(domains []string) []string {
	normalized := make([]string, 0, len(domains))
	for _, domain := range domains {
		domain = strings.TrimSuffix(strings.TrimSpace(domain), ".")
		if domain != "" {
			normalized = append(normalized, domain)
		}
	}

	return normalized
}
