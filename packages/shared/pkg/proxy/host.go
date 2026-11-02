package proxy

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/e2b-dev/infra/packages/shared/pkg/id"
)

const sandboxSharedHostSubdomain = "sandbox."

func GetTargetFromRequest() func(r *http.Request) (sandboxId string, port uint64, err error) {
	return func(r *http.Request) (sandboxId string, port uint64, err error) {
		if shouldParseHeaders(r.Host) && hasRoutingHeaders(r.Header) {
			var ok bool
			sandboxId, port, ok, err = parseHeaders(r.Header)
			if err != nil {
				return "", 0, err
			} else if ok {
				if err := id.ValidateSandboxID(sandboxId); err != nil {
					return "", 0, ErrInvalidSandboxID
				}

				return sandboxId, port, nil
			}
		}

		sandboxId, port, err = parseHost(r.Host)
		if err != nil {
			return "", 0, err
		}

		if err := id.ValidateSandboxID(sandboxId); err != nil {
			return "", 0, ErrInvalidSandboxID
		}

		return sandboxId, port, nil
	}
}

func shouldParseHeaders(host string) bool {
	_, sharedHost := SandboxSharedHostDomain(host)

	return isLocalRequestHost(host) || sharedHost
}

func requestHostname(host string) string {
	return (&url.URL{Host: host}).Hostname()
}

func isLocalRequestHost(host string) bool {
	host = requestHostname(host)
	ip := net.ParseIP(host)

	// An IP address cannot encode sandbox routing info (like {port}-{sandboxId}.{domain}),
	// so header-based routing is the only mechanism that can work for IP hosts.
	return host == "localhost" || ip != nil
}

func SandboxSharedHostDomain(host string) (string, bool) {
	domain, ok := strings.CutPrefix(requestHostname(host), sandboxSharedHostSubdomain)

	return domain, ok && domain != ""
}

func hasRoutingHeaders(h http.Header) bool {
	return h.Get(headerSandboxID) != "" || h.Get(headerSandboxPort) != ""
}

// expectedUnroutableHost reports whether an ErrInvalidHost for this request is
// the designed rejection of a host that can never carry sandbox routing: a bare
// IP or localhost (isLocalRequestHost) that also provides no routing headers.
// Such requests arrive continuously from external scanners, health probes and
// stale clients on a public endpoint, so callers log them at Debug not Warn.
func expectedUnroutableHost(host string, header http.Header) bool {
	return isLocalRequestHost(host) && !hasRoutingHeaders(header)
}

func parseHost(host string) (sandboxID string, port uint64, err error) {
	dot := strings.Index(host, ".")

	// There must be always domain part used
	if dot == -1 {
		return "", 0, ErrInvalidHost
	}

	// Keep only the left-most subdomain part, i.e. everything before the
	host = host[:dot]

	hostParts := strings.Split(host, "-")
	if len(hostParts) < 2 {
		return "", 0, ErrInvalidHost
	}

	sandboxPortString := hostParts[0]
	sandboxID = hostParts[1]

	sandboxPort, err := parsePort(sandboxPortString)
	if err != nil {
		return "", 0, err
	}

	return sandboxID, sandboxPort, nil
}

type MissingHeaderError struct {
	Header string
}

func (e MissingHeaderError) Error() string {
	return fmt.Sprintf("Missing header: %s", e.Header)
}

const (
	headerSandboxID   = "E2b-Sandbox-Id"
	headerSandboxPort = "E2b-Sandbox-Port"
)

func parseHeaders(h http.Header) (sandboxID string, port uint64, ok bool, err error) {
	sandboxID = h.Get(headerSandboxID)
	portString := h.Get(headerSandboxPort)

	if sandboxID == "" && portString == "" {
		return "", 0, false, nil
	}

	if sandboxID == "" {
		return "", 0, false, MissingHeaderError{Header: headerSandboxID}
	}

	if portString == "" {
		return "", 0, false, MissingHeaderError{Header: headerSandboxPort}
	}

	port, err = parsePort(portString)
	if err != nil {
		return "", 0, false, err
	}

	return sandboxID, port, true, nil
}

func parsePort(s string) (uint64, error) {
	port, err := strconv.ParseUint(s, 10, 16)
	if err != nil {
		return 0, InvalidSandboxPortError{s, err}
	}

	if port == 0 {
		return 0, InvalidSandboxPortError{s, errors.New("port must not be zero")}
	}

	return port, nil
}