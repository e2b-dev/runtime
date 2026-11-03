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
		if hasRoutingHeaders(r.Header) && shouldParseHeaders(r.Host) {
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
	hostname := requestHostname(host)
	_, sharedHost := sharedHostDomain(hostname)

	return sharedHost || isLocalHostname(hostname)
}

func requestHostname(host string) string {
	return (&url.URL{Host: host}).Hostname()
}

func isLocalHostname(hostname string) bool {
	// An IP address cannot encode sandbox routing info (like {port}-{sandboxId}.{domain}),
	// so header-based routing is the only mechanism that can work for IP hosts.
	return hostname == "localhost" || net.ParseIP(hostname) != nil
}

func SandboxSharedHostDomain(host string) (string, bool) {
	return sharedHostDomain(requestHostname(host))
}

func sharedHostDomain(hostname string) (string, bool) {
	domain, ok := strings.CutPrefix(hostname, sandboxSharedHostSubdomain)

	return domain, ok && domain != ""
}

// MatchSandboxHost reports whether host addresses sandboxID and port, read as
// GetTargetFromRequest reads it: <port>-<id>.<domain>, or sandbox.<domain>
// with the routing headers in h. On a match it returns the host's domain, the
// part after the sandbox label, as written; otherwise it returns "" and false.
func MatchSandboxHost(host string, h http.Header, sandboxID string, port uint64) (domain string, ok bool) {
	hostname := requestHostname(host)

	var hostID string
	var hostPort uint64
	var err error
	if domain, ok = sharedHostDomain(hostname); ok {
		hostID, hostPort, ok, err = parseHeaders(h)
	} else {
		_, domain, ok = strings.Cut(hostname, ".")
		hostID, hostPort, err = parseHost(host)
	}

	if !ok || err != nil || hostID != sandboxID || hostPort != port {
		return "", false
	}

	return domain, true
}

func hasRoutingHeaders(h http.Header) bool {
	return h.Get(headerSandboxID) != "" || h.Get(headerSandboxPort) != ""
}

func parseHost(host string) (sandboxID string, port uint64, err error) {
	dot := strings.Index(host, ".")

	// There must be always domain part used
	if dot == -1 {
		return "", 0, ErrInvalidHost
	}

	// Keep only the left-most subdomain part, i.e. everything before the
	host = host[:dot]

	sandboxPortString, rest, found := strings.Cut(host, "-")
	if !found {
		return "", 0, ErrInvalidHost
	}

	sandboxID, _, _ = strings.Cut(rest, "-")

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
