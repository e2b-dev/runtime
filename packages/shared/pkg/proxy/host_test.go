package proxy

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetTargetFromRequest(t *testing.T) {
	t.Parallel()

	getTargetFromRequest := GetTargetFromRequest()

	tests := []struct {
		name     string
		host     string
		headers  http.Header
		wantID   string
		wantPort uint64

		wantErrIs, wantErrAs error
	}{
		{
			name:      "sandbox-host-with-client-id",
			host:      "49983-isv6ril5xadwn1k9t2jye-6532622b.e2b.app",
			wantID:    "isv6ril5xadwn1k9t2jye",
			wantPort:  49983,
			wantErrIs: nil,
		},
		{
			name:      "sandbox-host-with-client-id-and-dash-domain",
			host:      "49983-isv6ril5xadwn1k9t2jye-6532622b.e2b-test.app",
			wantID:    "isv6ril5xadwn1k9t2jye",
			wantPort:  49983,
			wantErrIs: nil,
		},
		{
			name:      "sandbox-host-with-client-id-and-subdomain",
			host:      "49983-isv6ril5xadwn1k9t2jye-6532622b.demo.e2b.app",
			wantID:    "isv6ril5xadwn1k9t2jye",
			wantPort:  49983,
			wantErrIs: nil,
		},
		{
			name:      "sandbox-host-without-client-id",
			host:      "49983-isv6ril5xadwn1k9t2jye.e2b.app",
			wantID:    "isv6ril5xadwn1k9t2jye",
			wantPort:  49983,
			wantErrIs: nil,
		},
		{
			name:      "sandbox-host-with-dash-domain-and-without-client-id",
			host:      "49983-isv6ril5xadwn1k9t2jye.e2b-test.app",
			wantID:    "isv6ril5xadwn1k9t2jye",
			wantPort:  49983,
			wantErrIs: nil,
		},
		{
			name:      "sandbox-host-with-subdomain-and-without-client-id",
			host:      "49983-isv6ril5xadwn1k9t2jye.demo.e2b.app",
			wantID:    "isv6ril5xadwn1k9t2jye",
			wantPort:  49983,
			wantErrIs: nil,
		},
		{
			name:      "sandbox-host-without-port-part",
			host:      "isv6ril5xadwn1k9t2jye.e2b.app",
			wantID:    "isv6ril5xadwn1k9t2jye",
			wantPort:  49983,
			wantErrIs: ErrInvalidHost,
		},
		{
			name:      "sandbox-host-with-invalid-port-part",
			host:      "abcd-isv6ril5xadwn1k9t2jye.e2b.app",
			wantID:    "isv6ril5xadwn1k9t2jye",
			wantPort:  49983,
			wantErrAs: InvalidSandboxPortError{},
		},
		{
			name:      "sandbox-host-with-zero-port",
			host:      "0-isv6ril5xadwn1k9t2jye.e2b.app",
			wantErrAs: InvalidSandboxPortError{},
		},
		{
			name:      "sandbox-host-with-port-above-max",
			host:      "65536-isv6ril5xadwn1k9t2jye.e2b.app",
			wantErrAs: InvalidSandboxPortError{},
		},
		{
			name:     "sandbox-host-with-max-port",
			host:     "65535-isv6ril5xadwn1k9t2jye.e2b.app",
			wantID:   "isv6ril5xadwn1k9t2jye",
			wantPort: 65535,
		},
		{
			name: "headers: zero port",
			host: "localhost:1234",
			headers: http.Header{
				headerSandboxID:   []string{"isv6ril5xadwn1k9t2jye"},
				headerSandboxPort: []string{"0"},
			},
			wantErrAs: InvalidSandboxPortError{},
		},
		{
			name: "headers: port above max",
			host: "localhost:1234",
			headers: http.Header{
				headerSandboxID:   []string{"isv6ril5xadwn1k9t2jye"},
				headerSandboxPort: []string{"70000"},
			},
			wantErrAs: InvalidSandboxPortError{},
		},
		{
			name:      "sandbox-host-without-domain",
			host:      "49983-isv6ril5xadwn1k9t2jye",
			wantID:    "isv6ril5xadwn1k9t2jye",
			wantPort:  49983,
			wantErrIs: ErrInvalidHost,
		},
		{
			name:      "sandbox-host-with-missing-domain-and-port",
			host:      "49983-isv6ril5xadwn1k9t2jye:8080",
			wantID:    "isv6ril5xadwn1k9t2jye",
			wantPort:  49983,
			wantErrIs: ErrInvalidHost,
		},
		{
			name:      "sandbox-host-with-missing-domain",
			host:      "49983-isv6ril5xadwn1k9t2jye",
			wantID:    "isv6ril5xadwn1k9t2jye",
			wantPort:  49983,
			wantErrIs: ErrInvalidHost,
		},
		{
			name: "headers: happy path",
			host: "localhost:1234",
			headers: http.Header{
				headerSandboxID:   []string{"isv6ril5xadwn1k9t2jye"},
				headerSandboxPort: []string{"8080"},
			},
			wantID:   "isv6ril5xadwn1k9t2jye",
			wantPort: 8080,
		},
		{
			name: "headers: loopback IPv4",
			host: "127.0.0.1:1234",
			headers: http.Header{
				headerSandboxID:   []string{"isv6ril5xadwn1k9t2jye"},
				headerSandboxPort: []string{"8080"},
			},
			wantID:   "isv6ril5xadwn1k9t2jye",
			wantPort: 8080,
		},
		{
			name: "headers: loopback IPv6",
			host: "[::1]:1234",
			headers: http.Header{
				headerSandboxID:   []string{"isv6ril5xadwn1k9t2jye"},
				headerSandboxPort: []string{"8080"},
			},
			wantID:   "isv6ril5xadwn1k9t2jye",
			wantPort: 8080,
		},
		{
			name: "headers: invalid sandbox id with colon",
			host: "localhost:1234",
			headers: http.Header{
				headerSandboxID:   []string{"sbx:123"},
				headerSandboxPort: []string{"8080"},
			},
			wantErrIs: ErrInvalidSandboxID,
		},
		{
			name: "headers: invalid sandbox id with uppercase",
			host: "localhost:1234",
			headers: http.Header{
				headerSandboxID:   []string{"SBX_UPPER"},
				headerSandboxPort: []string{"8080"},
			},
			wantErrIs: ErrInvalidSandboxID,
		},
		{
			name: "headers: missing sandbox id",
			host: "localhost:1234",
			headers: http.Header{
				headerSandboxPort: []string{"8080"},
			},
			wantErrIs: MissingHeaderError{Header: headerSandboxID},
		},
		{
			name: "headers: missing sandbox port",
			host: "localhost:1234",
			headers: http.Header{
				headerSandboxID: []string{"isv6ril5xadwn1k9t2jye"},
			},
			wantErrIs: MissingHeaderError{Header: headerSandboxPort},
		},
		{
			name: "headers: sandbox shared host",
			host: "sandbox.e2b.app",
			headers: http.Header{
				headerSandboxID:   []string{"isv6ril5xadwn1k9t2jye"},
				headerSandboxPort: []string{"49983"},
			},
			wantID:   "isv6ril5xadwn1k9t2jye",
			wantPort: 49983,
		},
		{
			name: "headers: private IPv4 (bridge/LAN)",
			host: "192.168.100.66:3002",
			headers: http.Header{
				headerSandboxID:   []string{"isv6ril5xadwn1k9t2jye"},
				headerSandboxPort: []string{"49983"},
			},
			wantID:   "isv6ril5xadwn1k9t2jye",
			wantPort: 49983,
		},
		{
			name: "headers: private IPv4 (10.x network)",
			host: "10.0.0.5:3002",
			headers: http.Header{
				headerSandboxID:   []string{"isv6ril5xadwn1k9t2jye"},
				headerSandboxPort: []string{"49983"},
			},
			wantID:   "isv6ril5xadwn1k9t2jye",
			wantPort: 49983,
		},
		{
			name: "headers: public IPv4",
			host: "34.120.5.10:3002",
			headers: http.Header{
				headerSandboxID:   []string{"isv6ril5xadwn1k9t2jye"},
				headerSandboxPort: []string{"49983"},
			},
			wantID:   "isv6ril5xadwn1k9t2jye",
			wantPort: 49983,
		},
		{
			name: "headers: ignored on regular sandbox host",
			host: "49983-isv6ril5xadwn1k9t2jye.e2b.app",
			headers: http.Header{
				headerSandboxID:   []string{"iother5b5aiixd410phsjv"},
				headerSandboxPort: []string{"3000"},
			},
			wantID:   "isv6ril5xadwn1k9t2jye",
			wantPort: 49983,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := &http.Request{
				Host:   tt.host,
				Header: tt.headers,
			}
			gotID, gotPort, err := getTargetFromRequest(req)

			// Compare error presence and, when present, the concrete type.
			if (err != nil) != (tt.wantErrIs != nil || tt.wantErrAs != nil) {
				t.Fatalf("ParseHost(%q) error = %v, wantErr %v", tt.host, err, tt.wantErrIs)
			}

			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)

				return // no further checks when an error was expected
			}

			if tt.wantErrAs != nil {
				_, ok := errors.AsType[InvalidSandboxPortError](err)
				require.True(t, ok, "error %v is not an InvalidSandboxPortError", err)

				return // no further checks when an error was expected
			}

			if gotID != tt.wantID {
				t.Errorf("ParseHost(%q) sandboxID = %q, want %q", tt.host, gotID, tt.wantID)
			}

			if gotPort != tt.wantPort {
				t.Errorf("ParseHost(%q) port = %d, want %d", tt.host, gotPort, tt.wantPort)
			}
		})
	}
}

func TestMatchSandboxHost(t *testing.T) {
	t.Parallel()

	const (
		sandboxID = "isv6ril5xadwn1k9t2jye"
		otherID   = "iother5b5aiixd410phsjv"
	)
	routing := func(id, port string) http.Header {
		return http.Header{headerSandboxID: {id}, headerSandboxPort: {port}}
	}

	tests := []struct {
		name       string
		host       string
		headers    http.Header
		wantDomain string
		wantOK     bool
		// local marks hosts GetTargetFromRequest reads from the routing
		// headers alone; they have no domain, so MatchSandboxHost refuses them.
		local bool
	}{
		{name: "sandbox host", host: "3000-" + sandboxID + ".e2b.dev", wantDomain: "e2b.dev", wantOK: true},
		{name: "sandbox host with a client ID", host: "3000-" + sandboxID + "-6532622b.e2b.dev", wantDomain: "e2b.dev", wantOK: true},
		{name: "sandbox host under a subdomain", host: "3000-" + sandboxID + ".demo.e2b.app", wantDomain: "demo.e2b.app", wantOK: true},
		{name: "sandbox host with a port leaves the port out of the domain", host: "3000-" + sandboxID + ".e2b.dev:443", wantDomain: "e2b.dev", wantOK: true},
		{name: "domain is returned as written", host: "3000-" + sandboxID + ".E2B.dev.", wantDomain: "E2B.dev.", wantOK: true},
		{name: "routing headers do not override a sandbox host", host: "3000-" + sandboxID + ".e2b.dev", headers: routing(otherID, "8000"), wantDomain: "e2b.dev", wantOK: true},
		{name: "sandbox host naming another sandbox", host: "3000-" + otherID + ".e2b.dev"},
		{name: "sandbox host naming another port", host: "8000-" + sandboxID + ".e2b.dev"},
		{name: "sandbox host without a domain", host: "3000-" + sandboxID},
		{name: "sandbox host with an invalid port", host: "http-" + sandboxID + ".e2b.dev"},
		{name: "shared host with routing headers naming the sandbox", host: "sandbox.e2b.dev", headers: routing(sandboxID, "3000"), wantDomain: "e2b.dev", wantOK: true},
		{name: "shared host with a port", host: "sandbox.e2b.dev:443", headers: routing(sandboxID, "3000"), wantDomain: "e2b.dev", wantOK: true},
		{name: "shared host with routing headers naming another sandbox", host: "sandbox.e2b.dev", headers: routing(otherID, "3000")},
		{name: "shared host with routing headers naming another port", host: "sandbox.e2b.dev", headers: routing(sandboxID, "8000")},
		{name: "shared host without routing headers", host: "sandbox.e2b.dev"},
		{name: "shared host without the port header", host: "sandbox.e2b.dev", headers: http.Header{headerSandboxID: {sandboxID}}},
		{name: "shared host without a domain", host: "sandbox.", headers: routing(sandboxID, "3000")},
		{name: "localhost with routing headers", host: "localhost:3000", headers: routing(sandboxID, "3000"), local: true},
		{name: "IP address with routing headers", host: "127.0.0.1", headers: routing(sandboxID, "3000"), local: true},
	}

	getTargetFromRequest := GetTargetFromRequest()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			domain, ok := MatchSandboxHost(tt.host, tt.headers, sandboxID, 3000)

			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantDomain, domain)

			// MatchSandboxHost must agree with how requests are routed.
			id, port, err := getTargetFromRequest(&http.Request{Host: tt.host, Header: tt.headers})
			routed := err == nil && id == sandboxID && port == 3000
			assert.Equal(t, routed && !tt.local, ok, "routed to the sandbox: %t (id %q, port %d, err %v)", routed, id, port, err)
		})
	}
}
