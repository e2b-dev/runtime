package proxy

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	noopmetric "go.opentelemetry.io/otel/metric/noop"

	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
)

func TestRestoreForwardedHost(t *testing.T) {
	t.Parallel()

	const planeHost = "3000-isv6ril5xadwn1k9t2jye.plane.example"
	domains := []string{"global.example"}

	for _, tt := range []struct {
		name      string
		host      string
		forwarded []string
		headers   map[string]string
		domains   []string
		wantHost  string
	}{
		{name: "restores the same sandbox under an allowed domain", host: planeHost, forwarded: []string{"3000-isv6ril5xadwn1k9t2jye.global.example"}, wantHost: "3000-isv6ril5xadwn1k9t2jye.global.example"},
		{name: "matches the domain case-insensitively and keeps a port", host: planeHost, forwarded: []string{"3000-isv6ril5xadwn1k9t2jye.Global.Example:443"}, wantHost: "3000-isv6ril5xadwn1k9t2jye.Global.Example:443"},
		{
			name:      "restores the shared host when its routing headers name the same sandbox",
			host:      planeHost,
			forwarded: []string{"sandbox.global.example"},
			headers:   map[string]string{"E2b-Sandbox-Id": "isv6ril5xadwn1k9t2jye", "E2b-Sandbox-Port": "3000"},
			wantHost:  "sandbox.global.example",
		},
		{
			name:      "keeps the host when the shared host's headers name another port",
			host:      planeHost,
			forwarded: []string{"sandbox.global.example"},
			headers:   map[string]string{"E2b-Sandbox-Id": "isv6ril5xadwn1k9t2jye", "E2b-Sandbox-Port": "8000"},
			wantHost:  planeHost,
		},
		{name: "keeps the host for a port other than 443", host: planeHost, forwarded: []string{"3000-isv6ril5xadwn1k9t2jye.global.example:6666"}, wantHost: planeHost},
		{name: "keeps the host for an empty port", host: planeHost, forwarded: []string{"3000-isv6ril5xadwn1k9t2jye.global.example:"}, wantHost: planeHost},
		{name: "keeps the host for another sandbox", host: planeHost, forwarded: []string{"3000-iother5b5aiixd410phsjv.global.example"}, wantHost: planeHost},
		{name: "keeps the host for another port", host: planeHost, forwarded: []string{"8000-isv6ril5xadwn1k9t2jye.global.example"}, wantHost: planeHost},
		{name: "keeps the host for an unlisted domain", host: planeHost, forwarded: []string{"3000-isv6ril5xadwn1k9t2jye.evil.example"}, wantHost: planeHost},
		{name: "keeps the host for an arbitrary host", host: planeHost, forwarded: []string{"evil.example"}, wantHost: planeHost},
		{name: "keeps the host for a list", host: planeHost, forwarded: []string{"3000-isv6ril5xadwn1k9t2jye.global.example, evil.example"}, wantHost: planeHost},
		{name: "keeps the host for repeated headers", host: planeHost, forwarded: []string{"3000-isv6ril5xadwn1k9t2jye.global.example", "evil.example"}, wantHost: planeHost},
		{name: "keeps the host with no allowed domains", host: planeHost, forwarded: []string{"3000-isv6ril5xadwn1k9t2jye.global.example"}, domains: []string{}, wantHost: planeHost},
		{name: "keeps the host without the header", host: planeHost, wantHost: planeHost},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+tt.host+"/", nil)
			r.Host = tt.host
			for _, value := range tt.forwarded {
				r.Header.Add(headerForwardedHost, value)
			}
			for name, value := range tt.headers {
				r.Header.Set(name, value)
			}
			allowed := domains
			if tt.domains != nil {
				allowed = tt.domains
			}

			// Act
			restoreForwardedHost(r, "isv6ril5xadwn1k9t2jye", 3000, allowed)

			// Assert
			assert.Equal(t, tt.wantHost, r.Host)
			if tt.wantHost != tt.host {
				assert.Empty(t, r.Header.Values(headerForwardedHost), "a restored header is dropped")
			} else {
				assert.Equal(t, tt.forwarded, r.Header.Values(headerForwardedHost), "a header not restored is left as it came")
			}
		})
	}
}

func TestNormalizeForwardedHostDomains(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"Global.Example", "other.example"}, normalizeForwardedHostDomains([]string{" Global.Example. ", "", "other.example"}))
}

// The orchestrator, and so the sandbox, receives the restored Host and no
// X-Forwarded-Host; without an allowed domain it receives the plane's host.
func TestNewClientProxy_RestoresAForwardedHost(t *testing.T) {
	t.Parallel()

	const (
		planeHost  = "3000-isv6ril5xadwn1k9t2jye.plane.example"
		globalHost = "3000-isv6ril5xadwn1k9t2jye.global.example"
	)

	for _, tt := range []struct {
		name     string
		domains  []string
		wantHost string
	}{
		{name: "allowed domain", domains: []string{"global.example"}, wantHost: globalHost},
		{name: "no allowed domains", domains: nil, wantHost: planeHost},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			var gotHost, gotForwarded atomic.Value
			orchestrator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotHost.Store(r.Host)
				gotForwarded.Store(r.Header.Get(headerForwardedHost))
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(orchestrator.Close)
			node := orchestrator.Listener.Addr().(*net.TCPAddr)

			p, _, err := NewClientProxy(noopmetric.NewMeterProvider(), "forwarded-host", 0, uint16(node.Port), fixedCatalog{route: &catalog.SandboxInfo{OrchestratorIP: node.IP.String()}}, nil, newFF(t), tt.domains)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, p.Close()) })
			frontend := httptest.NewServer(p.Handler)
			t.Cleanup(frontend.Close)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, frontend.URL+"/", nil)
			require.NoError(t, err)
			req.Host = planeHost
			req.Header.Set(headerForwardedHost, globalHost)

			// Act
			resp, err := frontend.Client().Do(req)
			require.NoError(t, err)
			_, _ = io.Copy(io.Discard, resp.Body)
			require.NoError(t, resp.Body.Close())

			// Assert
			require.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, tt.wantHost, gotHost.Load())
			assert.Empty(t, gotForwarded.Load())
		})
	}
}
