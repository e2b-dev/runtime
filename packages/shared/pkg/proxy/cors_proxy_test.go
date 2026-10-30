package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/connlimit"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
)

func httpDo(t *testing.T, method, proxyURL string, headers http.Header) (*http.Response, error) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, proxyURL, nil)
	if err != nil {
		return nil, err
	}

	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	return testHTTPClient(t).Do(req)
}

func preflightHeaders() http.Header {
	return http.Header{
		"Origin":                        {"https://app.example.com"},
		"Access-Control-Request-Method": {http.MethodGet},
	}
}

func assertAnsweredPreflight(t *testing.T, resp *http.Response) {
	t.Helper()

	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Methods"))
	assert.NotEmpty(t, resp.Header.Get("Access-Control-Max-Age"))
}

// Every response the proxy synthesizes for a routing failure must be readable
// by browser JS, and the preflight that precedes it must succeed — otherwise
// the browser reports an opaque network error instead of the status and body.
func TestProxyCORSOnDestinationErrors(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name       string
		err        error
		wantStatus int
	}{
		{"sandbox not found", NewErrSandboxNotFound("test-sandbox"), http.StatusBadGateway},
		{"resume permission denied", NewErrSandboxResumePermissionDenied("test-sandbox"), http.StatusForbidden},
		{"still transitioning", NewErrSandboxStillTransitioning("test-sandbox"), http.StatusConflict},
		{"internal route", NewErrInternalRoute("test-sandbox", "/init"), http.StatusNotFound},
		{"resource exhausted", NewErrSandboxResourceExhausted("test-sandbox", "rate limit hit"), http.StatusTooManyRequests},
		{"missing traffic access token", NewErrMissingTrafficAccessToken("test-sandbox", "e2b-traffic-access-token"), http.StatusForbidden},
		{"invalid traffic access token", NewErrInvalidTrafficAccessToken("test-sandbox", "e2b-traffic-access-token"), http.StatusForbidden},
		{"invalid host", ErrInvalidHost, http.StatusBadRequest},
		{"unexpected routing error", errors.New("routing table on fire"), http.StatusInternalServerError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			getDestination := func(*http.Request) (*pool.Destination, error) {
				return nil, tt.err
			}

			proxy, port, err := newTestProxy(t, getDestination)
			require.NoError(t, err)
			t.Cleanup(func() {
				proxy.Close()
			})

			proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)

			t.Run("preflight gets a 2xx", func(t *testing.T) {
				t.Parallel()

				resp, err := httpDo(t, http.MethodOptions, proxyURL, preflightHeaders())
				require.NoError(t, err)
				t.Cleanup(func() { _ = resp.Body.Close() })

				assertAnsweredPreflight(t, resp)
			})

			t.Run("error response carries allow-origin", func(t *testing.T) {
				t.Parallel()

				resp, err := httpDo(t, http.MethodGet, proxyURL, http.Header{"Origin": {"https://app.example.com"}})
				require.NoError(t, err)
				t.Cleanup(func() { _ = resp.Body.Close() })

				assert.Equal(t, tt.wantStatus, resp.StatusCode)
				assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
			})
		})
	}
}

// The connection-limit refusal happens after a destination resolved, so it has
// its own preflight handling — cover it separately.
func TestProxyCORSOnConnectionLimit(t *testing.T) {
	t.Parallel()

	var lisCfg net.ListenConfig
	listener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	backend, err := newTestBackend(listener, "backend-conn-limit")
	require.NoError(t, err)
	defer backend.Close()

	getDestination := func(*http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:           backend.url,
			SandboxId:     "test-sandbox",
			RequestLogger: logger.NewNopLogger(),
			ConnectionKey: backend.id,
		}, nil
	}

	proxy, port, err := newTestProxyWithConnLimit(t, getDestination, &ConnectionLimitConfig{
		Limiter:     connlimit.NewConnectionLimiter(),
		GetMaxLimit: func(context.Context) int { return 0 },
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		proxy.Close()
	})

	proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)

	t.Run("preflight gets a 2xx", func(t *testing.T) {
		t.Parallel()

		resp, err := httpDo(t, http.MethodOptions, proxyURL, preflightHeaders())
		require.NoError(t, err)
		t.Cleanup(func() { _ = resp.Body.Close() })

		assertAnsweredPreflight(t, resp)
	})

	t.Run("error response carries allow-origin", func(t *testing.T) {
		t.Parallel()

		resp, err := httpDo(t, http.MethodGet, proxyURL, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = resp.Body.Close() })

		assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
		assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
	})
}

// An unreachable upstream lands in the reverse proxy's ErrorHandler; those
// synthesized responses need CORS too, whether templated (closed port) or raw.
func TestProxyCORSOnUnreachableUpstream(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name               string
		defaultToPortError bool
		wantStatus         int
	}{
		{"closed port template", true, http.StatusBadGateway},
		{"plain bad gateway", false, http.StatusBadGateway},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Grab a port that nothing listens on.
			var lisCfg net.ListenConfig
			l, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			deadURL, err := url.Parse(fmt.Sprintf("http://%s", l.Addr().String()))
			require.NoError(t, err)
			require.NoError(t, l.Close())

			getDestination := func(*http.Request) (*pool.Destination, error) {
				return &pool.Destination{
					Url:                deadURL,
					SandboxId:          "test-sandbox",
					SandboxPort:        3000,
					RequestLogger:      logger.NewNopLogger(),
					ConnectionKey:      "dead-backend",
					DefaultToPortError: tt.defaultToPortError,
				}, nil
			}

			proxy, port, err := newTestProxy(t, getDestination)
			require.NoError(t, err)
			t.Cleanup(func() {
				proxy.Close()
			})

			proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)

			t.Run("preflight gets a 2xx", func(t *testing.T) {
				t.Parallel()

				resp, err := httpDo(t, http.MethodOptions, proxyURL, preflightHeaders())
				require.NoError(t, err)
				t.Cleanup(func() { _ = resp.Body.Close() })

				assertAnsweredPreflight(t, resp)
			})

			t.Run("error response carries allow-origin", func(t *testing.T) {
				t.Parallel()

				resp, err := httpDo(t, http.MethodGet, proxyURL, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = resp.Body.Close() })

				assert.Equal(t, tt.wantStatus, resp.StatusCode)
				assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
			})
		})
	}
}

// A reachable upstream owns its own CORS policy: the proxy must neither add
// headers to its responses nor answer preflights on its behalf.
func TestProxyDoesNotTouchCORSOfReachableUpstream(t *testing.T) {
	t.Parallel()

	var lisCfg net.ListenConfig
	listener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	backend, err := newTestBackend(listener, "backend-cors")
	require.NoError(t, err)
	defer backend.Close()

	getDestination := func(*http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:           backend.url,
			SandboxId:     "test-sandbox",
			RequestLogger: logger.NewNopLogger(),
			ConnectionKey: backend.id,
		}, nil
	}

	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	t.Cleanup(func() {
		proxy.Close()
	})

	proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)

	resp, err := httpDo(t, http.MethodGet, proxyURL, http.Header{"Origin": {"https://app.example.com"}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	assertBackendOutput(t, backend, resp)
	assert.Empty(t, resp.Header.Values("Access-Control-Allow-Origin"), "the proxy must not inject CORS into upstream responses")

	preflight, err := httpDo(t, http.MethodOptions, proxyURL, preflightHeaders())
	require.NoError(t, err)
	t.Cleanup(func() { _ = preflight.Body.Close() })

	body, err := io.ReadAll(preflight.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, preflight.StatusCode)
	assert.Equal(t, backend.id, string(body), "the upstream, not the proxy, should answer the preflight")
	assert.Equal(t, uint64(2), backend.RequestCount(), "both requests should have reached the backend")
}

// Every error the proxy writes must go through cors.Error, or a browser can't
// read it. This guards against a raw http.Error slipping back in.
func TestNoRawHTTPErrorInProxyPackage(t *testing.T) {
	t.Parallel()

	for _, dir := range []string{".", "pool", "template", "tracking"} {
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)

		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}

			path := filepath.Join(dir, name)
			content, err := os.ReadFile(path)
			require.NoError(t, err)

			assert.NotContains(t, string(content), "http.Error(", "%s: use cors.Error instead of http.Error so browsers can read the response", path)
		}
	}
}

func TestRetiredOwnerAnswersPreflight(t *testing.T) {
	t.Parallel()
	p := pool.New(4, 1, time.Minute, false, nil)
	target := &url.URL{Scheme: "http", Host: "127.0.0.1:1"}
	d, err := destinationFor(target, "retired")(nil)
	require.NoError(t, err)
	d.Owner = p.NewOwner()
	require.NoError(t, d.Owner.Close(t.Context()))
	r := httptest.NewRequestWithContext(t.Context(), http.MethodOptions, "http://example/", nil)
	r.Header.Set("Origin", "https://caller.example")
	r.Header.Set("Access-Control-Request-Method", "POST")
	w := httptest.NewRecorder()
	handler(p, func(*http.Request) (*pool.Destination, error) { return d, nil }, nil).ServeHTTP(w, r)
	assert.GreaterOrEqual(t, w.Code, 200)
	assert.Less(t, w.Code, 300)
	assert.NotEmpty(t, w.Header().Get("Access-Control-Allow-Origin"))
	assert.Zero(t, p.TotalConnections())
}
