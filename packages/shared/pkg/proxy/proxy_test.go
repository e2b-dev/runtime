package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/shared/pkg/connlimit"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
)

// testBackend represents a test backend server
type testBackend struct {
	server       *http.Server
	listener     net.Listener
	url          *url.URL
	requestCount *atomic.Uint64
	id           string
	cancel       context.CancelFunc
}

func (b *testBackend) RequestCount() uint64 {
	return b.requestCount.Load()
}

const bodyWriteDelayHeader = "body-write-delay"

var testHTTPClients sync.Map

// newTestBackend creates a new test backend server
func newTestBackend(listener net.Listener, id string) (*testBackend, error) {
	var requestCount atomic.Uint64

	ctx, cancel := context.WithCancel(context.Background())

	backend := &testBackend{
		server: &http.Server{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-ctx.Done():
					w.WriteHeader(http.StatusBadGateway)

					return
				default:
				}

				requestCount.Add(1)

				w.WriteHeader(http.StatusOK)

				// Flush the headers, so we can read the headers and body separately after .Do() returns.
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}

				// Check for "body-write-delay" header (interpreted as seconds)
				delayHeader := r.Header.Get(bodyWriteDelayHeader)

				if delayHeader != "" {
					if n, err := time.ParseDuration(delayHeader); err == nil {
						time.Sleep(n)
					}
				}

				w.Write([]byte(id))
			}),
		},
		listener:     listener,
		requestCount: &requestCount,
		id:           id,
		cancel:       cancel,
	}

	// Start the server
	go backend.server.Serve(backend.listener)

	// Parse the URL
	backendURL, err := url.Parse(fmt.Sprintf("http://%s", listener.Addr().String()))
	if err != nil {
		listener.Close()

		return nil, fmt.Errorf("failed to parse backend URL: %w", err)
	}
	backend.url = backendURL

	return backend, nil
}

// Interrupt closes the listener.
// We close the listener directly because we want to simulate ungraceful shutdown of the backend
// that happens when a sandbox is killed.
func (b *testBackend) Interrupt() error {
	var errs []error
	err := b.listener.Close()
	if err != nil {
		errs = append(errs, err)
	}

	b.cancel()

	return errors.Join(errs...)
}

func (b *testBackend) Close() error {
	return b.server.Close()
}

func assertBackendOutput(t *testing.T, backend *testBackend, resp *http.Response) {
	t.Helper()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "status code should be 200")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, string(body), backend.id, "backend id should be the same")
}

func assertStreamError(t *testing.T, resp *http.Response) {
	t.Helper()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "status code should be 200")

	_, err := io.ReadAll(resp.Body)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

// newTestProxy creates a new proxy server for testing
func newTestProxy(t *testing.T, getDestination func(r *http.Request) (*pool.Destination, error)) (*Proxy, uint, error) {
	t.Helper()

	return newTestProxyWithConnLimit(t, getDestination, nil)
}

func newTestProxyWithConnLimit(t *testing.T, getDestination func(r *http.Request) (*pool.Destination, error), connLimitConfig *ConnectionLimitConfig) (*Proxy, uint, error) {
	t.Helper()

	// Find a free port for the proxy
	var lisCfg net.ListenConfig
	l, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get free port: %w", err)
	}
	port := l.Addr().(*net.TCPAddr).Port

	// Set up the proxy server
	proxy := New(
		uint16(port),
		SandboxProxyRetries,
		20*time.Second, // Short idle timeout
		getDestination,
		connLimitConfig,
		false,
		nil,
	)

	// Start the proxy server
	go func() {
		proxy.Serve(l)
	}()

	return proxy, uint(port), nil
}

func TestProxyRoutesToTargetServer(t *testing.T) {
	t.Parallel()
	var lisCfg net.ListenConfig
	listener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	backend, err := newTestBackend(listener, "backend-1")
	require.NoError(t, err)
	defer backend.Close()

	// Set up a routing function that always returns the backend
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

	assert.Equal(t, uint64(0), proxy.TotalPoolConnections())
	assert.Equal(t, uint64(0), backend.RequestCount())

	// Make a request to the proxy
	proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)
	resp, err := httpGet(t, proxyURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	assertBackendOutput(t, backend, resp)

	assert.Equal(t, uint64(1), backend.RequestCount(), "backend should have been called once")
	assert.Equal(t, uint64(1), proxy.TotalPoolConnections(), "proxy should have established one connection")
}

func TestProxyRoutesToHTTPSBackendWithSelfSignedCertificate(t *testing.T) {
	t.Parallel()

	backend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte("secure backend"))
		assert.NoError(t, err)
	}))
	t.Cleanup(backend.Close)

	backendURL, err := url.Parse(backend.URL)
	require.NoError(t, err)

	proxy, port, err := newTestProxy(t, func(*http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:                   backendURL,
			SandboxId:             "test-sandbox",
			RequestLogger:         logger.NewNopLogger(),
			ConnectionKey:         "https-backend",
			InsecureSkipTLSVerify: true,
		}, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })

	resp, err := httpGet(t, fmt.Sprintf("http://127.0.0.1:%d", port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "secure backend", string(body))
}

// The 502 is the point: the same backend that the sibling test reaches must be
// unreachable without the flag.
func TestProxyRejectsHTTPSBackendWithoutInsecureSkipTLSVerify(t *testing.T) {
	t.Parallel()

	backend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte("secure backend"))
		assert.NoError(t, err)
	}))
	t.Cleanup(backend.Close)

	backendURL, err := url.Parse(backend.URL)
	require.NoError(t, err)

	proxy, port, err := newTestProxy(t, func(*http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:           backendURL,
			SandboxId:     "test-sandbox",
			RequestLogger: logger.NewNopLogger(),
			ConnectionKey: "https-backend-verified",
		}, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })

	resp, err := httpGet(t, fmt.Sprintf("http://127.0.0.1:%d", port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

func TestProxyResumePermissionDeniedErrorTemplate(t *testing.T) {
	t.Parallel()

	getDestination := func(*http.Request) (*pool.Destination, error) {
		return nil, NewErrSandboxResumePermissionDenied("test-sandbox")
	}

	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	t.Cleanup(func() {
		proxy.Close()
	})

	t.Run("json for non-browser", func(t *testing.T) {
		t.Parallel()
		proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)
		resp, err := httpGet(t, proxyURL)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = resp.Body.Close()
		})

		require.Equal(t, http.StatusForbidden, resp.StatusCode)
		require.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))

		var response struct {
			SandboxID string `json:"sandboxId"`
			Message   string `json:"message"`
			Code      int    `json:"code"`
		}
		err = json.NewDecoder(resp.Body).Decode(&response)
		require.NoError(t, err)
		require.Equal(t, "test-sandbox", response.SandboxID)
		require.Equal(t, "This sandbox can't be resumed with the credentials provided. Check your access token and try again.", response.Message)
		require.Equal(t, http.StatusForbidden, response.Code)
	})

	t.Run("html for browser", func(t *testing.T) {
		t.Parallel()
		proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)
		headers := http.Header{
			"User-Agent": {"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
		}

		resp, err := httpGetWithHeaders(t, proxyURL, headers)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = resp.Body.Close()
		})

		require.Equal(t, http.StatusForbidden, resp.StatusCode)
		require.Equal(t, "text/html; charset=utf-8", resp.Header.Get("Content-Type"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Contains(t, string(body), "Unable to resume sandbox")
		assert.Contains(t, string(body), "test-sandbox")
	})
}

func TestProxySandboxStillTransitioningErrorTemplate(t *testing.T) {
	t.Parallel()

	getDestination := func(*http.Request) (*pool.Destination, error) {
		return nil, NewErrSandboxStillTransitioning("test-sandbox")
	}

	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	t.Cleanup(func() {
		proxy.Close()
	})

	t.Run("json for non-browser", func(t *testing.T) {
		t.Parallel()
		proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)
		resp, err := httpGet(t, proxyURL)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = resp.Body.Close()
		})

		require.Equal(t, http.StatusConflict, resp.StatusCode)
		require.Equal(t, "2", resp.Header.Get("Retry-After"))
		require.Equal(t, "Retry-After", resp.Header.Get("Access-Control-Expose-Headers"))
		require.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))

		var response struct {
			SandboxID string `json:"sandboxId"`
			Message   string `json:"message"`
			Code      int    `json:"code"`
		}
		err = json.NewDecoder(resp.Body).Decode(&response)
		require.NoError(t, err)
		require.Equal(t, "test-sandbox", response.SandboxID)
		require.Equal(t, "The sandbox is still transitioning. Try again in a moment.", response.Message)
		require.Equal(t, http.StatusConflict, response.Code)
	})

	t.Run("html for browser", func(t *testing.T) {
		t.Parallel()
		proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)
		headers := http.Header{
			"User-Agent": {"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
		}

		resp, err := httpGetWithHeaders(t, proxyURL, headers)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = resp.Body.Close()
		})

		require.Equal(t, http.StatusConflict, resp.StatusCode)
		require.Equal(t, "2", resp.Header.Get("Retry-After"))
		require.Equal(t, "Retry-After", resp.Header.Get("Access-Control-Expose-Headers"))
		require.Equal(t, "text/html; charset=utf-8", resp.Header.Get("Content-Type"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Contains(t, string(body), "Sandbox still transitioning")
		assert.Contains(t, string(body), "test-sandbox")
	})
}

func TestProxySandboxCapacityErrorTemplate(t *testing.T) {
	t.Parallel()

	getDestination := func(r *http.Request) (*pool.Destination, error) {
		return nil, NewErrSandboxCapacity("test-sandbox", r.URL.Query().Get("reason"))
	}

	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	t.Cleanup(func() {
		proxy.Close()
	})

	t.Run("json for non-browser", func(t *testing.T) {
		t.Parallel()
		proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)
		resp, err := httpGet(t, proxyURL)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = resp.Body.Close()
		})

		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		require.Equal(t, "2", resp.Header.Get("Retry-After"))
		require.Equal(t, "Retry-After", resp.Header.Get("Access-Control-Expose-Headers"))
		require.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))

		var response struct {
			SandboxID string `json:"sandboxId"`
			Message   string `json:"message"`
			Code      int    `json:"code"`
			ErrorCode string `json:"error_code"`
		}
		err = json.NewDecoder(resp.Body).Decode(&response)
		require.NoError(t, err)
		require.Equal(t, "test-sandbox", response.SandboxID)
		require.Equal(t, "The sandbox cannot be resumed right now. Try again in a moment.", response.Message)
		require.Equal(t, http.StatusServiceUnavailable, response.Code)
		require.Empty(t, response.ErrorCode)
	})

	t.Run("json never carries the reason", func(t *testing.T) {
		t.Parallel()
		proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello?reason=transport%%3A+dial+tcp+10.1.2.3%%3A5008", port)
		resp, err := httpGet(t, proxyURL)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = resp.Body.Close()
		})

		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		require.Equal(t, "2", resp.Header.Get("Retry-After"))
		require.Equal(t, "Retry-After", resp.Header.Get("Access-Control-Expose-Headers"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NotContains(t, string(body), "dial tcp")
		require.Contains(t, string(body), `"message":"The sandbox cannot be resumed right now. Try again in a moment."`)
	})

	t.Run("html never carries the reason", func(t *testing.T) {
		t.Parallel()
		proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello?reason=transport%%3A+dial+tcp+10.1.2.3%%3A5008", port)
		headers := http.Header{
			"User-Agent": {"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
		}

		resp, err := httpGetWithHeaders(t, proxyURL, headers)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = resp.Body.Close()
		})

		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NotContains(t, string(body), "dial tcp")
		require.NotContains(t, string(body), "10.1.2.3")
	})

	t.Run("html for browser", func(t *testing.T) {
		t.Parallel()
		proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)
		headers := http.Header{
			"User-Agent": {"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
		}

		resp, err := httpGetWithHeaders(t, proxyURL, headers)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = resp.Body.Close()
		})

		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		require.Equal(t, "2", resp.Header.Get("Retry-After"))
		require.Equal(t, "Retry-After", resp.Header.Get("Access-Control-Expose-Headers"))
		require.Equal(t, "text/html; charset=utf-8", resp.Header.Get("Content-Type"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Contains(t, string(body), "Sandbox cannot be resumed")
		assert.Contains(t, string(body), "cannot be resumed right now")
		assert.Contains(t, string(body), "test-sandbox")
	})
}

// A request shed for lack of room to wait renders the capacity page without
// Retry-After, so clients that honour the header do not return in one wave,
// and with an error_code, so a client or an ingress that strips the header
// still tells it from the capacity answer a retry can help, which keeps the
// header and carries no code (TestProxySandboxCapacityErrorTemplate).
func TestProxySandboxShedErrorTemplate(t *testing.T) {
	t.Parallel()

	getDestination := func(_ *http.Request) (*pool.Destination, error) {
		return nil, NewErrSandboxShed("test-sandbox", "parking lot full")
	}

	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	t.Cleanup(func() {
		proxy.Close()
	})

	resp, err := httpGet(t, fmt.Sprintf("http://127.0.0.1:%d/hello", port))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = resp.Body.Close()
	})

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Empty(t, resp.Header.Get("Retry-After"))
	require.Empty(t, resp.Header.Get("Access-Control-Expose-Headers"))

	var response struct {
		SandboxID string `json:"sandboxId"`
		Message   string `json:"message"`
		Code      int    `json:"code"`
		ErrorCode string `json:"error_code"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	require.Equal(t, "test-sandbox", response.SandboxID)
	require.Equal(t, "The sandbox cannot be resumed right now. Try again in a moment.", response.Message)
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Equal(t, "proxy_parking_lot_full", response.ErrorCode)
	require.NotContains(t, response.Message, "parking lot")
}

// A placement refusal no retry helps renders the same 503 page family with
// its own wording and no Retry-After.
func TestProxySandboxPlacementRefusedErrorTemplate(t *testing.T) {
	t.Parallel()

	getDestination := func(_ *http.Request) (*pool.Destination, error) {
		return nil, NewErrSandboxPlacementRefused("test-sandbox", "Failed to place sandbox: no available orchestrator at version 0.3 or above", "sandbox_feature_unsupported", 0)
	}

	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	t.Cleanup(func() {
		proxy.Close()
	})

	t.Run("json for non-browser", func(t *testing.T) {
		t.Parallel()
		resp, err := httpGet(t, fmt.Sprintf("http://127.0.0.1:%d/hello", port))
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = resp.Body.Close()
		})

		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		require.Empty(t, resp.Header.Get("Retry-After"))

		var response struct {
			SandboxID string `json:"sandboxId"`
			Message   string `json:"message"`
			Code      int    `json:"code"`
			ErrorCode string `json:"error_code"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
		require.Equal(t, "test-sandbox", response.SandboxID)
		require.Equal(t, "The sandbox cannot be resumed: no node in this cluster can run it.", response.Message)
		require.Equal(t, http.StatusServiceUnavailable, response.Code)
		require.Equal(t, "sandbox_feature_unsupported", response.ErrorCode)
	})

	t.Run("html for browser", func(t *testing.T) {
		t.Parallel()
		resp, err := httpGetWithHeaders(t, fmt.Sprintf("http://127.0.0.1:%d/hello", port), http.Header{
			"User-Agent": {"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = resp.Body.Close()
		})

		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		require.Empty(t, resp.Header.Get("Retry-After"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Contains(t, string(body), "no node in this cluster can run it")
		assert.NotContains(t, string(body), "no available orchestrator at version")
		assert.NotContains(t, string(body), "Try again", "a page for a refusal no retry helps offers a retry")
	})
}

// A request whose own context ended before it was routed is answered with
// 408 and no page: a client that half-closed its connection is still
// reading, and an unwritten answer would reach it as an empty 200.
func TestProxyAnswersRequestTimeoutWhenTheRequestEndedBeforeRouting(t *testing.T) {
	t.Parallel()

	proxy := New(0, SandboxProxyRetries, time.Second, func(r *http.Request) (*pool.Destination, error) {
		return nil, fmt.Errorf("resolving: %w", r.Context().Err())
	}, nil, false, nil)
	t.Cleanup(func() { _ = proxy.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "http://sandbox.e2b.app/", nil)
	rr := httptest.NewRecorder()
	proxy.Handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusRequestTimeout, rr.Code)
	require.NotContains(t, rr.Body.String(), "sandbox", "an error page was rendered for a request that had ended")
}

// The same over a real connection: the client sends its request and closes
// its write side while routing waits, the server's EOF cancels the request
// context, and the client reads 408 rather than an empty 200.
func TestProxyHalfClosedClientReadsRequestTimeout(t *testing.T) {
	t.Parallel()

	proxy, port, err := newTestProxy(t, func(r *http.Request) (*pool.Destination, error) {
		<-r.Context().Done()

		return nil, fmt.Errorf("resolving: %w", r.Context().Err())
	})
	require.NoError(t, err)
	t.Cleanup(func() { proxy.Close() })

	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = fmt.Fprint(conn, "GET /hello HTTP/1.1\r\nHost: sandbox.e2b.app\r\n\r\n")
	require.NoError(t, err)
	tcpConn, ok := conn.(*net.TCPConn)
	require.True(t, ok)
	require.NoError(t, tcpConn.CloseWrite())
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusRequestTimeout, resp.StatusCode)
}

func TestProxyInternalRouteErrorTemplate(t *testing.T) {
	t.Parallel()

	getDestination := func(r *http.Request) (*pool.Destination, error) {
		return nil, NewErrInternalRoute("test-sandbox", r.URL.Path)
	}

	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	t.Cleanup(func() {
		proxy.Close()
	})

	t.Run("json for non-browser", func(t *testing.T) {
		t.Parallel()
		proxyURL := fmt.Sprintf("http://127.0.0.1:%d/init", port)
		resp, err := httpGet(t, proxyURL)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = resp.Body.Close()
		})

		require.Equal(t, http.StatusNotFound, resp.StatusCode)
		require.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))

		var response struct {
			SandboxID string `json:"sandboxId"`
			Message   string `json:"message"`
			Path      string `json:"path"`
			Code      int    `json:"code"`
		}
		err = json.NewDecoder(resp.Body).Decode(&response)
		require.NoError(t, err)
		assert.Equal(t, "This endpoint is reserved for the E2B control plane and is not reachable through the sandbox URL", response.Message)
		assert.Equal(t, "/init", response.Path)
		assert.Equal(t, http.StatusNotFound, response.Code)
		assert.Empty(t, response.SandboxID, "the response should not name the sandbox")
	})

	t.Run("html for browser", func(t *testing.T) {
		t.Parallel()
		proxyURL := fmt.Sprintf("http://127.0.0.1:%d/init", port)
		headers := http.Header{
			"User-Agent": {"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
		}

		resp, err := httpGetWithHeaders(t, proxyURL, headers)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = resp.Body.Close()
		})

		require.Equal(t, http.StatusNotFound, resp.StatusCode)
		require.Equal(t, "text/html; charset=utf-8", resp.Header.Get("Content-Type"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Contains(t, string(body), "Endpoint not available")
		assert.Contains(t, string(body), "/init")
		assert.NotContains(t, string(body), "test-sandbox")
	})
}

func TestProxyTeamSandboxLimitError(t *testing.T) {
	t.Parallel()

	getDestination := func(*http.Request) (*pool.Destination, error) {
		return nil, NewErrSandboxResourceExhausted("test-sandbox", "rate limit hit")
	}

	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	t.Cleanup(func() {
		proxy.Close()
	})

	t.Run("json for non-browser", func(t *testing.T) {
		t.Parallel()
		proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)
		resp, err := httpGet(t, proxyURL)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = resp.Body.Close()
		})

		require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
		require.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))

		var response struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}
		err = json.NewDecoder(resp.Body).Decode(&response)
		require.NoError(t, err)
		require.Equal(t, http.StatusTooManyRequests, response.Code)
		require.Equal(t, "rate limit hit", response.Message)
	})

	t.Run("html for browser", func(t *testing.T) {
		t.Parallel()
		proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)
		headers := http.Header{
			"User-Agent": {"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
		}

		resp, err := httpGetWithHeaders(t, proxyURL, headers)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = resp.Body.Close()
		})

		require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
		require.Equal(t, "text/html; charset=utf-8", resp.Header.Get("Content-Type"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Contains(t, string(body), "Sandbox limit reached")
		assert.Contains(t, string(body), "rate limit hit")
	})
}

func httpGet(t *testing.T, proxyURL string) (*http.Response, error) {
	t.Helper()

	return httpGetWithHeaders(t, proxyURL, nil)
}

func httpGetWithBodyWriteDelay(t *testing.T, proxyURL string, bodyWriteDelay time.Duration) (*http.Response, error) {
	t.Helper()

	return httpGetWithHeaders(t, proxyURL, http.Header{bodyWriteDelayHeader: {bodyWriteDelay.String()}})
}

func httpGetWithHeaders(t *testing.T, proxyURL string, headers http.Header) (*http.Response, error) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, proxyURL, nil)
	if err != nil {
		return nil, err
	}

	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	rsp, err := testHTTPClient(t).Do(req)
	if err != nil {
		return nil, err
	}

	return rsp, nil
}

func testHTTPClient(t *testing.T) *http.Client {
	t.Helper()

	transport := &http.Transport{}
	client := &http.Client{Transport: transport}

	actual, loaded := testHTTPClients.LoadOrStore(t.Name(), client)
	if !loaded {
		t.Cleanup(func() {
			transport.CloseIdleConnections()
			testHTTPClients.Delete(t.Name())
		})
	}

	return actual.(*http.Client)
}

type instrumentedConn struct {
	net.Conn

	listener *instrumentedListener
}

func (c *instrumentedConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if err != nil {
		c.listener.AddReadError(err)
	}

	return n, err
}

func (l *instrumentedListener) AddReadError(err error) {
	select {
	case l.FirstReadErr <- err:
	default:
	}
}

type instrumentedListener struct {
	net.Listener

	FirstReadErr chan error
}

func newInstrumentedListener(l net.Listener) *instrumentedListener {
	return &instrumentedListener{
		Listener:     l,
		FirstReadErr: make(chan error, 1),
	}
}

func (l *instrumentedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	return &instrumentedConn{Conn: conn, listener: l}, nil
}

func TestProxyReusesConnections(t *testing.T) {
	t.Parallel()
	var lisCfg net.ListenConfig
	listener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	backend, err := newTestBackend(listener, "backend-1")
	require.NoError(t, err)
	defer backend.Close()

	// Set up a routing function that always returns the backend
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
	defer proxy.Close()

	// Make two requests to the proxy
	proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)

	// First request
	resp1, err := httpGet(t, proxyURL)
	require.NoError(t, err)
	defer resp1.Body.Close()

	assertBackendOutput(t, backend, resp1)

	// Second request
	resp2, err := httpGet(t, proxyURL)
	require.NoError(t, err)
	defer resp2.Body.Close()

	assertBackendOutput(t, backend, resp2)

	// Verify that only one connection was established
	assert.Equal(t, uint64(2), backend.RequestCount(), "backend should have been called twice")
	assert.Equal(t, uint64(1), proxy.TotalPoolConnections(), "proxy should have used one connection")
}

func TestProxyCloseIdleConnectionsFromPool(t *testing.T) {
	t.Parallel()
	var lisCfg net.ListenConfig
	listener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	backend, err := newTestBackend(listener, "backend-1")
	require.NoError(t, err)
	defer backend.Close()

	var owner atomic.Pointer[pool.Owner]
	getDestination := func(*http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:           backend.url,
			SandboxId:     "test-sandbox",
			RequestLogger: logger.NewNopLogger(),
			ConnectionKey: backend.id,
			Owner:         owner.Load(),
		}, nil
	}

	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	defer proxy.Close()
	owner.Store(proxy.NewOwner())

	// Make a request to the proxy
	proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)
	resp, err := httpGet(t, proxyURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	assertBackendOutput(t, backend, resp)

	assert.Equal(t, uint64(1), proxy.TotalPoolConnections(), "proxy should have established one connection")
	assert.Equal(t, int64(1), proxy.CurrentPoolConnections(), "proxy should have established one connection that is still alive")
	assert.Equal(t, uint64(1), backend.RequestCount(), "backend should have been called once")

	require.NoError(t, owner.Load().Close(t.Context()))

	assert.Equal(t, uint64(1), proxy.TotalPoolConnections(), "proxy should have still one connection in the pool")
	assert.Equal(t, int64(0), proxy.CurrentPoolConnections(), "proxy should have removed the connection from the pool that is still alive")
}

func TestProxyResetAliveConnectionsFromPool(t *testing.T) {
	t.Parallel()
	var lisCfg net.ListenConfig

	listener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	instrumentedListener := newInstrumentedListener(listener)

	backend, err := newTestBackend(instrumentedListener, "backend-1")
	require.NoError(t, err)
	defer backend.Close()

	var owner atomic.Pointer[pool.Owner]
	getDestination := func(*http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:           backend.url,
			SandboxId:     "test-sandbox",
			RequestLogger: logger.NewNopLogger(),
			ConnectionKey: backend.id,
			Owner:         owner.Load(),
		}, nil
	}

	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	defer proxy.Close()
	owner.Store(proxy.NewOwner())

	requestEnded := make(chan struct{}, 1)

	go func() {
		defer close(requestEnded)

		// Make a request to the proxy
		proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)
		resp, err := httpGetWithBodyWriteDelay(t, proxyURL, 10*time.Second)
		assert.NoError(t, err)
		defer resp.Body.Close()

		assertStreamError(t, resp)
	}()

	// Wait for the request to start being processed by the backend
	time.Sleep(1 * time.Second)

	assert.Equal(t, uint64(1), proxy.TotalPoolConnections(), "proxy should have established one connection")
	assert.Equal(t, int64(1), proxy.CurrentPoolConnections(), "proxy should have established one connection that is still alive")
	assert.Equal(t, uint64(1), backend.RequestCount(), "backend should have been called once")

	require.NoError(t, owner.Load().Close(t.Context()))

	assert.Equal(t, uint64(1), proxy.TotalPoolConnections(), "proxy should have still one connection in the pool")
	assert.Equal(t, int64(0), proxy.CurrentPoolConnections(), "proxy should have removed the connection from the pool that is still alive")

	select {
	case <-requestEnded:
	case <-t.Context().Done():
		t.Fatalf("request timed out: %v", t.Context().Err())
	}

	select {
	case readErr, ok := <-instrumentedListener.FirstReadErr:
		if !ok {
			t.Fatalf("read error channel closed")
		}
		require.ErrorContains(t, readErr, "connection reset by peer")

		// io.EOF is returned for the FIN packet.
		require.NotErrorIs(t, readErr, io.EOF, "server connection should have read error other than EOF")
	case <-t.Context().Done():
		t.Fatalf("read error timed out: %v", t.Context().Err())
	}
}

// This is a test that verify that the proxy reuse fails when the backend changes.
func TestProxyReuseConnectionsWhenBackendChangesFails(t *testing.T) {
	t.Parallel()
	// Create first backend
	var lisCfg net.ListenConfig
	listener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	backend1, err := newTestBackend(listener, "backend-1")
	require.NoError(t, err)
	defer backend1.Close()

	// Get the address of the first backend
	backendAddr := backend1.listener.Addr().String()

	backendMapping := map[string]string{
		backendAddr: backend1.id,
	}
	var backendMappingMutex sync.Mutex

	// Set up a routing function that returns the current backend
	getDestination := func(_ *http.Request) (*pool.Destination, error) {
		backendMappingMutex.Lock()
		defer backendMappingMutex.Unlock()

		backendKey, ok := backendMapping[backendAddr]
		if !ok {
			return nil, errors.New("backend not found")
		}

		return &pool.Destination{
			Url:           backend1.url,
			SandboxId:     "backend1",
			RequestLogger: logger.NewNopLogger(),
			ConnectionKey: backendKey,
		}, nil
	}

	// Create proxy with the initial routing function
	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	defer proxy.Close()

	proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)

	// Make request to first backend
	resp1, err := httpGet(t, proxyURL)
	require.NoError(t, err)
	defer resp1.Body.Close()

	assertBackendOutput(t, backend1, resp1)

	assert.Equal(t, uint64(1), proxy.TotalPoolConnections(), "proxy should have used one connection")
	assert.Equal(t, uint64(1), backend1.RequestCount(), "first backend should have been called once")

	// Close the first backend
	backend1.Interrupt()

	// Create second backend on the same address
	listener, err = lisCfg.Listen(t.Context(), "tcp", backendAddr)
	require.NoError(t, err)

	backend2, err := newTestBackend(listener, "backend-2")
	require.NoError(t, err)
	defer backend2.Close()

	// Make request to second backend
	resp2, err := httpGet(t, proxyURL)
	require.NoError(t, err)
	defer resp2.Body.Close()

	assert.Equal(t, http.StatusBadGateway, resp2.StatusCode, "status code should be 502")
}

func TestProxyDoesNotReuseConnectionsWhenBackendChanges(t *testing.T) {
	t.Parallel()
	// Create first backend
	var lisCfg net.ListenConfig
	listener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	backend1, err := newTestBackend(listener, "backend-1")
	require.NoError(t, err)
	defer backend1.Close()

	// Get the address of the first backend
	backendAddr := backend1.listener.Addr().String()

	backendMapping := map[string]string{
		backendAddr: backend1.id,
	}
	var backendMappingMutex sync.Mutex

	// Set up a routing function that returns the current backend
	getDestination := func(_ *http.Request) (*pool.Destination, error) {
		backendMappingMutex.Lock()
		defer backendMappingMutex.Unlock()

		backendKey, ok := backendMapping[backendAddr]
		if !ok {
			return nil, errors.New("backend not found")
		}

		return &pool.Destination{
			Url:           backend1.url,
			SandboxId:     "backend1",
			RequestLogger: logger.NewNopLogger(),
			ConnectionKey: backendKey,
		}, nil
	}

	// Create proxy with the initial routing function
	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	defer proxy.Close()

	proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)

	// Make request to first backend
	resp1, err := httpGet(t, proxyURL)
	require.NoError(t, err)
	defer resp1.Body.Close()

	assertBackendOutput(t, backend1, resp1)

	assert.Equal(t, uint64(1), proxy.TotalPoolConnections(), "proxy should have reused the connection")
	assert.Equal(t, uint64(1), backend1.RequestCount(), "first backend should have been called once")

	// Close the first backend
	backend1.Interrupt()

	// Create second backend on the same address
	listener, err = lisCfg.Listen(t.Context(), "tcp", backendAddr)
	require.NoError(t, err)

	backend2, err := newTestBackend(listener, "backend-2")
	require.NoError(t, err)
	defer backend2.Close()

	backendMappingMutex.Lock()
	backendMapping[backend2.listener.Addr().String()] = backend2.id
	backendMappingMutex.Unlock()

	// Make request to second backend
	resp2, err := httpGet(t, proxyURL)
	require.NoError(t, err)
	defer resp2.Body.Close()

	assertBackendOutput(t, backend2, resp2)

	assert.Equal(t, uint64(1), backend2.RequestCount(), "second backend should have been called once")
	assert.Equal(t, uint64(1), backend1.RequestCount(), "first backend should have been called once")
	assert.Equal(t, uint64(2), proxy.TotalPoolConnections(), "proxy should not have reused the connection")
}

// reservedPort holds a TCP socket that is bound but not yet listening:
// connections are refused while the port stays reserved for this test, and
// listen turns the same socket into a live listener. This avoids the
// close-and-rebind race where another process grabs the port in between.
type reservedPort struct {
	fd     int
	addr   string
	closed bool
}

func reserveTCPPort(t *testing.T) *reservedPort {
	t.Helper()

	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	require.NoError(t, err)
	r := &reservedPort{fd: fd}
	t.Cleanup(r.close)

	require.NoError(t, syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}))
	sa, err := syscall.Getsockname(fd)
	require.NoError(t, err)
	r.addr = fmt.Sprintf("127.0.0.1:%d", sa.(*syscall.SockaddrInet4).Port)

	return r
}

// close releases the raw fd unless listen already handed it off.
func (r *reservedPort) close() {
	if r.closed {
		return
	}
	r.closed = true
	_ = syscall.Close(r.fd)
}

func (r *reservedPort) listen() (net.Listener, error) {
	if err := syscall.Listen(r.fd, 128); err != nil {
		return nil, fmt.Errorf("listen on reserved port: %w", err)
	}
	if err := syscall.SetNonblock(r.fd, true); err != nil {
		return nil, fmt.Errorf("set nonblock: %w", err)
	}
	r.closed = true // fd ownership moves to the os.File below
	f := os.NewFile(uintptr(r.fd), "reserved-port")
	defer f.Close() // net.FileListener dups the fd

	return net.FileListener(f)
}

// TestProxyRetriesOnDelayedBackendStartup simulates the scenario where a backend
// server starts up after the initial connection attempt (like envd port forwarding delay).
func TestProxyRetriesOnDelayedBackendStartup(t *testing.T) {
	t.Parallel()
	reserved := reserveTCPPort(t)
	backendAddr := reserved.addr

	backendURL, err := url.Parse(fmt.Sprintf("http://%s", backendAddr))
	require.NoError(t, err)

	getDestination := func(_ *http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:           backendURL,
			SandboxId:     "test-sandbox",
			RequestLogger: logger.NewNopLogger(),
			ConnectionKey: "delayed-backend",
		}, nil
	}

	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	defer proxy.Close()

	type backendResult struct {
		backend *testBackend
		err     error
	}
	backendReady := make(chan backendResult, 1)

	// Start backend after a delay (simulating envd port forwarding)
	go func() {
		// Wait 300ms before starting the backend (should succeed on retry 2 or 3)
		time.Sleep(300 * time.Millisecond)

		listener, err := reserved.listen()
		if err != nil {
			backendReady <- backendResult{nil, fmt.Errorf("failed to create delayed backend listener: %w", err)}

			return
		}

		backend, err := newTestBackend(listener, "delayed-backend")
		if err != nil {
			listener.Close()
			backendReady <- backendResult{nil, fmt.Errorf("failed to create delayed backend: %w", err)}

			return
		}

		backendReady <- backendResult{backend, nil}
	}()

	// Make request - this should retry and eventually succeed
	proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)
	start := time.Now()

	resp, err := httpGet(t, proxyURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	elapsed := time.Since(start)

	// Wait for backend to be ready before checking
	result := <-backendReady
	require.NoError(t, result.err)
	backend := result.backend
	defer backend.Close()

	assertBackendOutput(t, backend, resp)

	// Verify that it took at least the delay time (proving retries happened)
	assert.GreaterOrEqual(t, elapsed, 300*time.Millisecond, "request should have waited for backend to start")
	assert.Less(t, elapsed, 2*time.Second, "request should have succeeded before all retries exhausted")

	// Verify the connection was established
	assert.Equal(t, uint64(1), backend.RequestCount(), "backend should have been called once")
	assert.Equal(t, uint64(1), proxy.TotalPoolConnections(), "proxy should have established one connection")
}

type data struct {
	Tag     string      `json:"tag"`
	Host    string      `json:"host"`
	Headers http.Header `json:"headers"`
}

// TestChangeResponseHeader creates three http servers:
// - internal server (returns "internal")
// - masked server (returns "masked")
// - proxy server (should proxy to the internal server)
// The internal and masked server both return a constant string. The proxy, when masked,
// should return the "internal" server and not "masked" server.
func TestChangeResponseHeader(t *testing.T) {
	t.Parallel()

	var lisCfg net.ListenConfig

	// Pre-listen on ephemeral ports so we know the servers are accepting
	// connections before the test starts firing requests, and to avoid
	// hardcoded-port collisions between parallel test runs.
	internalListener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	internalAddr := internalListener.Addr().(*net.TCPAddr)
	internalPort := uint64(internalAddr.Port) //nolint:gosec // test-only port number always fits
	internalURL, err := url.Parse(fmt.Sprintf("http://%s", internalAddr.String()))
	require.NoError(t, err)

	maskedListener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	maskedAddr := maskedListener.Addr().(*net.TCPAddr)
	maskedHost := maskedAddr.String()

	proxyListener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	proxyAddr := proxyListener.Addr().(*net.TCPAddr)
	proxyPort := uint16(proxyAddr.Port) //nolint:gosec // test-only port number always fits
	proxyURL, err := url.Parse(fmt.Sprintf("http://%s", proxyAddr.String()))
	require.NoError(t, err)

	client := &http.Client{}

	proxy := New(proxyPort, 1, time.Second, func(_ *http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:                                internalURL,
			SandboxId:                          "12345",
			SandboxPort:                        internalPort,
			DefaultToPortError:                 false,
			RequestLogger:                      logger.L(),
			ConnectionKey:                      "connection-key",
			IncludeSandboxIdInProxyErrorLogger: true,
			MaskRequestHost:                    new(maskedHost),
		}, nil
	}, nil, false, nil)

	go func() {
		err := proxy.Serve(proxyListener)
		assert.ErrorIs(t, err, http.ErrServerClosed)
	}()

	t.Cleanup(func() {
		err := proxy.Close()
		assert.NoError(t, err)
	})

	internalServer := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			encErr := json.NewEncoder(w).Encode(data{"internal", r.Host, r.Header})
			assert.NoError(t, encErr)
		}),
	}
	go func() {
		serveErr := internalServer.Serve(internalListener)
		assert.ErrorIs(t, serveErr, http.ErrServerClosed)
	}()
	t.Cleanup(func() {
		assert.NoError(t, internalServer.Close())
	})

	maskedServer := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			encErr := json.NewEncoder(w).Encode(data{"masked", r.Host, r.Header})
			assert.NoError(t, encErr)
		}),
	}
	go func() {
		serveErr := maskedServer.Serve(maskedListener)
		assert.ErrorIs(t, serveErr, http.ErrServerClosed)
	}()
	t.Cleanup(func() {
		assert.NoError(t, maskedServer.Close())
	})

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, proxyURL.String(), nil)
	require.NoError(t, err)
	req.Host = fmt.Sprintf("localhost:%d", proxyPort)
	req.Header.Set("e2b-testing", "test123")

	// Retry while the proxy goroutine is still binding to the port. Once the
	// proxy is up, the internal server is guaranteed listening (we pre-bound
	// its listener above), so we don't need to retry on bad status.
	var rsp *http.Response
	for range 50 {
		rsp, err = client.Do(req)
		if err == nil {
			t.Cleanup(func() {
				assert.NoError(t, rsp.Body.Close())
			})

			break
		}

		if errors.Is(err, syscall.ECONNREFUSED) {
			time.Sleep(20 * time.Millisecond)

			continue
		}

		require.NoError(t, err)
	}

	require.NotNil(t, rsp, "response should not be nil")
	body, err := io.ReadAll(rsp.Body)
	require.NoError(t, err)
	require.Equal(t, 200, rsp.StatusCode, string(body))

	var data data
	err = json.Unmarshal(body, &data)
	require.NoError(t, err)

	assert.Equal(t, "internal", data.Tag)
	assert.Equal(t, maskedHost, data.Host)
	assert.Equal(t, "test123", data.Headers.Get("E2b-Testing"))
	assert.Equal(t, fmt.Sprintf("localhost:%d", proxyPort), data.Headers.Get("X-Forwarded-Host"))
}

func TestConnectionLimitBlocksExcessConnections(t *testing.T) {
	t.Parallel()

	var lisCfg net.ListenConfig
	listener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	backend, err := newTestBackend(listener, "backend-limited")
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

	const maxConns = 2
	const holdDelay = 2 * time.Second

	var acquiredCount atomic.Int64
	var releasedCount atomic.Int64
	var blockedCount atomic.Int64

	connLimitConfig := &ConnectionLimitConfig{
		Limiter:     connlimit.NewConnectionLimiter(),
		GetMaxLimit: func(context.Context) int { return maxConns },
		OnConnectionAcquired: func(_ context.Context, _ int64) {
			acquiredCount.Add(1)
		},
		OnConnectionReleased: func(_ context.Context, _ int64) {
			releasedCount.Add(1)
		},
		OnConnectionBlocked: func(_ context.Context) {
			blockedCount.Add(1)
		},
	}

	proxy, port, err := newTestProxyWithConnLimit(t, getDestination, connLimitConfig)
	require.NoError(t, err)
	defer proxy.Close()

	proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)

	// Hold maxConns connections open by delaying body write on the backend.
	// Each request returns 200 headers immediately, but keeps the proxy handler
	// busy for holdDelay while the backend writes the body.
	// We must read the full body to keep the client connection alive,
	// otherwise closing the body releases the proxy handler early.
	var wg errgroup.Group
	for range maxConns {
		wg.Go(func() error {
			resp, reqErr := httpGetWithBodyWriteDelay(t, proxyURL, holdDelay)
			if reqErr != nil {
				return reqErr
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}

			// Block until the backend finishes writing the delayed body,
			// keeping the proxy handler (and connection slot) occupied.
			_, err := io.ReadAll(resp.Body)

			return err
		})
	}

	// Give the held requests a moment to be proxied and acquire their slots.
	time.Sleep(500 * time.Millisecond)

	// The next request should be blocked (429).
	blockedResp, err := httpGet(t, proxyURL)
	require.NoError(t, err)
	assert.Equal(t, http.StatusTooManyRequests, blockedResp.StatusCode, "request over limit should get 429")
	blockedResp.Body.Close()

	assert.Equal(t, int64(maxConns), acquiredCount.Load(), "OnConnectionAcquired should have been called for each acquired slot")
	assert.Equal(t, int64(1), blockedCount.Load(), "OnConnectionBlocked should have been called once")

	// Wait for the held connections to finish naturally.
	err = wg.Wait()
	require.NoError(t, err)

	assert.Equal(t, int64(maxConns), releasedCount.Load(), "OnConnectionReleased should have been called for each released slot")

	// After releasing, a new request should succeed.
	resp, err := httpGet(t, proxyURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	assertBackendOutput(t, backend, resp)
}

func TestProxyRejectsInvalidSandboxPort(t *testing.T) {
	t.Parallel()

	getTargetFromRequest := GetTargetFromRequest()
	getDestination := func(r *http.Request) (*pool.Destination, error) {
		if _, _, err := getTargetFromRequest(r); err != nil {
			return nil, err
		}

		return nil, errors.New("request was routed")
	}

	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	t.Cleanup(func() {
		proxy.Close()
	})

	for _, sandboxPort := range []string{"abc", "0", "65536"} {
		t.Run(sandboxPort, func(t *testing.T) {
			t.Parallel()

			resp, err := httpGetWithHeaders(t, fmt.Sprintf("http://127.0.0.1:%d/", port), http.Header{
				headerSandboxID:   {"isv6ril5xadwn1k9t2jye"},
				headerSandboxPort: {sandboxPort},
			})
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Contains(t, string(body), "Invalid sandbox port")
		})
	}
}

func TestSharedIngressPolicy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, token, path string
		port              uint64
		success           bool
	}{
		{name: "missing token", port: 8443},
		{name: "wrong token", token: "wrong", port: 8443},
		{name: "authorized", token: "secret", port: 8443, success: true},
		{name: "envd exemption", port: 49983, success: true},
		{name: "internal before lookup", port: 49983, path: "/init"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mask, token := "localhost:${PORT}", "secret"
			config := &orchestrator.SandboxNetworkIngressConfig{TrafficAccessToken: &token, MaskRequestHost: &mask, HttpsPorts: []uint32{8443, 49983}}
			lookedUp := false
			resolver := func(r *http.Request) (*pool.Destination, error) {
				return ResolveIngress(r, func(string, uint64) (IngressTarget, error) {
					lookedUp = true

					return IngressTarget{Config: config, Destination: pool.Destination{Url: &url.URL{Host: "127.0.0.1"}, RequestLogger: logger.NewNopLogger()}}, nil
				})
			}
			r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://example"+tc.path, nil)
			r.Host = fmt.Sprintf("%d-im9r2ycjiy2534qsdy1oo.e2b.app", tc.port)
			r.Header.Set(trafficAccessTokenHeader, tc.token)
			d, err := resolver(r)
			if tc.success {
				require.NoError(t, err)
				if tc.port == 49983 {
					assert.Equal(t, "http", d.Url.Scheme)
					assert.Nil(t, d.MaskRequestHost)
				} else {
					assert.Equal(t, "https", d.Url.Scheme)
					require.NotNil(t, d.MaskRequestHost)
					assert.Equal(t, "localhost:8443", *d.MaskRequestHost)
				}
				assert.True(t, d.InsecureSkipTLSVerify)
			} else {
				require.Error(t, err)
				switch tc.name {
				case "missing token":
					var e *MissingTrafficAccessTokenError
					require.ErrorAs(t, err, &e)
				case "wrong token":
					var e *InvalidTrafficAccessTokenError
					require.ErrorAs(t, err, &e)
				case "internal before lookup":
					var e *InternalRouteError
					require.ErrorAs(t, err, &e)
					assert.False(t, lookedUp)
				}
				// A preflight cannot carry the token, so it fails the policy;
				// the browser still needs the CORS answer, from the error path.
				r.Method = http.MethodOptions
				r.Header.Set("Origin", "https://caller.example")
				r.Header.Set("Access-Control-Request-Method", "POST")
				recorder := httptest.NewRecorder()
				p := pool.New(4, 1, time.Second, false, nil)
				handler(p, resolver, nil).ServeHTTP(recorder, r)
				assert.GreaterOrEqual(t, recorder.Code, 200)
				assert.Less(t, recorder.Code, 300)
				assert.NotEmpty(t, recorder.Header().Get("Access-Control-Allow-Origin"))
			}
		})
	}
}

func TestRetiredConnectStreamPreservesFrontendReuse(t *testing.T) {
	t.Parallel()
	backend := fullDuplexSandbox(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/next" {
			w.WriteHeader(http.StatusNoContent)

			return
		}
		w.Header().Set("Content-Type", "application/connect+json")
		_, _ = w.Write(dataEnvelope(`{}`))
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
	})
	var owner atomic.Pointer[pool.Owner]
	target := destinationFor(backend, "retired")
	proxy, port, _, _ := startProxyWithIdleTimeout(t, func(r *http.Request) (*pool.Destination, error) {
		d, err := target(r)
		d.Owner = owner.Load()

		return d, err
	}, time.Minute)
	owner.Store(proxy.NewOwner())
	conn, reader := dialProxy(t, port)
	_, err := fmt.Fprintf(conn, "GET /stream HTTP/1.1\r\nHost: example\r\n\r\n")
	require.NoError(t, err)
	response, err := http.ReadResponse(reader, nil)
	require.NoError(t, err)
	first := make([]byte, len(dataEnvelope(`{}`)))
	_, err = io.ReadFull(response.Body, first)
	require.NoError(t, err)
	require.NoError(t, owner.Load().Close(t.Context()))
	// The retired owner's successor, as a reset lifecycle gets one.
	owner.Store(proxy.NewOwner())
	_, err = io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.False(t, response.Close, "completed Connect response should retain frontend keepalive")
	// Past the retired write deadline: the server clears it after each
	// response, so the kept frontend serves the next request.
	time.Sleep(2 * retiredWriteTimeout)
	_, err = fmt.Fprintf(conn, "GET /next HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\n")
	require.NoError(t, err)
	next, err := http.ReadResponse(reader, nil)
	require.NoError(t, err)
	defer next.Body.Close()
	require.Equal(t, http.StatusNoContent, next.StatusCode)
}

func TestRetiredOwnerSettlesStalledUpload(t *testing.T) {
	t.Parallel()
	for _, framing := range []struct {
		name    string
		request string
	}{
		{"content length", "Content-Length: 1000\r\n\r\npartial-body"},
		{"chunked", "Transfer-Encoding: chunked\r\n\r\nc\r\npartial-body\r\n"},
	} {
		t.Run(framing.name, func(t *testing.T) {
			t.Parallel()
			arrived := make(chan struct{})
			closed := make(chan error, 1)
			backend := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				_, err := io.ReadFull(r.Body, make([]byte, 4))
				assert.NoError(t, err)
				close(arrived)
				_, err = io.Copy(io.Discard, r.Body)
				closed <- err
			}))
			defer backend.Close()
			target, err := url.Parse(backend.URL)
			require.NoError(t, err)
			var owner *pool.Owner
			proxy, port, _, _ := startProxyWithIdleTimeout(t, func(*http.Request) (*pool.Destination, error) {
				return &pool.Destination{Url: target, SandboxId: "upload", RequestLogger: logger.NewNopLogger(), ConnectionKey: "upload", Owner: owner}, nil
			}, time.Minute)
			owner = proxy.NewOwner()
			conn, reader := dialProxy(t, port)
			defer conn.Close()
			_, err = fmt.Fprintf(conn, "POST / HTTP/1.1\r\nHost: example\r\n%s", framing.request)
			require.NoError(t, err)
			select {
			case <-arrived:
			case <-time.After(5 * time.Second):
				t.Fatal("backend did not receive upload")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			require.NoError(t, owner.Close(ctx))
			select {
			case err := <-closed:
				require.Error(t, err, "retirement must interrupt the backend upload")
			case <-ctx.Done():
				t.Fatal("backend socket remained open")
			}
			response, err := http.ReadResponse(reader, nil)
			require.NoError(t, err)
			defer response.Body.Close()
			require.True(t, response.Close, "interrupted uploads cannot reuse their frontend connection")
			_, err = io.ReadAll(response.Body)
			require.NoError(t, err)
		})
	}
}

func TestIngressSchemeForPort(t *testing.T) {
	t.Parallel()
	envdPort := uint64(consts.DefaultEnvdServerPort)
	for _, tt := range []struct {
		name    string
		ingress *orchestrator.SandboxNetworkIngressConfig
		port    uint64
		want    string
	}{
		{name: "nil ingress defaults to http", port: 8080, want: "http"},
		{name: "no HTTPS ports defaults to http", ingress: &orchestrator.SandboxNetworkIngressConfig{}, port: 8080, want: "http"},
		{name: "port in HTTPS ports uses https", ingress: &orchestrator.SandboxNetworkIngressConfig{HttpsPorts: []uint32{443, 8443}}, port: 8443, want: "https"},
		{name: "port not in HTTPS ports uses http", ingress: &orchestrator.SandboxNetworkIngressConfig{HttpsPorts: []uint32{443, 8443}}, port: 8080, want: "http"},
		{name: "envd port stays http even when listed", ingress: &orchestrator.SandboxNetworkIngressConfig{HttpsPorts: []uint32{uint32(envdPort)}}, port: envdPort, want: "http"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://example/", nil)
			r.Host = fmt.Sprintf("%d-im9r2ycjiy2534qsdy1oo.e2b.app", tt.port)
			d, err := ResolveIngress(r, func(string, uint64) (IngressTarget, error) {
				return IngressTarget{Config: tt.ingress, Destination: pool.Destination{Url: &url.URL{Host: "127.0.0.1"}, RequestLogger: logger.NewNopLogger()}}, nil
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, d.Url.Scheme)
		})
	}
}

// A stalled upload refused at admission must not be drained by the server
// before its refusal, nor leave its body on a kept frontend.
func TestRefusedUploadClosesFrontend(t *testing.T) {
	t.Parallel()
	backend := fullDuplexSandbox(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	var owner atomic.Pointer[pool.Owner]
	target := destinationFor(backend, "refused")
	proxy, port, _, _ := startProxyWithIdleTimeout(t, func(r *http.Request) (*pool.Destination, error) {
		d, err := target(r)
		d.Owner = owner.Load()

		return d, err
	}, time.Minute)
	owner.Store(proxy.NewOwner())
	require.NoError(t, owner.Load().Close(t.Context()))
	conn, reader := dialProxy(t, port)
	defer conn.Close()
	_, err := fmt.Fprintf(conn, "POST /upload HTTP/1.1\r\nHost: example\r\nContent-Length: 1000\r\n\r\npartial")
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	response, err := http.ReadResponse(reader, nil)
	require.NoError(t, err, "the refusal must not wait for the rest of the upload")
	defer response.Body.Close()
	assert.Equal(t, http.StatusBadGateway, response.StatusCode)
	assert.True(t, response.Close, "a frontend with an unread body cannot be kept")
	_, err = io.ReadAll(response.Body)
	require.NoError(t, err)
	_, err = reader.ReadByte()
	require.ErrorIs(t, err, io.EOF, "the server must close the frontend after the refusal")
}

// A retired stream whose request body the server had already taken, every
// Connect RPC's shape, must not poison the kept frontend: the read deadline
// that cuts a transport read reaches the server's background read and cancels
// the connection's context, so the frontend is closed instead of reused.
func TestRetiredRequestWithTakenBodyDoesNotPoisonFrontend(t *testing.T) {
	t.Parallel()
	backend := fullDuplexSandbox(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/next" {
			w.WriteHeader(http.StatusNoContent)

			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/connect+json")
		_, _ = w.Write(dataEnvelope(`{}`))
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
	})
	var owner atomic.Pointer[pool.Owner]
	target := destinationFor(backend, "taken")
	proxy, port, _, _ := startProxyWithIdleTimeout(t, func(r *http.Request) (*pool.Destination, error) {
		d, err := target(r)
		d.Owner = owner.Load()

		return d, err
	}, time.Minute)
	owner.Store(proxy.NewOwner())
	conn, reader := dialProxy(t, port)
	defer conn.Close()
	_, err := fmt.Fprintf(conn, "POST /stream HTTP/1.1\r\nHost: example\r\nContent-Length: 2\r\n\r\n{}")
	require.NoError(t, err)
	response, err := http.ReadResponse(reader, nil)
	require.NoError(t, err)
	_, err = io.ReadFull(response.Body, make([]byte, len(dataEnvelope(`{}`))))
	require.NoError(t, err)
	require.NoError(t, owner.Load().Close(t.Context()))
	owner.Store(proxy.NewOwner())
	_, err = io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = fmt.Fprintf(conn, "GET /next HTTP/1.1\r\nHost: example\r\n\r\n")
	require.NoError(t, err)
	next, err := http.ReadResponse(reader, nil)
	if err != nil {
		require.True(t, errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF), "a closed frontend is fine; a poisoned one is not: %v", err)

		return
	}
	defer next.Body.Close()
	require.Equal(t, http.StatusNoContent, next.StatusCode, "the next request on the kept frontend must be served, not refused")
}

// A client that stops reading a download holds the handler in its frontend
// write; retirement must still settle, on the write deadline, well inside a
// release budget.
func TestRetiredOwnerSettlesStalledDownload(t *testing.T) {
	t.Parallel()
	backend := fullDuplexSandbox(t, func(w http.ResponseWriter, _ *http.Request) {
		buf := make([]byte, 64<<10)
		for {
			if _, err := w.Write(buf); err != nil {
				return
			}
		}
	})
	var owner atomic.Pointer[pool.Owner]
	target := destinationFor(backend, "download")
	proxy, port, _, _ := startProxyWithIdleTimeout(t, func(r *http.Request) (*pool.Destination, error) {
		d, err := target(r)
		d.Owner = owner.Load()

		return d, err
	}, time.Minute)
	owner.Store(proxy.NewOwner())
	conn, reader := dialProxy(t, port)
	defer conn.Close()
	_, err := fmt.Fprintf(conn, "GET /big HTTP/1.1\r\nHost: example\r\n\r\n")
	require.NoError(t, err)
	response, err := http.ReadResponse(reader, nil)
	require.NoError(t, err)
	defer response.Body.Close()
	// Read nothing more: the windows fill and the handler blocks writing.
	require.Eventually(t, func() bool { return proxy.CurrentPoolConnections() == 1 }, 5*time.Second, 10*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	started := time.Now()
	require.NoError(t, owner.Load().Close(ctx))
	require.Less(t, time.Since(started), 3*retiredWriteTimeout, "a stalled reader must be cut at the write deadline, not waited for")
}
