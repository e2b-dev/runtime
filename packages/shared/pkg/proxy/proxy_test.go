package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/shared/pkg/connlimit"
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
		assert.Contains(t, string(body), "Unable to Resume Sandbox")
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
		require.Equal(t, "text/html; charset=utf-8", resp.Header.Get("Content-Type"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Contains(t, string(body), "Sandbox Still Transitioning")
		assert.Contains(t, string(body), "test-sandbox")
	})
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
		assert.Contains(t, string(body), "Endpoint Not Available")
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
		assert.Contains(t, string(body), "Sandbox Limit Reached")
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

	// Make a request to the proxy
	proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)
	resp, err := httpGet(t, proxyURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	assertBackendOutput(t, backend, resp)

	assert.Equal(t, uint64(1), proxy.TotalPoolConnections(), "proxy should have established one connection")
	assert.Equal(t, int64(1), proxy.CurrentPoolConnections(), "proxy should have established one connection that is still alive")
	assert.Equal(t, uint64(1), backend.RequestCount(), "backend should have been called once")

	// Remove the connection from the pool
	err = proxy.RemoveFromPool(backend.id)
	require.NoError(t, err)

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

	// Remove the connection from the pool
	err = proxy.RemoveFromPool(backend.id)
	require.NoError(t, err)

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
	}, nil, false)

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
