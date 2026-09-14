package proxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"

	"github.com/e2b-dev/infra/packages/shared/pkg/httpserver"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
)

func TestProxyForwardsNativeGRPCOverH2C(t *testing.T) {
	t.Parallel()

	backendURL, proto, requests := startH2COnlyBackend(t)

	proxy, port, err := newTestProxy(t, func(*http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:           backendURL,
			SandboxId:     "test-sandbox",
			RequestLogger: logger.NewNopLogger(),
			ConnectionKey: "grpc-backend",
		}, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })

	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/pkg.Service/Method", port),
		strings.NewReader("frame"),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("Te", "trailers")

	resp, err := h2cClient().Do(req)
	require.NoError(t, err)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "grpc-ok", string(body))
	assert.Equal(t, "0", resp.Trailer.Get("Grpc-Status"))
	assert.Equal(t, uint64(1), requests.Load())
	gotProto, ok := proto.Load().(string)
	require.True(t, ok)
	assert.Equal(t, "HTTP/2.0", gotProto)
}

func TestProxySendsHTTP1GRPCOverH2CToTheBackend(t *testing.T) {
	t.Parallel()

	backendURL, proto, requests := startH2COnlyBackend(t)

	proxy, port, err := newTestProxy(t, func(*http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:           backendURL,
			SandboxId:     "test-sandbox",
			RequestLogger: logger.NewNopLogger(),
			ConnectionKey: "grpc-http1-frontend",
		}, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })

	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/pkg.Service/Method", port),
		strings.NewReader("frame"),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("Te", "trailers")

	resp, err := (&http.Client{}).Do(req)
	require.NoError(t, err)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "grpc-ok", string(body))
	assert.Equal(t, "0", resp.Trailer.Get("Grpc-Status"))
	assert.Equal(t, uint64(1), requests.Load())
	gotProto, ok := proto.Load().(string)
	require.True(t, ok)
	assert.Equal(t, "HTTP/2.0", gotProto)
}

func TestProxyKeepsHTTP1BackendWhenFrontendIsHTTP2(t *testing.T) {
	t.Parallel()

	var lisCfg net.ListenConfig
	listener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	backend, err := newTestBackend(listener, "http1-backend")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backend.Close()) })

	proxy, port, err := newTestProxy(t, func(*http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:           backend.url,
			SandboxId:     "test-sandbox",
			RequestLogger: logger.NewNopLogger(),
			ConnectionKey: backend.id,
		}, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })

	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		fmt.Sprintf("http://127.0.0.1:%d/hello", port),
		nil,
	)
	require.NoError(t, err)

	resp, err := h2cClient().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	assertBackendOutput(t, backend, resp)
	assert.Equal(t, uint64(1), backend.RequestCount())
}

func h2cClient() *http.Client {
	return &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			},
		},
	}
}

func startH2COnlyBackend(t *testing.T) (*url.URL, *atomic.Value, *atomic.Uint64) {
	t.Helper()

	var proto atomic.Value
	var requests atomic.Uint64

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		proto.Store(r.Proto)

		if r.ProtoMajor != 2 {
			http.Error(w, "backend requires HTTP/2", http.StatusHTTPVersionNotSupported)

			return
		}

		if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			http.Error(w, "missing grpc content type", http.StatusUnsupportedMediaType)

			return
		}

		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("Trailer", "Grpc-Status")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("grpc-ok"))
		w.Header().Set("Grpc-Status", "0")
	})

	server := &http.Server{Handler: handler}
	httpserver.ConfigureH2C(server)

	var lisCfg net.ListenConfig
	listener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	backendURL, err := url.Parse("http://" + listener.Addr().String())
	require.NoError(t, err)

	return backendURL, &proto, &requests
}
