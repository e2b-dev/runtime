package httpserver

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
)

// h2cClient speaks HTTP/2 in cleartext from the first byte, with prior knowledge.
func h2cClient() *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			},
		},
	}
}

func TestConfigureH2CAcceptsHTTP2AndHTTP1(t *testing.T) {
	t.Parallel()

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	server := httptest.NewUnstartedServer(nil)
	server.Config.Handler = handler
	ConfigureH2C(server.Config)
	server.Start()
	t.Cleanup(server.Close)

	h2Req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	require.NoError(t, err)

	h2Resp, err := h2cClient().Do(h2Req)
	require.NoError(t, err)
	defer h2Resp.Body.Close()

	require.Equal(t, http.StatusNoContent, h2Resp.StatusCode)
	require.Equal(t, "HTTP/2.0", h2Resp.Proto)

	h1Req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	require.NoError(t, err)

	h1Resp, err := server.Client().Do(h1Req)
	require.NoError(t, err)
	defer h1Resp.Body.Close()

	require.Equal(t, http.StatusNoContent, h1Resp.StatusCode)
	require.Equal(t, "HTTP/1.1", h1Resp.Proto)
}

// Shutdown waits for a stream on an h2c connection to finish, as it does for an
// HTTP/1 request, instead of returning while the stream still runs.
func TestConfigureH2CShutdownWaitsForAnH2CStream(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	t.Cleanup(finish)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusNoContent)
	}))
	ConfigureH2C(server.Config)
	server.Start()
	t.Cleanup(server.Close)

	type result struct {
		status int
		proto  string
		err    error
	}
	results := make(chan result, 1)
	go func() {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
		if err != nil {
			results <- result{err: err}

			return
		}
		resp, err := h2cClient().Do(req)
		if err != nil {
			results <- result{err: err}

			return
		}
		defer resp.Body.Close()
		results <- result{status: resp.StatusCode, proto: resp.Proto}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the h2c request never reached the handler")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	shutdown := make(chan error, 1)
	go func() { shutdown <- server.Config.Shutdown(ctx) }()
	select {
	case err := <-shutdown:
		t.Fatalf("Shutdown returned while an h2c stream was running: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	finish()
	res := <-results
	require.NoError(t, res.err)
	assert.Equal(t, http.StatusNoContent, res.status)
	assert.Equal(t, "HTTP/2.0", res.proto)
	select {
	case err := <-shutdown:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("Shutdown did not return after the stream finished")
	}
}

// net/http reads the h2c preface under the server's header timeout, so a peer
// that stalls inside it is cut off like one that stalls in a request line.
func TestConfigureH2CBoundsAStalledPreface(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		sent string
	}{
		{name: "inside the request line", sent: "PRI * HT"},
		{name: "inside the preface", sent: "PRI * HTTP/2.0\r\n\r\nS"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// What arrived before the timeout may still be served as an HTTP/1 request.
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
			}))
			server.Config.ReadHeaderTimeout = 100 * time.Millisecond
			ConfigureH2C(server.Config)
			server.Start()
			t.Cleanup(server.Close)

			var dialer net.Dialer
			conn, err := dialer.DialContext(t.Context(), "tcp", server.Listener.Addr().String())
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			_, err = io.WriteString(conn, tt.sent)
			require.NoError(t, err)

			require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
			_, err = io.Copy(io.Discard, conn)
			require.NoError(t, err, "the server should close the connection at its header timeout")
		})
	}
}

// An HTTP/1.1 request asking to upgrade to h2c is served as HTTP/1.1, its whole
// body passed to the handler.
func TestConfigureH2CAnswersAnUpgradeRequestOverHTTP1(t *testing.T) {
	t.Parallel()

	body := strings.Repeat("a", 2<<20)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, err := io.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			return
		}
		assert.Len(t, got, len(body))
		w.WriteHeader(http.StatusNoContent)
	}))
	ConfigureH2C(server.Config)
	server.Start()
	t.Cleanup(server.Close)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Connection", "Upgrade, HTTP2-Settings")
	req.Header.Set("HTTP2-Settings", "AAMAAABkAAQAAP__")
	req.Header.Set("Upgrade", "h2c")

	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, "HTTP/1.1", resp.Proto)
}

func TestConfigureH2CPreservesParentIdleTimeout(t *testing.T) {
	t.Parallel()

	const parentIdleTimeout = 620 * time.Second

	server := &http.Server{
		Handler:     http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		IdleTimeout: parentIdleTimeout,
	}
	ConfigureH2C(server)

	require.Equal(t, parentIdleTimeout, server.IdleTimeout)
}
