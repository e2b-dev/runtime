package proxy

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
)

func TestProxyShutdownTracksUpgradedConnection(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"graceful", "deadline fallback", "forced"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			backendDone := make(chan struct{})
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				defer close(backendDone)
				conn, buffered, err := http.NewResponseController(w).Hijack()
				if !assert.NoError(t, err) {
					return
				}
				defer conn.Close()
				_, err = buffered.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: tunnel\r\nConnection: Upgrade\r\n\r\n")
				if !assert.NoError(t, err) || !assert.NoError(t, buffered.Flush()) {
					return
				}
				_, _ = io.Copy(conn, buffered)
			}))
			t.Cleanup(backend.Close)
			backendURL, err := url.Parse(backend.URL)
			require.NoError(t, err)
			proxy, port, err := newTestProxy(t, func(*http.Request) (*pool.Destination, error) {
				return &pool.Destination{
					Url:           backendURL,
					ConnectionKey: "upgraded-connection",
					RequestLogger: logger.NewNopLogger(),
				}, nil
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = proxy.Close() })

			var dialer net.Dialer
			client, err := dialer.DialContext(t.Context(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
			require.NoError(t, err)
			t.Cleanup(func() { _ = client.Close() })
			require.NoError(t, client.SetDeadline(time.Now().Add(5*time.Second)))
			_, err = io.WriteString(client, "GET /stream HTTP/1.1\r\nHost: sandbox.test\r\nUpgrade: tunnel\r\nConnection: Upgrade\r\n\r\n")
			require.NoError(t, err)
			buffered := bufio.NewReader(client)
			resp, err := http.ReadResponse(buffered, nil)
			require.NoError(t, err)
			require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
			require.NoError(t, resp.Body.Close())

			echo := func() {
				t.Helper()
				_, err := io.WriteString(client, "ping\n")
				require.NoError(t, err)
				got, err := buffered.ReadString('\n')
				require.NoError(t, err)
				require.Equal(t, "ping\n", got)
			}
			echo()

			switch mode {
			case "graceful":
				started := make(chan struct{})
				proxy.RegisterOnShutdown(func() { close(started) })
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- proxy.Shutdown(ctx) }()
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("shutdown did not start")
				}
				select {
				case err := <-done:
					t.Fatalf("shutdown returned before the peer closed: %v", err)
				case <-time.After(100 * time.Millisecond):
				}
				echo()
				require.NoError(t, client.Close())
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-ctx.Done():
					t.Fatal("shutdown did not finish after the peer closed")
				}
			case "deadline fallback":
				ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
				defer cancel()
				require.ErrorIs(t, proxy.Shutdown(ctx), context.DeadlineExceeded)
				echo()
				require.NoError(t, proxy.Close())
			case "forced":
				require.NoError(t, proxy.Close())
			}

			select {
			case <-backendDone:
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown left the backend tunnel open")
			}
			assert.Zero(t, proxy.CurrentServerConnections())
		})
	}
}

func TestProxyShutdownDrainsH2CStream(t *testing.T) {
	t.Parallel()

	finishResponse := make(chan struct{})
	finish := sync.OnceFunc(func() { close(finishResponse) })
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "before shutdown\n")
		if !assert.NoError(t, http.NewResponseController(w).Flush()) {
			return
		}
		select {
		case <-finishResponse:
			_, _ = io.WriteString(w, "after shutdown\n")
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(backend.Close)
	t.Cleanup(finish)
	backendURL, err := url.Parse(backend.URL)
	require.NoError(t, err)
	proxy, port, err := newTestProxy(t, func(*http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:           backendURL,
			ConnectionKey: "h2c-stream",
			RequestLogger: logger.NewNopLogger(),
		}, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, proxy.Close()) })

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			},
		},
	}
	t.Cleanup(client.CloseIdleConnections)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/stream", port), nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, 2, resp.ProtoMajor)
	buffered := bufio.NewReader(resp.Body)
	first, err := buffered.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "before shutdown\n", first)

	started := make(chan struct{})
	proxy.RegisterOnShutdown(func() { close(started) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- proxy.Shutdown(ctx) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("shutdown did not start")
	}
	select {
	case err := <-done:
		t.Fatalf("shutdown returned before the stream finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	finish()
	rest, err := io.ReadAll(buffered)
	require.NoError(t, err)
	require.Equal(t, "after shutdown\n", string(rest))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("shutdown did not finish after the stream completed")
	}
	assert.Zero(t, proxy.CurrentServerConnections())
}

func TestProxyShutdownReleasesFailedH2CPreface(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		tail     string
		keepOpen bool
	}{
		{name: "missing tail"},
		{name: "partial tail", tail: "S"},
		{name: "invalid tail", tail: "BADBAD"},
		{name: "stalled preface", keepOpen: true},
		{name: "stalled partial tail", tail: "S", keepOpen: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			proxy := New(0, SandboxProxyRetries, time.Minute, func(*http.Request) (*pool.Destination, error) {
				return nil, errors.New("unexpected proxy request")
			}, nil, false)
			hijacked := make(chan struct{})
			proxy.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateHijacked {
					close(hijacked)
				}
			}
			var listenConfig net.ListenConfig
			listener, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			serveDone := make(chan error, 1)
			go func() { serveDone <- proxy.Serve(listener) }()
			t.Cleanup(func() {
				assert.NoError(t, proxy.Close())
				assert.ErrorIs(t, <-serveDone, http.ErrServerClosed)
			})

			var dialer net.Dialer
			client, err := dialer.DialContext(t.Context(), "tcp", listener.Addr().String())
			require.NoError(t, err)
			t.Cleanup(func() { _ = client.Close() })
			_, err = io.WriteString(client, "PRI * HTTP/2.0\r\n\r\n"+tt.tail)
			require.NoError(t, err)
			select {
			case <-hijacked:
			case <-time.After(5 * time.Second):
				t.Fatal("H2C preface was not hijacked")
			}
			if tt.keepOpen {
				ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
				defer cancel()
				require.ErrorIs(t, proxy.Shutdown(ctx), context.DeadlineExceeded)
				require.NoError(t, proxy.Close())
				require.NoError(t, client.SetReadDeadline(time.Now().Add(5*time.Second)))
				_, err = client.Read(make([]byte, 1))
				require.ErrorIs(t, err, io.EOF)
			} else {
				require.NoError(t, client.Close())
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				require.NoError(t, proxy.Shutdown(ctx))
			}
			assert.Zero(t, proxy.CurrentServerConnections())
		})
	}
}
