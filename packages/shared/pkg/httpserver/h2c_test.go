package httpserver

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
)

func TestConfigureH2CRejectsIncompleteOversizedUpgrade(t *testing.T) {
	t.Parallel()

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("oversized upgrade reached the application handler")
	}))
	ConfigureH2C(server.Config)
	server.Start()
	t.Cleanup(server.Close)

	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), "tcp", server.Listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	_, err = fmt.Fprintf(conn, "POST /upload HTTP/1.1\r\nHost: sandbox.test\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: AAMAAABkAAQAAP__\r\nContent-Length: %d\r\n\r\n", h2cUpgradeBodyLimit+2)
	require.NoError(t, err)
	_, err = io.WriteString(conn, strings.Repeat("x", h2cUpgradeBodyLimit+1))
	require.NoError(t, err)

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.True(t, resp.Close)
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

	h2Client := &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			},
		},
	}

	h2Req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	require.NoError(t, err)

	h2Resp, err := h2Client.Do(h2Req)
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

func TestConfigureH2CLimitsUpgradeRequestBodyOnly(t *testing.T) {
	t.Parallel()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			t.Errorf("copy request body: %v", err)

			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	server := httptest.NewUnstartedServer(nil)
	server.Config.Handler = handler
	ConfigureH2C(server.Config)
	server.Start()
	t.Cleanup(server.Close)

	h1Req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		server.URL,
		strings.NewReader(strings.Repeat("a", h2cUpgradeBodyLimit+1)),
	)
	require.NoError(t, err)

	h1Resp, err := server.Client().Do(h1Req)
	require.NoError(t, err)
	defer h1Resp.Body.Close()

	require.Equal(t, http.StatusNoContent, h1Resp.StatusCode)

	upgradeReq, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		server.URL,
		struct{ io.Reader }{strings.NewReader(strings.Repeat("a", h2cUpgradeBodyLimit+1))},
	)
	require.NoError(t, err)
	upgradeReq.Header.Set("Connection", "Upgrade, HTTP2-Settings")
	upgradeReq.Header.Set("HTTP2-Settings", "AAMAAABkAAQAAP__")
	upgradeReq.Header.Set("Upgrade", "h2c")

	upgradeResp, err := server.Client().Do(upgradeReq)
	require.NoError(t, err)
	defer upgradeResp.Body.Close()

	require.Equal(t, http.StatusInternalServerError, upgradeResp.StatusCode)
}

func TestConfigureH2CLimitsAllStdlibUpgradeMatches(t *testing.T) {
	t.Parallel()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			t.Errorf("copy request body: %v", err)

			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	server := httptest.NewUnstartedServer(nil)
	server.Config.Handler = handler
	ConfigureH2C(server.Config)
	server.Start()
	t.Cleanup(server.Close)

	tests := []struct {
		name       string
		connection string
		upgrade    string
	}{
		{
			name:       "upgrade header token list",
			connection: "Upgrade, HTTP2-Settings",
			upgrade:    "foo, h2c",
		},
		{
			name:       "connection only names HTTP2 settings",
			connection: "HTTP2-Settings",
			upgrade:    "h2c",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequestWithContext(
				t.Context(),
				http.MethodPost,
				server.URL,
				struct{ io.Reader }{strings.NewReader(strings.Repeat("a", h2cUpgradeBodyLimit+1))},
			)
			require.NoError(t, err)
			req.Header.Set("Connection", tt.connection)
			req.Header.Set("HTTP2-Settings", "AAMAAABkAAQAAP__")
			req.Header.Set("Upgrade", tt.upgrade)

			resp, err := server.Client().Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		})
	}
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

func TestNewHTTP2ServerConfiguresH2SpecificTimeouts(t *testing.T) {
	t.Parallel()

	h2Server := newHTTP2Server()

	require.Equal(t, uint32(100), h2Server.MaxConcurrentStreams)
	require.Zero(t, h2Server.IdleTimeout)
	require.Equal(t, 30*time.Second, h2Server.ReadIdleTimeout)
}
