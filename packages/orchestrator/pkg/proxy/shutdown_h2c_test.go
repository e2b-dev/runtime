//go:build linux

package proxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"

	"github.com/e2b-dev/infra/packages/shared/pkg/connlimit"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	reverseproxy "github.com/e2b-dev/infra/packages/shared/pkg/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
)

func TestSandboxProxyCloseInterruptsStalledH2CUpload(t *testing.T) {
	t.Parallel()

	for _, forced := range []bool{false, true} {
		t.Run(fmt.Sprintf("forced=%t", forced), func(t *testing.T) {
			t.Parallel()

			backendStarted := make(chan error, 1)
			backend := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				_, err := io.ReadFull(r.Body, make([]byte, 1))
				backendStarted <- err
				_, _ = io.Copy(io.Discard, r.Body)
			}))
			t.Cleanup(backend.Close)
			backendURL, err := url.Parse(backend.URL)
			require.NoError(t, err)

			handlerDone := make(chan struct{})
			proxy := &SandboxProxy{proxy: reverseproxy.New(0, reverseproxy.SandboxProxyRetries, time.Minute,
				func(*http.Request) (*pool.Destination, error) {
					return &pool.Destination{
						Url:           backendURL,
						ConnectionKey: "upload-lifecycle",
						RequestLogger: logger.NewNopLogger(),
					}, nil
				}, &reverseproxy.ConnectionLimitConfig{
					Limiter:              connlimit.NewConnectionLimiter(),
					GetMaxLimit:          func(context.Context) int { return 10 },
					OnConnectionReleased: func(context.Context, int64) { close(handlerDone) },
				}, true)}
			var listenConfig net.ListenConfig
			listener, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			serveDone := make(chan error, 1)
			go func() { serveDone <- proxy.proxy.Serve(listener) }()
			t.Cleanup(func() {
				assert.NoError(t, proxy.proxy.Close())
				assert.ErrorIs(t, <-serveDone, http.ErrServerClosed)
			})

			client := &http.Client{Transport: &http2.Transport{
				AllowHTTP: true,
				DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, network, addr)
				},
			}}
			t.Cleanup(client.CloseIdleConnections)
			reader, writer := io.Pipe()
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+listener.Addr().String()+"/upload", reader)
			require.NoError(t, err)
			req.ContentLength = 8193
			clientDone := make(chan struct{})
			go func() {
				defer close(clientDone)
				resp, err := client.Do(req)
				if err == nil {
					_ = resp.Body.Close()
				}
			}()
			t.Cleanup(func() {
				_ = writer.Close()
				_ = reader.Close()
				select {
				case <-clientDone:
				case <-time.After(5 * time.Second):
					t.Error("upload client did not exit")
				}
			})
			_, err = io.WriteString(writer, strings.Repeat("x", 8192))
			require.NoError(t, err)
			select {
			case err := <-backendStarted:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("backend did not receive the upload")
			}
			backend.CloseClientConnections()

			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			if forced {
				cancel()
			}
			require.NoError(t, proxy.Close(ctx))
			select {
			case <-handlerDone:
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown left the upload handler blocked")
			}
			assert.Zero(t, proxy.proxy.CurrentServerConnections())
		})
	}
}
