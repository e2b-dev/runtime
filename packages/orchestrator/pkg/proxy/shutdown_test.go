//go:build linux

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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/connlimit"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	reverseproxy "github.com/e2b-dev/infra/packages/shared/pkg/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
)

func TestSandboxProxyCloseInterruptsStalledUpload(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name           string
		forced         bool
		closeErr       error
		completeUpload bool
	}{
		{name: "deadline fallback"},
		{name: "forced", forced: true},
		{name: "close failure", forced: true, closeErr: errors.New("listener close failed")},
		{name: "shutdown failure", closeErr: errors.New("listener shutdown failed"), completeUpload: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
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
			if tt.closeErr != nil {
				listener = &closeErrorListener{Listener: listener, err: tt.closeErr}
			}
			serveDone := make(chan error, 1)
			go func() { serveDone <- proxy.proxy.Serve(listener) }()
			t.Cleanup(func() {
				assert.NoError(t, proxy.proxy.Close())
				assert.ErrorIs(t, <-serveDone, http.ErrServerClosed)
			})

			var dialer net.Dialer
			client, err := dialer.DialContext(t.Context(), "tcp", listener.Addr().String())
			require.NoError(t, err)
			t.Cleanup(func() { _ = client.Close() })
			contentLength := 1024
			if tt.completeUpload {
				contentLength = 1
			}
			_, err = fmt.Fprintf(client, "POST /upload HTTP/1.1\r\nHost: sandbox.test\r\nContent-Length: %d\r\n\r\nx", contentLength)
			require.NoError(t, err)
			select {
			case err := <-backendStarted:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("backend did not receive the upload")
			}
			backend.CloseClientConnections()
			if tt.completeUpload {
				select {
				case <-handlerDone:
				case <-time.After(5 * time.Second):
					t.Fatal("completed upload did not finish")
				}
			}

			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			if tt.forced {
				cancel()
			}
			err = proxy.Close(ctx)
			if tt.closeErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.closeErr)
			}

			select {
			case <-handlerDone:
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown left the upload handler blocked")
			}
			assert.Zero(t, proxy.proxy.CurrentServerConnections())
		})
	}
}

type closeErrorListener struct {
	net.Listener

	err error
}

func (l *closeErrorListener) Close() error {
	_ = l.Listener.Close()

	return l.err
}
