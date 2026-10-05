//go:build linux

package factories

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClosePprofServerFinishesActiveRequest(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		mode     string
		forced   bool
		closeErr error
	}{
		{mode: "graceful"},
		{mode: "deadline fallback"},
		{mode: "forced", forced: true},
		{mode: "forced close failure", forced: true, closeErr: errors.New("listener close failed")},
		{mode: "shutdown failure", closeErr: errors.New("listener shutdown failed")},
	} {
		t.Run(tt.mode, func(t *testing.T) {
			t.Parallel()

			release := make(chan struct{})
			handlerDone := make(chan bool, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_ = http.NewResponseController(w).Flush()
				select {
				case <-release:
					_, _ = io.WriteString(w, "complete")
					handlerDone <- true
				case <-r.Context().Done():
					handlerDone <- false
				}
			}))
			if tt.closeErr != nil {
				server.Listener = &pprofCloseErrorListener{Listener: server.Listener, err: tt.closeErr}
			}
			server.Start()
			t.Cleanup(server.Close)
			shutdownStarted := make(chan struct{})
			server.Config.RegisterOnShutdown(func() { close(shutdownStarted) })

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/debug/pprof/profile", nil)
			require.NoError(t, err)
			resp, err := server.Client().Do(req)
			require.NoError(t, err)
			t.Cleanup(func() { _ = resp.Body.Close() })

			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			if tt.forced {
				cancel()
			}

			closeDone := make(chan error, 1)
			go func() { closeDone <- closePprofServer(ctx, server.Config) }()
			if !tt.forced {
				select {
				case <-shutdownStarted:
				case <-time.After(5 * time.Second):
					t.Fatal("pprof shutdown did not start")
				}
			}
			graceful := tt.mode == "graceful" || tt.mode == "shutdown failure"
			if graceful {
				close(release)
			}

			select {
			case err := <-closeDone:
				if tt.closeErr == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, tt.closeErr)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("pprof shutdown did not finish")
			}
			if tt.mode == "deadline fallback" {
				require.NoError(t, ctx.Err(), "pprof must enforce its own shutdown deadline")
			}
			select {
			case completed := <-handlerDone:
				assert.Equal(t, graceful, completed)
			case <-time.After(5 * time.Second):
				t.Fatal("pprof request did not exit")
			}

			if graceful {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				assert.Equal(t, "complete", string(body))
			}
		})
	}
}

type pprofCloseErrorListener struct {
	net.Listener

	err error
}

func (l *pprofCloseErrorListener) Close() error {
	_ = l.Listener.Close()

	return l.err
}
