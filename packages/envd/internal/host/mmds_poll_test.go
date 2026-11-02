package host

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/utils"
)

func TestPollForMMDSOptsHTTP(t *testing.T) {
	for _, collector := range []string{"http://collector.test", ""} {
		t.Run("collector="+collector, func(t *testing.T) {
			runDir := t.TempDir()
			want := MMDSOpts{SandboxID: "fixture-sandbox", TemplateID: "fixture-template", LogsCollectorAddress: collector}
			var tokenCalls, optionCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, mmdsDefaultAddress, r.Host)
				switch r.URL.Path {
				case "/latest/api/token":
					assert.Equal(t, http.MethodPut, r.Method)
					assert.Equal(t, "60", r.Header.Get("X-metadata-token-ttl-seconds"))
					tokenCalls.Add(1)
					_, _ = w.Write([]byte("fixture-metadata-token"))
				case "/":
					assert.Equal(t, http.MethodGet, r.Method)
					assert.Equal(t, "fixture-metadata-token", r.Header.Get("X-metadata-token"))
					assert.Equal(t, "application/json", r.Header.Get("Accept"))
					if optionCalls.Add(1) == 1 {
						// A failed options fetch must retry token + options, not
						// publish partial identity or terminate the cloud refresh.
						_, _ = w.Write([]byte("invalid fixture json"))
						return
					}
					_ = json.NewEncoder(w).Encode(want)
				default:
					t.Errorf("unexpected metadata path: %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			t.Cleanup(server.Close)
			transport := &http.Transport{
				DisableKeepAlives: true,
				DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
					assert.Equal(t, mmdsDefaultAddress+":80", address)
					return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
				},
			}
			t.Cleanup(transport.CloseIdleConnections)
			client := &http.Client{Timeout: mmdsAccessTokenRequestClientTimeout, Transport: transport}
			env := utils.NewEnvVars()
			env.Store("EXPLICIT_INIT_VALUE", "retained")
			out := make(chan *MMDSOpts, 1)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			pollForMMDSOpts(ctx, out, env, false, client, runDir)
			require.NoError(t, ctx.Err())
			require.EqualValues(t, 2, tokenCalls.Load())
			require.EqualValues(t, 2, optionCalls.Load())
			for key, wantValue := range map[string]string{"E2B_SANDBOX_ID": want.SandboxID, "E2B_TEMPLATE_ID": want.TemplateID, "EXPLICIT_INIT_VALUE": "retained"} {
				value, ok := env.Load(key)
				require.True(t, ok)
				require.Equal(t, wantValue, value)
			}
			for name, wantValue := range map[string]string{".E2B_SANDBOX_ID": want.SandboxID, ".E2B_TEMPLATE_ID": want.TemplateID} {
				value, err := os.ReadFile(filepath.Join(runDir, name))
				require.NoError(t, err)
				require.Equal(t, wantValue, string(value))
			}
			if collector == "" {
				require.Empty(t, out, "no log options are published without a collector")
			} else {
				select {
				case opts := <-out:
					require.Equal(t, want, *opts)
				default:
					t.Fatal("successful metadata fetch did not publish log options")
				}
			}
		})
	}
}

func TestNativePollForMMDSOptsDoesNotFetch(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
		},
	}
	t.Cleanup(transport.CloseIdleConnections)
	runDir := t.TempDir()
	env := utils.NewEnvVars()
	env.Store("E2B_SANDBOX_ID", "explicit-native-id")
	env.Store("E2B_TEMPLATE_ID", "explicit-template-id")
	out := make(chan *MMDSOpts, 1)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	pollForMMDSOpts(ctx, out, env, true, &http.Client{Transport: transport}, runDir)
	require.NoError(t, ctx.Err(), "native must return without waiting for metadata")
	require.Zero(t, requests.Load(), "native must not issue token or options requests")
	require.Empty(t, out)
	for key, want := range map[string]string{"E2B_SANDBOX_ID": "explicit-native-id", "E2B_TEMPLATE_ID": "explicit-template-id"} {
		value, ok := env.Load(key)
		require.True(t, ok)
		require.Equal(t, want, value)
	}
	files, err := os.ReadDir(runDir)
	require.NoError(t, err)
	require.Empty(t, files, "native must not write cloud identity files")
}
