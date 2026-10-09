package featureflags

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldlog"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:paralleltest // Constructors read process-wide credentials and endpoint configuration.
func TestCustomServiceEndpoints(t *testing.T) {
	originalKey := launchDarklyApiKey
	t.Cleanup(func() { launchDarklyApiKey = originalKey })
	launchDarklyApiKey = "local-project"

	for _, constructor := range []struct {
		name string
		new  func(string, string, string, ...Option) (*Client, error)
	}{
		{name: "default", new: NewClient},
		{name: "log level", new: func(environment, service, version string, opts ...Option) (*Client, error) {
			return NewClientWithLogLevel(environment, service, version, ldlog.Error, opts...)
		}},
	} {
		t.Run(constructor.name, func(t *testing.T) {
			updates := make(chan struct{}, 1)
			events := make(chan struct{}, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "local-project", r.Header.Get("Authorization"))
				switch r.URL.Path {
				case "/all":
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = fmt.Fprint(w, `event: put
data: {"path":"/","data":{"flags":{"local-bool":{"key":"local-bool","on":true,"version":1,"fallthrough":{"variation":0},"variations":[true]},"local-json":{"key":"local-json","on":true,"version":1,"fallthrough":{"variation":0},"variations":[{"enabled":true}]}},"segments":{}}}

`)
					w.(http.Flusher).Flush()
					for {
						select {
						case <-updates:
							_, _ = fmt.Fprint(w, `event: patch
data: {"path":"/flags/local-bool","data":{"key":"local-bool","on":true,"version":2,"fallthrough":{"variation":0},"variations":[false]}}

`)
							w.(http.Flusher).Flush()
						case <-r.Context().Done():
							return
						}
					}
				case "/bulk":
					_, _ = io.Copy(io.Discard, r.Body)
					w.WriteHeader(http.StatusAccepted)
					select {
					case events <- struct{}{}:
					default:
					}
				case "/diagnostic":
					w.WriteHeader(http.StatusAccepted)
				default:
					t.Errorf("unexpected SDK request: %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			t.Cleanup(server.Close)
			t.Setenv("LAUNCH_DARKLY_BASE_URL", server.URL)
			client, err := constructor.new("local", "test", "1.0.0")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, client.Close(context.WithoutCancel(t.Context()))) })
			require.True(t, client.Live())

			boolean := BoolFlag{name: "local-bool", fallback: false}
			require.True(t, client.BoolFlag(t.Context(), boolean))
			value := client.JSONFlag(t.Context(), JSONFlag{name: "local-json", fallback: ldvalue.Null()})
			require.True(t, value.GetByKey("enabled").BoolValue())

			updates <- struct{}{}
			require.Eventually(t, func() bool {
				return !client.BoolFlag(t.Context(), boolean)
			}, 5*time.Second, 10*time.Millisecond)
			client.ld.Flush()
			select {
			case <-events:
			case <-time.After(5 * time.Second):
				t.Fatal("SDK did not send events to the configured endpoint")
			}
		})
	}
}

//nolint:paralleltest // Constructors read process-wide credentials and endpoint configuration.
func TestCustomEndpointWithoutKeyStaysOffline(t *testing.T) {
	originalKey := launchDarklyApiKey
	t.Cleanup(func() { launchDarklyApiKey = originalKey })
	launchDarklyApiKey = ""
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	t.Setenv("LAUNCH_DARKLY_BASE_URL", server.URL)

	client, err := NewClient("local", "test", "1.0.0")
	require.NoError(t, err)
	require.False(t, client.Live())
	require.True(t, client.BoolFlag(t.Context(), BoolFlag{name: "absent-local-flag", fallback: true}))
	require.NoError(t, client.Close(context.WithoutCancel(t.Context())))
	require.Zero(t, requests.Load())
}
