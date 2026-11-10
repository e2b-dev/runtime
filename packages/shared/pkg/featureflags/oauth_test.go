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
	ldclient "github.com/launchdarkly/go-server-sdk/v7"
	"github.com/launchdarkly/go-server-sdk/v7/subsystems"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func configureOAuthTest(t *testing.T, server *httptest.Server) {
	t.Helper()
	originalKey, originalTransport := launchDarklyApiKey, http.DefaultTransport
	t.Cleanup(func() {
		launchDarklyApiKey, http.DefaultTransport = originalKey, originalTransport
	})
	launchDarklyApiKey = ""
	http.DefaultTransport = server.Client().Transport
	t.Setenv("LAUNCH_DARKLY_BASE_URL", server.URL)
	t.Setenv("LAUNCH_DARKLY_OAUTH_TOKEN_URL", server.URL+"/token")
	t.Setenv("LAUNCH_DARKLY_OAUTH_CLIENT_ID", "deployment-client")
	t.Setenv("LAUNCH_DARKLY_OAUTH_CLIENT_SECRET", "deployment-secret")
	t.Setenv("LAUNCH_DARKLY_OAUTH_AUDIENCE", "gateway-audience")
}

//nolint:paralleltest // Constructors read process-wide credentials and use the default HTTP transport.
func TestOAuthClient(t *testing.T) {
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
			var tokens, streams, firstTokenRequests atomic.Int64
			reconnect := make(chan struct{})
			events, diagnostics := make(chan string, 2), make(chan string, 2)
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/token" {
					assert.Equal(t, http.MethodPost, r.Method)
					if !assert.NoError(t, r.ParseForm()) {
						w.WriteHeader(http.StatusBadRequest)

						return
					}
					assert.Equal(t, "client_credentials", r.Form.Get("grant_type"))
					assert.Equal(t, "deployment-client", r.Form.Get("client_id"))
					assert.Equal(t, "deployment-secret", r.Form.Get("client_secret"))
					assert.Equal(t, "gateway-audience", r.Form.Get("audience"))
					n := tokens.Add(1)
					// The startup mint and the first SDK request both get a
					// token already inside the refresh window, so every later
					// request proves a refresh happened.
					expiresIn := 3600
					if n <= 2 {
						expiresIn = 1
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = fmt.Fprintf(w, `{"access_token":"token-%d","token_type":"Bearer","expires_in":%d}`, n, expiresIn)

					return
				}
				auth := r.Header.Get("Authorization")
				assert.Contains(t, []string{"Bearer token-2", "Bearer token-3"}, auth)
				if auth == "Bearer token-2" {
					firstTokenRequests.Add(1)
				}
				switch r.URL.Path {
				case "/all":
					n := streams.Add(1)
					if n > 1 {
						assert.Equal(t, "Bearer token-3", auth)
					}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = fmt.Fprintf(w, "event: put\ndata: {\"path\":\"/\",\"data\":{\"flags\":{\"oauth-flag\":{\"key\":\"oauth-flag\",\"on\":true,\"version\":%d,\"fallthrough\":{\"variation\":0},\"variations\":[%t]}},\"segments\":{}}}\n\n", n, n == 1)
					w.(http.Flusher).Flush()
					if n == 1 {
						select {
						case <-reconnect:
						case <-r.Context().Done():
						}
					} else {
						<-r.Context().Done()
					}
				case "/bulk":
					_, _ = io.Copy(io.Discard, r.Body)
					w.WriteHeader(http.StatusAccepted)
					select {
					case events <- auth:
					default:
					}
				case "/diagnostic":
					w.WriteHeader(http.StatusAccepted)
					diagnostics <- auth
				default:
					t.Errorf("unexpected SDK request: %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			t.Cleanup(server.Close)
			configureOAuthTest(t, server)
			client, err := constructor.new("test", "test-service", "1.0.0")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, client.Close(context.WithoutCancel(t.Context()))) })
			assert.True(t, client.Live())
			flag := BoolFlag{name: "oauth-flag", fallback: false}
			assert.True(t, client.BoolFlag(t.Context(), flag))
			client.ld.Flush()
			for _, ch := range []chan string{events, diagnostics} {
				select {
				case <-ch:
				case <-time.After(5 * time.Second):
					t.Fatal("SDK did not send an authenticated event request")
				}
			}
			assert.EqualValues(t, 3, tokens.Load())
			assert.EqualValues(t, 1, firstTokenRequests.Load())

			close(reconnect)
			require.Eventually(t, func() bool {
				return !client.BoolFlag(t.Context(), flag)
			}, 5*time.Second, 10*time.Millisecond)
			client.ld.Flush()
			select {
			case auth := <-events:
				assert.Equal(t, "Bearer token-3", auth)
			case <-time.After(5 * time.Second):
				t.Fatal("SDK did not send events after token refresh")
			}
			assert.EqualValues(t, 3, tokens.Load())
		})
	}
}

//nolint:paralleltest // Constructors read process-wide credentials.
func TestOAuthConfigurationRejected(t *testing.T) {
	for _, tc := range []struct {
		name, key, value string
	}{
		{name: "missing gateway", key: "LAUNCH_DARKLY_BASE_URL"},
		{name: "missing client ID", key: "LAUNCH_DARKLY_OAUTH_CLIENT_ID"},
		{name: "missing client secret", key: "LAUNCH_DARKLY_OAUTH_CLIENT_SECRET"},
		{name: "missing token URL", key: "LAUNCH_DARKLY_OAUTH_TOKEN_URL"},
		{name: "insecure gateway", key: "LAUNCH_DARKLY_BASE_URL", value: "http://flags.example.test"},
		{name: "insecure token URL", key: "LAUNCH_DARKLY_OAUTH_TOKEN_URL", value: "http://auth.example.test/token"},
		{name: "gateway credentials", key: "LAUNCH_DARKLY_BASE_URL", value: "https://user:secret@flags.example.test"},
		{name: "token URL query", key: "LAUNCH_DARKLY_OAUTH_TOKEN_URL", value: "https://auth.example.test/token?secret=hidden"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			originalKey := launchDarklyApiKey
			t.Cleanup(func() { launchDarklyApiKey = originalKey })
			launchDarklyApiKey = ""
			t.Setenv("LAUNCH_DARKLY_BASE_URL", "https://flags.example.test")
			t.Setenv("LAUNCH_DARKLY_OAUTH_TOKEN_URL", "https://auth.example.test/token")
			t.Setenv("LAUNCH_DARKLY_OAUTH_CLIENT_ID", "deployment-client")
			t.Setenv("LAUNCH_DARKLY_OAUTH_CLIENT_SECRET", "deployment-secret")
			t.Setenv(tc.key, tc.value)
			client, err := NewClient("test", "test-service", "1.0.0")
			require.Error(t, err)
			assert.Nil(t, client)
			assert.NotContains(t, err.Error(), "hidden")
			assert.NotContains(t, err.Error(), "deployment-secret")
		})
	}
}

//nolint:paralleltest // Constructors read process-wide credentials and use the default HTTP transport.
func TestOAuthStartupRejected(t *testing.T) {
	for _, tc := range []struct {
		name    string
		token   http.HandlerFunc
		gateway int
	}{
		{name: "issuer rejects identity", token: func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = fmt.Fprint(w, `{"error":"invalid_client","error_description":"private-response-body"}`)
		}},
		{name: "token URL redirects", token: func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/unexpected", http.StatusTemporaryRedirect)
		}},
		{name: "gateway rejects identity", token: issueToken, gateway: http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gatewayRequests atomic.Int64
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/token" {
					tc.token(w, r)

					return
				}
				gatewayRequests.Add(1)
				w.WriteHeader(tc.gateway)
			}))
			t.Cleanup(server.Close)
			configureOAuthTest(t, server)

			client, err := NewClientWithLogLevel("test", "test-service", "1.0.0", ldlog.Error, WithStartOnInitTimeout())
			require.Error(t, err)
			assert.Nil(t, client)
			assert.NotContains(t, err.Error(), "private-response-body")
			assert.NotContains(t, err.Error(), "deployment-secret")
			if tc.gateway == 0 {
				assert.Zero(t, gatewayRequests.Load())
			}
		})
	}
}

//nolint:paralleltest // OAuth configuration uses process-wide environment and HTTP transport.
func TestOAuthTransportFailures(t *testing.T) {
	for _, mode := range []string{"issuer unavailable", "gateway redirect"} {
		t.Run(mode, func(t *testing.T) {
			var forwarded atomic.Int64
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/token":
					if mode == "issuer unavailable" {
						w.WriteHeader(http.StatusServiceUnavailable)

						return
					}
					issueToken(w, r)
				case "/all":
					http.Redirect(w, r, "/unexpected", http.StatusTemporaryRedirect)
				default:
					forwarded.Add(1)
				}
			}))
			t.Cleanup(server.Close)
			configureOAuthTest(t, server)
			var cfg ldclient.Config
			key, err := configureOAuth(&cfg)
			require.NoError(t, err)
			if mode == "issuer unavailable" {
				return
			}
			httpConfig, err := cfg.HTTP.Build(subsystems.BasicClientContext{SDKKey: key})
			require.NoError(t, err)
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/all", nil)
			require.NoError(t, err)
			res, err := httpConfig.CreateHTTPClient().Do(req)
			require.NoError(t, err)
			defer res.Body.Close()
			assert.Equal(t, http.StatusTemporaryRedirect, res.StatusCode)
			assert.Zero(t, forwarded.Load())
		})
	}
}

// Once the gateway has accepted the identity, a 401 is retried with a fresh
// token instead of stopping the SDK for the life of the process.
//
//nolint:paralleltest // Constructors read process-wide credentials and use the default HTTP transport.
func TestOAuthGatewayRejectionAfterStart(t *testing.T) {
	var tokens, streams, rejected atomic.Int64
	events := make(chan string, 4)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			n := tokens.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"access_token":"token-%d","token_type":"Bearer","expires_in":3600}`, n)
		case "/all":
			n := streams.Add(1)
			if n == 2 {
				rejected.Add(1)
				w.WriteHeader(http.StatusUnauthorized)

				return
			}
			if n > 2 {
				assert.Equal(t, "Bearer token-2", r.Header.Get("Authorization"))
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprintf(w, "event: put\ndata: {\"path\":\"/\",\"data\":{\"flags\":{\"oauth-flag\":{\"key\":\"oauth-flag\",\"on\":true,\"version\":%d,\"fallthrough\":{\"variation\":0},\"variations\":[%t]}},\"segments\":{}}}\n\n", n, n == 1)
			w.(http.Flusher).Flush()
			if n > 1 {
				<-r.Context().Done()
			}
		case "/bulk":
			_, _ = io.Copy(io.Discard, r.Body)
			if rejected.Load() > 0 && r.Header.Get("Authorization") == "Bearer token-1" {
				w.WriteHeader(http.StatusUnauthorized)

				return
			}
			w.WriteHeader(http.StatusAccepted)
			select {
			case events <- r.Header.Get("Authorization"):
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
	configureOAuthTest(t, server)

	client, err := NewClientWithLogLevel("test", "test-service", "1.0.0", ldlog.Error)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close(context.WithoutCancel(t.Context()))) })
	flag := BoolFlag{name: "oauth-flag", fallback: false}
	require.True(t, client.BoolFlag(t.Context(), flag))

	require.Eventually(t, func() bool {
		return !client.BoolFlag(t.Context(), flag)
	}, 10*time.Second, 10*time.Millisecond, "stream did not recover from the gateway rejection")
	assert.True(t, client.Live())
	assert.EqualValues(t, 2, tokens.Load())

	client.ld.Flush()
	require.Eventually(t, func() bool {
		select {
		case auth := <-events:
			return auth == "Bearer token-2"
		default:
			client.ld.Flush()

			return false
		}
	}, 10*time.Second, 100*time.Millisecond, "events did not resume with the fresh token")
}

func issueToken(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprint(w, `{"access_token":"token","token_type":"Bearer","expires_in":3600}`)
}
