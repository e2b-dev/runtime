package main

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"

	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/httpserver"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/servicetest"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// postStatus POSTs path on a new connection and returns the status.
func postStatus(t *testing.T, addr, path string) int {
	t.Helper()

	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+addr+path, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	return resp.StatusCode
}

// The port serves the gin server through the listener: an unauthenticated
// request reaches the application and gets its own 401, the port reports
// its mode from startup, and its flag moves it.
func TestHTTPServerServesThroughTheMTLSListener(t *testing.T) {
	t.Parallel()

	flags, source := servicetest.Flags(t)
	proc, metrics := servicetest.NewProcess(t, flags.StringReader())
	var settings mtls.ListenerSettings
	httpTLS, err := proc.Listener("http", httpModeFlag, settings)
	require.NoError(t, err)
	swagger, err := api.GetSpec()
	require.NoError(t, err)
	swagger.Servers = nil
	authenticate := sharedauth.CreateAuthenticationFunc([]sharedauth.Authenticator{sharedauth.NewAdminJWTAuthenticator(nil), sharedauth.NewAdminApiKeyAuthenticator("admin-key")}, nil)
	s := newHTTPServer(0, logger.NewNopLogger(), telemetry.NewNoopClient(), swagger, authenticate, api.ServerInterface(nil))
	inner, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	listener := mtls.WrapHTTPServer(s, inner, httpTLS, httpHealthPaths(settings))
	httpserver.ConfigureH2C(s)
	errs := startHTTPServer(s, listener)
	t.Cleanup(func() {
		_ = s.Close()
		<-errs
	})
	path := "/admin/user-profiles/resolve"

	assert.Equal(t, int64(1), servicetest.ListenerMode(t, metrics, "http", "off", mtls.SourceFallback), "reported from startup")
	assert.Equal(t, http.StatusUnauthorized, postStatus(t, inner.Addr().String(), path), "the application answers, not the guard")
	assert.Equal(t, int64(1), servicetest.Point(t, metrics, mtls.MetricHandshakes, attribute.String(mtls.AttrListener, "http"), attribute.String(mtls.AttrOutcome, mtls.OutcomePlaintext)))

	servicetest.SetFlag(source, httpModeFlag, "permissive")
	assert.Equal(t, http.StatusUnauthorized, postStatus(t, inner.Addr().String(), path))
	assert.Equal(t, int64(1), servicetest.ListenerMode(t, metrics, "http", "permissive", mtls.SourceFlag))
}

// With the environment's list unset a plaintext probe still reaches the
// health route under required, and a list the environment sets extends it.
func TestHTTPHealthPathsKeepTheProbeRoute(t *testing.T) {
	t.Parallel()

	assert.ElementsMatch(t, []string{"/health"}, httpHealthPaths(mtls.ListenerSettings{}))
	assert.ElementsMatch(t, []string{"/ready", "/health"}, httpHealthPaths(mtls.ListenerSettings{HealthPaths: []string{"/ready"}}))
}
