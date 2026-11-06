package proxy

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	reverseproxy "github.com/e2b-dev/infra/packages/shared/pkg/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
)

type stubResumer struct {
	route *catalog.SandboxInfo
	err   error
}

func (s stubResumer) Resume(_ context.Context, _ string, _ uint64, _ string, _ string) (*catalog.SandboxInfo, error) {
	return s.route, s.err
}

type recordingResumer struct {
	sandboxID          string
	sandboxPort        uint64
	trafficAccessToken string
	envdAccessToken    string
}

func (r *recordingResumer) Resume(_ context.Context, sandboxID string, sandboxPort uint64, trafficAccessToken string, envdAccessToken string) (*catalog.SandboxInfo, error) {
	r.sandboxID = sandboxID
	r.sandboxPort = sandboxPort
	r.trafficAccessToken = trafficAccessToken
	r.envdAccessToken = envdAccessToken

	return testRoute, nil
}

// placementStatusWithCode is the status the API sends for a placement
// failure: the gRPC code its HTTP status maps to, with the semantic error
// code as an ErrorInfo detail (the shape resumeStatusError on the API side
// builds).
func placementStatusWithCode(t *testing.T, grpcCode codes.Code, code string) error {
	t.Helper()

	st, err := status.New(grpcCode, "Failed to place sandbox: "+code).
		WithDetails(&errdetails.ErrorInfo{Reason: code, Domain: proxygrpc.ResumeErrorDomain})
	require.NoError(t, err)

	return st.Err()
}

// placementStatusWithHTTPStatus is placementStatusWithCode with the HTTP
// status the API answered with carried on the detail, as the API sends it
// since it attaches the status.
func placementStatusWithHTTPStatus(t *testing.T, grpcCode codes.Code, code string, httpStatus int) error {
	t.Helper()

	st, err := status.New(grpcCode, "Failed to place sandbox: "+code).
		WithDetails(&errdetails.ErrorInfo{
			Reason:   code,
			Domain:   proxygrpc.ResumeErrorDomain,
			Metadata: map[string]string{proxygrpc.ResumeErrorHTTPStatusKey: strconv.Itoa(httpStatus)},
		})
	require.NoError(t, err)

	return st.Err()
}

func newFF(t *testing.T) *featureflags.Client {
	t.Helper()

	source := ldtestdata.DataSource()

	ff, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return ff
}

func newFFWithOrchAcceptsCombinedHost(t *testing.T, enabled bool) *featureflags.Client {
	t.Helper()

	source := ldtestdata.DataSource()
	source.Update(source.Flag(featureflags.OrchAcceptsCombinedHostFlag.Key()).VariationForAll(enabled))

	ff, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return ff
}

// A record hit is the route, placement included, as the orchestrator wrote it.
func TestCatalogResolution_CatalogHit(t *testing.T) {
	t.Parallel()

	c := catalog.NewRedisSandboxCatalog(redis_utils.SetupInstance(t))

	stored := &catalog.SandboxInfo{
		OrchestratorIP: "10.0.0.1",
		ExecutionID:    "exec",
		StartedAt:      time.Unix(1_700_000_000, 0).UTC(),
		LifecycleID:    "lifecycle",
		NetworkPlacement: &catalog.NetworkPlacement{
			NetworkMode: catalog.NetworkModeEgressRouter, RouterID: "router", PortableIP: "10.9.0.4", IngressAddress: "10.0.0.1", IngressPort: 5007,
		},
	}
	require.NoError(t, c.StoreSandbox(t.Context(), "sbx", stored, time.Minute))

	route, err := catalogResolution(t.Context(), "sbx", 8000, "", "", c, nil)
	require.NoError(t, err)
	require.Equal(t, stored, route)
}

func TestClientProxyMaskRequestHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		flagEnabled bool
		host        string
		want        *string
	}{
		{
			name:        "flag disabled masks sandbox shared host",
			flagEnabled: false,
			host:        "sandbox.e2b.app",
			want:        new("49983-sbx.e2b.app"),
		},
		{
			name:        "flag enabled preserves sandbox shared host",
			flagEnabled: true,
			host:        "sandbox.e2b.app",
			want:        nil,
		},
		{
			name:        "flag enabled preserves sandbox shared host with port",
			flagEnabled: true,
			host:        "sandbox.e2b.app:443",
			want:        nil,
		},
		{
			name:        "leaves localhost unchanged",
			flagEnabled: false,
			host:        "localhost:3000",
			want:        nil,
		},
		{
			name:        "leaves loopback unchanged",
			flagEnabled: false,
			host:        "127.0.0.1:3000",
			want:        nil,
		},
		{
			name:        "leaves regular sandbox host unchanged",
			flagEnabled: true,
			host:        "49983-sbx.e2b.app",
			want:        nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ff := newFFWithOrchAcceptsCombinedHost(t, tt.flagEnabled)

			require.Equal(t, tt.want, clientProxyMaskRequestHost(t.Context(), ff, tt.host, "sbx", 49983))
		})
	}
}

func TestCatalogResolution_CatalogMiss(t *testing.T) {
	t.Parallel()

	c := catalog.NewRedisSandboxCatalog(redis_utils.SetupInstance(t))

	_, err := catalogResolution(t.Context(), "sbx", 8000, "", "", c, nil)
	require.ErrorIs(t, err, ErrNodeNotFound)
}

type errorCatalog struct {
	err error
}

func (e errorCatalog) GetSandbox(_ context.Context, _ string) (*catalog.SandboxInfo, error) {
	return nil, e.err
}

func (e errorCatalog) StoreSandbox(_ context.Context, _ string, _ *catalog.SandboxInfo, _ time.Duration) error {
	return nil
}

func (e errorCatalog) DeleteSandbox(_ context.Context, _, _, _ string) error {
	return nil
}

func (e errorCatalog) Close(_ context.Context) error {
	return nil
}

func TestCatalogResolution_CatalogReturnsGenericError(t *testing.T) {
	t.Parallel()

	c := errorCatalog{err: errors.New("catalog unavailable")}

	route, err := catalogResolution(t.Context(), "sbx", 8000, "", "", c, nil)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrNodeNotFound)
	require.NotErrorIs(t, err, ErrNodeRouteUnavailable)
	require.Nil(t, route)
	require.Contains(t, err.Error(), "catalog unavailable")
}

func TestHandlePausedSandbox(t *testing.T) {
	t.Parallel()

	internalErr := status.Error(codes.Internal, "boom")
	deadlineErr := status.Error(codes.DeadlineExceeded, "orchestrator did not answer")
	internalPlacementErr := placementStatusWithCode(t, codes.Internal, "internal_server_error")
	createRefusedErr := placementStatusWithCode(t, codes.Internal, proxygrpc.PlacementCreateRefused)
	failedPreconditionErr := status.Error(codes.FailedPrecondition, "sandbox resume precondition failed")

	tests := []struct {
		name       string
		resumer    SandboxResumer
		trafficTok string
		envdTok    string
		wantRoute  *catalog.SandboxInfo
		wantResult autoResumeResult
		wantErrIs  error
		checkErr   func(*testing.T, error)
	}{
		{
			name:       "no resumer",
			resumer:    nil,
			wantResult: autoResumeNotAllowed,
		},
		{
			name:       "no resumer ignores tokens",
			resumer:    nil,
			trafficTok: "wrong-token",
			envdTok:    "envd-token",
			wantResult: autoResumeNotAllowed,
		},
		{
			name:       "resume returns not found",
			resumer:    stubResumer{err: status.Error(codes.NotFound, "not allowed")},
			trafficTok: "token",
			wantResult: autoResumeNotAllowed,
		},
		{
			name:       "resume returns snapshot not found",
			resumer:    stubResumer{err: status.Error(codes.NotFound, "snapshot not found")},
			trafficTok: "token",
			wantResult: autoResumeNotAllowed,
		},
		{
			name:       "resume permission denied",
			resumer:    stubResumer{err: status.Error(codes.PermissionDenied, "team is blocked")},
			trafficTok: "token",
			wantResult: autoResumePermissionDenied,
			checkErr: func(t *testing.T, err error) {
				t.Helper()

				var deniedErr *reverseproxy.SandboxResumePermissionDeniedError
				require.ErrorAs(t, err, &deniedErr)
				require.Equal(t, "sbx", deniedErr.SandboxID())
				// The API's refusal stays the cause, so the log says why.
				require.Equal(t, codes.PermissionDenied, status.Code(err))
				require.ErrorContains(t, err, "team is blocked")
			},
		},
		{
			name:       "resume resource exhausted",
			resumer:    stubResumer{err: status.Error(codes.ResourceExhausted, "rate limit hit")},
			trafficTok: "token",
			wantResult: autoResumeResourceExhausted,
			checkErr: func(t *testing.T, err error) {
				t.Helper()

				var exhaustedErr *reverseproxy.SandboxResourceExhaustedError
				require.ErrorAs(t, err, &exhaustedErr)
				require.Equal(t, "sbx", exhaustedErr.SandboxID())
				require.Equal(t, "rate limit hit", exhaustedErr.Message)
			},
		},
		{
			name:       "resume still transitioning",
			resumer:    stubResumer{err: status.Error(codes.FailedPrecondition, proxygrpc.SandboxStillTransitioningMessage)},
			trafficTok: "token",
			wantResult: autoResumeErrored,
			checkErr: func(t *testing.T, err error) {
				t.Helper()

				var transitioningErr *reverseproxy.SandboxStillTransitioningError
				require.ErrorAs(t, err, &transitioningErr)
				require.Equal(t, "sbx", transitioningErr.SandboxID())
			},
		},
		{
			name:       "failed precondition with other message stays generic",
			resumer:    stubResumer{err: failedPreconditionErr},
			trafficTok: "token",
			wantResult: autoResumeErrored,
			wantErrIs:  failedPreconditionErr,
			checkErr: func(t *testing.T, err error) {
				t.Helper()

				var transitioningErr *reverseproxy.SandboxStillTransitioningError
				require.NotErrorAs(t, err, &transitioningErr)
			},
		},
		{
			name:       "resume unavailable is a capacity error",
			resumer:    stubResumer{err: status.Error(codes.Unavailable, "no capacity")},
			trafficTok: "token",
			wantResult: autoResumeErrored,
			checkErr: func(t *testing.T, err error) {
				t.Helper()

				var capacityErr *reverseproxy.SandboxCapacityError
				require.ErrorAs(t, err, &capacityErr)
				require.Equal(t, "sbx", capacityErr.SandboxID())
				require.Equal(t, "no capacity", capacityErr.Message)
			},
		},
		{
			name:       "resume returns generic grpc error",
			resumer:    stubResumer{err: internalErr},
			trafficTok: "token",
			wantResult: autoResumeErrored,
			wantErrIs:  internalErr,
		},
		{
			// The API sends a placement timeout as 504, which maps to
			// DeadlineExceeded; the code it carries says a retry helps.
			name:       "placement timeout is a capacity error",
			resumer:    stubResumer{err: placementStatusWithCode(t, codes.DeadlineExceeded, proxygrpc.PlacementTimeout)},
			trafficTok: "token",
			wantResult: autoResumeErrored,
			checkErr: func(t *testing.T, err error) {
				t.Helper()

				var capacityErr *reverseproxy.SandboxCapacityError
				require.ErrorAs(t, err, &capacityErr)
				var refusedErr *reverseproxy.SandboxPlacementRefusedError
				require.NotErrorAs(t, err, &refusedErr)
			},
		},
		{
			// A create that failed on the node is a 500 with a code that
			// says to retry.
			name:       "create failed is a capacity error",
			resumer:    stubResumer{err: placementStatusWithCode(t, codes.Internal, proxygrpc.PlacementCreateFailed)},
			trafficTok: "token",
			wantResult: autoResumeErrored,
			checkErr: func(t *testing.T, err error) {
				t.Helper()

				var capacityErr *reverseproxy.SandboxCapacityError
				require.ErrorAs(t, err, &capacityErr)
				var refusedErr *reverseproxy.SandboxPlacementRefusedError
				require.NotErrorAs(t, err, &refusedErr)
			},
		},
		{
			name:       "deadline exceeded without a code stays generic",
			resumer:    stubResumer{err: deadlineErr},
			trafficTok: "token",
			wantResult: autoResumeErrored,
			wantErrIs:  deadlineErr,
		},
		{
			// A create the node refused for good is a placement refusal no
			// retry helps, whatever gRPC code its HTTP status mapped to.
			name:       "create refused is a permanent placement refusal",
			resumer:    stubResumer{err: createRefusedErr},
			trafficTok: "token",
			wantResult: autoResumeErrored,
			checkErr: func(t *testing.T, err error) {
				t.Helper()

				var refusedErr *reverseproxy.SandboxPlacementRefusedError
				require.ErrorAs(t, err, &refusedErr)
				require.Equal(t, proxygrpc.PlacementCreateRefused, refusedErr.ErrorCode)
				require.Equal(t, http.StatusServiceUnavailable, refusedErr.StatusCode)
			},
		},
		{
			name:       "create refused as a bad request keeps the API's 400",
			resumer:    stubResumer{err: placementStatusWithCode(t, codes.InvalidArgument, proxygrpc.PlacementCreateRefused)},
			trafficTok: "token",
			wantResult: autoResumeErrored,
			checkErr: func(t *testing.T, err error) {
				t.Helper()

				var refusedErr *reverseproxy.SandboxPlacementRefusedError
				require.ErrorAs(t, err, &refusedErr)
				require.Equal(t, http.StatusBadRequest, refusedErr.StatusCode)
			},
		},
		{
			// The API answers a refusal of a stored build as 500; the
			// Internal that maps to must not fold it into the 503 page.
			name:       "create refused as a server error keeps the API's 500",
			resumer:    stubResumer{err: placementStatusWithHTTPStatus(t, codes.Internal, proxygrpc.PlacementCreateRefused, http.StatusInternalServerError)},
			trafficTok: "token",
			wantResult: autoResumeErrored,
			checkErr: func(t *testing.T, err error) {
				t.Helper()

				var refusedErr *reverseproxy.SandboxPlacementRefusedError
				require.ErrorAs(t, err, &refusedErr)
				require.Equal(t, http.StatusInternalServerError, refusedErr.StatusCode)
			},
		},
		{
			name:       "create refused with a carried status that is no error falls back to the page's 503",
			resumer:    stubResumer{err: placementStatusWithHTTPStatus(t, codes.Internal, proxygrpc.PlacementCreateRefused, http.StatusOK)},
			trafficTok: "token",
			wantResult: autoResumeErrored,
			checkErr: func(t *testing.T, err error) {
				t.Helper()

				var refusedErr *reverseproxy.SandboxPlacementRefusedError
				require.ErrorAs(t, err, &refusedErr)
				require.Equal(t, http.StatusServiceUnavailable, refusedErr.StatusCode)
			},
		},
		{
			name:       "internal with the generic placement code stays generic",
			resumer:    stubResumer{err: internalPlacementErr},
			trafficTok: "token",
			wantResult: autoResumeErrored,
			wantErrIs:  internalPlacementErr,
		},
		{
			name:       "resume succeeds",
			resumer:    stubResumer{route: testRoute},
			trafficTok: "token",
			wantRoute:  testRoute,
			wantResult: autoResumeSucceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			route, res, err := handlePausedSandbox(t.Context(), "sbx", 8000, tt.trafficTok, tt.envdTok, tt.resumer)

			require.Equal(t, tt.wantResult, res)
			require.Same(t, tt.wantRoute, route)
			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)
			} else if tt.checkErr == nil {
				require.NoError(t, err)
			}
			if tt.checkErr != nil {
				require.Error(t, err)
				tt.checkErr(t, err)
			}
		})
	}
}

func TestHandlePausedSandbox_PassesPortAndTokenToResumer(t *testing.T) {
	t.Parallel()

	resumer := &recordingResumer{}

	route, res, err := handlePausedSandbox(t.Context(), "sbx", 49983, "token", "envd-token", resumer)
	require.NoError(t, err)
	require.Equal(t, autoResumeSucceeded, res)
	require.Same(t, testRoute, route)
	require.Equal(t, "sbx", resumer.sandboxID)
	require.EqualValues(t, 49983, resumer.sandboxPort)
	require.Equal(t, "token", resumer.trafficAccessToken)
	require.Equal(t, "envd-token", resumer.envdAccessToken)
}

func TestNewClientProxy_Construction(t *testing.T) {
	t.Parallel()

	c := catalog.NewRedisSandboxCatalog(redis_utils.SetupInstance(t))
	ff := newFF(t)

	p, _, err := NewClientProxy(noopmetric.NewMeterProvider(), "test-service", 0, 5007, c, nil, ff)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.EqualValues(t, 0, p.CurrentServerConnections())
	require.EqualValues(t, 0, p.CurrentPoolConnections())
}

type fixedCatalog struct {
	errorCatalog

	route *catalog.SandboxInfo
}

func (c fixedCatalog) GetSandbox(context.Context, string) (*catalog.SandboxInfo, error) {
	return c.route, nil
}

// countingServer is a real TCP HTTP server that answers body to the
// expected request, with the customer's Host and access token, and counts
// the connections it accepted.
func countingServer(t *testing.T, body, executionID string) (*net.TCPAddr, *atomic.Int32) {
	t.Helper()

	var accepted atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/commands" || r.Host != "49983-sbx.e2b.app" || r.Header.Get("X-Access-Token") != "sandbox-token" {
			w.WriteHeader(http.StatusBadRequest)

			return
		}
		var expected []string
		if executionID != "" {
			expected = []string{executionID}
		}
		assert.Equal(t, expected, r.Header.Values(pool.ExpectedExecutionIDHeader))
		_, _ = w.Write([]byte(body))
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			accepted.Add(1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)

	return server.Listener.Addr().(*net.TCPAddr), &accepted
}

func fullRouterRoute(router *net.TCPAddr) *catalog.SandboxInfo {
	return &catalog.SandboxInfo{OrchestratorIP: "127.0.0.1", ExecutionID: "execution", NetworkPlacement: &catalog.NetworkPlacement{
		NetworkMode: catalog.NetworkModeFullRouter, RouterID: "router", IngressAddress: router.IP.String(), IngressPort: uint32(router.Port),
	}}
}

// Legacy and egress-router traffic reaches the orchestrator proxy at the
// route's orchestrator IP and the configured port, never the placement's
// ingress endpoint, and full-router traffic reaches the Router's ingress
// endpoint and never the orchestrator, whether the route came from the
// record or from a resume. A full-router route without its endpoint, an
// unknown mode and a route without an orchestrator IP are answered before
// anything is dialed.
func TestNewClientProxy_RoutesByNetworkMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		route      func(node, router *net.TCPAddr) *catalog.SandboxInfo
		wantStatus int
		wantBody   string
		dialed     string
	}{
		{
			name: "legacy",
			route: func(node, _ *net.TCPAddr) *catalog.SandboxInfo {
				return &catalog.SandboxInfo{OrchestratorIP: node.IP.String()}
			},
			wantStatus: http.StatusOK,
			wantBody:   "selected-orchestrator",
			dialed:     "node",
		},
		{
			name: "egress-router",
			route: func(node, router *net.TCPAddr) *catalog.SandboxInfo {
				return &catalog.SandboxInfo{OrchestratorIP: node.IP.String(), ExecutionID: "execution", NetworkPlacement: &catalog.NetworkPlacement{
					NetworkMode: catalog.NetworkModeEgressRouter, RouterID: "router", IngressAddress: router.IP.String(), IngressPort: uint32(router.Port),
				}}
			},
			wantStatus: http.StatusOK,
			wantBody:   "selected-orchestrator",
			dialed:     "node",
		},
		{
			name: "full-router",
			route: func(_, router *net.TCPAddr) *catalog.SandboxInfo {
				return fullRouterRoute(router)
			},
			wantStatus: http.StatusOK,
			wantBody:   "router",
			dialed:     "router",
		},
		{
			name: "full-router without its execution",
			route: func(_, router *net.TCPAddr) *catalog.SandboxInfo {
				route := fullRouterRoute(router)
				route.ExecutionID = ""

				return route
			},
			wantStatus: http.StatusBadGateway,
			wantBody:   `"message":"The sandbox was not found"`,
		},
		{
			name: "full-router without its Router's ingress address",
			route: func(_, router *net.TCPAddr) *catalog.SandboxInfo {
				route := fullRouterRoute(router)
				route.NetworkPlacement.IngressAddress = ""

				return route
			},
			wantStatus: http.StatusBadGateway,
			wantBody:   `"message":"The sandbox was not found"`,
		},
		{
			name: "full-router with zero ingress port",
			route: func(_, router *net.TCPAddr) *catalog.SandboxInfo {
				route := fullRouterRoute(router)
				route.NetworkPlacement.IngressPort = 0

				return route
			},
			wantStatus: http.StatusBadGateway,
			wantBody:   `"message":"The sandbox was not found"`,
		},
		{
			name: "full-router with oversized ingress port",
			route: func(_, router *net.TCPAddr) *catalog.SandboxInfo {
				route := fullRouterRoute(router)
				route.NetworkPlacement.IngressPort = 65536

				return route
			},
			wantStatus: http.StatusBadGateway,
			wantBody:   `"message":"The sandbox was not found"`,
		},
		{
			name: "unknown mode",
			route: func(node, _ *net.TCPAddr) *catalog.SandboxInfo {
				return &catalog.SandboxInfo{OrchestratorIP: node.IP.String(), NetworkPlacement: &catalog.NetworkPlacement{NetworkMode: "future-mode"}}
			},
			wantStatus: http.StatusBadGateway,
			wantBody:   `"message":"The sandbox was not found"`,
		},
		{
			name: "no orchestrator IP",
			route: func(*net.TCPAddr, *net.TCPAddr) *catalog.SandboxInfo {
				return &catalog.SandboxInfo{}
			},
			wantStatus: http.StatusBadGateway,
			wantBody:   `"message":"The sandbox was not found"`,
		},
	}

	for _, tt := range tests {
		for _, source := range []string{"record", "resume"} {
			t.Run(tt.name+" from "+source, func(t *testing.T) {
				t.Parallel()

				node, nodeDials := countingServer(t, "selected-orchestrator", "")
				router, routerDials := countingServer(t, "router", "execution")
				route := tt.route(node, router)
				var c catalog.SandboxesCatalog = fixedCatalog{route: route}
				var resumer SandboxResumer
				if source == "resume" {
					c, resumer = errorCatalog{err: catalog.ErrSandboxNotFound}, stubResumer{route: route}
				}
				p, _, err := NewClientProxy(noopmetric.NewMeterProvider(), "network-mode", 0, uint16(node.Port), c, resumer, newFF(t))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, p.Close()) })

				for _, connection := range []string{"", pool.ExpectedExecutionIDHeader} {
					req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://49983-sbx.e2b.app/commands", nil)
					req.Header.Set("X-Access-Token", "sandbox-token")
					req.Header.Add(pool.ExpectedExecutionIDHeader, "forged-first")
					req.Header.Add(pool.ExpectedExecutionIDHeader, "forged-second")
					req.Header.Set("Connection", connection)
					rr := httptest.NewRecorder()
					p.Handler.ServeHTTP(rr, req)

					require.Equal(t, tt.wantStatus, rr.Code)
					require.Contains(t, rr.Body.String(), tt.wantBody)
					require.Empty(t, rr.Header().Get("Retry-After"))
					require.Equal(t, tt.dialed == "node", nodeDials.Load() == 1, "the orchestrator is dialed only for its own ingress")
					require.Equal(t, tt.dialed == "router", routerDials.Load() == 1, "the Router is dialed only for full-router")
					require.LessOrEqual(t, nodeDials.Load()+routerDials.Load(), int32(1))
				}
			})
		}
	}
}

func TestNewClientProxy_ExecutionChangesOnReusedConnection(t *testing.T) {
	t.Parallel()

	var connections atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Header.Get(pool.ExpectedExecutionIDHeader))
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)
	route := fullRouterRoute(server.Listener.Addr().(*net.TCPAddr))
	p, _, err := NewClientProxy(noopmetric.NewMeterProvider(), "execution-routing", 0, 5008, fixedCatalog{route: route}, nil, newFF(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, p.Close()) })
	for _, execution := range []string{"before-resume", "after-resume"} {
		route.ExecutionID = execution
		r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://8000-sbx.e2b.app/", nil)
		r.Header.Set(pool.ExpectedExecutionIDHeader, "forged")
		w := httptest.NewRecorder()
		p.Handler.ServeHTTP(w, r)
		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, execution, w.Body.String())
	}
	require.Equal(t, int32(1), connections.Load(), "the execution is resolved per request, not per connection")
}

// upgradingRouter is a Router's ingress listener that switches every
// request to an echo stream, and counts the connections it accepted.
func upgradingRouter(t *testing.T) (*net.TCPAddr, *atomic.Int32) {
	t.Helper()

	var accepted atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Upgrade") != "echo" || r.Host != "49983-sbx.e2b.app" {
			w.WriteHeader(http.StatusBadRequest)

			return
		}
		conn, buffered, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := buffered.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n"); err != nil || buffered.Flush() != nil {
			return
		}
		_, _ = io.Copy(conn, buffered)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			accepted.Add(1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)

	return server.Listener.Addr().(*net.TCPAddr), &accepted
}

// With the routing record in real Redis, a record naming another Router
// endpoint serves the next request, while an upgraded stream established
// through the old endpoint keeps working and nothing dials the old
// endpoint again.
func TestNewClientProxy_FullRouterFollowsANewEndpoint(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	records := catalog.NewRedisSandboxCatalog(redis_utils.SetupInstance(t))
	old, oldDials := upgradingRouter(t)
	moved, movedDials := countingServer(t, "moved", "execution")
	require.NoError(t, records.StoreSandbox(ctx, "sbx", fullRouterRoute(old), time.Minute))
	p, _, err := NewClientProxy(noopmetric.NewMeterProvider(), "new-endpoint", 0, 1, records, nil, newFF(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, p.Close()) })
	frontend := httptest.NewServer(p.Handler)
	t.Cleanup(frontend.Close)

	stream, err := (&net.Dialer{}).DialContext(ctx, "tcp", frontend.Listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.Close() })
	_, err = stream.Write([]byte("GET /stream HTTP/1.1\r\nHost: 49983-sbx.e2b.app\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n"))
	require.NoError(t, err)
	reader := bufio.NewReader(stream)
	switched, err := http.ReadResponse(reader, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = switched.Body.Close() })
	require.Equal(t, http.StatusSwitchingProtocols, switched.StatusCode)
	echo := func(message string) {
		t.Helper()
		_, err := stream.Write([]byte(message))
		require.NoError(t, err)
		got := make([]byte, len(message))
		_, err = io.ReadFull(reader, got)
		require.NoError(t, err)
		require.Equal(t, message, string(got))
	}
	echo("before")

	require.NoError(t, records.StoreSandbox(ctx, "sbx", fullRouterRoute(moved), time.Minute))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, frontend.URL+"/commands", nil)
	require.NoError(t, err)
	req.Host = "49983-sbx.e2b.app"
	req.Header.Set("X-Access-Token", "sandbox-token")
	resp, err := frontend.Client().Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "moved", string(body))
	require.EqualValues(t, 1, movedDials.Load())

	echo("after")
	require.EqualValues(t, 1, oldDials.Load(), "the old endpoint is not dialed again")
}

// A probe request is answered from the routing outcome and never dials the
// orchestrator: "true" for a sandbox routed or held here, "false" only for one
// the API says is not, and no answer when the outcome cannot tell. A held
// sandbox must never answer "false", or its traffic would be sent elsewhere.
func TestNewClientProxy_AnswersProbeRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		catalog    func(node *net.TCPAddr) catalog.SandboxesCatalog
		resumer    func(node *net.TCPAddr) SandboxResumer
		wantStatus int
		wantAnswer string
	}{
		{
			name: "running sandbox",
			catalog: func(node *net.TCPAddr) catalog.SandboxesCatalog {
				return fixedCatalog{route: &catalog.SandboxInfo{OrchestratorIP: node.IP.String()}}
			},
			wantStatus: http.StatusNoContent,
			wantAnswer: "true",
		},
		{
			name: "resumed sandbox",
			resumer: func(node *net.TCPAddr) SandboxResumer {
				return stubResumer{route: &catalog.SandboxInfo{OrchestratorIP: node.IP.String()}}
			},
			wantStatus: http.StatusNoContent,
			wantAnswer: "true",
		},
		{
			name:       "sandbox on another plane",
			resumer:    resumeError(status.Error(codes.NotFound, "snapshot not found")),
			wantStatus: http.StatusBadGateway,
			wantAnswer: "false",
		},
		{
			name: "record without an orchestrator IP is held here",
			catalog: func(*net.TCPAddr) catalog.SandboxesCatalog {
				return fixedCatalog{route: &catalog.SandboxInfo{}}
			},
			wantStatus: http.StatusBadGateway,
			wantAnswer: "true",
		},
		{
			name: "record with an unknown network mode is held here",
			catalog: func(node *net.TCPAddr) catalog.SandboxesCatalog {
				return fixedCatalog{route: &catalog.SandboxInfo{OrchestratorIP: node.IP.String(), NetworkPlacement: &catalog.NetworkPlacement{NetworkMode: "future-mode"}}}
			},
			wantStatus: http.StatusBadGateway,
			wantAnswer: "true",
		},
		{
			name: "catalog read failure cannot tell",
			catalog: func(*net.TCPAddr) catalog.SandboxesCatalog {
				return errorCatalog{err: errors.New("redis down")}
			},
			resumer:    resumeError(status.Error(codes.NotFound, "snapshot not found")),
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "resume failing without a known code cannot tell",
			resumer:    resumeError(status.Error(codes.Internal, "failed to get team")),
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "catalog miss without a resumer cannot tell",
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "sandbox found but resume denied",
			resumer:    resumeError(status.Error(codes.PermissionDenied, "denied")),
			wantStatus: http.StatusForbidden,
			wantAnswer: "true",
		},
		{
			name:       "resume unavailable cannot tell",
			resumer:    resumeError(status.Error(codes.Unavailable, "")),
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "placement the API attempted is held here",
			resumer:    resumeError(placementStatusWithCode(t, codes.DeadlineExceeded, proxygrpc.PlacementTimeout)),
			wantStatus: http.StatusServiceUnavailable,
			wantAnswer: "true",
		},
		{
			name:       "unavailable with a placement code is held here",
			resumer:    resumeError(placementStatusWithCode(t, codes.Unavailable, proxygrpc.PlacementTimeout)),
			wantStatus: http.StatusServiceUnavailable,
			wantAnswer: "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			node, nodeDials := countingServer(t, "orchestrator", "")
			var c catalog.SandboxesCatalog = errorCatalog{err: catalog.ErrSandboxNotFound}
			if tt.catalog != nil {
				c = tt.catalog(node)
			}
			var resumer SandboxResumer
			if tt.resumer != nil {
				resumer = tt.resumer(node)
			}
			p, _, err := NewClientProxy(noopmetric.NewMeterProvider(), "probe", 0, uint16(node.Port), c, resumer, newFF(t))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, p.Close()) })

			req := httptest.NewRequestWithContext(t.Context(), http.MethodHead, "http://49983-sbx.e2b.app/", nil)
			req.Header.Set(reverseproxy.HeaderProbe, "true")
			rr := httptest.NewRecorder()
			p.Handler.ServeHTTP(rr, req)

			require.Equal(t, tt.wantStatus, rr.Code)
			require.Equal(t, tt.wantAnswer, rr.Header().Get(reverseproxy.HeaderProbe))
			require.Zero(t, nodeDials.Load(), "a probe request never reaches the orchestrator")
		})
	}
}

func resumeError(err error) func(*net.TCPAddr) SandboxResumer {
	return func(*net.TCPAddr) SandboxResumer {
		return stubResumer{err: err}
	}
}

// countingCatalog counts the record lookups it serves.
type countingCatalog struct {
	catalog.SandboxesCatalog

	lookups atomic.Int32
}

func (c *countingCatalog) GetSandbox(ctx context.Context, sandboxID string) (*catalog.SandboxInfo, error) {
	c.lookups.Add(1)

	return c.SandboxesCatalog.GetSandbox(ctx, sandboxID)
}

// A sandbox without a routing record is resumed over a real gRPC connection.
// The full-router route the API answered with is the resolution, whole, after
// one record lookup and with no wait for the record to be published, and
// the forwarding step sends it to the Router.
func TestCatalogResolution_MissCarriesTheResumedFullRouterRoute(t *testing.T) {
	t.Parallel()

	srv := &fakeSandboxServer{resp: &proxygrpc.SandboxResumeResponse{NetworkPlacement: &orchestrator.SandboxNetworkPlacement{
		ExecutionId: "execution", NetworkMode: "full-router", RouterId: "router", IngressAddress: "10.8.0.2", IngressPort: 5010,
	}}}
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	proxygrpc.RegisterSandboxServiceServer(server, srv)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	resumer, err := NewGRPCPausedSandboxResumer(t.Context(), listener.Addr().String(), GRPCOAuthConfig{}, false, insecure.NewCredentials())
	require.NoError(t, err)
	t.Cleanup(func() { _ = resumer.(*grpcPausedSandboxResumer).Close(context.WithoutCancel(t.Context())) })
	c := &countingCatalog{SandboxesCatalog: catalog.NewRedisSandboxCatalog(redis_utils.SetupInstance(t))}

	route, err := catalogResolution(t.Context(), "sbx", 3000, "traffic-token", "envd-token", c, resumer)
	require.NoError(t, err)
	require.Equal(t, &catalog.SandboxInfo{ExecutionID: "execution", NetworkPlacement: &catalog.NetworkPlacement{
		NetworkMode: catalog.NetworkModeFullRouter, RouterID: "router", IngressAddress: "10.8.0.2", IngressPort: 5010,
	}}, route)
	require.EqualValues(t, 1, c.lookups.Load())
	require.Equal(t, "sbx", srv.gotSandboxID)
	require.Equal(t, []string{"3000"}, srv.gotMetadata.Get(proxygrpc.MetadataSandboxRequestPort))
	require.Equal(t, []string{"traffic-token"}, srv.gotMetadata.Get(proxygrpc.MetadataTrafficAccessToken))
	require.Equal(t, []string{"envd-token"}, srv.gotMetadata.Get(proxygrpc.MetadataEnvdAccessToken))

	host, err := routeHost(route, 5007)
	require.NoError(t, err)
	require.Equal(t, "10.8.0.2:5010", host, "the resumed route is forwarded to its Router, not the orchestrator")
}

func TestNewClientProxy_HandlerErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		url              string
		resumer          SandboxResumer
		wantStatus       int
		wantRetryAfter   string
		wantBodyContains []string
		wantBodyOmits    []string
	}{
		{
			name:       "sandbox not found",
			url:        "http://49983-sbx.e2b.app/",
			wantStatus: http.StatusBadGateway,
			wantBodyContains: []string{
				`"sandboxId":"sbx"`,
				`"message":"The sandbox was not found"`,
			},
		},
		{
			name:           "resume unavailable keeps the transport error off the page",
			url:            "http://49983-sbx.e2b.app/",
			resumer:        stubResumer{err: status.Error(codes.Unavailable, "transport: Error while dialing: dial tcp 10.1.2.3:5008: connect: connection refused")},
			wantStatus:     http.StatusServiceUnavailable,
			wantRetryAfter: "2",
			wantBodyContains: []string{
				`"sandboxId":"sbx"`,
				`"message":"The sandbox cannot be resumed right now. Try again in a moment."`,
			},
			wantBodyOmits: []string{"dial tcp", "10.1.2.3"},
		},
		{
			name:           "resume unavailable without a message",
			url:            "http://49983-sbx.e2b.app/",
			resumer:        stubResumer{err: status.Error(codes.Unavailable, "")},
			wantStatus:     http.StatusServiceUnavailable,
			wantRetryAfter: "2",
			wantBodyContains: []string{
				`"sandboxId":"sbx"`,
				`"message":"The sandbox cannot be resumed right now. Try again in a moment."`,
			},
		},
		{
			// A 504 from the API maps to DeadlineExceeded; the code it
			// carries puts it on the capacity page, not the not-found page
			// the SDKs read as the sandbox being gone.
			name:           "placement timeout answers the capacity page",
			url:            "http://49983-sbx.e2b.app/",
			resumer:        stubResumer{err: placementStatusWithCode(t, codes.DeadlineExceeded, proxygrpc.PlacementTimeout)},
			wantStatus:     http.StatusServiceUnavailable,
			wantRetryAfter: "2",
			wantBodyContains: []string{
				`"sandboxId":"sbx"`,
				`"message":"The sandbox cannot be resumed right now. Try again in a moment."`,
			},
			wantBodyOmits: []string{"Failed to place sandbox"},
		},
		{
			name:           "create failed answers the capacity page",
			url:            "http://49983-sbx.e2b.app/",
			resumer:        stubResumer{err: placementStatusWithCode(t, codes.Internal, proxygrpc.PlacementCreateFailed)},
			wantStatus:     http.StatusServiceUnavailable,
			wantRetryAfter: "2",
			wantBodyContains: []string{
				`"sandboxId":"sbx"`,
				`"message":"The sandbox cannot be resumed right now. Try again in a moment."`,
			},
			wantBodyOmits: []string{"Failed to place sandbox"},
		},
		{
			name:       "create refused answers the refused page with the code and no Retry-After",
			url:        "http://49983-sbx.e2b.app/",
			resumer:    stubResumer{err: placementStatusWithCode(t, codes.Internal, proxygrpc.PlacementCreateRefused)},
			wantStatus: http.StatusServiceUnavailable,
			wantBodyContains: []string{
				`"sandboxId":"sbx"`,
				`"message":"The sandbox cannot be resumed: no node in this cluster can run it."`,
				`"error_code":"` + proxygrpc.PlacementCreateRefused + `"`,
			},
			wantBodyOmits: []string{"Failed to place sandbox", "was not found"},
		},
		{
			// A refusal the API answered as a bad request keeps its 400: a
			// 503 would invite the SDKs' retries, each starting a resume.
			name:       "create refused as a bad request answers the refused page as 400",
			url:        "http://49983-sbx.e2b.app/",
			resumer:    stubResumer{err: placementStatusWithCode(t, codes.InvalidArgument, proxygrpc.PlacementCreateRefused)},
			wantStatus: http.StatusBadRequest,
			wantBodyContains: []string{
				`"sandboxId":"sbx"`,
				`"error_code":"` + proxygrpc.PlacementCreateRefused + `"`,
				`"code":400`,
			},
		},
		{
			// The API answers a refusal of a stored build as 500, which maps
			// to Internal; the page answers with the API's 500 and no
			// Retry-After, since a 503 would invite the SDKs' retries, each
			// starting a resume for an answer no retry changes.
			name:       "create refused as a server error answers the refused page as 500",
			url:        "http://49983-sbx.e2b.app/",
			resumer:    stubResumer{err: placementStatusWithHTTPStatus(t, codes.Internal, proxygrpc.PlacementCreateRefused, http.StatusInternalServerError)},
			wantStatus: http.StatusInternalServerError,
			wantBodyContains: []string{
				`"sandboxId":"sbx"`,
				`"message":"The sandbox cannot be resumed: no node in this cluster can run it."`,
				`"error_code":"` + proxygrpc.PlacementCreateRefused + `"`,
				`"code":500`,
			},
			wantBodyOmits: []string{"Failed to place sandbox", "was not found"},
		},
		{
			name:       "deadline exceeded without a code stays on the not-found page",
			url:        "http://49983-sbx.e2b.app/",
			resumer:    stubResumer{err: status.Error(codes.DeadlineExceeded, "orchestrator did not answer")},
			wantStatus: http.StatusBadGateway,
			wantBodyContains: []string{
				`"message":"The sandbox was not found"`,
			},
		},
		{
			name:       "resume permission denied",
			url:        "http://49983-sbx.e2b.app/",
			resumer:    stubResumer{err: status.Error(codes.PermissionDenied, "denied")},
			wantStatus: http.StatusForbidden,
			wantBodyContains: []string{
				`"sandboxId":"sbx"`,
				`credentials provided`,
			},
		},
		{
			name:       "resume resource exhausted",
			url:        "http://49983-sbx.e2b.app/",
			resumer:    stubResumer{err: status.Error(codes.ResourceExhausted, "rate limit")},
			wantStatus: http.StatusTooManyRequests,
			wantBodyContains: []string{
				`"sandboxId":"sbx"`,
				`"message":"rate limit"`,
			},
		},
		{
			name:           "resume still transitioning on a user port",
			url:            "http://3000-sbx.e2b.app/",
			resumer:        stubResumer{err: status.Error(codes.FailedPrecondition, proxygrpc.SandboxStillTransitioningMessage)},
			wantStatus:     http.StatusConflict,
			wantRetryAfter: "2",
			wantBodyContains: []string{
				`"sandboxId":"sbx"`,
				`still transitioning`,
			},
		},
		{
			// The SDK reads an envd-port 409 as "already exists".
			name:           "resume still transitioning on the envd port",
			url:            "http://49983-sbx.e2b.app/",
			resumer:        stubResumer{err: status.Error(codes.FailedPrecondition, proxygrpc.SandboxStillTransitioningMessage)},
			wantStatus:     http.StatusServiceUnavailable,
			wantRetryAfter: "2",
			wantBodyContains: []string{
				`"sandboxId":"sbx"`,
				`still transitioning`,
			},
		},
		{
			name:       "invalid host",
			url:        "http://invalid-host/",
			wantStatus: http.StatusBadRequest,
			wantBodyContains: []string{
				"Invalid host",
			},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := catalog.NewRedisSandboxCatalog(redis_utils.SetupInstance(t))
			ff := newFF(t)
			p, _, err := NewClientProxy(noopmetric.NewMeterProvider(), "handler-errors-"+tt.name, uint16(i), 5007, c, tt.resumer, ff)
			require.NoError(t, err)

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, tt.url, nil)
			rr := httptest.NewRecorder()
			p.Handler.ServeHTTP(rr, req)

			require.Equal(t, tt.wantStatus, rr.Code)
			require.Equal(t, tt.wantRetryAfter, rr.Header().Get("Retry-After"))
			wantExposed := ""
			if tt.wantRetryAfter != "" {
				// A browser hides the delay from a cross-origin page unless
				// the response lists the header.
				wantExposed = "Retry-After"
			}
			require.Equal(t, wantExposed, rr.Header().Get("Access-Control-Expose-Headers"))
			for _, want := range tt.wantBodyContains {
				require.Contains(t, rr.Body.String(), want)
			}
			for _, omitted := range tt.wantBodyOmits {
				require.NotContains(t, rr.Body.String(), omitted)
			}
		})
	}
}

func TestNewClientProxy_DuplicateMetricsRegistrationReturnsErrors(t *testing.T) {
	t.Parallel()

	c := catalog.NewRedisSandboxCatalog(redis_utils.SetupInstance(t))
	ff := newFF(t)

	// noop meter provider should not error; this is a sanity test that NewClientProxy
	// works repeatedly for separate service names without leaking metric registrations.
	for range 3 {
		_, _, err := NewClientProxy(noopmetric.NewMeterProvider(), "service", 0, 5007, c, nil, ff)
		require.NoError(t, err)
	}
}

// Sanity assertion that the proxy honors the configured idle timeout.
func TestNewClientProxy_HasIdleTimeout(t *testing.T) {
	t.Parallel()

	c := catalog.NewRedisSandboxCatalog(redis_utils.SetupInstance(t))
	ff := newFF(t)

	p, _, err := NewClientProxy(noopmetric.NewMeterProvider(), "service-idle", 0, 5007, c, nil, ff)
	require.NoError(t, err)
	require.GreaterOrEqual(t, p.IdleTimeout, idleTimeout)
	require.Less(t, p.IdleTimeout, 2*idleTimeout)
}

// Validate the Construction test exercises pool size accessor too.
func TestNewClientProxy_PoolAccessors(t *testing.T) {
	t.Parallel()

	c := catalog.NewRedisSandboxCatalog(redis_utils.SetupInstance(t))
	ff := newFF(t)

	p, _, err := NewClientProxy(noopmetric.NewMeterProvider(), "service-pool", 0, 5007, c, nil, ff)
	require.NoError(t, err)
	require.GreaterOrEqual(t, p.CurrentPoolSize(), 0)

	// Even on no-op meter providers, the proxy must still be wired up correctly.
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://49983-sbx.e2b.app/", nil)
	rr := httptest.NewRecorder()
	p.Handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusBadGateway, rr.Code)
	require.Contains(t, rr.Body.String(), `"sandboxId":"sbx"`)
}

// TestClientProxyForRouterIngress is the client proxy half of a Router's
// ingress test, which runs it in the Router's namespace with
// E2B_CLIENT_PROXY_FOR_ROUTER naming the Router's ingress endpoint, the
// address to serve on and a ready file. It serves the client proxy with a
// routing record that places every sandbox in full-router mode at that
// endpoint, and an orchestrator proxy port beside it that must never be
// dialed, touches the ready file and serves until its stdin closes.
func TestClientProxyForRouterIngress(t *testing.T) { //nolint:paralleltest // serves the Router test alone
	config := strings.Split(os.Getenv("E2B_CLIENT_PROXY_FOR_ROUTER"), ",")
	if len(config) != 3 {
		t.Skip("NOT RUN: needs a Router ingress test to set E2B_CLIENT_PROXY_FOR_ROUTER")
	}
	router, serve, ready := netip.MustParseAddrPort(config[0]), netip.MustParseAddrPort(config[1]), config[2]
	orchestratorProxy, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp4", netip.AddrPortFrom(serve.Addr(), 0).String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = orchestratorProxy.Close() })
	var orchestratorDials atomic.Int32
	go func() {
		for {
			conn, err := orchestratorProxy.Accept()
			if err != nil {
				return
			}
			orchestratorDials.Add(1)
			_ = conn.Close()
		}
	}()
	route := &catalog.SandboxInfo{OrchestratorIP: serve.Addr().String(), ExecutionID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("exec-s")).String(), NetworkPlacement: &catalog.NetworkPlacement{
		NetworkMode: catalog.NetworkModeFullRouter, RouterID: "router", IngressAddress: router.Addr().String(), IngressPort: uint32(router.Port()),
	}}
	orchestratorPort := uint16(orchestratorProxy.Addr().(*net.TCPAddr).Port)
	p, _, err := NewClientProxy(noopmetric.NewMeterProvider(), "router-ingress", 0, orchestratorPort, fixedCatalog{route: route}, nil, newFF(t))
	require.NoError(t, err)
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp4", serve.String())
	require.NoError(t, err)
	go func() { _ = p.Serve(listener) }()
	t.Cleanup(func() { _ = p.Close() })
	require.NoError(t, os.WriteFile(ready, nil, 0o600))
	_, _ = io.Copy(io.Discard, os.Stdin)
	require.Zero(t, orchestratorDials.Load(), "nothing reached the orchestrator proxy")
}
