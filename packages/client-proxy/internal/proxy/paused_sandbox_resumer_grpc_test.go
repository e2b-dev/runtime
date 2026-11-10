package proxy

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/servicetest"
	reverseproxy "github.com/e2b-dev/infra/packages/shared/pkg/proxy"
	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
)

type fakeSandboxServer struct {
	proxygrpc.UnimplementedSandboxServiceServer

	resp *proxygrpc.SandboxResumeResponse
	err  error

	gotSandboxID string
	gotMetadata  metadata.MD
	calls        atomic.Int32
}

func (f *fakeSandboxServer) ResumeSandbox(ctx context.Context, req *proxygrpc.SandboxResumeRequest) (*proxygrpc.SandboxResumeResponse, error) {
	f.calls.Add(1)
	f.gotSandboxID = req.GetSandboxId()
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		f.gotMetadata = md
	}

	if f.err != nil {
		return nil, f.err
	}

	return f.resp, nil
}

func startFakeServer(t *testing.T, srv proxygrpc.SandboxServiceServer) *grpc.ClientConn {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	proxygrpc.RegisterSandboxServiceServer(server, srv)

	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	return conn
}

func TestNewGRPCPausedSandboxResumer_EmptyAddressErrors(t *testing.T) {
	t.Parallel()

	r, err := NewGRPCPausedSandboxResumer(t.Context(), "  ", GRPCOAuthConfig{}, false, insecure.NewCredentials())
	require.Error(t, err)
	require.Nil(t, r)
}

func TestNewGRPCPausedSandboxResumer_OAuthMisconfiguredErrors(t *testing.T) {
	t.Parallel()

	r, err := NewGRPCPausedSandboxResumer(t.Context(), "127.0.0.1:1234", GRPCOAuthConfig{ClientID: "only-id"}, false, insecure.NewCredentials())
	require.Error(t, err)
	require.Nil(t, r)
}

func TestNewGRPCPausedSandboxResumer_InsecureSucceeds(t *testing.T) {
	t.Parallel()

	r, err := NewGRPCPausedSandboxResumer(t.Context(), "127.0.0.1:1234", GRPCOAuthConfig{}, false, insecure.NewCredentials())
	require.NoError(t, err)
	require.NotNil(t, r)

	closer, ok := r.(interface {
		Close(ctx context.Context) error
	})
	require.True(t, ok)
	require.NoError(t, closer.Close(t.Context()))
}

func TestNewGRPCPausedSandboxResumer_TLSSucceeds(t *testing.T) {
	t.Parallel()

	r, err := NewGRPCPausedSandboxResumer(t.Context(), "127.0.0.1:1234", GRPCOAuthConfig{}, true, insecure.NewCredentials())
	require.NoError(t, err)
	require.NotNil(t, r)

	closer, ok := r.(interface {
		Close(ctx context.Context) error
	})
	require.True(t, ok)
	require.NoError(t, closer.Close(t.Context()))
}

func TestGRPCPausedSandboxResumer_ResumeSendsMetadataAndReturnsRoute(t *testing.T) {
	t.Parallel()

	srv := &fakeSandboxServer{
		resp: &proxygrpc.SandboxResumeResponse{OrchestratorIp: "  10.0.0.5  ", NetworkPlacement: &orchestrator.SandboxNetworkPlacement{
			ExecutionId: "execution", NetworkMode: "egress-router", RouterId: "router", IngressAddress: "10.0.0.5", IngressPort: 5007,
		}},
	}
	conn := startFakeServer(t, srv)

	r := &grpcPausedSandboxResumer{
		conn:   conn,
		client: proxygrpc.NewSandboxServiceClient(conn),
		auth:   noopGrpcResumeAuth{},
	}

	route, err := r.Resume(t.Context(), "sbx-123", 49983, "traffic-token", "envd-token")
	require.NoError(t, err)
	require.Equal(t, &catalog.SandboxInfo{OrchestratorIP: "10.0.0.5", ExecutionID: "execution", NetworkPlacement: &catalog.NetworkPlacement{
		NetworkMode: catalog.NetworkModeEgressRouter, RouterID: "router", IngressAddress: "10.0.0.5", IngressPort: 5007,
	}}, route)
	require.EqualValues(t, 1, srv.calls.Load())
	require.Equal(t, "sbx-123", srv.gotSandboxID)
	require.Equal(t, []string{"49983"}, srv.gotMetadata.Get(proxygrpc.MetadataSandboxRequestPort))
	require.Equal(t, []string{"traffic-token"}, srv.gotMetadata.Get(proxygrpc.MetadataTrafficAccessToken))
	require.Equal(t, []string{"envd-token"}, srv.gotMetadata.Get(proxygrpc.MetadataEnvdAccessToken))
}

func TestGRPCPausedSandboxResumer_ResumeOmitsEmptyTokens(t *testing.T) {
	t.Parallel()

	srv := &fakeSandboxServer{
		resp: &proxygrpc.SandboxResumeResponse{OrchestratorIp: "10.0.0.6"},
	}
	conn := startFakeServer(t, srv)

	r := &grpcPausedSandboxResumer{
		conn:   conn,
		client: proxygrpc.NewSandboxServiceClient(conn),
		auth:   noopGrpcResumeAuth{},
	}

	route, err := r.Resume(t.Context(), "sbx-456", 8080, "", "")
	require.NoError(t, err)
	require.Equal(t, &catalog.SandboxInfo{OrchestratorIP: "10.0.0.6"}, route, "an answer without a placement stays legacy")
	require.EqualValues(t, 1, srv.calls.Load())
	require.Empty(t, srv.gotMetadata.Get(proxygrpc.MetadataTrafficAccessToken))
	require.Empty(t, srv.gotMetadata.Get(proxygrpc.MetadataEnvdAccessToken))
	require.Equal(t, []string{"8080"}, srv.gotMetadata.Get(proxygrpc.MetadataSandboxRequestPort))
}

func TestGRPCPausedSandboxResumer_ResumeReturnsServerError(t *testing.T) {
	t.Parallel()

	srv := &fakeSandboxServer{
		err: status.Error(codes.NotFound, "missing"),
	}
	conn := startFakeServer(t, srv)

	r := &grpcPausedSandboxResumer{
		conn:   conn,
		client: proxygrpc.NewSandboxServiceClient(conn),
		auth:   noopGrpcResumeAuth{},
	}

	route, err := r.Resume(t.Context(), "sbx", 80, "", "")
	require.Error(t, err)
	require.Nil(t, route)
	st, ok := status.FromError(errors.Unwrap(err))
	require.True(t, ok)
	require.Equal(t, codes.NotFound, st.Code())
	require.EqualValues(t, 1, srv.calls.Load())
}

// The API's error code survives the wire and the resumer's wrapping: a
// status built the way the API builds it, served over gRPC and read through
// the resumer's error, still classifies as the API meant it.
func TestGRPCPausedSandboxResumer_ErrorCodeSurvivesTheWire(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		code      string
		permanent bool
	}{
		{proxygrpc.PlacementFeatureUnsupported, true},
		{proxygrpc.SnapshotGuestAgentUnresponsive, true},
		{proxygrpc.PlacementNoCompatibleNode, false},
		{proxygrpc.PlacementCapacityUnavailable, false},
	} {
		t.Run(tt.code, func(t *testing.T) {
			t.Parallel()

			st, err := status.New(codes.Unavailable, "Failed to place sandbox").
				WithDetails(&errdetails.ErrorInfo{Reason: tt.code, Domain: proxygrpc.ResumeErrorDomain})
			require.NoError(t, err)
			srv := &fakeSandboxServer{err: st.Err()}
			conn := startFakeServer(t, srv)

			r := &grpcPausedSandboxResumer{
				conn:   conn,
				client: proxygrpc.NewSandboxServiceClient(conn),
				auth:   noopGrpcResumeAuth{},
			}

			_, err = r.Resume(t.Context(), "sbx", 80, "", "")
			require.Error(t, err)
			received, ok := resumeStatus(err)
			require.True(t, ok)
			require.Equal(t, codes.Unavailable, received.Code())
			require.Equal(t, tt.permanent, permanentResumeRefusal(received))
			require.Equal(t, !tt.permanent, retryableResumeError(err))
		})
	}
}

type errAuth struct {
	err error
}

func (e errAuth) authorize(_ context.Context) (context.Context, error) {
	return nil, e.err
}

func TestGRPCPausedSandboxResumer_ResumeAuthError(t *testing.T) {
	t.Parallel()

	srv := &fakeSandboxServer{
		resp: &proxygrpc.SandboxResumeResponse{OrchestratorIp: "10.0.0.7"},
	}
	conn := startFakeServer(t, srv)

	authErr := errors.New("auth failed")
	r := &grpcPausedSandboxResumer{
		conn:   conn,
		client: proxygrpc.NewSandboxServiceClient(conn),
		auth:   errAuth{err: authErr},
	}

	route, err := r.Resume(t.Context(), "sbx", 80, "", "")
	require.ErrorIs(t, err, authErr)
	require.Nil(t, route)
	require.EqualValues(t, 0, srv.calls.Load())
}

// cancellingAuth ends the attempt while the token is being fetched, the way
// a slow refresh outlives an attempt deadline.
type cancellingAuth struct {
	cancel context.CancelFunc
}

func (a cancellingAuth) authorize(ctx context.Context) (context.Context, error) {
	a.cancel()

	return ctx, nil
}

// An attempt whose context ended during the token refresh never dials.
func TestGRPCPausedSandboxResumer_ResumeDoesNotDialAfterAttemptEnded(t *testing.T) {
	t.Parallel()

	srv := &fakeSandboxServer{
		resp: &proxygrpc.SandboxResumeResponse{OrchestratorIp: "10.0.0.8"},
	}
	conn := startFakeServer(t, srv)

	ctx, cancel := context.WithCancel(t.Context())
	r := &grpcPausedSandboxResumer{
		conn:   conn,
		client: proxygrpc.NewSandboxServiceClient(conn),
		auth:   cancellingAuth{cancel: cancel},
	}

	route, err := r.Resume(ctx, "sbx", 80, "", "")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, route)
	require.EqualValues(t, 0, srv.calls.Load())
}

// A token endpoint that never answers fails the attempt within the refresh
// timeout, and the flight that made the attempt ends with the unavailable
// page.
func TestResumeFlights_StalledTokenRefreshEndsTheFlight(t *testing.T) {
	t.Parallel()

	tokens := stalledTokenServer(t)

	auth, err := newGrpcResumeAuthWithTimeout(t.Context(), GRPCOAuthConfig{
		ClientID:     "client-id",
		ClientSecret: "secret",
		TokenURL:     tokens.URL,
	}, 100*time.Millisecond)
	require.NoError(t, err)

	srv := &fakeSandboxServer{
		resp: &proxygrpc.SandboxResumeResponse{OrchestratorIp: "10.0.0.9"},
	}
	conn := startFakeServer(t, srv)
	flights := newTestResumeFlights(t, newMetricReader(t), &grpcPausedSandboxResumer{
		conn:   conn,
		client: proxygrpc.NewSandboxServiceClient(conn),
		auth:   auth,
	})
	flights.featureFlags = newFFWithParking(t, true, 15000, 512)

	start := time.Now()
	route, err := flights.Resume(t.Context(), "sbx", 80, "", "")
	require.Less(t, time.Since(start), 5*time.Second)
	require.Nil(t, route)

	var capacityErr *reverseproxy.SandboxCapacityError
	require.ErrorAs(t, err, &capacityErr)
	require.Contains(t, capacityErr.Message, errResumeToken.Error())
	require.EqualValues(t, 0, srv.calls.Load())
	require.Zero(t, flightsInRegistry(flights))
}

// A token refresh that outlasts the attempt ends with the attempt's
// deadline, not with the refresh's own timeout: the attempt answers while
// the refresh is still blocked (the token server answers only at cleanup),
// no RPC is made, and the flight is gone from the registry. A watchdog
// catches a hang; the attempt's own deadline is not asserted on the clock.
func TestResumeFlights_StalledTokenRefreshEndsAtTheAttemptDeadline(t *testing.T) {
	t.Parallel()

	const attemptDeadline = 2 * time.Second

	tokens := stalledTokenServer(t)

	// The refresh timeout sits far beyond the watchdog, so only an attempt
	// ending at its own deadline can pass.
	auth, err := newGrpcResumeAuthWithTimeout(t.Context(), GRPCOAuthConfig{
		ClientID:     "client-id",
		ClientSecret: "secret",
		TokenURL:     tokens.URL,
	}, 5*time.Minute)
	require.NoError(t, err)

	srv := &fakeSandboxServer{
		resp: &proxygrpc.SandboxResumeResponse{OrchestratorIp: "10.0.0.9"},
	}
	conn := startFakeServer(t, srv)
	flights := newTestResumeFlights(t, newMetricReader(t), &grpcPausedSandboxResumer{
		conn:   conn,
		client: proxygrpc.NewSandboxServiceClient(conn),
		auth:   auth,
	})
	flights.featureFlags = newFFWithParking(t, true, 15000, 512)
	flights.attemptCeiling = attemptDeadline

	result := resumeInBackgroundWith(flights, t.Context(), 80, "", "")
	var served resumeResult
	select {
	case served = <-result:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the attempt did not end with its deadline while the token refresh stayed blocked")
	}
	require.Nil(t, served.route)

	var capacityErr *reverseproxy.SandboxCapacityError
	require.ErrorAs(t, served.err, &capacityErr)
	require.Equal(t, errAttemptDeadline.Error(), capacityErr.Message)
	require.EqualValues(t, 0, srv.calls.Load())
	require.Zero(t, flightsInRegistry(flights))
}

func TestGRPCPausedSandboxResumer_Init(t *testing.T) {
	t.Parallel()

	srv := &fakeSandboxServer{}
	conn := startFakeServer(t, srv)

	r := &grpcPausedSandboxResumer{
		conn:   conn,
		client: proxygrpc.NewSandboxServiceClient(conn),
		auth:   noopGrpcResumeAuth{},
	}

	// Should not panic
	r.Init(t.Context())
}

func TestGRPCPausedSandboxResumer_Close(t *testing.T) {
	t.Parallel()

	srv := &fakeSandboxServer{}
	conn := startFakeServer(t, srv)

	r := &grpcPausedSandboxResumer{
		conn:   conn,
		client: proxygrpc.NewSandboxServiceClient(conn),
		auth:   noopGrpcResumeAuth{},
	}

	require.NoError(t, r.Close(t.Context()))
}

func TestNewGRPCPausedSandboxResumer_InternalDialsWithTheHopCredentials(t *testing.T) {
	t.Parallel()

	creds := servicetest.NewCredentials()
	r, err := NewGRPCPausedSandboxResumer(t.Context(), servicetest.GRPCServer(t), GRPCOAuthConfig{}, false, creds)
	require.NoError(t, err)
	resumer, ok := r.(*grpcPausedSandboxResumer)
	require.True(t, ok)
	t.Cleanup(func() { _ = resumer.Close(t.Context()) })

	resumer.conn.Connect()
	servicetest.AwaitHandshake(t, creds)
}

func TestDialCredentials_EdgeKeepsItsTLS(t *testing.T) {
	t.Parallel()

	hop := servicetest.NewCredentials()
	creds := dialCredentials(true, hop)
	assert.NotSame(t, hop, creds)
	assert.Equal(t, "tls", creds.Info().SecurityProtocol)
}
