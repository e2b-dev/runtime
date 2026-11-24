package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/config"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/server"
	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/mtlstest"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const testMethod = "/test.Service/Call"

var testServiceDesc = grpc.ServiceDesc{
	ServiceName: "test.Service",
	HandlerType: (*any)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Call",
		Handler: func(_ any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			in := new(emptypb.Empty)
			if err := dec(in); err != nil {
				return nil, err
			}
			handler := func(context.Context, any) (any, error) { return &emptypb.Empty{}, nil }
			if interceptor == nil {
				return handler(ctx, in)
			}

			return interceptor(ctx, in, &grpc.UnaryServerInfo{FullMethod: testMethod}, handler)
		},
	}},
}

func TestMutualTLS(t *testing.T) {
	t.Parallel()
	const apiID = "spiffe://test.example/api"
	const serverID = "spiffe://test.example/scheduling-api"
	ca := mtlstest.NewRootCA(t, "trusted")
	newProcess := func(t *testing.T, signer *mtlstest.CA, identity string) *mtls.Process {
		t.Helper()
		dir := mtlstest.WriteFiles(t, signer.Leaf(t, mtlstest.LeafSpec{SPIFFEID: identity}), ca.BundlePEM())
		proc, err := mtls.NewProcess(t.Context(), telemetry.NewNoopClient().MeterProvider, nil, nil,
			mtls.WithFileConfig(mtls.FileConfig{CertFile: dir.CertFile, KeyFile: dir.KeyFile, CAFile: dir.CAFile}))
		require.NoError(t, err)

		return proc
	}
	cfg, err := config.ServerTLS(newProcess(t, ca, serverID), mtls.ListenerSettings{Allow: []string{apiID}})
	require.NoError(t, err)
	srv, err := server.New(telemetry.NewNoopClient(), cfg, &fakeStorage{}, &fakeState{})
	require.NoError(t, err)
	srv.RegisterService(&testServiceDesc, struct{}{})
	healthv1.RegisterHealthServer(srv, health.NewServer())
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	for _, tc := range []struct {
		name               string
		signer             *mtlstest.CA
		identity, expected string
		allowed            bool
	}{
		{"trusted API", ca, apiID, serverID, true},
		{"wrong caller identity", ca, "spiffe://test.example/other", serverID, false},
		{"untrusted caller", mtlstest.NewRootCA(t, "other"), apiID, serverID, false},
		{"wrong server identity", ca, apiID, "spiffe://test.example/orchestrator", false},
		{"plaintext", nil, "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			creds := insecure.NewCredentials()
			if tc.signer != nil {
				hop, err := newProcess(t, tc.signer, tc.identity).Hop("scheduling", "", mtls.HopSettings{Mode: "on", Expect: []string{tc.expected}})
				require.NoError(t, err)
				creds = mtls.NewClientCredentials(hop)
			}
			conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(creds))
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			err = conn.Invoke(ctx, testMethod, &emptypb.Empty{}, &emptypb.Empty{})
			if !tc.allowed {
				require.Error(t, err)
				if tc.signer == nil {
					_, err = healthv1.NewHealthClient(conn).Check(ctx, &healthv1.HealthCheckRequest{})
					require.NoError(t, err, "plaintext is limited to health probes")
					stream, err := statepb.NewStateServiceClient(conn).TeamItems(ctx, statepb.TeamItemsRequest_builder{TeamId: uuid.NewString()}.Build())
					require.NoError(t, err)
					_, err = stream.Recv()
					require.Equal(t, codes.Unauthenticated, status.Code(err), "plaintext streams are rejected: %v", err)
				}

				return
			}
			require.NoError(t, err)
		})
	}
}
