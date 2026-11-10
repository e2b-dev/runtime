package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/config"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/db/dbtest"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/server"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/store"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/mtlstest"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

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
	pool := dbtest.MigratedPool(t)
	srv, err := server.New(telemetry.NewNoopClient(), store.New(pool, pool), cfg)
	require.NoError(t, err)
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
			client := scheduling.NewSchedulingServiceClient(conn)
			project, reservation := uuid.NewString(), uuid.NewString()
			_, err = client.Get(ctx, scheduling.GetRequest_builder{ProjectId: project, SandboxId: sandboxID}.Build())
			if !tc.allowed {
				require.Error(t, err)
				if tc.signer == nil {
					_, err = healthv1.NewHealthClient(conn).Check(ctx, &healthv1.HealthCheckRequest{})
					require.NoError(t, err, "plaintext is limited to health probes")
				}

				return
			}
			require.NoError(t, err)
			_, err = client.Reserve(ctx, reserveRequest(project, sandboxID, reservation, 10))
			require.NoError(t, err)
			_, err = client.Put(ctx, putRequest(project, reservation))
			require.NoError(t, err)
			got, err := client.Get(ctx, scheduling.GetRequest_builder{ProjectId: project, SandboxId: sandboxID}.Build())
			require.NoError(t, err)
			requireSandbox(t, project, got.GetSandbox())
		})
	}
}
