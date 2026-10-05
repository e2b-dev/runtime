//go:build linux

package service

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

func TestServiceInfoSandboxLimit(t *testing.T) {
	t.Parallel()

	for name, roles := range map[string][]orchestratorinfo.ServiceInfoRole{
		"sandbox": {orchestratorinfo.ServiceInfoRole_Orchestrator},
		"builder": {orchestratorinfo.ServiceInfoRole_TemplateBuilder},
		"mixed":   {orchestratorinfo.ServiceInfoRole_Orchestrator, orchestratorinfo.ServiceInfoRole_TemplateBuilder},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			info := &ServiceInfo{Roles: roles}
			info.MaxSandboxes.Store(200)

			listener := bufconn.Listen(1 << 20)
			server := grpc.NewServer()
			orchestratorinfo.RegisterInfoServiceServer(server, NewInfoService(
				info, sandbox.NewSandboxesMap(), metrics.NewHostMetrics(),
			))
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(func() {
				server.Stop()
				_ = listener.Close()
			})
			conn, err := grpc.NewClient("passthrough:///sandbox-limit",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return listener.DialContext(ctx)
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			client := orchestratorinfo.NewInfoServiceClient(conn)
			assertLimit := func(want int64) {
				t.Helper()
				response, err := client.ServiceInfo(t.Context(), &emptypb.Empty{})
				require.NoError(t, err)
				require.Equal(t, want, response.GetMaxSandboxes())
			}

			assertLimit(200)
			for _, limit := range []int64{320, 80, 0, -1} {
				info.MaxSandboxes.Store(limit)
				assertLimit(limit)
			}
		})
	}
}
