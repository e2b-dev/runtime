//go:build linux

package service

import (
	"context"
	"net"
	"sync"
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

func TestWorkOwnershipHandoff(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{}
	parentDone := info.TrackWork()
	childDone := info.TrackWork()
	require.Equal(t, int64(2), info.OutstandingWork())
	parentDone()
	require.Equal(t, int64(1), info.OutstandingWork())
	childDone()
	require.Zero(t, info.OutstandingWork())
}

func TestConcurrentWorkOwnership(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{}
	var started, finished sync.WaitGroup
	release := make(chan struct{})
	for range 32 {
		started.Add(1)
		finished.Go(func() {
			done := info.TrackWork()
			defer done()
			started.Done()
			<-release
		})
	}
	started.Wait()
	require.Equal(t, int64(32), info.OutstandingWork())
	close(release)
	finished.Wait()
	require.Zero(t, info.OutstandingWork())
}

func TestServiceInfoReportsOutstandingWork(t *testing.T) {
	t.Parallel()

	for name, roles := range map[string][]orchestratorinfo.ServiceInfoRole{
		"sandbox": {orchestratorinfo.ServiceInfoRole_Orchestrator},
		"builder": {orchestratorinfo.ServiceInfoRole_TemplateBuilder},
		"mixed":   {orchestratorinfo.ServiceInfoRole_Orchestrator, orchestratorinfo.ServiceInfoRole_TemplateBuilder},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			info := &ServiceInfo{Roles: roles}
			listener := bufconn.Listen(1 << 20)
			server := grpc.NewServer()
			orchestratorinfo.RegisterInfoServiceServer(server, NewInfoService(info, sandbox.NewSandboxesMap(), metrics.NewHostMetrics()))
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(func() {
				server.Stop()
				_ = listener.Close()
			})
			conn, err := grpc.NewClient("passthrough:///work-info",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return listener.DialContext(ctx)
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			client := orchestratorinfo.NewInfoServiceClient(conn)
			assertCount := func(want uint64) {
				t.Helper()

				response, err := client.ServiceInfo(t.Context(), &emptypb.Empty{})
				require.NoError(t, err)
				require.Equal(t, want, response.GetOutstandingWork())
				require.Zero(t, response.GetMetricSandboxesRunning())
			}

			assertCount(0)
			parentDone := info.TrackWork()
			childDone := info.TrackWork()
			assertCount(2)
			parentDone()
			assertCount(1)
			childDone()
			assertCount(0)
		})
	}
}
