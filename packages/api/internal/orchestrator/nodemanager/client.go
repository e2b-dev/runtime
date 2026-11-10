package nodemanager

import (
	"fmt"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	grpcshared "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

var OrchestratorToApiNodeStateMapper = map[orchestratorinfo.ServiceInfoStatus]api.NodeStatus{
	orchestratorinfo.ServiceInfoStatus_Healthy:      api.NodeStatusReady,
	orchestratorinfo.ServiceInfoStatus_Draining:     api.NodeStatusDraining,
	orchestratorinfo.ServiceInfoStatus_Unhealthy:    api.NodeStatusUnhealthy,
	orchestratorinfo.ServiceInfoStatus_Standby:      api.NodeStatusStandby,
	orchestratorinfo.ServiceInfoStatus_ShuttingDown: api.NodeStatusShuttingDown,
	orchestratorinfo.ServiceInfoStatus_NotReady:     api.NodeStatusNotReady,
}

func NewClient(tracerProvider trace.TracerProvider, meterProvider metric.MeterProvider, host string, creds credentials.TransportCredentials) (*clusters.GRPCClient, error) {
	conn, err := grpc.NewClient(host,
		grpc.WithTransportCredentials(creds),
		grpc.WithStatsHandler(
			otelgrpc.NewClientHandler(
				otelgrpc.WithTracerProvider(tracerProvider),
				otelgrpc.WithMeterProvider(meterProvider),
				grpcshared.WithSandboxClientMetricAttributes(),
			),
		),
		grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Time:                30 * time.Second, // Send ping every 30s
				Timeout:             5 * time.Second,  // Wait 5s for response
				PermitWithoutStream: true,             // Allow pings even without active streams
			},
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to establish GRPC connection: %w", err)
	}

	return clusters.NewGRPCClient(conn, "orchestrator"), nil
}
