package clusters

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"

	grpcshared "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// NodeClient is a gRPC connection to one orchestrator or template-manager node.
type NodeClient struct {
	conn *grpc.ClientConn
	info infogrpc.InfoServiceClient
}

// NewNodeClient connects lazily to address ("host:port") with creds: the
// connection is made on the first call, so an unreachable node is not an error here.
func NewNodeClient(tel *telemetry.Client, address string, creds credentials.TransportCredentials) (*NodeClient, error) {
	conn, err := grpc.NewClient(address, dialOptions(tel, creds)...)
	if err != nil {
		return nil, fmt.Errorf("failed to create node client for %s: %w", address, err)
	}

	return &NodeClient{conn: conn, info: infogrpc.NewInfoServiceClient(conn)}, nil
}

// Dialer returns a DialFunc that connects to each discovered node's address with creds.
func Dialer(tel *telemetry.Client, creds credentials.TransportCredentials) DialFunc {
	return func(item servicediscovery.Instance) (*NodeClient, error) {
		return NewNodeClient(tel, item.Address(), creds)
	}
}

func dialOptions(tel *telemetry.Client, creds credentials.TransportCredentials) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithStatsHandler(
			otelgrpc.NewClientHandler(
				otelgrpc.WithTracerProvider(tel.TracerProvider),
				otelgrpc.WithMeterProvider(tel.MeterProvider),
				grpcshared.WithSandboxClientMetricAttributes(),
			),
		),
		grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Time:                30 * time.Second,
				Timeout:             5 * time.Second,
				PermitWithoutStream: true,
			},
		),
		grpc.WithTransportCredentials(creds),
	}
}

func (c *NodeClient) ServiceInfo(ctx context.Context) (*infogrpc.ServiceInfoResponse, error) {
	return c.info.ServiceInfo(ctx, &emptypb.Empty{})
}

func (c *NodeClient) State() connectivity.State {
	return c.conn.GetState()
}

func (c *NodeClient) Close() error {
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("failed to close node connection: %w", err)
	}

	return nil
}
