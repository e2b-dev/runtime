package clusters

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	grpcshared "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

type instanceAuthorization struct {
	secret            string
	serviceInstanceID string
	tls               bool
}

func (a instanceAuthorization) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{consts.EdgeRpcAuthHeader: a.secret, consts.EdgeRpcServiceInstanceIDHeader: a.serviceInstanceID}, nil
}

func (a instanceAuthorization) RequireTransportSecurity() bool {
	return a.tls
}

func createClient(tel *telemetry.Client, endpoint string, endpointTLS bool, plaintext credentials.TransportCredentials) (*GRPCClient, error) {
	conn, err := grpc.NewClient(endpoint, dialOptions(tel, endpoint, endpointTLS, plaintext)...)
	if err != nil {
		return nil, fmt.Errorf("failed to create client client: %w", err)
	}

	return NewGRPCClient(conn, "cluster-orchestrator"), nil
}

func dialOptions(tel *telemetry.Client, endpoint string, endpointTLS bool, plaintext credentials.TransportCredentials) []grpc.DialOption {
	grpcOptions := []grpc.DialOption{
		grpc.WithStatsHandler(
			otelgrpc.NewClientHandler(
				otelgrpc.WithTracerProvider(tel.TracerProvider),
				otelgrpc.WithMeterProvider(tel.MeterProvider),
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
	}

	if endpointTLS {
		// (2025-06) AWS ALB with TLS termination is using TLS 1.2 as default so this is why we are not using TLS 1.3+ here
		cred := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
		grpcOptions = append(grpcOptions, grpc.WithAuthority(endpoint), grpc.WithTransportCredentials(cred))
	} else {
		grpcOptions = append(grpcOptions, grpc.WithTransportCredentials(plaintext))
	}

	return grpcOptions
}
