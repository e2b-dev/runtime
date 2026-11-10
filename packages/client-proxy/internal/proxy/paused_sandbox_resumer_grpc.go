package proxy

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
)

type grpcPausedSandboxResumer struct {
	conn   *grpc.ClientConn
	client proxygrpc.SandboxServiceClient
	auth   grpcResumeAuth
}

type GRPCOAuthConfig struct {
	ClientID     string
	ClientSecret string
	TokenURL     string
}

// NewGRPCPausedSandboxResumer dials the api's internal port through
// internal, or, with useTLS, the edge port with system-root TLS.
func NewGRPCPausedSandboxResumer(ctx context.Context, address string, oauthConfig GRPCOAuthConfig, useTLS bool, internal credentials.TransportCredentials) (PausedSandboxResumer, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return nil, errors.New("api grpc address is required")
	}

	auth, err := newGrpcResumeAuth(ctx, oauthConfig)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(dialCredentials(useTLS, internal)),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("create grpc client: %w", err)
	}

	return &grpcPausedSandboxResumer{
		conn:   conn,
		client: proxygrpc.NewSandboxServiceClient(conn),
		auth:   auth,
	}, nil
}

// dialCredentials is internal, or, with useTLS, system-root TLS.
func dialCredentials(useTLS bool, internal credentials.TransportCredentials) credentials.TransportCredentials {
	if useTLS {
		return credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}

	return internal
}

func (c *grpcPausedSandboxResumer) Init(ctx context.Context) {
	e2bgrpc.ObserveConnection(ctx, c.conn, "api-resumer")
}

func (c *grpcPausedSandboxResumer) Close(_ context.Context) error {
	return c.conn.Close()
}

func (c *grpcPausedSandboxResumer) Resume(ctx context.Context, sandboxId string, sandboxPort uint64, trafficAccessToken string, envdAccessToken string) (*catalog.SandboxInfo, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, proxygrpc.MetadataSandboxRequestPort, strconv.FormatUint(sandboxPort, 10))

	if trafficAccessToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, proxygrpc.MetadataTrafficAccessToken, trafficAccessToken)
	}

	if envdAccessToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, proxygrpc.MetadataEnvdAccessToken, envdAccessToken)
	}

	ctx, err := c.auth.authorize(ctx)
	if err != nil {
		return nil, err
	}

	// The token may have arrived as the attempt ended; an attempt that
	// ended must not dial.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	resp, err := c.client.ResumeSandbox(ctx, &proxygrpc.SandboxResumeRequest{
		SandboxId: sandboxId,
	})
	if err != nil {
		return nil, fmt.Errorf("grpc resume: %w", err)
	}

	return resumeRoute(resp), nil
}

// resumeRoute is the route the API answered with, in the routing record's
// form. An absent placement is the legacy network, as in the record.
func resumeRoute(resp *proxygrpc.SandboxResumeResponse) *catalog.SandboxInfo {
	route := &catalog.SandboxInfo{OrchestratorIP: strings.TrimSpace(resp.GetOrchestratorIp())}
	if placement := resp.GetNetworkPlacement(); placement != nil {
		route.ExecutionID = placement.GetExecutionId()
		route.NetworkPlacement = catalog.NetworkPlacementOf(placement)
	}

	return route
}
