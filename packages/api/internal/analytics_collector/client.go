package analyticscollector

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
)

type Analytics struct {
	client     AnalyticsCollectorClient
	connection *grpc.ClientConn
}

// NewAnalyticsWithClient wraps an existing collector client — tests inject a
// recording client through it.
func NewAnalyticsWithClient(client AnalyticsCollectorClient) *Analytics {
	return &Analytics{client: client}
}

// NewAnalytics creates a client for the analytics collector.
//
// host is either a bare hostname, in which case the collector is assumed to be
// served on the default HTTPS port (the managed deployment puts it behind a
// load balancer on 443), or an explicit "host:port" address. An empty host
// returns a no-op client that drops every event.
//
// useTLS must be false for collectors that speak plaintext gRPC, such as the
// one running in local development; the per-RPC API key is then sent over an
// unencrypted connection, so only do that on loopback. plaintext is the
// in-cluster hop's credentials, used when useTLS is false.
func NewAnalytics(host, grpcAPIKey string, useTLS bool, plaintext credentials.TransportCredentials) (*Analytics, error) {
	var client AnalyticsCollectorClient
	var connection *grpc.ClientConn

	// Run dummy client if host is not provided
	if host != "" {
		target := host
		if _, _, err := net.SplitHostPort(host); err != nil {
			// No port in host, default to the HTTPS one.
			target = net.JoinHostPort(host, "443")
		}

		var cred credentials.TransportCredentials
		if useTLS {
			systemRoots, err := x509.SystemCertPool()
			if err != nil {
				errMsg := fmt.Errorf("failed to read system root certificate pool: %w", err)

				return nil, errMsg
			}

			cred = credentials.NewTLS(&tls.Config{
				RootCAs:    systemRoots,
				MinVersion: tls.VersionTLS13,
			})
		} else {
			// The in-cluster hop: plaintext while its mutual TLS mode is off.
			cred = plaintext
		}

		conn, err := grpc.NewClient(
			target,
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
			grpc.WithPerRPCCredentials(newGRPCAPIKey(grpcAPIKey, useTLS)),
			grpc.WithAuthority(host),
			grpc.WithTransportCredentials(cred),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create GRPC client: %w", err)
		}

		connection = conn
		client = NewAnalyticsCollectorClient(connection)
	}

	return &Analytics{client: client, connection: connection}, nil
}

func (a *Analytics) Close() error {
	if a.connection == nil {
		return nil
	}

	err := a.connection.Close()
	if err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}

	return nil
}

func (a *Analytics) Init(ctx context.Context) {
	e2bgrpc.ObserveConnection(ctx, a.connection, "analytics-collector")
}

func (a *Analytics) InstanceStarted(ctx context.Context, in *InstanceStartedEvent, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	if a.client == nil {
		return nil, nil
	}

	return a.client.InstanceStarted(ctx, in, opts...)
}

func (a *Analytics) RunningInstances(ctx context.Context, in *RunningInstancesEvent, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	if a.client == nil {
		return nil, nil
	}

	return a.client.RunningInstances(ctx, in, opts...)
}

func (a *Analytics) InstanceStopped(ctx context.Context, in *InstanceStoppedEvent, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	if a.client == nil {
		return nil, nil
	}

	return a.client.InstanceStopped(ctx, in, opts...)
}
