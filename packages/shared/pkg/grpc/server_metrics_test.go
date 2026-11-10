package grpc

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// The team and resume flag reach the server's RPC duration metric from the
// caller's metadata, since the payload is not decoded yet when otelgrpc tags it.
func TestSandboxMetricAttributesComeFromMetadata(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		md   metadata.MD
		want map[string]string
	}{
		"team and resume": {
			md:   metadata.Pairs(TeamIDMetadataKey, "team-1", IsResumeMetadataKey, "true"),
			want: map[string]string{"team.id": "team-1", "sandbox.resume": "true"},
		},
		"no metadata": {
			md:   metadata.MD{},
			want: map[string]string{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			reader := sdkmetric.NewManualReader()
			client := serveSandboxServiceWithMeter(t, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

			_, err := client.ResumeSandbox(metadata.NewOutgoingContext(t.Context(), tc.md), &proxygrpc.SandboxResumeRequest{})
			require.NoError(t, err)

			got := awaitRPCAttrs(t, reader, "rpc.server.")

			for _, key := range []string{"team.id", "sandbox.resume"} {
				want, ok := tc.want[key]
				if !ok {
					assert.NotContains(t, got, key)

					continue
				}
				assert.Equal(t, want, got[key], key)
			}
		})
	}
}

// The caller's RPC duration metric carries the same team and resume flag, read
// from the metadata it sends, so the client side splits by team on its own.
func TestSandboxClientMetricAttributesComeFromOutgoingMetadata(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	client := serveSandboxServiceWithMeter(t, sdkmetric.NewMeterProvider(),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(
			otelgrpc.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))),
			WithSandboxClientMetricAttributes(),
		)),
	)

	ctx := metadata.AppendToOutgoingContext(t.Context(), TeamIDMetadataKey, "team-1", IsResumeMetadataKey, "true")
	_, err := client.ResumeSandbox(ctx, &proxygrpc.SandboxResumeRequest{})
	require.NoError(t, err)

	got := awaitRPCAttrs(t, reader, "rpc.client.")
	assert.Equal(t, "team-1", got["team.id"])
	assert.Equal(t, "true", got["sandbox.resume"])
}

// serveSandboxServiceWithMeter starts a metered sandbox service and dials it with
// clientOpts on top of plaintext credentials.
func serveSandboxServiceWithMeter(t *testing.T, meterProvider *sdkmetric.MeterProvider, clientOpts ...grpc.DialOption) proxygrpc.SandboxServiceClient {
	t.Helper()

	server := NewGRPCServer(&telemetry.Client{
		TracerProvider: tracenoop.NewTracerProvider(),
		MeterProvider:  meterProvider,
	}, WithSandboxMetricAttributes())
	proxygrpc.RegisterSandboxServiceServer(server, respondingService{})

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(listener.Addr().String(), append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, clientOpts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return proxygrpc.NewSandboxServiceClient(conn)
}

// awaitRPCAttrs waits for the one duration data point whose metric name starts
// with prefix, which lands when the RPC ends, and returns its attributes.
func awaitRPCAttrs(t *testing.T, reader *sdkmetric.ManualReader, prefix string) map[string]string {
	t.Helper()

	var got map[string]string
	require.Eventually(t, func() bool {
		got = rpcAttrs(t, reader, prefix)

		return got != nil
	}, 5*time.Second, 10*time.Millisecond, "%s duration is recorded when the RPC ends", prefix)

	return got
}

// rpcAttrs returns the attributes of the one duration data point whose metric
// name starts with prefix, or nil before it is recorded.
func rpcAttrs(t *testing.T, reader *sdkmetric.ManualReader, prefix string) map[string]string {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if !strings.HasPrefix(m.Name, prefix) {
				continue
			}

			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok || len(hist.DataPoints) == 0 {
				continue
			}

			out := map[string]string{}
			for _, kv := range hist.DataPoints[0].Attributes.ToSlice() {
				if kv.Value.Type() == attribute.BOOL {
					out[string(kv.Key)] = kv.Value.Emit()

					continue
				}
				out[string(kv.Key)] = kv.Value.AsString()
			}

			return out
		}
	}

	return nil
}
