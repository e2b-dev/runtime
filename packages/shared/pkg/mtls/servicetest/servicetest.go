// Package servicetest helps a service's tests prove its mtls wiring: a
// configured Process whose instruments the test reads, a flag client the
// test drives, credentials that count a dial site's handshakes, and a
// plaintext gRPC server to dial.
package servicetest

import (
	"context"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
)

// NewProcess builds a configured Process over an empty certificate
// directory, as on a pod whose Secret is not written yet, recording into
// the returned reader. reader is the service's flag client, or nil. Flag
// modes are re-read hourly, so only the test's own calls move the gauges.
func NewProcess(tb testing.TB, reader mtls.FlagReader) (*mtls.Process, *sdkmetric.ManualReader) {
	tb.Helper()

	metrics := sdkmetric.NewManualReader()
	dir := tb.TempDir()
	proc, err := mtls.NewProcess(tb.Context(), sdkmetric.NewMeterProvider(sdkmetric.WithReader(metrics)), reader, logger.NewNopLogger(),
		mtls.WithFileConfig(mtls.FileConfig{
			CertFile: filepath.Join(dir, "tls.crt"),
			KeyFile:  filepath.Join(dir, "tls.key"),
			CAFile:   filepath.Join(dir, "ca.crt"),
		}),
		mtls.WithWatchInterval(time.Hour))
	require.NoError(tb, err, "servicetest: process")

	return proc, metrics
}

// Flags returns a flag client over a test data source, and the source. An
// unset flag is missing, so its mode stays on the fallback.
func Flags(tb testing.TB) (*featureflags.Client, *ldtestdata.TestDataSource) {
	tb.Helper()

	source := ldtestdata.DataSource()
	client, err := featureflags.NewClientWithDatasource(source)
	require.NoError(tb, err, "servicetest: flag client")
	tb.Cleanup(func() { _ = client.Close(context.WithoutCancel(tb.Context())) })

	return client, source
}

// SetFlag serves value for key to every context.
func SetFlag(source *ldtestdata.TestDataSource, key, value string) {
	source.Update(source.Flag(key).ValueForAll(ldvalue.String(value)))
}

// ListenerMode is the mode gauge of listener in mode from source.
func ListenerMode(tb testing.TB, metrics *sdkmetric.ManualReader, listener, mode, source string) int64 {
	tb.Helper()

	return Point(tb, metrics, mtls.MetricMode,
		attribute.String(mtls.AttrListener, listener), attribute.String(mtls.AttrMode, mode), attribute.String(mtls.AttrSource, source))
}

// HopMode is the client mode gauge of hop in mode from source.
func HopMode(tb testing.TB, metrics *sdkmetric.ManualReader, hop, mode, source string) int64 {
	tb.Helper()

	return Point(tb, metrics, mtls.MetricClientMode,
		attribute.String(mtls.AttrClientHop, hop), attribute.String(mtls.AttrMode, mode), attribute.String(mtls.AttrSource, source))
}

// Point is the value of the int64 gauge or counter name whose attributes
// are exactly attrs, or -1 when that series is absent.
func Point(tb testing.TB, metrics *sdkmetric.ManualReader, name string, attrs ...attribute.KeyValue) int64 {
	tb.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(tb, metrics.Collect(tb.Context(), &rm), "servicetest: collect")
	want := attribute.NewSet(attrs...)
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			var points []metricdata.DataPoint[int64]
			switch data := m.Data.(type) {
			case metricdata.Gauge[int64]:
				points = data.DataPoints
			case metricdata.Sum[int64]:
				points = data.DataPoints
			}
			for _, p := range points {
				if p.Attributes.Equals(&want) {
					return p.Value
				}
			}
		}
	}

	return -1
}

// Credentials are plaintext client credentials that count their handshakes,
// standing in for a hop's credentials at a dial site.
type Credentials struct {
	credentials.TransportCredentials

	handshakes atomic.Int64
}

// NewCredentials returns plaintext credentials that count their handshakes.
func NewCredentials() *Credentials {
	return &Credentials{TransportCredentials: insecure.NewCredentials()}
}

// ClientHandshake implements credentials.TransportCredentials.
func (c *Credentials) ClientHandshake(ctx context.Context, authority string, conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	c.handshakes.Add(1)

	return c.TransportCredentials.ClientHandshake(ctx, authority, conn)
}

// Clone returns c, so a clone counts into the same total.
func (c *Credentials) Clone() credentials.TransportCredentials {
	return c
}

// Handshakes is how many connections were dialled with these credentials.
func (c *Credentials) Handshakes() int64 {
	return c.handshakes.Load()
}

// AwaitHandshake waits up to five seconds for creds to dial once, for a
// client that connects in the background.
func AwaitHandshake(tb testing.TB, creds *Credentials) {
	tb.Helper()

	require.Eventually(tb, func() bool { return creds.Handshakes() > 0 }, 5*time.Second, 10*time.Millisecond,
		"servicetest: no handshake through the credentials")
}

// GRPCServer serves the gRPC health service in plaintext on a loopback port
// until the test ends, and returns its address.
func GRPCServer(tb testing.TB) string {
	tb.Helper()

	var lc net.ListenConfig
	lis, err := lc.Listen(tb.Context(), "tcp", "127.0.0.1:0")
	require.NoError(tb, err, "servicetest: listen")
	server := grpc.NewServer()
	healthpb.RegisterHealthServer(server, health.NewServer())
	go func() { _ = server.Serve(lis) }()
	tb.Cleanup(server.Stop)

	return lis.Addr().String()
}
