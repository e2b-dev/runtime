package mtls

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/mtlstest"
)

// Example identities. The trust domain and names are fixtures, not deployments.
const (
	trustDomain = "cluster-a.example.internal"
	serverID    = "spiffe://cluster-a.example.internal/ns/platform/sa/server"
	clientID    = "spiffe://cluster-a.example.internal/ns/platform/sa/client"
	strangerID  = "spiffe://cluster-a.example.internal/ns/platform/sa/stranger"
	otherDomain = "spiffe://cluster-b.example.internal/ns/platform/sa/client"
	serverDNS   = "server.platform.svc.cluster.local"
)

// fixture is one identity: a root, an intermediate, a leaf, its files on
// disk and the Files that loaded them, plus the logger and metric reader
// every component built on the fixture shares.
type fixture struct {
	root    *mtlstest.CA
	inter   *mtlstest.CA
	leaf    *mtlstest.Leaf
	dir     mtlstest.Dir
	files   *Files
	log     logger.Logger
	logs    *observer.ObservedLogs
	metrics *Metrics
	reader  *sdkmetric.ManualReader
}

// newFixture mints serverID under a fresh root and intermediate and loads its
// files with a reload interval long enough that only explicit reloads happen.
func newFixture(t *testing.T, dnsNames ...string) *fixture {
	t.Helper()

	root := mtlstest.NewRootCA(t, "root")
	inter := root.Intermediate(t, "intermediate")
	leaf := inter.Leaf(t, mtlstest.LeafSpec{SPIFFEID: serverID, DNSNames: dnsNames})
	dir := mtlstest.WriteFiles(t, leaf, root.BundlePEM())
	log, logs := testLogger(t)
	metrics, reader := testMetrics(t)
	files := NewFiles(t.Context(),
		FileConfig{CertFile: dir.CertFile, KeyFile: dir.KeyFile, CAFile: dir.CAFile},
		WithReloadInterval(time.Hour), WithFilesLogger(log), WithFilesMetrics(metrics))
	require.NotNil(t, files.Bundle())

	return &fixture{root: root, inter: inter, leaf: leaf, dir: dir, files: files, log: log, logs: logs, metrics: metrics, reader: reader}
}

// peerLeaf mints another identity under the fixture's intermediate, so the
// fixture's roots verify it.
func (fx *fixture) peerLeaf(t *testing.T, id string, dnsNames ...string) *mtlstest.Leaf {
	t.Helper()

	return fx.inter.Leaf(t, mtlstest.LeafSpec{SPIFFEID: id, DNSNames: dnsNames})
}

// peerFiles loads files for another identity under the fixture's root, for
// the other end of a handshake.
func (fx *fixture) peerFiles(t *testing.T, id string) *Files {
	t.Helper()

	leaf := fx.peerLeaf(t, id)
	dir := mtlstest.WriteFiles(t, leaf, fx.root.BundlePEM())
	files := NewFiles(t.Context(),
		FileConfig{CertFile: dir.CertFile, KeyFile: dir.KeyFile, CAFile: dir.CAFile},
		WithReloadInterval(time.Hour), WithFilesLogger(fx.log), WithFilesMetrics(fx.metrics))
	require.NotNil(t, files.Bundle())

	return files
}

// newServerConfig builds a listener configuration over the fixture that
// admits the allowed names, with a short handshake deadline.
func newServerConfig(t *testing.T, fx *fixture, mode ModeSource, allowed ...string) ServerConfig {
	t.Helper()

	return ServerConfig{
		Name:             "test-listener",
		Files:            fx.files,
		Mode:             mode,
		Allow:            mustAllow(t, allowed...),
		HandshakeTimeout: time.Second,
		Logger:           fx.log,
		Metrics:          fx.metrics,
	}
}

// newClientConfig builds a client hop configuration presenting files and
// expecting one of the server names.
func newClientConfig(t *testing.T, files *Files, log logger.Logger, mode ClientModeSource, serverName string, expected ...string) ClientConfig {
	t.Helper()

	return ClientConfig{
		Name:              "test-hop",
		Files:             files,
		Mode:              mode,
		ServerName:        serverName,
		ExpectedServerIDs: mustAllow(t, expected...),
		Logger:            log,
	}
}

func mustAllow(t *testing.T, entries ...string) *AllowList {
	t.Helper()

	list, err := ParseAllowList(entries)
	require.NoError(t, err)

	return list
}

// rawClientTLS is a hand-built client that presents leaf (nil for none) and
// does not verify the server, for tests about what the server admits.
func rawClientTLS(leaf *mtlstest.Leaf, nextProtos ...string) *tls.Config {
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		NextProtos:         nextProtos,
		ServerName:         serverDNS,
		InsecureSkipVerify: true, // the server is not under test on this side
	}
	if leaf != nil {
		cfg.Certificates = []tls.Certificate{leaf.TLS}
	}

	return cfg
}

// handshakeResult is what one in-memory handshake produced.
type handshakeResult struct {
	serverErr error
	clientErr error
	client    tls.ConnectionState
}

// handshake runs one TLS handshake between a server built from serverCfg and
// a client built from clientCfg over a loopback TCP connection. The server
// writes one byte once it has accepted, so a refusal surfaces as the client's
// read error even though a TLS 1.3 client finishes its handshake before the
// server has verified it. A kernel-buffered socket, not net.Pipe: on a resumed
// session the server writes its alert while the client is still writing its
// Finished, and a synchronous pipe deadlocks both sides.
func handshake(t *testing.T, serverCfg, clientCfg *tls.Config) handshakeResult {
	t.Helper()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	var dialer net.Dialer
	clientSide, err := dialer.DialContext(t.Context(), "tcp", ln.Addr().String())
	require.NoError(t, err)
	serverSide, err := ln.Accept()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = serverSide.Close()
		_ = clientSide.Close()
	})
	server := tls.Server(serverSide, serverCfg)
	client := tls.Client(clientSide, clientCfg)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	serverDone := make(chan error, 1)
	go func() {
		err := server.HandshakeContext(ctx)
		if err == nil {
			_, err = server.Write([]byte{1})
		}
		serverDone <- err
	}()

	var result handshakeResult
	result.clientErr = client.HandshakeContext(ctx)
	if result.clientErr == nil {
		var ack [1]byte
		_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, result.clientErr = io.ReadFull(client, ack[:])
	}
	result.client = client.ConnectionState()
	// Unblock a server still writing an alert.
	_ = clientSide.Close()
	result.serverErr = <-serverDone

	return result
}

// testLogger returns a logger whose entries the test can inspect, so tests
// never replace the global logger and can run in parallel.
func testLogger(t *testing.T) (logger.Logger, *observer.ObservedLogs) {
	t.Helper()

	core, logs := observer.New(zap.DebugLevel)

	return logger.NewTracedLoggerFromCore(core), logs
}

func logMessages(logs *observer.ObservedLogs) []string {
	entries := logs.All()
	messages := make([]string, 0, len(entries))
	for _, entry := range entries {
		messages = append(messages, entry.Message)
	}

	return messages
}

// mtlstestLeafSpecNoSAN is a leaf with no URI SAN at all.
func mtlstestLeafSpecNoSAN() mtlstest.LeafSpec {
	return mtlstest.LeafSpec{URIs: []string{}}
}

// testMetrics builds the package instruments on a provider the test reads
// directly, so metric assertions never touch the global provider.
func testMetrics(t *testing.T) (*Metrics, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.WithoutCancel(t.Context())) })

	metrics, err := NewMetrics(provider)
	require.NoError(t, err)

	return metrics, reader
}

// pointValue collects and returns the int64 data point of name whose
// attribute set is exactly attrs. Sums and gauges both qualify.
func pointValue(t *testing.T, reader *sdkmetric.ManualReader, name string, attrs ...attribute.KeyValue) (int64, bool) {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	want := attribute.NewSet(attrs...)

	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					if dp.Attributes.Equals(&want) {
						return dp.Value, true
					}
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					if dp.Attributes.Equals(&want) {
						return dp.Value, true
					}
				}
			}
		}
	}

	return 0, false
}

// mustPoint is pointValue that fails the test when the series is absent.
func mustPoint(t *testing.T, reader *sdkmetric.ManualReader, name string, attrs ...attribute.KeyValue) int64 {
	t.Helper()

	value, ok := pointValue(t, reader, name, attrs...)
	require.True(t, ok, "metric %s with %v is absent", name, attrs)

	return value
}

// awaitPoint waits for the series to reach want, for a count recorded by a
// goroutine the test cannot join, such as a listener's classification of a
// connection the client has already given up on.
func awaitPoint(t *testing.T, reader *sdkmetric.ManualReader, want int64, name string, attrs ...attribute.KeyValue) {
	t.Helper()

	require.Eventually(t, func() bool {
		value, ok := pointValue(t, reader, name, attrs...)

		return ok && value == want
	}, 5*time.Second, 10*time.Millisecond, "metric %s with %v did not reach %d", name, attrs, want)
}
