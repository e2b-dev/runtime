package mtls

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// watchedProcess is a configured process over an empty certificate
// directory whose flag-backed modes are re-read every few milliseconds.
func watchedProcess(t *testing.T, flags FlagReader) (*Process, *sdkmetric.ManualReader, *observer.ObservedLogs) {
	t.Helper()

	provider, reader := testProvider(t)
	log, logs := testLogger(t)
	proc, err := NewProcess(t.Context(), provider, flags, log, WithFileConfig(emptyFileConfig(t)), WithWatchInterval(10*time.Millisecond))
	require.NoError(t, err)

	return proc, reader, logs
}

func TestWatchedListenerGaugeFollowsItsFlagWithoutTraffic(t *testing.T) {
	t.Parallel()

	flags := newFakeFlags()
	proc, reader, _ := watchedProcess(t, flags)
	cfg, err := proc.Listener("public_grpc", "listener-flag", ListenerSettings{Allow: []string{clientID}})
	require.NoError(t, err)
	_ = NewServerCredentials(cfg)

	flags.set("listener-flag", "permissive")

	awaitPoint(t, reader, 1, MetricMode, attribute.String(AttrListener, "public_grpc"), attribute.String(AttrMode, "permissive"), attribute.String(AttrSource, SourceFlag))
	awaitPoint(t, reader, 0, MetricMode, attribute.String(AttrListener, "public_grpc"), attribute.String(AttrMode, "off"), attribute.String(AttrSource, SourceFallback))
}

func TestWatchedHopGaugeFollowsItsFlagWithoutADial(t *testing.T) {
	t.Parallel()

	flags := newFakeFlags()
	proc, reader, _ := watchedProcess(t, flags)
	hop, err := proc.Hop("peers", "hop-flag", HopSettings{Expect: []string{serverID}})
	require.NoError(t, err)
	_ = NewClientCredentials(hop)

	flags.set("hop-flag", "on")

	awaitPoint(t, reader, 1, MetricClientMode, attribute.String(AttrClientHop, "peers"), attribute.String(AttrMode, "on"), attribute.String(AttrSource, SourceFlag))
}

// A flag-backed hop builds with no expected server names, as before its
// names are set, and a flip to on is refused, as a listener refuses
// required with an empty allow-list: it stays off rather than refusing
// every server it dials.
func TestWatchedHopRefusesAnOnFlipWithoutExpectedNames(t *testing.T) {
	t.Parallel()

	flags := newFakeFlags()
	flags.set("hop-flag", "off")
	proc, reader, logs := watchedProcess(t, flags)
	hop, err := proc.Hop("peers", "hop-flag", HopSettings{Expect: []string{" "}})
	require.NoError(t, err, "a flag-backed hop builds with no expected server names")
	_ = NewClientCredentials(hop)

	flags.set("hop-flag", "on")

	assert.Eventually(t, func() bool {
		return slices.Contains(logMessages(logs), "mtls: hop mode flag asks for on with no expected server names; staying")
	}, 5*time.Second, 10*time.Millisecond)
	awaitPoint(t, reader, 1, MetricClientMode, attribute.String(AttrClientHop, "peers"), attribute.String(AttrMode, "off"), attribute.String(AttrSource, SourceRefused))
	_, on := pointValue(t, reader, MetricClientMode, attribute.String(AttrClientHop, "peers"), attribute.String(AttrMode, "on"), attribute.String(AttrSource, SourceFlag))
	assert.False(t, on, "on was never in force")
}

func TestNewClientCredentialsReportsTheHopModeBeforeAnyDial(t *testing.T) {
	t.Parallel()

	metrics, reader := testMetrics(t)
	_ = NewClientCredentials(ClientConfig{Name: "idle-hop", Mode: StaticClientMode(ClientOff), Metrics: metrics})

	assert.Equal(t, int64(1), mustPoint(t, reader, MetricClientMode, attribute.String(AttrClientHop, "idle-hop"), attribute.String(AttrMode, "off"), attribute.String(AttrSource, SourceFallback)))
}

// A keep-alive plaintext connection admitted while the listener was off
// is closed by the server once the listener's flag enters required.
func TestWatchedListenerClosesWhatRequiredWouldRefuseOverHTTP(t *testing.T) {
	t.Parallel()

	flags := newFakeFlags()
	proc, _, logs := watchedProcess(t, flags)
	cfg, err := proc.Listener("http", "listener-flag", ListenerSettings{Allow: []string{clientID}})
	require.NoError(t, err)
	addr := serveWrapped(t, cfg, teapot())

	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = conn.Write([]byte("GET /health HTTP/1.1\r\nHost: test\r\n\r\n"))
	require.NoError(t, err)
	buffered := bufio.NewReader(conn)
	resp, err := http.ReadResponse(buffered, nil)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusTeapot, resp.StatusCode)

	flags.set("listener-flag", "required")

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = buffered.ReadByte()
	require.Error(t, err)
	require.NotErrorIs(t, err, os.ErrDeadlineExceeded, "the server closed the connection rather than leaving it open")
	// The watcher logs after it has closed the connections, so the client
	// can see the close before the line is written.
	assert.Eventually(t, func() bool {
		return slices.Contains(logMessages(logs), "mtls: required listener closed the connections it refuses")
	}, 5*time.Second, 10*time.Millisecond)
}

func TestWatchedListenerClosesWhatRequiredWouldRefuseOverGRPC(t *testing.T) {
	t.Parallel()

	flags := newFakeFlags()
	proc, _, _ := watchedProcess(t, flags)
	cfg, err := proc.Listener("internal_grpc", "listener-flag", ListenerSettings{Allow: []string{clientID}})
	require.NoError(t, err)
	server := grpc.NewServer(grpc.Creds(NewServerCredentials(cfg)))
	healthpb.RegisterHealthServer(server, health.NewServer())
	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, connectivity.Ready, conn.GetState())

	flags.set("listener-flag", "required")

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	assert.True(t, conn.WaitForStateChange(ctx, connectivity.Ready), "the server closed the plaintext connection")
}

// admitPlaintext registers one end of a pipe as plaintext, as a handshake
// that read mode does, and returns the other end with a read deadline. The
// deadline is set first: a tick may close the pipe as soon as it is
// registered, and a closed pipe refuses a new deadline but reads io.EOF.
func admitPlaintext(t *testing.T, plaintext func(context.Context, string, net.Conn, Mode) *PlaintextConn, key string, mode Mode) net.Conn {
	t.Helper()

	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	require.NoError(t, client.SetReadDeadline(time.Now().Add(5*time.Second)))
	plaintext(t.Context(), key, server, mode)

	return client
}

// awaitClosed waits for the server to close conn's other end.
func awaitClosed(t *testing.T, conn net.Conn, msg string) {
	t.Helper()

	_, err := conn.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, msg)
}

// A handshake that read permissive before the flip can register its
// connection after the watcher closed the others; a later tick closes it.
func TestWatchedListenerClosesWhatRegistersAfterTheFlip(t *testing.T) {
	t.Parallel()

	flags := newFakeFlags()
	proc, _, _ := watchedProcess(t, flags)
	cfg, err := proc.Listener("internal_grpc", "listener-flag", ListenerSettings{Allow: []string{clientID}})
	require.NoError(t, err)
	creds := NewServerCredentials(cfg)

	early := admitPlaintext(t, creds.plaintext, "early", ModeOff)
	flags.set("listener-flag", "required")
	awaitClosed(t, early, "the flip closed the connection admitted before it")

	late := admitPlaintext(t, creds.plaintext, "late", ModePermissive)
	awaitClosed(t, late, "a later tick closed the connection permissive admitted")
}

// Required admits a plaintext connection for its health path, and the ticks
// that close what a weaker mode admitted leave it open.
func TestWatchedListenerSparesAPlaintextProbeRequiredAdmitted(t *testing.T) {
	t.Parallel()

	flags := newFakeFlags()
	flags.set("listener-flag", "required")
	proc, _, _ := watchedProcess(t, flags)
	cfg, err := proc.Listener("http", "listener-flag", ListenerSettings{Allow: []string{clientID}})
	require.NoError(t, err)
	var lc net.ListenConfig
	inner, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &http.Server{Handler: teapot(), ReadHeaderTimeout: time.Second}
	l := WrapHTTPServer(server, inner, cfg, []string{"/health"})
	go func() { _ = server.Serve(l) }()
	t.Cleanup(func() { _ = server.Close() })

	var dialer net.Dialer
	probe, err := dialer.DialContext(t.Context(), "tcp", inner.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = probe.Close() })
	require.NoError(t, probe.SetReadDeadline(time.Now().Add(5*time.Second)))
	buffered := bufio.NewReader(probe)
	health := func() error {
		if _, err := probe.Write([]byte("GET /health HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
			return err
		}
		resp, err := http.ReadResponse(buffered, nil)
		if err != nil {
			return err
		}

		return resp.Body.Close()
	}
	require.NoError(t, health())

	// Each connection permissive admitted after the probe is closed by a
	// tick that also judged the probe; the second proves the first tick
	// finished.
	awaitClosed(t, admitPlaintext(t, l.plaintext, "tick-1", ModePermissive), "a tick ran")
	awaitClosed(t, admitPlaintext(t, l.plaintext, "tick-2", ModePermissive), "a second tick ran")

	assert.NoError(t, health(), "the probe's keep-alive connection is still open")
}

func TestStaticModesAreNotWatched(t *testing.T) {
	t.Parallel()

	proc, _, _ := watchedProcess(t, newFakeFlags())
	static, err := proc.Listener("http", "", ListenerSettings{})
	require.NoError(t, err)
	assert.Nil(t, static.watched, "no flag key: the mode changes by restart")

	envOnly, _, _ := watchedProcess(t, nil)
	cfg, err := envOnly.Listener("http", "listener-flag", ListenerSettings{})
	require.NoError(t, err)
	assert.Nil(t, cfg.watched, "no flag reader: the mode changes by restart")
}
