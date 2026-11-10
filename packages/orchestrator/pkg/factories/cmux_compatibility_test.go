//go:build linux

package factories

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/soheilhy/cmux"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestCMUXMatchedListenerClosePreservesAcceptedConnection(t *testing.T) {
	t.Parallel()
	mux, err := NewCMUXServer(t.Context(), 0, noop.NewMeterProvider(), noWrap)
	require.NoError(t, err)
	listener := mux.Match(cmux.Any())
	done := make(chan struct{})
	go func() { _ = mux.Serve(); close(done) }()
	t.Cleanup(func() {
		_ = listener.Close()
		mux.Close()
		waitCMUXShutdown(t, done, "fixture did not join cmux Serve")
	})
	_, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	client, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", port))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	unblockAccept := context.AfterFunc(ctx, func() { _ = listener.Close() })
	accepted, err := listener.Accept()
	unblockAccept()
	require.NoError(t, err)
	t.Cleanup(func() { _ = accepted.Close() })
	require.NoError(t, listener.Close())
	waitCMUXShutdown(t, done, "closing admission must let Serve exit")
	require.NoError(t, accepted.SetReadDeadline(time.Now().Add(time.Second)))
	_, err = io.WriteString(client, "owned")
	require.NoError(t, err)
	buf := make([]byte, 5)
	_, err = io.ReadFull(accepted, buf)
	require.NoError(t, err, "matched listener Close must not preempt an admitted HTTP/gRPC user")
	require.Equal(t, "owned", string(buf))
	var closes sync.WaitGroup
	for range 4 {
		closes.Go(mux.Close)
	}
	closes.Wait()
	require.NoError(t, client.SetReadDeadline(time.Now().Add(time.Second)))
	_, err = client.Read(make([]byte, 1))
	require.Error(t, err)
	if netErr, ok := errors.AsType[net.Error](err); ok {
		require.False(t, netErr.Timeout(), "final CMUX Close must close the retained connection")
	}
}

func TestCMUXShutdownPreservesHTTPAndGRPC(t *testing.T) {
	t.Parallel()
	mux, err := NewCMUXServer(t.Context(), 0, noop.NewMeterProvider(), noWrap)
	require.NoError(t, err)
	httpListener := mux.Match(cmux.HTTP1Fast())
	grpcListener := mux.Match(cmux.Any())
	httpServer := NewHTTPServer()
	httpServer.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })
	grpcServer := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, health.NewServer())
	muxDone, httpDone, grpcDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() { _ = mux.Serve(); close(muxDone) }()
	go func() { _ = httpServer.Serve(httpListener); close(httpDone) }()
	go func() { _ = grpcServer.Serve(grpcListener); close(grpcDone) }()
	t.Cleanup(func() {
		_ = httpServer.Close()
		grpcServer.Stop()
		mux.Close()
		waitCMUXShutdown(t, httpDone, "fixture did not join HTTP Serve")
		waitCMUXShutdown(t, grpcDone, "fixture did not join gRPC Serve")
		waitCMUXShutdown(t, muxDone, "fixture did not join cmux Serve")
	})
	_, port, err := net.SplitHostPort(httpListener.Addr().String())
	require.NoError(t, err)
	addr := net.JoinHostPort("127.0.0.1", port)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	client := &http.Client{Timeout: time.Second}
	t.Cleanup(client.CloseIdleConnections)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/health", nil)
	require.NoError(t, err)
	response, err := client.Do(request)
	require.NoError(t, err)
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, "ok", string(body))
	connection, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	_, err = grpc_health_v1.NewHealthClient(connection).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err, "the connection wrapper must preserve actual gRPC transport")
	require.NoError(t, httpServer.Shutdown(ctx))
	grpcServer.GracefulStop()
	mux.Close()
	waitCMUXShutdown(t, httpDone, "HTTP Serve did not finish")
	waitCMUXShutdown(t, grpcDone, "gRPC Serve did not finish")
	waitCMUXShutdown(t, muxDone, "cmux Serve did not finish")
}
