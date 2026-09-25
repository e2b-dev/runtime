//go:build unix

package proxy

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
)

// reservedPort holds a TCP socket that is bound but not yet listening:
// connections are refused while the port stays reserved for this test, and
// listen turns the same socket into a live listener. This avoids the
// close-and-rebind race where another process grabs the port in between.
type reservedPort struct {
	fd     int
	addr   string
	closed bool
}

func reserveTCPPort(t *testing.T) *reservedPort {
	t.Helper()

	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	require.NoError(t, err)
	r := &reservedPort{fd: fd}
	t.Cleanup(r.close)

	require.NoError(t, syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}))
	sa, err := syscall.Getsockname(fd)
	require.NoError(t, err)
	r.addr = fmt.Sprintf("127.0.0.1:%d", sa.(*syscall.SockaddrInet4).Port)

	return r
}

// close releases the raw fd unless listen already handed it off.
func (r *reservedPort) close() {
	if r.closed {
		return
	}
	r.closed = true
	_ = syscall.Close(r.fd)
}

func (r *reservedPort) listen() (net.Listener, error) {
	if err := syscall.Listen(r.fd, 128); err != nil {
		return nil, fmt.Errorf("listen on reserved port: %w", err)
	}
	if err := syscall.SetNonblock(r.fd, true); err != nil {
		return nil, fmt.Errorf("set nonblock: %w", err)
	}
	r.closed = true // fd ownership moves to the os.File below
	f := os.NewFile(uintptr(r.fd), "reserved-port")
	defer f.Close() // net.FileListener dups the fd

	return net.FileListener(f)
}

// TestProxyRetriesOnDelayedBackendStartup simulates the scenario where a backend
// server starts up after the initial connection attempt (like envd port forwarding delay).
func TestProxyRetriesOnDelayedBackendStartup(t *testing.T) {
	t.Parallel()
	reserved := reserveTCPPort(t)
	backendAddr := reserved.addr

	backendURL, err := url.Parse(fmt.Sprintf("http://%s", backendAddr))
	require.NoError(t, err)

	getDestination := func(_ *http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:           backendURL,
			SandboxId:     "test-sandbox",
			RequestLogger: logger.NewNopLogger(),
			ConnectionKey: "delayed-backend",
		}, nil
	}

	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	defer proxy.Close()

	type backendResult struct {
		backend *testBackend
		err     error
	}
	backendReady := make(chan backendResult, 1)

	// Start backend after a delay (simulating envd port forwarding)
	go func() {
		// Wait 300ms before starting the backend (should succeed on retry 2 or 3)
		time.Sleep(300 * time.Millisecond)

		listener, err := reserved.listen()
		if err != nil {
			backendReady <- backendResult{nil, fmt.Errorf("failed to create delayed backend listener: %w", err)}

			return
		}

		backend, err := newTestBackend(listener, "delayed-backend")
		if err != nil {
			listener.Close()
			backendReady <- backendResult{nil, fmt.Errorf("failed to create delayed backend: %w", err)}

			return
		}

		backendReady <- backendResult{backend, nil}
	}()

	// Make request - this should retry and eventually succeed
	proxyURL := fmt.Sprintf("http://127.0.0.1:%d/hello", port)
	start := time.Now()

	resp, err := httpGet(t, proxyURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	elapsed := time.Since(start)

	// Wait for backend to be ready before checking
	result := <-backendReady
	require.NoError(t, result.err)
	backend := result.backend
	defer backend.Close()

	assertBackendOutput(t, backend, resp)

	// Verify that it took at least the delay time (proving retries happened)
	assert.GreaterOrEqual(t, elapsed, 300*time.Millisecond, "request should have waited for backend to start")
	assert.Less(t, elapsed, 2*time.Second, "request should have succeeded before all retries exhausted")

	// Verify the connection was established
	assert.Equal(t, uint64(1), backend.RequestCount(), "backend should have been called once")
	assert.Equal(t, uint64(1), proxy.TotalPoolConnections(), "proxy should have established one connection")
}
