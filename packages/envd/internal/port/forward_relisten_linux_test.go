//go:build linux

package port

import (
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
)

// This test scans real sockets, so it must not run alongside tests that replace HOST_PROC.
func TestStartForwarding_SameProcessRelistens(t *testing.T) { //nolint:paralleltest
	if _, err := exec.LookPath("socat"); err != nil {
		t.Skip("socat is required for the forwarding integration test")
	}

	listen := func(address string) net.Listener {
		t.Helper()
		var lc net.ListenConfig
		listener, err := lc.Listen(t.Context(), "tcp4", address)
		require.NoError(t, err)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
				_, _ = io.WriteString(conn, "ready\n")
				_ = conn.Close()
			}
		}()
		t.Cleanup(func() {
			_ = listener.Close()
			<-done
		})

		return listener
	}
	listener := listen("127.0.0.1:0")
	address := listener.Addr().String()
	_, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	forwardAddress := net.JoinHostPort("127.0.0.2", port)

	l := zerolog.Nop()
	scanner := NewScanner(&l, 20*time.Millisecond)
	mgr := &socatCgroupManager{root: t.TempDir()}
	f := NewForwarder(&l, scanner, cgroups.NewWorkloadFreezer(mgr))
	// A second loopback address needs no interface changes or root privileges.
	f.sourceIP = net.IPv4(127, 0, 0, 2)
	ctx, cancel := context.WithCancel(t.Context())
	forwardDone := make(chan struct{})
	go func() {
		defer close(forwardDone)
		f.StartForwarding(ctx)
	}()
	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		scanner.ScanAndBroadcast()
	}()
	t.Cleanup(func() {
		scanner.Destroy()
		cancel()
		<-scanDone
		<-forwardDone
		for _, p := range f.ports {
			f.stopPortForwarding(p)
		}
	})

	reachesListener := func() bool {
		dialer := net.Dialer{Timeout: 100 * time.Millisecond}
		conn, err := dialer.DialContext(t.Context(), "tcp4", forwardAddress)
		if err != nil {
			return false
		}
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		payload := make([]byte, len("ready\n"))
		_, err = io.ReadFull(conn, payload)

		return err == nil && string(payload) == "ready\n"
	}
	require.Eventually(t, reachesListener, 5*time.Second, 20*time.Millisecond, "initial listener must be forwarded")
	var listenerPID int32
	for _, forward := range f.ExportForwards() {
		if strconv.FormatUint(uint64(forward.GetPort()), 10) == port {
			listenerPID = forward.GetListenerPid()
		}
	}
	require.EqualValues(t, os.Getpid(), listenerPID, "the scanner must identify the real listener process")

	require.NoError(t, listener.Close())
	require.Eventually(t, func() bool {
		dialer := net.Dialer{Timeout: 100 * time.Millisecond}
		conn, err := dialer.DialContext(t.Context(), "tcp4", forwardAddress)
		if err == nil {
			_ = conn.Close()
		}

		return err != nil
	}, 5*time.Second, 20*time.Millisecond, "the closed listener's forwarder must stop")

	listener = listen(address)
	require.Equal(t, address, listener.Addr().String())
	require.Eventually(t, reachesListener, 5*time.Second, 20*time.Millisecond, "forwarding must resume when the same process listens again")
}
