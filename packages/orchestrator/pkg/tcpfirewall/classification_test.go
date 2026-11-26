//go:build linux

package tcpfirewall

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/inetaf/tcpproxy"
	"github.com/stretchr/testify/require"
)

type classificationTarget func(net.Conn)

func (f classificationTarget) HandleConn(conn net.Conn) { f(conn) }

func TestClassificationAdmissionBoundsAndReleasesPendingConnections(t *testing.T) {
	a := newClassificationAdmission()
	var clients []net.Conn
	var servers []net.Conn
	defer func() {
		for _, conn := range clients {
			_ = conn.Close()
		}
		for _, conn := range servers {
			_ = conn.Close()
		}
	}()

	for range maxPendingClassificationsPerSandbox {
		client, server := net.Pipe()
		clients = append(clients, client)
		servers = append(servers, server)
		require.True(t, a.admit(server, "sandbox-a"))
	}
	require.EqualValues(t, maxPendingClassificationsPerSandbox, a.limiter.Count("sandbox-a"))

	extraClient, extraServer := net.Pipe()
	clients = append(clients, extraClient)
	servers = append(servers, extraServer)
	require.False(t, a.admit(extraServer, "sandbox-a"))
	require.True(t, a.admit(extraServer, "sandbox-b"), "another sandbox has its own budget")
	require.True(t, a.complete(servers[0]))
	require.False(t, a.complete(servers[0]), "a slot must only be released once")
	require.True(t, a.admit(servers[0], "sandbox-a"))
	require.EqualValues(t, maxPendingClassificationsPerSandbox, a.limiter.Count("sandbox-a"))
	require.EqualValues(t, 1, a.limiter.Count("sandbox-b"))
}

func TestClassificationAdmissionRejectsExpiredConnection(t *testing.T) {
	a := newClassificationAdmission()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	require.True(t, a.admit(server, "sandbox-a"))
	a.pending.Store(server, classificationSlot{key: "sandbox-a", deadline: time.Now().Add(-time.Second)})
	require.False(t, a.complete(server))
	require.Zero(t, a.limiter.Count("sandbox-a"))
}

func TestSlowHTTPClassificationIsRejectedBeforeForwarding(t *testing.T) {
	a := newClassificationAdmission()
	a.timeout = 500 * time.Millisecond
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	proxy := &tcpproxy.Proxy{}
	proxy.ListenFunc = func(_, _ string) (net.Listener, error) {
		return &resilientListener{
			Listener: ln,
			ctx:      context.Background(),
			onAccept: func(conn net.Conn) bool { return a.admit(conn, "sandbox-a") },
		}, nil
	}
	result := make(chan bool, 2)
	target := classificationTarget(func(conn net.Conn) {
		admitted := a.complete(tcpproxy.UnderlyingConn(conn))
		result <- admitted
		_ = conn.Close()
	})
	addr := ln.Addr().String()
	proxy.AddHTTPHostMatchRoute(addr, func(context.Context, string) bool { return true }, target)
	proxy.AddRoute(addr, target)
	require.NoError(t, proxy.Start())
	defer proxy.Close()

	slow, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer slow.Close()
	_, err = slow.Write([]byte("GET / HTTP/1.1\r\nHost: example"))
	require.NoError(t, err)
	require.False(t, receiveClassificationResult(t, result))
	require.Zero(t, a.limiter.Count("sandbox-a"))

	complete, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer complete.Close()
	_, err = complete.Write([]byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"))
	require.NoError(t, err)
	require.True(t, receiveClassificationResult(t, result))
	require.Zero(t, a.limiter.Count("sandbox-a"))
}

func receiveClassificationResult(t *testing.T, result <-chan bool) bool {
	t.Helper()
	select {
	case admitted := <-result:
		return admitted
	case <-time.After(2 * time.Second):
		t.Fatal("classification did not finish")
		return false
	}
}
