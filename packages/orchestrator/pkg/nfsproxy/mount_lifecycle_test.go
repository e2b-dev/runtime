//go:build linux

package nfsproxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	clientnfs "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"

	nfscfg "github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
)

// acceptBoundaryListener delays either the raw Accept call (keeping an already
// established socket in the kernel backlog) or its return to go-nfs. A barrier
// only after application binding would miss both publication races.
type acceptBoundaryListener struct {
	net.Listener

	before    bool
	hold      bool
	once      sync.Once
	entered   chan struct{}
	proceed   chan struct{}
	delivered atomic.Int32
}

func (l *acceptBoundaryListener) Accept() (net.Conn, error) {
	held := false
	l.once.Do(func() { held = l.hold })
	if held && l.before {
		close(l.entered)
		<-l.proceed
	}
	conn, err := l.Listener.Accept()
	if held && !l.before && err == nil {
		close(l.entered)
		<-l.proceed
	}
	if err == nil {
		l.delivered.Add(1)
	}

	return conn, err
}

// Send the real MOUNT RPC synchronously before publishing the replacement. This
// makes the old request buffered on the old socket, not just a scheduled client
// goroutine. Positive data checks still use the actual retained NFS client.
func writeLifecycleMount(t *testing.T, conn net.Conn, path string) {
	t.Helper()
	// XDR preserves declaration order: the RPC header follows XID and MsgType.
	request := struct {
		XID     uint32
		MsgType uint32
		Header  rpc.Header
		Dirpath string
	}{
		XID: 1, MsgType: 0,
		Header: rpc.Header{
			Rpcvers: 2, Prog: clientnfs.MountProg, Vers: clientnfs.MountVers,
			Proc: clientnfs.MountProc3MNT, Cred: rpc.NewAuthUnix("", 100, 101).Auth(), Verf: rpc.AuthNull,
		},
		Dirpath: path,
	}
	body := new(bytes.Buffer)
	require.NoError(t, xdr.Write(body, request))
	record := new(bytes.Buffer)
	require.NoError(t, binary.Write(record, binary.BigEndian, uint32(body.Len())|0x80000000))
	_, err := record.Write(body.Bytes())
	require.NoError(t, err)
	n, err := io.Copy(conn, record)
	require.NoError(t, err)
	require.Equal(t, int64(body.Len()+4), n)
}

func readLifecycleMountStatus(t *testing.T, conn net.Conn) uint32 {
	t.Helper()
	var record uint32
	require.NoError(t, binary.Read(conn, binary.BigEndian, &record))
	require.NotZero(t, record&0x80000000, "the small MOUNT reply must be one final RPC record")
	require.Less(t, record&0x7fffffff, uint32(4096))
	body := make([]byte, record&0x7fffffff)
	_, err := io.ReadFull(conn, body)
	require.NoError(t, err)
	var reply struct {
		XID         uint32
		MsgType     uint32
		ReplyStatus uint32
		Verifier    rpc.Auth
		Accept      uint32
		Mount       uint32
	}
	require.NoError(t, xdr.Read(bytes.NewReader(body), &reply))
	require.Equal(t, uint32(1), reply.XID)
	require.Equal(t, uint32(1), reply.MsgType)
	require.Zero(t, reply.ReplyStatus)
	require.Zero(t, reply.Accept)

	return reply.Mount
}

func TestMountRequestLifecycleAcrossConnectionBoundaries(t *testing.T) { //nolint:paralleltest // real proxy and process-wide telemetry
	for _, boundary := range []string{"accepted-before-publication", "backlog-before-publication", "mounted-before-publication"} { //nolint:paralleltest // process-wide telemetry
		t.Run(boundary, func(t *testing.T) {
			f := buildConcernFixture(t, nfscfg.Config{}, nil)
			oldPath := f.mountPath("first")
			oldSandbox, err := f.sandboxes.GetByHostPort("127.0.0.1:1")
			require.NoError(t, err)
			listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			barrier := &acceptBoundaryListener{
				Listener: listener, before: boundary == "backlog-before-publication",
				hold: boundary != "mounted-before-publication", entered: make(chan struct{}), proceed: make(chan struct{}),
			}
			var unblock sync.Once
			releaseAccept := func() { unblock.Do(func() { close(barrier.proceed) }) }
			var accepted, disconnected atomic.Int32
			addresses := make(chan string, 2)
			f.proxy.server.OnConnect = func(ctx context.Context, conn net.Conn) (context.Context, net.Conn) {
				accepted.Add(1)
				addresses <- conn.RemoteAddr().String()

				return onConnect(ctx, conn)
			}
			f.proxy.server.OnDisconnect = func(ctx context.Context, conn net.Conn) {
				onDisconnect(ctx, conn)
				disconnected.Add(1)
			}
			served := make(chan error, 1)
			var oldConn net.Conn
			var newClient *rpc.Client
			t.Cleanup(func() {
				releaseAccept()
				if newClient != nil {
					newClient.Close()
				}
				if oldConn != nil {
					require.NoError(t, oldConn.Close())
				}
				require.NoError(t, listener.Close())
				select {
				case err := <-served:
					require.NoError(t, err)
				case <-time.After(time.Second):
					t.Fatal("proxy listener did not join")
				}
				require.Eventually(t, func() bool {
					return accepted.Load() == barrier.delivered.Load() && accepted.Load() == disconnected.Load()
				}, time.Second, time.Millisecond,
					"observe disconnect for every accepted socket, not only listener close")
			})
			go func() { served <- f.proxy.Serve(barrier) }()
			oldConn, err = (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", listener.Addr().String())
			require.NoError(t, err, "the old TCP socket must be established before publication")
			require.NoError(t, oldConn.SetDeadline(time.Now().Add(5*time.Second)))
			if barrier.hold {
				select {
				case <-barrier.entered:
				case <-time.After(time.Second):
					t.Fatal("the real Accept boundary did not reach its barrier")
				}
			}
			writeLifecycleMount(t, oldConn, oldPath)
			if boundary == "mounted-before-publication" {
				require.Equal(t, uint32(clientnfs.MNT3Ok), readLifecycleMountStatus(t, oldConn))
			}
			require.NoError(t, f.sandboxes.NetworkReleased(t.Context(), "127.0.0.1"))
			replacement := &sandbox.Sandbox{
				Metadata: oldSandbox.Metadata, Resources: oldSandbox.Resources, LifecycleID: uuid.NewString(),
			}
			f.sandboxes.AssignNetwork(t.Context(), replacement)
			require.NoError(t, os.WriteFile(filepath.Join(f.paths[0], "replacement-data"), []byte("replacement lifecycle"), 0o600))
			if boundary == "mounted-before-publication" {
				writeLifecycleMount(t, oldConn, oldPath)
			}
			releaseAccept()
			require.Equal(t, uint32(clientnfs.MNT3ErrAcces), readLifecycleMountStatus(t, oldConn),
				"old buffered, queued-first, or repeated MOUNT cannot inherit the new route")
			writeLifecycleMount(t, oldConn, "/first")
			require.Equal(t, uint32(clientnfs.MNT3ErrAcces), readLifecycleMountStatus(t, oldConn), "no unqualified export fallback")
			require.Eventually(t, func() bool { return f.proxy.retirement.count() == 0 }, time.Second, time.Millisecond)
			require.Zero(t, f.proxy.chroots.Count(), "denied old requests must not allocate replacement roots")
			_, portText, err := net.SplitHostPort(listener.Addr().String())
			require.NoError(t, err)
			port, err := strconv.Atoi(portText)
			require.NoError(t, err)
			newClient, err = dialRetry(func() (*rpc.Client, error) { return clientnfs.DialServiceAtPort("127.0.0.1", port) })
			require.NoError(t, err)
			newClient.SetTimeout(time.Second)
			target, err := (&clientnfs.Mount{Client: newClient}).Mount(replacement.NFSMountPath("first"), rpc.NewAuthUnix("", 100, 101).Auth())
			require.NoError(t, err)
			file, err := target.Open("replacement-data")
			require.NoError(t, err)
			data, readErr := io.ReadAll(file)
			closeErr := file.Close()
			require.NoError(t, readErr)
			require.NoError(t, closeErr)
			require.Equal(t, "replacement lifecycle", string(data))
			oldAddress, newAddress := <-addresses, <-addresses
			require.NotEqual(t, oldAddress, newAddress, "the replacement oracle must use a distinct TCP connection")
			require.Equal(t, oldConn.LocalAddr().String(), oldAddress)
		})
	}
}

var _ net.Listener = (*acceptBoundaryListener)(nil)
