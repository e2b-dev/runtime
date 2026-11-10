//go:build linux

package nfsproxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	nfscfg "github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// A READ can return data and EOF together. Through the real proxy and client,
// the next READ must start after those bytes, including after the file grows.
func TestProxyReadAdvancesTerminalCursor(t *testing.T) {
	t.Parallel()

	for _, prefix := range []int{0, 3} {
		t.Run(fmt.Sprintf("prefix_%d", prefix), func(t *testing.T) {
			t.Parallel()

			builder := chrooted.NewBuilder(cfg.Config{PersistentVolumeMounts: map[string]string{"test": t.TempDir()}})
			teamID, volumeID := uuid.New(), uuid.New()
			root, err := builder.BuildVolumePath("test", teamID, volumeID)
			require.NoError(t, err)
			require.NoError(t, os.MkdirAll(root, 0o700))
			path := filepath.Join(root, "payload")
			payload := []byte("payload")
			require.NoError(t, os.WriteFile(path, payload, 0o600))

			lifecycleID := uuid.NewString()
			sandboxes := sandbox.NewSandboxesMap()
			sandboxes.AssignNetwork(t.Context(), &sandbox.Sandbox{
				LifecycleID: lifecycleID,
				Metadata: &sandbox.Metadata{
					Config: sandbox.NewConfig(sandbox.Config{
						VolumeMounts: []sandbox.VolumeMountConfig{{ID: volumeID, Name: "volume", Path: "/volume", Type: "test"}},
					}),
					Runtime: sandboxtypes.RuntimeMetadata{SandboxID: uuid.NewString(), TeamID: teamID.String()},
				},
				Resources: &sandbox.Resources{Slot: &network.Slot{Key: "test", HostIP: net.IPv4(127, 0, 0, 1)}},
			})
			proxy, err := NewProxy(t.Context(), builder, sandboxes, nfscfg.Config{})
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, sandboxes.NetworkReleased(context.WithoutCancel(t.Context()), "127.0.0.1"))
				require.Eventually(t, func() bool { return proxy.retirement.count() == 0 }, time.Second, time.Millisecond)
			})

			var lc net.ListenConfig
			listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			served := make(chan error, 1)
			go func() { served <- proxy.Serve(listener) }()
			t.Cleanup(func() { require.NoError(t, listener.Close()); require.NoError(t, <-served) })
			// The real proxy admits this loopback connection by sandbox IP;
			// use an unprivileged source port to avoid the privileged-port pool.
			client, err := dialRetry(func() (*rpc.Client, error) {
				return rpc.DialTCP("tcp", listener.Addr().String(), false)
			})
			require.NoError(t, err)
			t.Cleanup(client.Close)
			target, err := (&nfs.Mount{Client: client}).Mount("/"+lifecycleID+"/volume", rpc.NewAuthUnix("test", 1000, 1000).Auth())
			require.NoError(t, err)
			file, err := target.Open("/payload")
			require.NoError(t, err)
			// File.Close sends COMMIT, not a descriptor close. This read-only
			// test closes its transport instead of issuing an unrelated RPC.

			// Request more than the server's 32 KiB small-read threshold.
			buf := make([]byte, 32*1024+1)
			if prefix > 0 {
				n, err := file.Read(buf[:prefix])
				require.NoError(t, err)
				require.Equal(t, prefix, n)
				require.Equal(t, payload[:prefix], buf[:n])
			}

			n, err := file.Read(buf)
			require.Equal(t, len(payload)-prefix, n)
			require.ErrorIs(t, err, io.EOF)
			require.Equal(t, payload[prefix:], buf[:n])

			position, err := file.Seek(0, io.SeekCurrent)
			require.NoError(t, err)
			assert.Equal(t, int64(len(payload)), position, "terminal bytes must advance cursor")
			n, err = file.Read(buf)
			assert.Zero(t, n, "next READ must not replay terminal bytes")
			require.ErrorIs(t, err, io.EOF)

			appended := []byte("appended")
			writer, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
			require.NoError(t, err)
			written, writeErr := writer.Write(appended)
			closeErr := writer.Close()
			require.NoError(t, writeErr)
			require.NoError(t, closeErr)
			require.Equal(t, len(appended), written)

			n, err = file.Read(buf)
			assert.Equal(t, len(appended), n, "READ after growth starts at previous end")
			require.ErrorIs(t, err, io.EOF)
			assert.Equal(t, appended, buf[:n], "READ after growth returns only appended bytes")

			beforeReadAt, err := file.Seek(0, io.SeekCurrent)
			require.NoError(t, err)
			whole := slices.Concat(payload, appended)
			n, err = file.ReadAt(buf, 0)
			require.Equal(t, len(whole), n)
			require.ErrorIs(t, err, io.EOF)
			require.Equal(t, whole, buf[:n])
			afterReadAt, err := file.Seek(0, io.SeekCurrent)
			require.NoError(t, err)
			assert.Equal(t, beforeReadAt, afterReadAt, "ReadAt must preserve the sequential cursor")
		})
	}
}
