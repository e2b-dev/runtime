//go:build linux

package nfsproxy

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted/testutils"
	nfscfg "github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

const (
	benchPackages = 64
	benchFileSize = 2048
)

// benchProxy serves one sandbox's volume over NFS on the loopback interface
// and returns a dialer for client connections to it. Every connection issues
// its own MOUNT, so with n connections the handler holds n confined
// filesystems, as it does for n sandboxes.
type benchProxy struct {
	host       string
	port       int
	volumeName string
}

func startBenchProxy(b *testing.B) *benchProxy {
	b.Helper()

	teamID := uuid.New()
	volumeType := "volume-type-bench"
	volumeID := uuid.New()
	volumeName := "volume-bench"

	config := cfg.Config{
		PersistentVolumeMounts: map[string]string{
			volumeType: b.TempDir(),
		},
	}
	builder := chrooted.NewBuilder(config)
	fullVolumePath, err := builder.BuildVolumePath(volumeType, teamID, volumeID)
	require.NoError(b, err)
	require.NoError(b, os.MkdirAll(fullVolumePath, 0o755))

	sandboxes := sandbox.NewSandboxesMap()
	sandboxes.AssignNetwork(b.Context(), &sandbox.Sandbox{
		Metadata: &sandbox.Metadata{
			Config: sandbox.NewConfig(sandbox.Config{
				VolumeMounts: []sandbox.VolumeMountConfig{
					{ID: volumeID, Name: volumeName, Path: "/mnt/vol", Type: volumeType},
				},
			}),
			Runtime: sandboxtypes.RuntimeMetadata{
				SandboxID: uuid.NewString(),
				TeamID:    teamID.String(),
			},
		},
		Resources: &sandbox.Resources{
			Slot: &network.Slot{Key: "bench", HostIP: net.IPv4(127, 0, 0, 1)},
		},
	})

	listenConfig := net.ListenConfig{}
	listener, err := listenConfig.Listen(b.Context(), "tcp", "127.0.0.1:0")
	require.NoError(b, err)
	b.Cleanup(func() {
		_ = listener.Close()
	})

	proxy, err := NewProxy(b.Context(), builder, sandboxes, nfscfg.Config{})
	require.NoError(b, err)
	go func() {
		_ = proxy.Serve(listener)
	}()

	host, portText, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(b, err)
	port, err := strconv.Atoi(portText)
	require.NoError(b, err)

	return &benchProxy{host: host, port: port, volumeName: volumeName}
}

func (p *benchProxy) mount(b *testing.B) *nfs.Target {
	b.Helper()

	client, err := dialRetry(func() (*rpc.Client, error) {
		return nfs.DialServiceAtPort(p.host, p.port)
	})
	require.NoError(b, err)
	b.Cleanup(func() {
		client.Close()
	})

	mount := &nfs.Mount{Client: client}
	target, err := mount.Mount("/"+p.volumeName, rpc.NewAuthUnix("bench", 1000, 1000).Auth())
	require.NoError(b, err)

	return target
}

// nfsSmallFileWriter creates and writes small files over NFS the way a
// sandbox does: LOOKUP, CREATE, WRITE, COMMIT, then a GETATTR.
type nfsSmallFileWriter struct {
	target *nfs.Target
	root   string
	data   []byte

	knownDirs map[string]struct{}
}

func newNFSSmallFileWriter(target *nfs.Target, root string) *nfsSmallFileWriter {
	return &nfsSmallFileWriter{
		target:    target,
		root:      root,
		data:      make([]byte, benchFileSize),
		knownDirs: make(map[string]struct{}, benchPackages),
	}
}

func (w *nfsSmallFileWriter) writeFile(i int) error {
	dir := fmt.Sprintf("%s/node_modules/pkg-%d/lib", w.root, i%benchPackages)
	if _, ok := w.knownDirs[dir]; !ok {
		for _, d := range []string{w.root, w.root + "/node_modules", fmt.Sprintf("%s/node_modules/pkg-%d", w.root, i%benchPackages), dir} {
			if _, ok := w.knownDirs[d]; ok {
				continue
			}
			if _, err := w.target.Mkdir(d, 0o755); err != nil && !os.IsExist(err) {
				return fmt.Errorf("mkdir %q: %w", d, err)
			}
			w.knownDirs[d] = struct{}{}
		}
	}

	name := fmt.Sprintf("%s/index-%d.js", dir, i)

	f, err := w.target.OpenFile(name, 0o644)
	if err != nil {
		return fmt.Errorf("create %q: %w", name, err)
	}
	if _, err := f.Write(w.data); err != nil {
		return fmt.Errorf("write %q: %w", name, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("commit %q: %w", name, err)
	}
	if _, err := w.target.Getattr(name); err != nil {
		return fmt.Errorf("getattr %q: %w", name, err)
	}

	return nil
}

// BenchmarkNFSProxy_SmallFiles drives the full server (go-nfs, the handler
// middleware and the confined filesystem) from an in-process NFS client over
// loopback TCP. The connections sub-benchmarks run that many clients at once,
// each on its own connection and mount, as several sandboxes would.
func BenchmarkNFSProxy_SmallFiles(b *testing.B) {
	for _, connections := range []int{1, 4} {
		b.Run(fmt.Sprintf("connections=%d", connections), func(b *testing.B) {
			proxy := startBenchProxy(b)

			writers := make([]*nfsSmallFileWriter, connections)
			for c := range connections {
				writers[c] = newNFSSmallFileWriter(proxy.mount(b), fmt.Sprintf("/w%d", c))
			}

			before := testutils.SnapshotProcStats(b)

			b.ResetTimer()

			var wg sync.WaitGroup
			for c, w := range writers {
				wg.Go(func() {
					// Split b.N across the connections so ns/op stays per file.
					for i := c; i < b.N; i += connections {
						if err := w.writeFile(i); err != nil {
							b.Error(err)

							return
						}
					}
				})
			}
			wg.Wait()

			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "files/s")
			testutils.ReportProcStatsDelta(b, before, testutils.SnapshotProcStats(b), 1)
		})
	}
}

// BenchmarkNFSProxy_Getattr stats one file repeatedly. With attribute caching
// off in the sandbox (noac, lookupcache=none) this is the most frequent
// request the proxy serves.
func BenchmarkNFSProxy_Getattr(b *testing.B) {
	proxy := startBenchProxy(b)
	target := proxy.mount(b)

	w := newNFSSmallFileWriter(target, "/w0")
	require.NoError(b, w.writeFile(0))
	name := fmt.Sprintf("/w0/node_modules/pkg-0/lib/index-%d.js", 0)

	b.ResetTimer()
	for range b.N {
		_, err := target.Getattr(name)
		require.NoError(b, err)
	}
}
