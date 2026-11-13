//go:build linux

package fc

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

func TestStartScriptBuilder_Build(t *testing.T) {
	t.Parallel()
	config, err := cfg.ParseBuilder()
	require.NoError(t, err)

	tests := []struct {
		name                  string
		versions              Config
		files                 *storage.SandboxFiles
		rootfsPaths           RootfsPaths
		namespaceID           string
		expectedRootfsPath    string
		expectedKernelPath    string
		expectedScriptContent []string // parts that should be in the generated script
	}{
		{
			name: "basic_build_with_version_2",
			versions: Config{
				KernelVersion:      "6.1.0",
				FirecrackerVersion: "1.4.0",
			},
			files: createTestSandboxFiles("test-sandbox", "static-id"),
			rootfsPaths: RootfsPaths{
				TemplateVersion: 2,
				TemplateID:      "template-123",
				BuildID:         "build-456",
			},
			namespaceID:        "ns-789",
			expectedRootfsPath: "/fc-vm/rootfs.ext4",
			expectedKernelPath: "/fc-vm/6.1.0/vmlinux.bin",
			expectedScriptContent: []string{
				"mount --make-rprivate /",
				"mount -t tmpfs tmpfs /fc-vm -o X-mount.mkdir",
				"ln -s /orchestrator/sandbox/rootfs-test-sandbox-static-id.link /fc-vm/rootfs.ext4",
				"mkdir -p /fc-vm/6.1.0",
				"ln -s /fc-kernels/6.1.0/vmlinux.bin /fc-vm/6.1.0/vmlinux.bin",
				"nsenter --net=/var/run/netns/ns-789 -- /fc-versions/1.4.0/firecracker --api-sock",
				"fc-test-sandbox-static-id.sock",
			},
		},
		{
			name: "build_with_version_1_backward_compatibility",
			versions: Config{
				KernelVersion:      "5.10.0",
				FirecrackerVersion: "1.3.0",
			},
			files: createTestSandboxFiles("legacy-sandbox", "legacy-id"),
			rootfsPaths: RootfsPaths{
				TemplateVersion: 1,
				TemplateID:      "legacy-template",
				BuildID:         "legacy-build",
			},
			namespaceID:        "legacy-ns",
			expectedRootfsPath: "/mnt/disks/fc-envs/v1/legacy-template/builds/legacy-build/rootfs.ext4",
			expectedKernelPath: "/fc-vm/5.10.0/vmlinux.bin",
			expectedScriptContent: []string{
				"mount --make-rprivate /",
				"mount -t tmpfs tmpfs /mnt/disks/fc-envs/v1/legacy-template/builds/legacy-build -o X-mount.mkdir",
				"ln -s /orchestrator/sandbox/rootfs-legacy-sandbox-legacy-id.link /mnt/disks/fc-envs/v1/legacy-template/builds/legacy-build/rootfs.ext4",
				"mount -t tmpfs tmpfs /fc-vm/5.10.0 -o X-mount.mkdir",
				"ln -s /fc-kernels/5.10.0/vmlinux.bin /fc-vm/5.10.0/vmlinux.bin",
				"nsenter --net=/var/run/netns/legacy-ns -- /fc-versions/1.3.0/firecracker --api-sock",
				"fc-legacy-sandbox-legacy-id.sock",
			},
		},
		{
			name: "different_kernel_and_firecracker_versions",
			versions: Config{
				KernelVersion:      "6.2.1",
				FirecrackerVersion: "1.5.0-beta",
			},
			files: createTestSandboxFiles("custom-sandbox", "custom-id"),
			rootfsPaths: RootfsPaths{
				TemplateVersion: 2,
				TemplateID:      "custom-template",
				BuildID:         "custom-build",
			},
			namespaceID:        "custom-ns-id",
			expectedRootfsPath: "/fc-vm/rootfs.ext4",
			expectedKernelPath: "/fc-vm/6.2.1/vmlinux.bin",
			expectedScriptContent: []string{
				"mkdir -p /fc-vm/6.2.1",
				"ln -s /fc-kernels/6.2.1/vmlinux.bin /fc-vm/6.2.1/vmlinux.bin",
				"nsenter --net=/var/run/netns/custom-ns-id -- /fc-versions/1.5.0-beta/firecracker --api-sock",
				"fc-custom-sandbox-custom-id.sock",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			builder := NewStartScriptBuilder(config)

			// Call Build function directly with the four parameters
			result, err := builder.Build(tt.versions, tt.files, tt.rootfsPaths, tt.namespaceID)
			require.NoError(t, err)
			require.NotNil(t, result)

			// Test computed paths
			assert.Equal(t, tt.expectedRootfsPath, result.RootfsPath)
			assert.Equal(t, tt.expectedKernelPath, result.KernelPath)

			// Test that script is not empty
			assert.NotEmpty(t, result.Value)

			// Test that script contains expected content
			for _, expected := range tt.expectedScriptContent {
				if !strings.Contains(result.Value, expected) {
					t.Errorf("Script should contain expected content: %s\nActual script:\n%s", expected, result.Value)
				}
			}

			// Test script structure - should have all major sections
			if !strings.Contains(result.Value, "mount --make-rprivate /") {
				t.Error("Script should start with mount command")
			}
			if !strings.Contains(result.Value, "nsenter --net=") {
				t.Error("Script should end with firecracker execution via nsenter")
			}
			if strings.Contains(result.Value, "ip netns exec") {
				t.Error("Script must not contain ip netns exec (triggers a second mount namespace copy)")
			}

			// Test that the script has proper formatting (should not have extra spaces or newlines)
			lines := strings.Split(result.Value, "\n")
			if len(lines) <= 1 {
				t.Error("Script should have multiple lines")
			}
		})
	}
}

// TestStartScriptBuilder_NoIpNetnsExec is the core regression guard for the perf fix:
// `ip netns exec` internally calls unshare(CLONE_NEWNS), triggering a second mount-tree
// copy that contends on the namespace_sem global kernel lock under high concurrency.
func TestStartScriptBuilder_NoIpNetnsExec(t *testing.T) {
	t.Parallel()
	config, err := cfg.ParseBuilder()
	require.NoError(t, err)

	builder := NewStartScriptBuilder(config)

	for _, version := range []uint64{1, 2} {
		t.Run(fmt.Sprintf("template_v%d", version), func(t *testing.T) {
			t.Parallel()
			result, err := builder.Build(
				Config{KernelVersion: "6.1.0", FirecrackerVersion: "1.4.0"},
				createTestSandboxFiles("sbx", "sid"),
				RootfsPaths{TemplateVersion: version, TemplateID: "tmpl", BuildID: "bld"},
				"ns-42",
			)
			require.NoError(t, err)
			assert.NotContains(t, result.Value, "ip netns exec",
				"script must not contain ip netns exec (triggers a second mount namespace copy)")
		})
	}
}

// TestStartScriptBuilder_NsenterFormat verifies the nsenter command shape: full
// /var/run/netns/<id> path, the `--` separator so firecracker args are not parsed
// by nsenter, and the firecracker binary immediately after.
func TestStartScriptBuilder_NsenterFormat(t *testing.T) {
	t.Parallel()
	config, err := cfg.ParseBuilder()
	require.NoError(t, err)

	builder := NewStartScriptBuilder(config)

	for _, namespaceID := range []string{"ns-1", "ns-32766", "my-custom-ns"} {
		t.Run(namespaceID, func(t *testing.T) {
			t.Parallel()
			result, err := builder.Build(
				Config{KernelVersion: "6.1.0", FirecrackerVersion: "1.4.0"},
				createTestSandboxFiles("sbx", "sid"),
				RootfsPaths{TemplateVersion: 2, TemplateID: "tmpl", BuildID: "bld"},
				namespaceID,
			)
			require.NoError(t, err)
			assert.Contains(t, result.Value,
				fmt.Sprintf("nsenter --net=%s --", network.NetNSPath(namespaceID)),
				"nsenter must use the full netns path with a -- separator")
		})
	}
}

// TestStartScriptBuilder_NetNamespacePath pins the NetNamespacePath construction in
// buildArgs to network.NetNSPath, the single source of truth shared with the slot.
func TestStartScriptBuilder_NetNamespacePath(t *testing.T) {
	t.Parallel()
	config, err := cfg.ParseBuilder()
	require.NoError(t, err)

	builder := NewStartScriptBuilder(config)

	for _, namespaceID := range []string{"ns-1", "ns-100"} {
		t.Run(namespaceID, func(t *testing.T) {
			t.Parallel()
			args := builder.buildArgs(
				Config{KernelVersion: "6.1.0", FirecrackerVersion: "1.4.0"},
				createTestSandboxFiles("sbx", "sid"),
				RootfsPaths{TemplateVersion: 2, TemplateID: "tmpl", BuildID: "bld"},
				namespaceID,
			)
			assert.Equal(t, network.NetNSPath(namespaceID), args.NetNamespacePath)
			assert.Equal(t, "/var/run/netns/"+namespaceID, args.NetNamespacePath)
		})
	}
}

// BenchmarkBuild measures single-goroutine script generation cost for both
// template versions. This covers template rendering and path construction only;
// subprocess spawn and namespace syscalls (the actual perf win) require root +
// real netns files and cannot be measured here.
//
// Run with:
//
//	go test -bench=BenchmarkBuild -benchmem -count=5 \
//	    ./packages/orchestrator/pkg/sandbox/fc/
func BenchmarkBuild(b *testing.B) {
	config, err := cfg.ParseBuilder()
	if err != nil {
		b.Fatal(err)
	}
	builder := NewStartScriptBuilder(config)
	files := createTestSandboxFiles("sbx", "sid")

	b.Run("v2", func(b *testing.B) {
		b.ReportAllocs()
		versions := Config{KernelVersion: "6.1.0", FirecrackerVersion: "1.4.0"}
		rp := RootfsPaths{TemplateVersion: 2, TemplateID: "tmpl", BuildID: "bld"}
		for b.Loop() {
			if _, err := builder.Build(versions, files, rp, "ns-1"); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("v1", func(b *testing.B) {
		b.ReportAllocs()
		versions := Config{KernelVersion: "5.10.0", FirecrackerVersion: "1.3.0"}
		rp := RootfsPaths{TemplateVersion: 1, TemplateID: "tmpl", BuildID: "bld"}
		for b.Loop() {
			if _, err := builder.Build(versions, files, rp, "ns-1"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSandboxConcurrentStart measures script generation throughput under
// GOMAXPROCS parallelism, simulating concurrent sandbox startup at the
// template-rendering layer. It confirms StartScriptBuilder is stateless and
// contention-free in Go (the namespace_sem kernel lock that caused >400 ms
// latency under ip-netns-exec no longer appears in this path at all).
//
// Run with:
//
//	go test -bench=BenchmarkSandboxConcurrentStart -benchmem -count=5 \
//	    -cpu=1,4,8,16 \
//	    ./packages/orchestrator/pkg/sandbox/fc/
//
// Expected: throughput scales linearly with -cpu; no lock-induced flattening.
func BenchmarkSandboxConcurrentStart(b *testing.B) {
	config, err := cfg.ParseBuilder()
	if err != nil {
		b.Fatal(err)
	}
	builder := NewStartScriptBuilder(config)
	files := createTestSandboxFiles("sbx", "sid")
	versions := Config{KernelVersion: "6.1.0", FirecrackerVersion: "1.4.0"}
	rp := RootfsPaths{TemplateVersion: 2, TemplateID: "tmpl", BuildID: "bld"}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := builder.Build(versions, files, rp, "ns-1"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// createTestSandboxFiles creates a SandboxFiles instance for testing
func createTestSandboxFiles(sandboxID, staticID string) *storage.SandboxFiles {
	paths := storage.Paths{
		BuildID: "test-build",
	}

	cachePaths := storage.CachePaths{
		Paths:           paths,
		CacheIdentifier: "test-cache-id",
	}

	return cachePaths.NewSandboxFilesWithStaticID(sandboxID, staticID)
}
