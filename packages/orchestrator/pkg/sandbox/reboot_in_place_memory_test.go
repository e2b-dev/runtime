//go:build linux

package sandbox

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/clickhouse/pkg/hoststats"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/cgroup"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// resumedWithRegion is a sandbox memory-resumed from a snapshot with a 3072 MiB region, whose
// process has exited after a guest reset: its config carries no region, the snapshot does.
func resumedWithRegion(t *testing.T, versions fc.Config) (*Sandbox, *Factory, *int64) {
	t.Helper()

	builder := cfg.BuilderConfig{
		FirecrackerVersionsDir: t.TempDir(), HostKernelsDir: t.TempDir(),
		StorageConfig: storage.Config{SandboxCacheDir: t.TempDir()},
	}
	for _, path := range []string{versions.FirecrackerPath(builder), versions.HostKernelPath(builder)} {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, nil, 0o755))
	}
	flags, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { _ = flags.Close(context.WithoutCancel(t.Context())) })

	from := testMapSandbox(t, "resumed-with-region")
	from.Config = NewConfig(Config{Vcpu: 1, RamMB: 1024, Network: &orchestrator.SandboxNetworkConfig{}, FirecrackerConfig: versions, Envd: EnvdMetadata{Version: "0.9.0"}})
	from.Runtime = sandboxtypes.RuntimeMetadata{SandboxID: uuid.NewString(), ExecutionID: uuid.NewString(), SandboxType: sandboxtypes.SandboxTypeSandbox}
	from.Template = rebootHookTemplate{
		bootTemplate: bootTemplate{
			capacityOnlyTemplate: capacityOnlyTemplate{meta: metadata.Template{Version: metadata.CurrentVersion, VcpuCount: 1, MemoryHotplugMB: 3072}},
			snapfile:             filepath.Join(t.TempDir(), "snapfile"),
		},
		buildID: uuid.NewString(),
	}
	from.SetMemoryHotplugMBForTest(3072)
	from.rootfs = &handedOverRootfs{path: filepath.Join(t.TempDir(), "rootfs")}
	from.process = &fc.Process{Exit: utils.NewErrorOnce()}
	require.NoError(t, from.process.Exit.SetError(nil))
	from.sandboxes = NewSandboxesMap()
	from.slotRelease = newHandover(func(context.Context) error { return nil })
	from.rootfsRelease = newHandover(func(context.Context) error { return nil })
	from.networkRelease = func(context.Context, string) error { return nil }

	memfileBytes := new(int64)
	var hook networkAssignHookFunc = func(ctx context.Context, next *Sandbox, _ NetworkAssignReason) (*orchestrator.SandboxNetworkPlacement, error) {
		memfile, err := next.Template.Memfile(ctx)
		require.NoError(t, err)
		*memfileBytes, err = memfile.Size(ctx)
		require.NoError(t, err)

		return nil, errStopAtHook
	}
	factory := NewFactory(t.Context(), builder, &factoryNetworkPool{slot: from.Slot}, nil, flags, hoststats.NewNoopDelivery(), cgroup.NewNoopManager(), network.NewNoopEgressProxy(), hook, from.sandboxes)

	return from, factory, memfileBytes
}

var errStopAtHook = errors.New("stop at the hook")

// A sandbox resumed from a snapshot with a region and reset in place on a Firecracker without
// hotplug is refused before its slot and overlay are taken.
func TestStartInPlaceRefusesARegionBeforeTheTake(t *testing.T) {
	t.Parallel()

	from, factory, _ := resumedWithRegion(t, fc.Config{KernelVersion: "vmlinux-6.1.158", FirecrackerVersion: "v1.14-0.3.1"})

	next, err := factory.startInPlace(t.Context(), from, time.Now().Add(time.Hour))
	require.Nil(t, next)
	require.ErrorIs(t, err, fc.ErrMemoryHotplugFirecracker)

	_, slotStillOwned := from.slotRelease.take()
	_, rootfsStillOwned := from.rootfsRelease.take()
	assert.True(t, slotStillOwned, "the slot was not taken")
	assert.True(t, rootfsStillOwned, "the overlay was not taken")
}

// On a capable Firecracker the reset boots with the snapshot's region: the config carries it to
// the process and the cold-boot memfile covers it. The boot stops at the network-assign hook.
func TestStartInPlaceBootsWithTheSnapshotRegion(t *testing.T) {
	t.Parallel()

	from, factory, memfileBytes := resumedWithRegion(t, fc.Config{KernelVersion: "vmlinux-6.1.158", FirecrackerVersion: "v1.17-0.4.1"})

	next, err := factory.startInPlace(t.Context(), from, time.Now().Add(time.Hour))
	require.Nil(t, next)
	require.ErrorIs(t, err, errStopAtHook, "the start passes the region checks and reaches the hook")
	assert.Equal(t, int64(3072), from.Config.MemoryHotplugMB, "the boot's config carries the snapshot's region")
	assert.Equal(t, units.MBToBytes(1024+3072), *memfileBytes, "the cold-boot memfile covers boot RAM and the region")
}
