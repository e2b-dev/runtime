//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/cgroup"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd/testutils"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

type canceledConstructionTemplate struct {
	capacityOnlyTemplate

	err error
}

func (t canceledConstructionTemplate) Metadata() (metadata.Template, error) {
	return metadata.Template{}, t.err
}

func (canceledConstructionTemplate) Files() storage.CachePaths { return storage.CachePaths{} }

func (t canceledConstructionTemplate) Rootfs() (block.ReadonlyDevice, error) {
	return nil, t.err
}

type canceledConstructionPool struct{ called chan struct{} }

func (p canceledConstructionPool) Get(ctx context.Context, _ *orchestrator.SandboxNetworkConfig, _ sandboxtypes.EgressClass) (*network.Slot, error) {
	close(p.called)

	return nil, ctx.Err()
}

func (canceledConstructionPool) ReturnAsync(context.Context, *network.Slot, network.ReleaseNotify, time.Duration) error {
	panic("canceled construction did not acquire a slot")
}

func (canceledConstructionPool) Close(context.Context) error { return nil }

// Both actual constructor defers must preserve a cancellation cause even when a
// dependency reports only context.Canceled. The template fails before provider,
// UFFD, or Firecracker construction; no network slot or cgroup is acquired.
func TestFactoryFailedConstructorsPreserveCancellationCause(t *testing.T) {
	t.Parallel()
	for _, resume := range []bool{false, true} {
		t.Run(map[bool]string{false: "create", true: "resume"}[resume], func(t *testing.T) {
			t.Parallel()
			cause := errors.New("construction dependency canceled with its operational failure")
			primary := fmt.Errorf("template lookup interrupted: %w", context.Canceled)
			ctx, cancel := context.WithCancelCause(t.Context())
			cancel(cause)
			pool := canceledConstructionPool{called: make(chan struct{})}
			factory := NewFactory(t.Context(), cfg.BuilderConfig{
				StorageConfig: storage.Config{SandboxCacheDir: t.TempDir()},
			}, pool, nil, nil, nil, cgroup.NewNoopManager(), nil, nil, NewSandboxesMap())
			config := NewConfig(Config{Vcpu: 1})
			runtime := sandboxtypes.RuntimeMetadata{SandboxID: uuid.NewString(), ExecutionID: uuid.NewString()}
			template := canceledConstructionTemplate{err: primary}
			var sbx *Sandbox
			var err error
			if resume {
				sbx, err = factory.ResumeSandbox(ctx, template, config, runtime, time.Time{}, time.Time{}, nil)
			} else {
				sbx, err = factory.CreateSandbox(ctx, config, runtime, template, time.Minute, "", fc.ProcessOptions{}, nil)
			}
			if !resume {
				select {
				case <-pool.called:
				case <-time.After(time.Second):
					t.Fatal("owned network lookup did not complete")
				}
			}
			require.Nil(t, sbx)
			require.ErrorIs(t, err, primary)
			require.ErrorIs(t, err, cause, "the actual constructor must retain the cancellation diagnosis")
		})
	}
}

func TestFactoryFailedConstructorsDoNotAttributeIndependentErrorsToCancellation(t *testing.T) {
	t.Parallel()
	for _, resume := range []bool{false, true} {
		t.Run(map[bool]string{false: "create", true: "resume"}[resume], func(t *testing.T) {
			t.Parallel()
			cause := errors.New("construction dependency canceled with its operational failure")
			primary := errors.New("template version is unavailable")
			ctx, cancel := context.WithCancelCause(t.Context())
			cancel(cause)
			pool := canceledConstructionPool{called: make(chan struct{})}
			factory := NewFactory(t.Context(), cfg.BuilderConfig{
				StorageConfig: storage.Config{SandboxCacheDir: t.TempDir()},
			}, pool, nil, nil, nil, cgroup.NewNoopManager(), nil, nil, NewSandboxesMap())
			config := NewConfig(Config{Vcpu: 1})
			runtime := sandboxtypes.RuntimeMetadata{SandboxID: uuid.NewString(), ExecutionID: uuid.NewString()}
			template := canceledConstructionTemplate{err: primary}
			var sbx *Sandbox
			var err error
			if resume {
				sbx, err = factory.ResumeSandbox(ctx, template, config, runtime, time.Time{}, time.Time{}, nil)
			} else {
				sbx, err = factory.CreateSandbox(ctx, config, runtime, template, time.Minute, "", fc.ProcessOptions{}, nil)
			}
			if !resume {
				select {
				case <-pool.called:
				case <-time.After(time.Second):
					t.Fatal("owned network lookup did not complete")
				}
			}
			require.Nil(t, sbx)
			require.ErrorIs(t, err, primary)
			require.NotErrorIs(t, err, cause, "an independent template error must not inherit an unrelated cancellation diagnosis")
		})
	}
}

// bootTemplate serves zeroed rootfs and memory devices of one MiB and a
// snapfile, which are all a start reads before the guest runs.
type bootTemplate struct {
	capacityOnlyTemplate

	snapfile string
}

func (bootTemplate) Files() storage.CachePaths { return storage.CachePaths{} }

func (bootTemplate) Rootfs() (block.ReadonlyDevice, error) {
	return testutils.NewZeroDevice(1<<20, header.RootfsBlockSize)
}

func (bootTemplate) Memfile(context.Context) (block.ReadonlyDevice, error) {
	return testutils.NewZeroDevice(1<<20, header.PageSize)
}

func (t bootTemplate) Snapfile() (sbxtemplate.File, error) {
	return sbxtemplate.NewLocalFileLink(t.snapfile), nil
}

// startedDevicePool returns a populated NBD device pool, which a resume's
// rootfs connects to while the start goes on, and closes it on cleanup. It
// is nil without root, which the nbd needs.
func startedDevicePool(t *testing.T) *nbd.DevicePool {
	t.Helper()
	if os.Geteuid() != 0 {
		return nil
	}
	devices, err := nbd.NewDevicePool(1)
	require.NoError(t, err)
	populating, stop := context.WithCancel(t.Context())
	populated := make(chan struct{})
	go func() {
		defer close(populated)
		devices.Populate(populating)
	}()
	t.Cleanup(func() {
		stop()
		<-populated
		require.NoError(t, devices.Close(context.WithoutCancel(t.Context())))
	})

	return devices
}

type rebootHookTemplate struct {
	bootTemplate

	buildID string
}

func (t rebootHookTemplate) Files() storage.CachePaths {
	return storage.CachePaths{Paths: storage.Paths{BuildID: t.buildID}}
}

// The actual constructors run the network-assign hook with the start's
// reason after the sandbox is assigned its network and before Firecracker
// starts: an error fails the start there, an optional one lets it go on to
// Firecracker, and the slot is returned either way. A reboot shows the hook
// its absolute end time, not one counted from the create. A throwaway
// resume denies its slot's egress before the hook,
// which this fixture's slot cannot, so it is not started here.
func TestFactoryConstructorsRunTheNetworkAssignHook(t *testing.T) {
	t.Parallel()
	builder := cfg.BuilderConfig{
		FirecrackerVersionsDir: t.TempDir(), HostKernelsDir: t.TempDir(),
		StorageConfig: storage.Config{SandboxCacheDir: t.TempDir()},
	}
	versions := fc.Config{KernelVersion: "kernel", FirecrackerVersion: "firecracker"}
	for _, path := range []string{versions.FirecrackerPath(builder), versions.HostKernelPath(builder)} {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, nil, 0o755))
	}
	flags, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { _ = flags.Close(context.WithoutCancel(t.Context())) })
	hookFailure := errors.New("the network could not be completed")
	devices := startedDevicePool(t)
	rebootEnd := time.Now().Add(42 * time.Minute)

	for _, tc := range []struct {
		name  string
		start func(ctx context.Context, f *Factory, runtime sandboxtypes.RuntimeMetadata, config *Config, template bootTemplate) (*Sandbox, error)
		// devices is set when the constructor's rootfs needs NBD devices.
		devices  bool
		reason   NetworkAssignReason
		optional bool
		// endAt is the end the hook must see, zero for one counted from the
		// create.
		endAt time.Time
	}{
		{name: "create", reason: NetworkAssignReasonCreate, start: func(ctx context.Context, f *Factory, runtime sandboxtypes.RuntimeMetadata, config *Config, template bootTemplate) (*Sandbox, error) {
			return f.CreateSandbox(ctx, config, runtime, template, time.Minute, filepath.Join(filepath.Dir(template.snapfile), "rootfs"), fc.ProcessOptions{}, nil)
		}},
		{name: "reboot", devices: true, reason: NetworkAssignReasonReboot, endAt: rebootEnd, start: func(ctx context.Context, f *Factory, runtime sandboxtypes.RuntimeMetadata, config *Config, template bootTemplate) (*Sandbox, error) {
			config.RamMB = 1
			config.Envd.Version = "0.9.0"
			template.meta.FilesystemOnly = true

			return f.RebootSandbox(ctx, rebootHookTemplate{bootTemplate: template, buildID: uuid.NewString()}, config, runtime,
				rebootEnd, nil, false, false, nil)
		}},
		{name: "resume", devices: true, reason: NetworkAssignReasonResume, start: func(ctx context.Context, f *Factory, runtime sandboxtypes.RuntimeMetadata, config *Config, template bootTemplate) (*Sandbox, error) {
			return f.ResumeSandbox(ctx, template, config, runtime, time.Time{}, time.Time{}, nil)
		}},
		{name: "create with an optional error", optional: true, reason: NetworkAssignReasonCreate, start: func(ctx context.Context, f *Factory, runtime sandboxtypes.RuntimeMetadata, config *Config, template bootTemplate) (*Sandbox, error) {
			return f.CreateSandbox(ctx, config, runtime, template, time.Minute, filepath.Join(filepath.Dir(template.snapfile), "rootfs"), fc.ProcessOptions{}, nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.devices && devices == nil {
				t.Skip("the nbd requires root privileges to run")
			}
			pool := &factoryNetworkPool{slot: testMapSandbox(t, "pool").Slot}
			asked := make(chan NetworkAssignReason, 1)
			hook := networkAssignHookFunc(func(_ context.Context, sbx *Sandbox, reason NetworkAssignReason) (*orchestrator.SandboxNetworkPlacement, error) {
				assert.Same(t, pool.slot, sbx.Slot, "the hook sees the slot the start took")
				if !tc.endAt.IsZero() {
					assert.True(t, sbx.GetEndAt().Equal(tc.endAt), "the hook sees the absolute end time the start was given")
				}
				asked <- reason
				if tc.optional {
					return nil, OptionalNetworkAssignError(hookFailure)
				}

				return nil, hookFailure
			})
			factory := NewFactory(t.Context(), builder, pool, devices, flags, hoststats.NewNoopDelivery(), cgroup.NewNoopManager(), network.NewNoopEgressProxy(), hook, NewSandboxesMap())
			runtime := sandboxtypes.RuntimeMetadata{SandboxID: uuid.NewString(), ExecutionID: uuid.NewString(), SandboxType: sandboxtypes.SandboxTypeSandbox}
			config := NewConfig(Config{Vcpu: 1, Network: &orchestrator.SandboxNetworkConfig{}, FirecrackerConfig: versions})
			template := bootTemplate{snapfile: filepath.Join(t.TempDir(), "snapfile")}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()

			sbx, err := tc.start(ctx, factory, runtime, config, template)
			require.Nil(t, sbx)
			require.Error(t, err)
			select {
			case reason := <-asked:
				require.Equal(t, tc.reason, reason, "the hook is asked with the start's reason")
			default:
				require.FailNow(t, "the hook did not run", "%v", err)
			}
			if tc.optional {
				require.NotErrorIs(t, err, hookFailure, "the start went on past the hook, to Firecracker")
			} else {
				require.ErrorIs(t, err, hookFailure, "the start fails on the hook, before Firecracker starts")
			}
			require.Equal(t, 1, pool.returns, "the start returned its slot")
		})
	}
}

// A reboot in place hands the next lifecycle the overlay with the envd an
// offline upgrade installed on it and the start time of the execution it
// continues, so its network admission reads that version and not the
// template's, and sees an execution it already placed. The start stops at
// the hook.
func TestStartInPlaceHandsOverTheDiskVersionAndTheExecutionStart(t *testing.T) {
	t.Parallel()
	builder := cfg.BuilderConfig{
		FirecrackerVersionsDir: t.TempDir(), HostKernelsDir: t.TempDir(),
		StorageConfig: storage.Config{SandboxCacheDir: t.TempDir()},
	}
	versions := fc.Config{KernelVersion: "kernel", FirecrackerVersion: "firecracker"}
	for _, path := range []string{versions.FirecrackerPath(builder), versions.HostKernelPath(builder)} {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, nil, 0o755))
	}
	flags, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { _ = flags.Close(context.WithoutCancel(t.Context())) })

	from := testMapSandbox(t, "upgraded-offline")
	from.Config = NewConfig(Config{Vcpu: 1, RamMB: 1, Network: &orchestrator.SandboxNetworkConfig{}, FirecrackerConfig: versions, Envd: EnvdMetadata{Version: "0.9.0"}})
	from.Runtime = sandboxtypes.RuntimeMetadata{SandboxID: uuid.NewString(), ExecutionID: uuid.NewString(), SandboxType: sandboxtypes.SandboxTypeSandbox}
	from.Template = rebootHookTemplate{bootTemplate: bootTemplate{snapfile: filepath.Join(t.TempDir(), "snapfile")}, buildID: uuid.NewString()}
	from.rootfs = &handedOverRootfs{path: filepath.Join(t.TempDir(), "rootfs")}
	from.process = &fc.Process{Exit: utils.NewErrorOnce()}
	require.NoError(t, from.process.Exit.SetError(nil))
	from.sandboxes = NewSandboxesMap()
	from.slotRelease = newHandover(func(context.Context) error { return nil })
	rootfsClosed := false
	from.rootfsRelease = newHandover(func(context.Context) error {
		rootfsClosed = true

		return nil
	})
	from.networkRelease = func(context.Context, string) error { return nil }
	installed := "0.10.2"
	from.offlineEnvdVersion.Store(&installed)
	require.Equal(t, installed, from.ExpectedEnvdVersion())
	started := time.Now().Add(-time.Minute)
	from.SetExecutionStartedAt(started)

	pool := &factoryNetworkPool{slot: from.Slot}
	hookFailure := errors.New("stop at the hook")
	var seen string
	var seenStart time.Time
	hook := networkAssignHookFunc(func(_ context.Context, next *Sandbox, reason NetworkAssignReason) (*orchestrator.SandboxNetworkPlacement, error) {
		assert.Equal(t, NetworkAssignReasonReboot, reason)
		assert.Same(t, from.rootfs, next.rootfs, "the hook sees the handed-over overlay")
		seen = next.ExpectedEnvdVersion()
		seenStart = next.GetExecutionStartedAt()

		return nil, hookFailure
	})
	factory := NewFactory(t.Context(), builder, pool, nil, flags, hoststats.NewNoopDelivery(), cgroup.NewNoopManager(), network.NewNoopEgressProxy(), hook, from.sandboxes)

	next, err := factory.startInPlace(t.Context(), from, time.Now().Add(time.Hour))
	require.Nil(t, next)
	require.ErrorIs(t, err, hookFailure, "the start reaches the hook and stops there")
	assert.Equal(t, installed, seen, "the hook sees the envd installed on the handed-over overlay")
	assert.True(t, seenStart.Equal(started), "the hook sees the execution started, at %s not %s", started, seenStart)
	assert.True(t, rootfsClosed, "a start that fails at the hook closes the handed-over overlay")
}

// handedOverRootfs is a started overlay a reboot in place hands over; only its path is read.
type handedOverRootfs struct {
	rootfs.Provider

	path string
}

func (r *handedOverRootfs) Path() (string, error) { return r.path, nil }

// The network-assign hook sees the version an offline swap put on the rootfs. Nil pre-boot
// steps are skipped and the first failing step stops the rest.
func TestCreateSandboxRunsTheEnvdSwapBeforeTheNetworkAssignHook(t *testing.T) {
	t.Parallel()
	builder := cfg.BuilderConfig{
		FirecrackerVersionsDir: t.TempDir(), HostKernelsDir: t.TempDir(),
		StorageConfig: storage.Config{SandboxCacheDir: t.TempDir()},
	}
	versions := fc.Config{KernelVersion: "kernel", FirecrackerVersion: "firecracker"}
	for _, path := range []string{versions.FirecrackerPath(builder), versions.HostKernelPath(builder)} {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, nil, 0o755))
	}
	flags, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { _ = flags.Close(context.WithoutCancel(t.Context())) })
	hookFailure := errors.New("the network could not be completed")
	recoverFailure := errors.New("the rootfs could not be recovered")
	swapFailure := errors.New("the swap left the rootfs unbootable")

	for _, tc := range []struct {
		name       string
		swapped    string
		recoverErr error
		swapErr    error
		wantEnvd   string
		wantErr    error
		wantSteps  []string
	}{
		{name: "swapped", swapped: "0.10.2", wantEnvd: "0.10.2", wantErr: hookFailure, wantSteps: []string{"recover", "swap", "hook"}},
		{name: "skipped or failed swap", wantEnvd: "0.9.0", wantErr: hookFailure, wantSteps: []string{"recover", "swap", "hook"}},
		{name: "failed recovery", swapped: "0.10.2", recoverErr: recoverFailure, wantErr: recoverFailure, wantSteps: []string{"recover"}},
		{name: "fatal swap", swapErr: swapFailure, wantErr: swapFailure, wantSteps: []string{"recover", "swap"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var steps []string
			pool := &factoryNetworkPool{slot: testMapSandbox(t, "pool").Slot}
			hook := networkAssignHookFunc(func(_ context.Context, sbx *Sandbox, _ NetworkAssignReason) (*orchestrator.SandboxNetworkPlacement, error) {
				steps = append(steps, "hook")
				assert.Equal(t, tc.wantEnvd, sbx.ExpectedEnvdVersion(), "the hook sees the envd the guest will boot")

				return nil, hookFailure
			})
			factory := NewFactory(t.Context(), builder, pool, nil, flags, hoststats.NewNoopDelivery(), cgroup.NewNoopManager(), network.NewNoopEgressProxy(), hook, NewSandboxesMap())
			runtime := sandboxtypes.RuntimeMetadata{SandboxID: uuid.NewString(), ExecutionID: uuid.NewString(), SandboxType: sandboxtypes.SandboxTypeSandbox}
			config := NewConfig(Config{Vcpu: 1, Network: &orchestrator.SandboxNetworkConfig{}, FirecrackerConfig: versions, Envd: EnvdMetadata{Version: "0.9.0"}})
			template := bootTemplate{snapfile: filepath.Join(t.TempDir(), "snapfile")}
			recoverFS := func(context.Context, string) (string, error) {
				steps = append(steps, "recover")

				return "", tc.recoverErr
			}
			swap := func(context.Context, string) (string, error) {
				steps = append(steps, "swap")

				return tc.swapped, tc.swapErr
			}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()

			sbx, err := factory.CreateSandbox(ctx, config, runtime, template, time.Minute, filepath.Join(filepath.Dir(template.snapfile), "rootfs"),
				fc.ProcessOptions{}, nil, WithPreBoot(recoverFS, nil, swap))
			require.Nil(t, sbx)
			require.ErrorIs(t, err, tc.wantErr)
			assert.Equal(t, tc.wantSteps, steps)
			assert.Equal(t, 1, pool.returns, "the start returned its slot")
		})
	}
}

func TestPreserveConstructionCause(t *testing.T) {
	t.Parallel()
	cause := errors.New("construction dependency failed")
	primary := fmt.Errorf("construction interrupted: %w", context.Canceled)
	for _, tc := range []struct {
		name      string
		cause     error
		primary   error
		cancel    bool
		deadline  bool
		wantCause bool
		unchanged bool
	}{
		{name: "custom cause", cause: cause, primary: primary, cancel: true, wantCause: true},
		{name: "wrapped cancellation diagnosis", cause: fmt.Errorf("provider transport failed: %w", context.Canceled), primary: primary, cancel: true, wantCause: true},
		{name: "wrapped deadline diagnosis", cause: fmt.Errorf("provider deadline failed: %w", context.DeadlineExceeded), primary: fmt.Errorf("construction timed out: %w", context.DeadlineExceeded), deadline: true, wantCause: true},
		{name: "already preserved", cause: cause, primary: fmt.Errorf("construction failed: %w", cause), cancel: true, wantCause: true, unchanged: true},
		{name: "ordinary cancellation", primary: primary, cancel: true, unchanged: true},
		{name: "ordinary deadline", primary: fmt.Errorf("construction timed out: %w", context.DeadlineExceeded), deadline: true, unchanged: true},
		{name: "independent error with canceled context", cause: cause, primary: errors.New("invalid construction request"), cancel: true, unchanged: true},
		{name: "independent error with expired deadline", cause: cause, primary: errors.New("invalid construction request"), deadline: true, unchanged: true},
		{name: "uncanceled", primary: primary, unchanged: true},
		{name: "successful construction", cause: cause, cancel: true, unchanged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			if tc.cancel {
				cancel(tc.cause)
			}
			if tc.deadline {
				var stop context.CancelFunc
				ctx, stop = context.WithDeadlineCause(t.Context(), time.Unix(0, 0), tc.cause)
				defer stop()
			}
			got := preserveConstructionCause(ctx, tc.primary)
			if tc.primary != nil {
				require.ErrorIs(t, got, tc.primary)
			} else {
				require.NoError(t, got)
			}
			if tc.wantCause {
				require.ErrorIs(t, got, tc.cause)
			}
			if tc.unchanged {
				require.Equal(t, tc.primary, got)
			}
		})
	}
}
