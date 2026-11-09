//go:build linux

package finalize

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
)

// A rebuild over cached layers starts from their recorded size; the template must record the VM it booted.
func TestRecordMachineVcpusStampsBootedVmSize(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct{ cached, booted int64 }{
		"rebuilt larger":  {cached: 2, booted: 8},
		"rebuilt smaller": {cached: 8, booted: 2},
		"unrecorded":      {cached: 0, booted: 4},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sbx := sandbox.NewBalloonTestSandbox("finalize", "", nil)
			sbx.SetMachineVcpusForTest(tc.booted)
			action := recordMachineSize(func(_ context.Context, _ *sandbox.Sandbox, meta metadata.Template) (metadata.Template, error) {
				return meta, nil
			})

			got, err := action(t.Context(), sbx, metadata.Template{VcpuCount: tc.cached})
			require.NoError(t, err)
			assert.Equal(t, tc.booted, got.VcpuCount)
		})
	}
}

func TestSandboxConfigVcpus(t *testing.T) {
	t.Parallel()

	ppb := &PostProcessingBuilder{BuildContext: buildcontext.BuildContext{
		Config: config.TemplateConfig{VCpuCount: 2, MaxVCpuCount: 8},
	}}

	cfg := ppb.sandboxConfig(nil, nil)
	assert.EqualValues(t, 2, cfg.Vcpu)
	assert.EqualValues(t, 8, cfg.ConfiguredVmVcpus)
}

func TestRecordMachineVcpusActionErrorKeepsInput(t *testing.T) {
	t.Parallel()

	sbx := sandbox.NewBalloonTestSandbox("finalize", "", nil)
	sbx.SetMachineVcpusForTest(8)
	wantErr := errors.New("action failed")
	action := recordMachineSize(func(_ context.Context, _ *sandbox.Sandbox, meta metadata.Template) (metadata.Template, error) {
		return meta, wantErr
	})

	got, err := action(t.Context(), sbx, metadata.Template{VcpuCount: 2})
	require.ErrorIs(t, err, wantErr)
	assert.EqualValues(t, 2, got.VcpuCount)
}

// A rebuild over cached layers starts from their recorded region; the template must record the one it booted.
func TestRecordMachineSizeStampsBootedHotplugRegion(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct{ cached, booted int64 }{
		"region added":   {cached: 0, booted: 3072},
		"region removed": {cached: 3072, booted: 0},
		"region changed": {cached: 1024, booted: 2048},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sbx := sandbox.NewBalloonTestSandbox("finalize", "", nil)
			sbx.SetMemoryHotplugMBForTest(tc.booted)
			action := recordMachineSize(func(_ context.Context, _ *sandbox.Sandbox, meta metadata.Template) (metadata.Template, error) {
				return meta, nil
			})

			got, err := action(t.Context(), sbx, metadata.Template{MemoryHotplugMB: tc.cached})
			require.NoError(t, err)
			assert.Equal(t, tc.booted, got.MemoryHotplugMB)
		})
	}
}

func TestSandboxConfigMemoryHotplug(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct{ memoryMB, maxMemoryMB, want int64 }{
		"absent":    {memoryMB: 1024, maxMemoryMB: 0, want: 0},
		"equal":     {memoryMB: 1024, maxMemoryMB: 1024, want: 0},
		"resizable": {memoryMB: 1024, maxMemoryMB: 4096, want: 3072},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ppb := &PostProcessingBuilder{BuildContext: buildcontext.BuildContext{
				Config: config.TemplateConfig{VCpuCount: 2, MemoryMB: tc.memoryMB, MaxMemoryMB: tc.maxMemoryMB},
			}}

			cfg := ppb.sandboxConfig(nil, nil)
			assert.Equal(t, tc.memoryMB, cfg.RamMB, "boot RAM stays the template's memory size")
			assert.Equal(t, tc.want, cfg.MemoryHotplugMB)
		})
	}
}

// A region whose size or guest cannot carry it is the request's to change: a user error. A builder
// Firecracker or guest kernel too old for it is the fleet's: an internal error.
func TestFinalizeErrorClassifiesAnUnsupportedRegion(t *testing.T) {
	t.Parallel()

	meta := (&PostProcessingBuilder{}).Metadata()

	refused := finalizeError(meta, fmt.Errorf("create sandbox: %w", fc.Config{KernelVersion: "vmlinux-6.1.158", FirecrackerVersion: "v1.17-0.4.1"}.RefuseMemoryHotplug(100)))
	require.NotNil(t, phases.UnwrapPhaseBuildError(refused), "a region of the wrong size is the user's")
	require.ErrorIs(t, refused, fc.ErrMemoryHotplugUnsupported)

	for _, fleet := range []error{fc.ErrMemoryHotplugKernel, fc.ErrMemoryHotplugFirecracker} {
		got := finalizeError(meta, fmt.Errorf("create sandbox: %w", fleet))
		assert.Nil(t, phases.UnwrapPhaseBuildError(got), "%v is the fleet's", fleet)
		require.ErrorIs(t, got, fleet)
	}

	other := finalizeError(meta, errors.New("network slot unavailable"))
	assert.Nil(t, phases.UnwrapPhaseBuildError(other), "other boot failures stay the builder's")
	assert.ErrorContains(t, other, "error running start and ready commands in sandbox")
}
