//go:build linux

package server

import (
	"context"
	"runtime"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
)

// A VM size the host cannot boot must fail the request, not the build mid-way.
func TestValidateMaxVCpuCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		vcpu    int32
		max     int32
		unset   bool
		wantErr bool
		wantMsg string
	}{
		{name: "unset keeps the VM at vCpuCount", vcpu: 2, unset: true},
		{name: "explicit zero", vcpu: 2, max: 0, wantErr: true},
		{name: "negative", vcpu: 2, max: -1, wantErr: true, wantMsg: "maxVCpuCount must be at least 1"},
		{name: "equal to vCpuCount", vcpu: 2, max: 2},
		{name: "larger even", vcpu: 2, max: 8},
		{name: "odd online with even maximum", vcpu: 3, max: 4, wantErr: runtime.GOARCH != "arm64"},
		{name: "below vCpuCount", vcpu: 4, max: 2, wantErr: true, wantMsg: "maxVCpuCount"},
		{name: "no online vCPUs", vcpu: 0, max: 8, wantErr: true},
		{name: "odd on x86", vcpu: 2, max: 3, wantErr: runtime.GOARCH != "arm64"},
		{name: "above the Firecracker maximum", vcpu: 2, max: fc.MaxVcpus + 2, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &templatemanager.TemplateConfig{VCpuCount: tt.vcpu}
			if !tt.unset {
				cfg.MaxVCpuCount = &tt.max
			}

			err := validateMaxVCpuCount(cfg)
			if !tt.wantErr {
				require.NoError(t, err)

				return
			}
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, status.Convert(err).Message(), tt.wantMsg)
		})
	}
}

// A memory size Firecracker cannot give a hotplug region must fail the request, not the build mid-way.
func TestValidateMaxMemoryMB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		memoryMB int32
		max      int32
		unset    bool
		wantErr  bool
		wantMsg  string
	}{
		{name: "unset keeps the VM at memoryMB", memoryMB: 1024, unset: true},
		{name: "equal to memoryMB is no region", memoryMB: 1024, max: 1024},
		{name: "one slot", memoryMB: 1024, max: 1152},
		{name: "whole slots above boot RAM that is not a slot multiple", memoryMB: 1000, max: 1000 + 3*128},
		{name: "partial slot", memoryMB: 1024, max: 1100, wantErr: true, wantMsg: "multiple of 128 MiB"},
		{name: "below memoryMB", memoryMB: 2048, max: 1024, wantErr: true, wantMsg: "at least memoryMB"},
		{name: "explicit zero", memoryMB: 1024, max: 0, wantErr: true, wantMsg: "at least memoryMB"},
		{name: "no boot RAM", memoryMB: 0, max: 1024, wantErr: true, wantMsg: "memoryMB 0"},
		{name: "region above the cap", memoryMB: 1024, max: 1024 + fc.MaxMemoryHotplugMiB + fc.MemoryHotplugSlotMiB, wantErr: true, wantMsg: "MiB a region can have"},
		{name: "no region at any boot RAM", memoryMB: 2 * fc.MaxMemoryHotplugMiB, max: 2 * fc.MaxMemoryHotplugMiB},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &templatemanager.TemplateConfig{MemoryMB: tt.memoryMB}
			if !tt.unset {
				cfg.MaxMemoryMB = &tt.max
			}

			err := validateMaxMemoryMB(cfg)
			if !tt.wantErr {
				require.NoError(t, err)

				return
			}
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, status.Convert(err).Message(), tt.wantMsg)
		})
	}
}

// A builder refusal names its cause on the memory_sizing counter, keyed on the region size.
func TestMemoryRefusalReason(t *testing.T) {
	t.Parallel()

	builder := func(kernel, firecracker string) fc.Config {
		return fc.Config{KernelVersion: kernel, FirecrackerVersion: firecracker}
	}

	require.NoError(t, builder("vmlinux-6.1.158", "v1.17-0.4.1").RefuseMemoryHotplug(3072))
	require.NoError(t, builder("vmlinux-5.10.186", "v1.14-0.3.1").RefuseMemoryHotplug(0), "no region is never refused")

	assert.Equal(t, memoryRejectedFirecracker, fc.MemoryHotplugRefusalReason(builder("vmlinux-6.1.158", "v1.14-0.3.1").RefuseMemoryHotplug(3072)))
	assert.Equal(t, memoryRejectedFirecracker, fc.MemoryHotplugRefusalReason(builder("vmlinux-6.1.158", "v1.14.1_431f1fc").RefuseMemoryHotplug(3072)))
	assert.Equal(t, memoryRejectedKernel, fc.MemoryHotplugRefusalReason(builder("vmlinux-5.10.186", "v1.17-0.4.1").RefuseMemoryHotplug(3072)))
	assert.Equal(t, memoryRejectedInvalid, fc.MemoryHotplugRefusalReason(builder("vmlinux-6.1.158", "v1.17-0.4.1").RefuseMemoryHotplug(100)))
}

// The cap comes from the flag, evaluated per build; without the flag in LaunchDarkly it is 3,
// and 0 lifts it.
func TestValidateMemoryRatioFromFlag(t *testing.T) {
	t.Parallel()

	limit := int32(4224)
	cfg := &templatemanager.TemplateConfig{TemplateID: "tpl", TeamID: "team", MemoryMB: 1024, MaxMemoryMB: &limit}

	td := ldtestdata.DataSource()
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })
	ctx := featureflags.AddToContext(t.Context(), featureflags.TemplateContext("tpl"), featureflags.TeamContext("team"))

	ratio := int64(ff.IntFlag(ctx, featureflags.TemplateMemoryHotplugMaxRatioFlag))
	assert.Equal(t, int64(fc.DefaultMemoryHotplugMaxRatio), ratio, "an absent flag falls back to the default")
	err = validateMemoryRatio(cfg, ratio)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "the largest allowed maxMemoryMB is 4096")

	td.Update(td.Flag(featureflags.TemplateMemoryHotplugMaxRatioFlag.Key()).ValueForAll(ldvalue.Int(0)))
	require.NoError(t, validateMemoryRatio(cfg, int64(ff.IntFlag(ctx, featureflags.TemplateMemoryHotplugMaxRatioFlag))), "0 is no cap")

	td.Update(td.Flag(featureflags.TemplateMemoryHotplugMaxRatioFlag.Key()).ValueForAll(ldvalue.Int(4)))
	require.NoError(t, validateMemoryRatio(cfg, int64(ff.IntFlag(ctx, featureflags.TemplateMemoryHotplugMaxRatioFlag))))

	require.NoError(t, validateMemoryRatio(&templatemanager.TemplateConfig{MemoryMB: 1024}, 3), "no maximum, nothing to cap")
}
