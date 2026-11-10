//go:build linux

package finalize

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
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
			action := recordMachineVcpus(func(_ context.Context, _ *sandbox.Sandbox, meta metadata.Template) (metadata.Template, error) {
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
	action := recordMachineVcpus(func(_ context.Context, _ *sandbox.Sandbox, meta metadata.Template) (metadata.Template, error) {
		return meta, wantErr
	})

	got, err := action(t.Context(), sbx, metadata.Template{VcpuCount: 2})
	require.ErrorIs(t, err, wantErr)
	assert.EqualValues(t, 2, got.VcpuCount)
}
