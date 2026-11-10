//go:build linux

package steps

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
)

func TestUncachedStepRecordsIntermediateVmSize(t *testing.T) {
	t.Parallel()

	force := true
	b := &StepBuilder{
		BuildContext: buildcontext.BuildContext{Config: config.TemplateConfig{VCpuCount: 2, MaxVCpuCount: 8}},
		step:         &templatemanager.TemplateStep{Force: &force},
	}
	source := phases.LayerResult{Metadata: metadata.Template{VcpuCount: 8}}

	got, err := b.Layer(t.Context(), source, "hash")
	require.NoError(t, err)
	assert.Equal(t, int64(2), got.Metadata.VcpuCount)
	assert.Equal(t, int64(8), source.Metadata.VcpuCount)
}
