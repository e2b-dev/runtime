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

// A step that cold-boots a cached source has no region, even when its parent template has one;
// a step that resumes a source built in this build keeps the source's.
func TestStepCarriesTheRegionItsVmHas(t *testing.T) {
	t.Parallel()

	b := &StepBuilder{
		BuildContext: buildcontext.BuildContext{Config: config.TemplateConfig{VCpuCount: 2, MemoryMB: 1024, MaxMemoryMB: 4096}},
		step:         &templatemanager.TemplateStep{Force: new(true)},
	}

	coldBooted, err := b.Layer(t.Context(), phases.LayerResult{Metadata: metadata.Template{VcpuCount: 2, MemoryHotplugMB: 3072}, Cached: true}, "hash")
	require.NoError(t, err)
	assert.Zero(t, coldBooted.Metadata.MemoryHotplugMB)

	resumed, err := b.Layer(t.Context(), phases.LayerResult{Metadata: metadata.Template{VcpuCount: 2, MemoryHotplugMB: 3072}}, "hash")
	require.NoError(t, err)
	assert.Equal(t, int64(3072), resumed.Metadata.MemoryHotplugMB)
}
