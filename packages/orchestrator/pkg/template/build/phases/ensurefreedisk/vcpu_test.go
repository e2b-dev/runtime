//go:build linux

package ensurefreedisk

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
)

func TestUncachedDiskLayerRecordsIntermediateVmSize(t *testing.T) {
	t.Parallel()

	force := true
	b := &EnsureFreeDiskBuilder{BuildContext: buildcontext.BuildContext{
		Config: config.TemplateConfig{VCpuCount: 2, MaxVCpuCount: 8, Force: &force},
	}}
	source := phases.LayerResult{Metadata: metadata.Template{VcpuCount: 8}, Cached: true}

	got, err := b.Layer(t.Context(), source, "hash")
	require.NoError(t, err)
	assert.Equal(t, int64(2), got.Metadata.VcpuCount)
	assert.Equal(t, int64(8), source.Metadata.VcpuCount)
}

// No VM boots in the disk layer, so it keeps its source lineage's region.
func TestUncachedDiskLayerKeepsTheSourceRegion(t *testing.T) {
	t.Parallel()

	force := true
	b := &EnsureFreeDiskBuilder{BuildContext: buildcontext.BuildContext{
		Config: config.TemplateConfig{VCpuCount: 2, MemoryMB: 1024, MaxMemoryMB: 4096, Force: &force},
	}}
	source := phases.LayerResult{Metadata: metadata.Template{VcpuCount: 2, MemoryHotplugMB: 3072}, Cached: true}

	got, err := b.Layer(t.Context(), source, "hash")
	require.NoError(t, err)
	assert.Equal(t, int64(3072), got.Metadata.MemoryHotplugMB)
}
