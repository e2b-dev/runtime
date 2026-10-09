//go:build linux

package finalize

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// A rebuild over cached layers starts from their recorded size; the template must record the VM it boots.
func TestLayerStampsThisBuildsVcpuCount(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct{ cached, build int64 }{
		"rebuilt larger":  {cached: 2, build: 8},
		"rebuilt smaller": {cached: 8, build: 2},
		"unrecorded":      {cached: 0, build: 4},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ppb := &PostProcessingBuilder{
				BuildContext: buildcontext.BuildContext{
					Config:   config.TemplateConfig{VCpuCount: tc.build},
					Template: storage.Paths{BuildID: "build-1"},
				},
			}

			got, err := ppb.Layer(t.Context(), phases.LayerResult{Metadata: metadata.Template{VcpuCount: tc.cached}}, "hash")
			require.NoError(t, err)
			assert.Equal(t, tc.build, got.Metadata.VcpuCount)
		})
	}
}
