//go:build linux

package layer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
)

// A layer hands on the envd version its pause recorded, which NewVersionTemplate
// dropped, and nothing else the pause stamped: the build's own view of the layer stands.
func TestPausedLayerMetadata(t *testing.T) {
	t.Parallel()

	meta := metadata.Template{VcpuCount: 2, Template: metadata.TemplateMetadata{BuildID: "layer"}}
	paused := metadata.Template{VcpuCount: 4, RunningEnvdVersion: "0.5.12", Template: metadata.TemplateMetadata{BuildID: "layer"}}

	got := pausedLayerMetadata(meta, &sandbox.Snapshot{Metadata: paused})
	assert.Equal(t, "0.5.12", got.RunningEnvdVersion)
	assert.Equal(t, meta.WithRunningEnvdVersion("0.5.12"), got, "only the recorded envd version is taken from the pause")
}
