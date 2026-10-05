//go:build linux

package finalize

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc/cputemplate"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// The layer result starts as the source layer's metadata, the parent's for a from-template
// build, so a build with no template must clear an inherited one.
func TestLayerStampsThisBuildsCPUTemplate(t *testing.T) {
	t.Parallel()

	parent := &cputemplate.Template{X86TscKhz: 3_200_000}
	own := &cputemplate.Template{X86TscKhz: 2_400_000}

	tests := []struct {
		name    string
		inherit *cputemplate.Template
		build   *cputemplate.Template
		want    *cputemplate.Template
	}{
		{
			name:    "this build's template wins over an inherited one",
			inherit: parent,
			build:   own,
			want:    own,
		},
		{
			// The regression the cmdline stamp already had.
			name:    "no template clears an inherited one",
			inherit: parent,
			build:   nil,
			want:    nil,
		},
		{
			name:    "nothing inherited and nothing set stays empty",
			inherit: nil,
			build:   nil,
			want:    nil,
		},
		{
			// Never sent, so never recorded.
			name:    "a template that changes nothing is not recorded",
			inherit: nil,
			build:   &cputemplate.Template{},
			want:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ppb := &PostProcessingBuilder{
				BuildContext: buildcontext.BuildContext{
					Config:   config.TemplateConfig{CPUTemplate: tt.build},
					Template: storage.Paths{BuildID: "build-1"},
				},
			}

			got, err := ppb.Layer(
				t.Context(),
				phases.LayerResult{Metadata: metadata.Template{CPUTemplate: tt.inherit, BuildCPUTemplate: tt.inherit}},
				"hash",
			)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.Metadata.CPUTemplate)
			assert.Equal(t, tt.want, got.Metadata.BuildCPUTemplate)
		})
	}
}

// Stored metadata must not change when the build config it was copied from does.
func TestLayerCopiesTheCPUTemplate(t *testing.T) {
	t.Parallel()

	build := &cputemplate.Template{X86TscKhz: 3_200_000, KvmCapabilities: []string{"!121"}}

	ppb := &PostProcessingBuilder{
		BuildContext: buildcontext.BuildContext{
			Config:   config.TemplateConfig{CPUTemplate: build},
			Template: storage.Paths{BuildID: "build-1"},
		},
	}

	got, err := ppb.Layer(t.Context(), phases.LayerResult{}, "hash")
	require.NoError(t, err)
	require.NotNil(t, got.Metadata.CPUTemplate)

	build.X86TscKhz = 1
	build.KvmCapabilities[0] = "!1"
	assert.Equal(t, int64(3_200_000), got.Metadata.CPUTemplate.X86TscKhz)
	assert.Equal(t, []string{"!121"}, got.Metadata.CPUTemplate.KvmCapabilities,
		"the collections must be copied too, not shared with the build config")
	assert.NotSame(t, got.Metadata.CPUTemplate, got.Metadata.BuildCPUTemplate,
		"the running and build templates must not share storage")
}
