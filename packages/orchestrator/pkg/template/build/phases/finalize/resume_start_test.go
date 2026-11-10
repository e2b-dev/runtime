//go:build linux

package finalize

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

type resumingProfile struct{ guest.Profile }

func (resumingProfile) Template() guest.TemplateBuild { return guest.TemplateBuild{AlwaysResume: true} }

// A build that resumes its parent's memory, where the parent's start command is
// still running, cannot run a replacement start command beside it.
func TestLayerResumedStartOverride(t *testing.T) {
	t.Parallel()

	parentStart := &metadata.Start{StartCmd: "python -m http.server 8080"}
	resuming := resumingProfile{guest.Default()}

	tests := []struct {
		name      string
		profile   guest.Profile
		inherit   *metadata.Start
		startCmd  string
		readyCmd  string
		wantErr   bool
		wantStart *metadata.Start
	}{
		{name: "replacing a running start command is refused", profile: resuming, inherit: parentStart, startCmd: "serve", wantErr: true},
		{name: "a ready command beside an inherited start command keeps it on record", profile: resuming, inherit: parentStart, readyCmd: "curl localhost:8080", wantStart: &metadata.Start{StartCmd: parentStart.StartCmd, ReadyCmd: "curl localhost:8080"}},
		{name: "the grandchild of a ready-only child is refused", profile: resuming, inherit: &metadata.Start{StartCmd: parentStart.StartCmd, ReadyCmd: "curl localhost:8080"}, startCmd: "serve", wantErr: true},
		{name: "a start command with nothing inherited", profile: resuming, startCmd: "serve", wantStart: &metadata.Start{StartCmd: "serve"}},
		{name: "the inherited start command is kept", profile: resuming, inherit: parentStart, wantStart: parentStart},
		{name: "the default profile replaces the inherited start command", inherit: parentStart, startCmd: "serve", wantStart: &metadata.Start{StartCmd: "serve"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ppb := &PostProcessingBuilder{
				BuildContext: buildcontext.BuildContext{
					Config:   config.TemplateConfig{Guest: tt.profile, StartCmd: tt.startCmd, ReadyCmd: tt.readyCmd},
					Template: storage.Paths{BuildID: "build-1"},
				},
			}

			got, err := ppb.Layer(t.Context(), phases.LayerResult{Metadata: metadata.Template{Start: tt.inherit}}, "hash")
			if tt.wantErr {
				require.Error(t, err)
				assert.NotContains(t, err.Error(), parentStart.StartCmd, "the error never quotes the inherited command")

				return
			}
			require.NoError(t, err)
			require.NotNil(t, got.Metadata.Start)
			assert.Equal(t, tt.wantStart.StartCmd, got.Metadata.Start.StartCmd)
			assert.Equal(t, tt.wantStart.ReadyCmd, got.Metadata.Start.ReadyCmd)
		})
	}
}
