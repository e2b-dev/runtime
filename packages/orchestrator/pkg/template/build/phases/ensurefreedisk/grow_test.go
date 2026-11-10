//go:build linux

package ensurefreedisk

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// The phase mints a fresh UUID for its own layer artifact, so build.id has two
// candidates and only one of them is the build an operator filters on. Nothing
// fails if the mount logs the other one: the lines are simply absent from the
// build they belong to.
func TestOfflineDeviceRuntime_NamesTheBuildNotTheLayer(t *testing.T) {
	t.Parallel()

	layerBuildID := uuid.NewString()
	b := &EnsureFreeDiskBuilder{
		BuildContext: buildcontext.BuildContext{
			Config: config.TemplateConfig{
				TemplateID: "tmpl-offline-device",
				TeamID:     "team-offline-device",
			},
			Template: storage.Paths{BuildID: "build-offline-device"},
		},
	}

	runtime := b.offlineDeviceRuntime()

	require.Equal(t, "build-offline-device", runtime.BuildID)
	require.NotEqual(t, layerBuildID, runtime.BuildID)
	require.Equal(t, "tmpl-offline-device", runtime.TemplateID)
	require.Equal(t, "team-offline-device", runtime.TeamID)

	fields := map[string]string{}
	for _, f := range runtime.LogFields() {
		fields[f.Key] = f.String
	}

	require.Equal(t, "build-offline-device", fields["build.id"])
	require.Equal(t, "tmpl-offline-device", fields["template.id"])
	require.Equal(t, "team-offline-device", fields["team.id"])
	require.NotContains(t, fields, "sandbox.id")
}
