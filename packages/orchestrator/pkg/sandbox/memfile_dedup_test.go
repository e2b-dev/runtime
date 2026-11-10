//go:build linux

package sandbox

import (
	"context"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

type layerDedupGuest struct{ guest.Profile }

func (layerDedupGuest) Template() guest.TemplateBuild {
	return guest.TemplateBuild{DedupLayerMemfiles: true}
}

// Only a build layer of a guest that asks is deduplicated past memfile-diff-dedup,
// and only that one keeps its raw diff on failure; every other pause follows the flag.
func TestMemfileDedup(t *testing.T) {
	t.Parallel()

	off := map[string]any{"enabled": false, "bestEffort": true, "maxFetchWindowsPerBlock": 3}
	on := map[string]any{"enabled": true, "directIO": true}

	tests := []struct {
		name      string
		profile   guest.Profile
		useCase   SnapshotUseCase
		dedupFlag map[string]any
		layerOff  bool
		want      block.DedupOptions
		wantOn    bool
	}{
		{name: "default guest, build layer, flag off", useCase: SnapshotUseCaseBuild, dedupFlag: off},
		{name: "default guest, build layer, flag on", useCase: SnapshotUseCaseBuild, dedupFlag: on, want: block.DedupOptions{DirectIO: true}, wantOn: true},
		{name: "asking guest, build layer, flag off", profile: layerDedupGuest{guest.Default()}, useCase: SnapshotUseCaseBuild, dedupFlag: off, want: block.DedupOptions{BestEffort: true, Budget: block.DedupBudget{MaxFetchWindowsPerBlock: 3}, KeepRawOnError: true}, wantOn: true},
		{name: "asking guest, build layer, flag on", profile: layerDedupGuest{guest.Default()}, useCase: SnapshotUseCaseBuild, dedupFlag: on, want: block.DedupOptions{DirectIO: true, KeepRawOnError: true}, wantOn: true},
		{name: "asking guest, build layer, layer flag off", profile: layerDedupGuest{guest.Default()}, useCase: SnapshotUseCaseBuild, dedupFlag: off, layerOff: true},
		{name: "asking guest, sandbox pause, flag off", profile: layerDedupGuest{guest.Default()}, useCase: SnapshotUseCasePause, dedupFlag: off},
		{name: "asking guest, sandbox pause, flag on", profile: layerDedupGuest{guest.Default()}, useCase: SnapshotUseCasePause, dedupFlag: on, want: block.DedupOptions{DirectIO: true}, wantOn: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			td := ldtestdata.DataSource()
			td.Update(td.Flag(featureflags.MemfileDiffDedupFlag.Key()).ValueForAll(ldvalue.FromJSONMarshal(tt.dedupFlag)))
			if tt.layerOff {
				td.Update(td.Flag(featureflags.BuildLayerMemfileDedupFlag.Key()).VariationForAll(false))
			}
			ff, err := featureflags.NewClientWithDatasource(td)
			require.NoError(t, err)
			t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

			sbx := newTestGuestSandbox(tt.profile)
			sbx.featureFlags = ff

			got, gotOn := sbx.memfileDedup(t.Context(), tt.useCase)
			assert.Equal(t, tt.wantOn, gotOn)
			assert.Equal(t, tt.want, got)
		})
	}
}
