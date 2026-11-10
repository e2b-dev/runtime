package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The running envd version describes one memory snapshot: WithPrefetch rewrites that
// snapshot's metadata and keeps it, while the constructors of a new snapshot or build drop
// it, so only Pause's stamp sets it.
func TestNewTemplatesDropRunningEnvdVersionAndWithPrefetchKeepsIt(t *testing.T) {
	t.Parallel()

	base := Template{Version: CurrentVersion, Template: TemplateMetadata{BuildID: "build-1"}}.WithRunningEnvdVersion("0.10.0")

	assert.Equal(t, "0.10.0", base.WithPrefetch(&Prefetch{Memory: &MemoryPrefetchMapping{}}).RunningEnvdVersion)

	for name, got := range map[string]Template{
		"SameVersionTemplate": base.SameVersionTemplate(TemplateMetadata{BuildID: "build-2"}),
		"NewVersionTemplate":  base.NewVersionTemplate(TemplateMetadata{BuildID: "build-2"}),
		"BasedOn":             base.BasedOn(FromTemplate{Alias: "a", BuildID: "build-0"}),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Empty(t, got.RunningEnvdVersion)
		})
	}
}

func TestRunningEnvdVersionRoundTrips(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		version uint64
		want    string
	}{
		{name: "current metadata keeps it", version: CurrentVersion, want: "0.10.0"},
		{name: "V1 metadata discards it", version: DeprecatedVersion, want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reader, err := serialize(Template{Version: tc.version}.WithRunningEnvdVersion("0.10.0"))
			require.NoError(t, err)
			decoded, err := deserialize(reader)
			require.NoError(t, err)
			assert.Equal(t, tc.want, decoded.RunningEnvdVersion)
		})
	}
}
