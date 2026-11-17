package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every copy-constructor must carry the hotplug region: the memfile of a memory snapshot is boot RAM
// plus that region, and a cold boot of the lineage needs it to configure the same device again.
func TestCopyConstructorsCarryMemoryHotplugMB(t *testing.T) {
	t.Parallel()

	base := Template{Version: CurrentVersion, Template: TemplateMetadata{BuildID: "build-1"}, MemoryHotplugMB: 3072}

	tests := map[string]Template{
		"SameVersionTemplate": base.SameVersionTemplate(TemplateMetadata{BuildID: "build-2"}),
		"NewVersionTemplate":  base.NewVersionTemplate(TemplateMetadata{BuildID: "build-2"}),
		"WithPrefetch":        base.WithPrefetch(&Prefetch{Memory: &MemoryPrefetchMapping{}}),
		"WithVcpuCount":       base.WithVcpuCount(4),
		"WithMemoryHotplugMB": Template{Version: CurrentVersion}.WithMemoryHotplugMB(3072),
	}
	for name, got := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, int64(3072), got.MemoryHotplugMB)
		})
	}

	assert.Zero(t, base.BasedOn(FromTemplate{Alias: "a", BuildID: "build-0"}).MemoryHotplugMB,
		"a from-template build cold-boots its own VM and records its region in finalize")
}

func TestMemoryHotplugMBRoundTrips(t *testing.T) {
	t.Parallel()

	reader, err := serialize(Template{Version: CurrentVersion, Template: TemplateMetadata{BuildID: "b"}}.WithMemoryHotplugMB(1024))
	require.NoError(t, err)
	decoded, err := deserialize(reader)
	require.NoError(t, err)
	assert.Equal(t, int64(1024), decoded.MemoryHotplugMB)

	reader, err = serialize(Template{Version: CurrentVersion, Template: TemplateMetadata{BuildID: "b"}})
	require.NoError(t, err)
	decoded, err = deserialize(reader)
	require.NoError(t, err)
	assert.Zero(t, decoded.MemoryHotplugMB, "a template without a region reads as none")
}
