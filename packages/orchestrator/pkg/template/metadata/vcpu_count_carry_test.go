package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every copy-constructor must carry the VM's vCPU count, or a resume of a bigger-than-sandbox
// snapshot would trust the request instead of the snapshot. Same contract as cpu_template_carry_test.go.
func TestCopyConstructorsCarryVcpuCount(t *testing.T) {
	t.Parallel()

	base := Template{Version: CurrentVersion, Template: TemplateMetadata{BuildID: "build-1"}, VcpuCount: 16}

	tests := map[string]Template{
		"SameVersionTemplate": base.SameVersionTemplate(TemplateMetadata{BuildID: "build-2"}),
		"NewVersionTemplate":  base.NewVersionTemplate(TemplateMetadata{BuildID: "build-2"}),
		"WithPrefetch":        base.WithPrefetch(&Prefetch{Memory: &MemoryPrefetchMapping{}}),
		"WithVcpuCount":       Template{Version: CurrentVersion}.WithVcpuCount(16),
	}
	for name, got := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, int64(16), got.VcpuCount)
		})
	}

	assert.Zero(t, base.BasedOn(FromTemplate{Alias: "a", BuildID: "build-0"}).VcpuCount,
		"a from-template build cold-boots its own VM and records its capacity on pause")
}

func TestWithVcpuCountKeepsV1MemorySnapshotVersion(t *testing.T) {
	t.Parallel()

	stamped := V1TemplateVersion().WithVcpuCount(4)
	assert.Equal(t, uint64(DeprecatedVersion), stamped.Version)
	assert.Equal(t, int64(4), stamped.VcpuCount)

	reader, err := serialize(stamped)
	require.NoError(t, err)
	decoded, err := deserialize(reader)
	require.NoError(t, err)
	assert.Equal(t, uint64(DeprecatedVersion), decoded.Version)
	assert.Zero(t, decoded.VcpuCount, "V1 metadata discards the count and uses the legacy resume path")
}
