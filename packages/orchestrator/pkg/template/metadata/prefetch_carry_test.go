package metadata

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The pause-resume prefetch harvest relies on two metadata invariants: a plain
// pause (SameVersionTemplate) drops any existing prefetch mapping, and
// WithPrefetch is the only thing that puts one back. If either changes, the
// harvest's "re-attach the harvested mapping to the pause metadata" rationale
// no longer holds, so these lock the behaviour down.

func TestSameVersionTemplateDropsPrefetch(t *testing.T) {
	t.Parallel()

	base := Template{
		Version:  CurrentVersion,
		Template: TemplateMetadata{BuildID: "build-1"},
		Context:  Context{User: "root"},
		Prefetch: &Prefetch{Memory: &MemoryPrefetchMapping{Indices: []uint64{1, 2, 3}}},
	}

	same := base.SameVersionTemplate(TemplateMetadata{BuildID: "build-2"})

	assert.Nil(t, same.Prefetch, "SameVersionTemplate must drop the prefetch mapping")
	assert.Equal(t, "build-2", same.Template.BuildID)
	assert.Equal(t, base.Version, same.Version)
	assert.Equal(t, base.Context, same.Context)
}

func TestWithPrefetchSetsMappingAndPreservesFields(t *testing.T) {
	t.Parallel()

	base := Template{
		Version:   CurrentVersion,
		Template:  TemplateMetadata{BuildID: "build-1"},
		Context:   Context{User: "root"},
		FromImage: new("ubuntu:22.04"),
	}

	mapping := &MemoryPrefetchMapping{Indices: []uint64{4, 5}}
	got := base.WithPrefetch(&Prefetch{Memory: mapping})

	if assert.NotNil(t, got.Prefetch) {
		assert.Same(t, mapping, got.Prefetch.Memory, "the mapping must be carried through verbatim")
	}
	// Every other field is preserved unchanged.
	assert.Equal(t, base.Template.BuildID, got.Template.BuildID)
	assert.Equal(t, base.Context, got.Context)
	assert.Equal(t, base.FromImage, got.FromImage)
	// The value receiver returns a copy: the original is not mutated.
	assert.Nil(t, base.Prefetch)
}

// TestPrefetchSurvivesMetadataFileRoundTrip is the persist→resume link the
// consume path depends on: the harvest writes the mapping via ReplaceFile/UploadMetadata
// and the resume reads it via FromFile. If Prefetch did not survive JSON
// serialization (e.g. a missing/renamed tag), the whole feature would silently
// no-op and every in-memory test above would still pass — so assert the round trip.
func TestPrefetchSurvivesMetadataFileRoundTrip(t *testing.T) {
	t.Parallel()

	base := Template{
		Version:  CurrentVersion,
		Template: TemplateMetadata{BuildID: "build-rt", KernelVersion: "6.1", FirecrackerVersion: "1.14"},
		Context:  Context{User: "root"},
	}
	updated := base.WithPrefetch(&Prefetch{Memory: &MemoryPrefetchMapping{
		Indices:     []uint64{3, 1, 2},
		AccessTypes: []AccessType{"r", "w", "p"},
		BlockSize:   2 << 20,
	}})

	path := filepath.Join(t.TempDir(), "metadata.json")
	require.NoError(t, base.ToFile(path))
	require.NoError(t, updated.ReplaceFile(path))

	got, err := FromFile(path)
	require.NoError(t, err)

	require.NotNil(t, got.Prefetch, "Prefetch must survive ToFile/FromFile")
	require.NotNil(t, got.Prefetch.Memory)
	assert.Equal(t, updated.Prefetch.Memory.Indices, got.Prefetch.Memory.Indices, "ordered indices must be preserved")
	assert.Equal(t, updated.Prefetch.Memory.AccessTypes, got.Prefetch.Memory.AccessTypes)
	assert.Equal(t, updated.Prefetch.Memory.BlockSize, got.Prefetch.Memory.BlockSize)
}

func TestMemoryPrefetchMappingCount(t *testing.T) {
	t.Parallel()

	var nilMapping *MemoryPrefetchMapping
	assert.Equal(t, 0, nilMapping.Count(), "nil mapping counts as zero")
	assert.Equal(t, 0, (&MemoryPrefetchMapping{}).Count())
	assert.Equal(t, 3, (&MemoryPrefetchMapping{Indices: []uint64{1, 2, 3}}).Count())
}

// The origin rides with the mapping through the file, and a mapping written
// before origins existed reads back as unknown rather than as something else.
func TestPrefetchOrigin_FileRoundTrip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "metadata.json")
	stamped := Template{Version: CurrentVersion}.WithPrefetch(&Prefetch{Memory: &MemoryPrefetchMapping{Indices: []uint64{1}}, Origin: PrefetchOriginHarvest})
	require.NoError(t, stamped.ToFile(path))
	got, err := FromFile(path)
	require.NoError(t, err)
	assert.Equal(t, PrefetchOriginHarvest, got.Prefetch.Origin)
	assert.Equal(t, "harvest", got.Prefetch.InitOrigin())

	legacy := Template{Version: CurrentVersion}.WithPrefetch(&Prefetch{Memory: &MemoryPrefetchMapping{Indices: []uint64{1}}})
	require.NoError(t, legacy.ReplaceFile(path))
	got, err = FromFile(path)
	require.NoError(t, err)
	assert.Empty(t, got.Prefetch.Origin)
	assert.Equal(t, "unknown", got.Prefetch.InitOrigin())
	assert.Equal(t, "none", (*Prefetch)(nil).InitOrigin())
}
