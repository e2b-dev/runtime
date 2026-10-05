//go:build linux

package peerserver

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	templatemocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/mocks"
	peerservermocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerserver/mocks"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

func TestMetadataSource_Stream(t *testing.T) {
	t.Parallel()

	tmplMock := templatemocks.NewMockTemplate(t)
	tmplMock.EXPECT().Metadata().Return(metadata.Template{
		Template: metadata.TemplateMetadata{BuildID: "build-1"},
	}, nil)

	cache := peerservermocks.NewMockCache(t)
	cache.EXPECT().LookupPinned(mock.Anything, "build-1").Return(tmplMock, func() {}, true)

	src, release, err := ResolveBlob(t.Context(), cache, "build-1", storage.MetadataName)
	defer release()
	require.NoError(t, err)

	sender := &collectSender{}

	require.NoError(t, src.Stream(t.Context(), sender))
	assert.Contains(t, string(sender.data), "build-1")
}

// A prefetch mapping holds one entry per memory block touched during a resume,
// so metadata grows without bound and reaches the same per-message limit the
// header does. Unlike the header fixture this one scales linearly with the
// entry count, so it is sized from the bound and stays valid whatever
// sendChunkSize is set to.
func TestMetadataSource_Stream_ChunksOversizedMetadata(t *testing.T) {
	t.Parallel()

	tmplMock := templatemocks.NewMockTemplate(t)
	tmplMock.EXPECT().Metadata().Return(oversizedMetadata(), nil)

	cache := peerservermocks.NewMockCache(t)
	cache.EXPECT().LookupPinned(mock.Anything, "build-1").Return(tmplMock, func() {}, true)

	src, release, err := ResolveBlob(t.Context(), cache, "build-1", storage.MetadataName)
	defer release()
	require.NoError(t, err)

	sender := &collectSender{}
	require.NoError(t, src.Stream(t.Context(), sender))

	require.Greater(t, len(sender.data), sendChunkSize,
		"fixture is too small to exercise chunking")
	assert.Greater(t, len(sender.sends), 1)
	for i, n := range sender.sends {
		assert.LessOrEqual(t, n, sendChunkSize, "send %d exceeds the per-message bound", i)
	}

	// The split must be transparent: what the peer reassembles is the metadata.
	var got metadata.Template
	require.NoError(t, json.Unmarshal(sender.data, &got))
	assert.Equal(t, "build-1", got.Template.BuildID)
	assert.Equal(t, prefetchEntriesOverBound, got.Prefetch.Memory.Count())
}

// prefetchEntriesOverBound is the entry count needed to serialize past
// sendChunkSize. Each entry contributes an index and an access type; both
// encode to several bytes of JSON, so one entry per two bytes of the bound is a
// comfortable margin without making the fixture wasteful.
const prefetchEntriesOverBound = sendChunkSize / 2

func oversizedMetadata() metadata.Template {
	indices := make([]uint64, prefetchEntriesOverBound)
	accessTypes := make([]metadata.AccessType, prefetchEntriesOverBound)
	for i := range indices {
		indices[i] = uint64(i)
		accessTypes[i] = metadata.AccessRead
	}

	return metadata.Template{
		Template: metadata.TemplateMetadata{BuildID: "build-1"},
		Prefetch: &metadata.Prefetch{
			Memory: &metadata.MemoryPrefetchMapping{
				Indices:     indices,
				AccessTypes: accessTypes,
				BlockSize:   4096,
			},
		},
	}
}
