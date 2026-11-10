//go:build linux

package peerserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	buildmocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build/mocks"
	peerservermocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerserver/mocks"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

func TestSeekableSource_Size(t *testing.T) {
	t.Parallel()

	diff := buildmocks.NewMockDiff(t)
	diff.EXPECT().Size(mock.Anything).Return(int64(1234), nil)

	cache := peerservermocks.NewMockCache(t)
	cache.EXPECT().LookupDiff("build-1", build.DiffType(storage.MemfileName)).Return(diff, true)

	src, err := ResolveSeekable(cache, "build-1", storage.MemfileName)
	require.NoError(t, err)

	size, err := src.Size(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(1234), size)
}

func TestSeekableSource_Stream(t *testing.T) {
	t.Parallel()

	data := []byte("diff bytes")

	diff := buildmocks.NewMockDiff(t)
	diff.EXPECT().ReadAt(mock.Anything, mock.Anything, int64(0), (*storage.FrameTable)(nil)).
		Run(func(_ context.Context, p []byte, _ int64, _ *storage.FrameTable) { copy(p, data) }).
		Return(len(data), nil)
	diff.EXPECT().BlockSize().Return(int64(len(data)))

	cache := peerservermocks.NewMockCache(t)
	cache.EXPECT().LookupDiff("build-1", build.DiffType(storage.MemfileName)).Return(diff, true)

	src, err := ResolveSeekable(cache, "build-1", storage.MemfileName)
	require.NoError(t, err)

	sender := &collectSender{}
	err = src.Stream(t.Context(), 0, int64(len(data)), sender)
	require.NoError(t, err)
	assert.Equal(t, data, sender.data)
}
