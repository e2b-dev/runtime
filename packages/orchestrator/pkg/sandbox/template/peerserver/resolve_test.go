//go:build linux

package peerserver

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	templatemocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/mocks"
	peerservermocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerserver/mocks"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

func TestResolveSeekable_ReturnsErrNotAvailableWhenNotInCache(t *testing.T) {
	t.Parallel()

	for _, fileName := range []string{
		storage.MemfileName,
		storage.RootfsName,
	} {
		t.Run(fileName, func(t *testing.T) {
			t.Parallel()

			cache := peerservermocks.NewMockCache(t)
			cache.EXPECT().LookupDiff(mock.Anything, mock.Anything).Return(nil, false)

			_, err := ResolveSeekable(cache, "build-1", fileName)
			assert.ErrorIs(t, err, ErrNotAvailable)
		})
	}
}

func TestResolveSeekable_ReturnsErrorForUnknownFile(t *testing.T) {
	t.Parallel()

	cache := peerservermocks.NewMockCache(t)
	_, err := ResolveSeekable(cache, "build-1", "unknown.file")
	assert.ErrorIs(t, err, ErrUnknownFile)
}

func TestResolveBlob_ReturnsErrNotAvailableWhenNotInCache(t *testing.T) {
	t.Parallel()

	for _, fileName := range []string{
		storage.SnapfileName,
		storage.MetadataName,
		storage.MemfileName + storage.HeaderSuffix,
		storage.RootfsName + storage.HeaderSuffix,
	} {
		t.Run(fileName, func(t *testing.T) {
			t.Parallel()

			cache := peerservermocks.NewMockCache(t)
			cache.EXPECT().LookupPinned(mock.Anything, mock.Anything).Return(nil, func() {}, false)

			_, release, err := ResolveBlob(t.Context(), cache, "build-1", fileName)
			defer release()
			assert.ErrorIs(t, err, ErrNotAvailable)
		})
	}
}

func TestResolveBlob_ReturnsErrorForUnknownFile(t *testing.T) {
	t.Parallel()

	cache := peerservermocks.NewMockCache(t)
	cache.EXPECT().LookupPinned(mock.Anything, "build-1").Return(nil, func() {}, true)

	_, release, err := ResolveBlob(t.Context(), cache, "build-1", "unknown.file")
	defer release()
	assert.ErrorIs(t, err, ErrUnknownFile)
}

// countedPin hands out a pin release whose calls are counted, so a test can
// see whether a path returned the pin, and how many times.
func countedPin() (func(), *atomic.Int64) {
	var outstanding atomic.Int64
	outstanding.Add(1)

	return func() { outstanding.Add(-1) }, &outstanding
}

// Every served file hands the template pin to the caller, who returns it once
// the stream ends; the entry stays pinned while the source is read.
func TestResolveBlob_HandsThePinToTheCaller(t *testing.T) {
	t.Parallel()

	for _, fileName := range []string{
		storage.SnapfileName,
		storage.MetadataName,
		storage.MemfileName + storage.HeaderSuffix,
		storage.RootfsName + storage.HeaderSuffix,
	} {
		t.Run(fileName, func(t *testing.T) {
			t.Parallel()

			pin, outstanding := countedPin()
			cache := peerservermocks.NewMockCache(t)
			cache.EXPECT().LookupPinned(mock.Anything, "build-1").Return(templatemocks.NewMockTemplate(t), pin, true)

			src, release, err := ResolveBlob(t.Context(), cache, "build-1", fileName)
			require.NoError(t, err)
			require.NotNil(t, src)
			assert.Equal(t, int64(1), outstanding.Load(), "the source is read under the pin")

			release()
			assert.Zero(t, outstanding.Load())
		})
	}
}

// A name no source serves returns the pin before ResolveBlob returns, and the
// release it hands back is a no-op, so the caller's deferred call cannot
// return it a second time.
func TestResolveBlob_UnknownFileReturnsThePinItself(t *testing.T) {
	t.Parallel()

	pin, outstanding := countedPin()
	cache := peerservermocks.NewMockCache(t)
	cache.EXPECT().LookupPinned(mock.Anything, "build-1").Return(templatemocks.NewMockTemplate(t), pin, true)

	_, release, err := ResolveBlob(t.Context(), cache, "build-1", "unknown.file")
	require.ErrorIs(t, err, ErrUnknownFile)
	assert.Zero(t, outstanding.Load())

	release()
	assert.Zero(t, outstanding.Load())
}
