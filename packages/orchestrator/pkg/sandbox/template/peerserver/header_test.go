//go:build linux

package peerserver

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	blockmocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/mocks"
	templatemocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/mocks"
	peerservermocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerserver/mocks"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	storageheader "github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// durableDevice is a ReadonlyDevice that also exposes DurableHeader, like the
// real *template.Storage. Header() stands in for the live (provisional) header,
// DurableHeader for the deduped one a pause/peer must use instead.
type durableDevice struct {
	*blockmocks.MockReadonlyDevice

	durable    *storageheader.Header
	durableErr error
}

func (d durableDevice) DurableHeader(context.Context) (*storageheader.Header, error) {
	return d.durable, d.durableErr
}

func TestHeaderSource_Stream(t *testing.T) {
	t.Parallel()

	h, err := storageheader.NewHeader(&storageheader.Metadata{Version: 3, BlockSize: 4096, Size: 4096}, nil)
	require.NoError(t, err)

	dev := blockmocks.NewMockReadonlyDevice(t)
	dev.EXPECT().Header().Return(h)

	tmplMock := templatemocks.NewMockTemplate(t)
	tmplMock.EXPECT().Memfile(mock.Anything).Return(dev, nil)

	cache := peerservermocks.NewMockCache(t)
	cache.EXPECT().LookupPinned(mock.Anything, "build-1").Return(tmplMock, func() {}, true)

	src, release, err := ResolveBlob(t.Context(), cache, "build-1", storage.MemfileName+storage.HeaderSuffix)
	defer release()
	require.NoError(t, err)

	sender := &collectSender{}

	require.NoError(t, src.Stream(t.Context(), sender))
	assert.NotEmpty(t, sender.data)
}

func TestHeaderSource_Stream_NilHeader(t *testing.T) {
	t.Parallel()

	dev := blockmocks.NewMockReadonlyDevice(t)
	dev.EXPECT().Header().Return(nil)

	tmplMock := templatemocks.NewMockTemplate(t)
	tmplMock.EXPECT().Memfile(mock.Anything).Return(dev, nil)

	cache := peerservermocks.NewMockCache(t)
	cache.EXPECT().LookupPinned(mock.Anything, "build-1").Return(tmplMock, func() {}, true)

	src, release, err := ResolveBlob(t.Context(), cache, "build-1", storage.MemfileName+storage.HeaderSuffix)
	defer release()
	require.NoError(t, err)

	err = src.Stream(t.Context(), &collectSender{})
	assert.ErrorIs(t, err, ErrNotAvailable)
}

// When the device exposes DurableHeader, Stream must serve that (the deduped
// header) rather than the live one, so a peer never receives provisional
// synthetic-build-id mappings it can't resolve. The live Header() returns nil
// here, so the stream succeeds only if DurableHeader is used.
func TestHeaderSource_Stream_ServesDurableHeader(t *testing.T) {
	t.Parallel()

	deduped, err := storageheader.NewHeader(&storageheader.Metadata{Version: 3, BlockSize: 4096, Size: 4096}, nil)
	require.NoError(t, err)

	mockDev := blockmocks.NewMockReadonlyDevice(t)
	mockDev.EXPECT().Header().Return(nil).Maybe()
	dev := durableDevice{MockReadonlyDevice: mockDev, durable: deduped}

	tmplMock := templatemocks.NewMockTemplate(t)
	tmplMock.EXPECT().Memfile(mock.Anything).Return(dev, nil)

	cache := peerservermocks.NewMockCache(t)
	cache.EXPECT().LookupPinned(mock.Anything, "build-1").Return(tmplMock, func() {}, true)

	src, release, err := ResolveBlob(t.Context(), cache, "build-1", storage.MemfileName+storage.HeaderSuffix)
	defer release()
	require.NoError(t, err)

	sender := &collectSender{}
	require.NoError(t, src.Stream(t.Context(), sender))
	assert.NotEmpty(t, sender.data)
}

// A header on a heavily fragmented build serializes past the transport's
// per-message limit. It must reach the peer as several bounded messages that
// reassemble into the original, not as one oversized message the receiver
// refuses.
func TestHeaderSource_Stream_ChunksOversizedHeader(t *testing.T) {
	t.Parallel()

	h := oversizedHeader(t)

	dev := blockmocks.NewMockReadonlyDevice(t)
	dev.EXPECT().Header().Return(h)

	tmplMock := templatemocks.NewMockTemplate(t)
	tmplMock.EXPECT().Memfile(mock.Anything).Return(dev, nil)

	cache := peerservermocks.NewMockCache(t)
	cache.EXPECT().LookupPinned(mock.Anything, "build-1").Return(tmplMock, func() {}, true)

	src, release, err := ResolveBlob(t.Context(), cache, "build-1", storage.MemfileName+storage.HeaderSuffix)
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

	// The split must be transparent: what the peer reassembles is the header.
	got, err := storageheader.DeserializeBytes(sender.data)
	require.NoError(t, err)
	assert.Equal(t, h.Mapping.Len(), got.Mapping.Len())
	assert.Equal(t, h.Metadata.Size, got.Metadata.Size)
}

// oversizedHeader builds a header that serializes past sendChunkSize. The V5
// mapping section is LZ4-compressed, so uniform mappings collapse to almost
// nothing; distinct random build IDs are what make it incompressible, and the
// build-ID table is the bulk of the result.
func oversizedHeader(t *testing.T) *storageheader.Header {
	t.Helper()

	const (
		blockSize = storageheader.PageSize
		entries   = 65535 // the per-header cap on distinct build IDs
	)

	mappings := make([]storageheader.BuildMap, entries)
	for i := range mappings {
		mappings[i] = storageheader.BuildMap{
			Offset:             uint64(i) * blockSize,
			Length:             blockSize,
			BuildId:            uuid.New(),
			BuildStorageOffset: uint64(i) * blockSize,
		}
	}

	h, err := storageheader.NewHeader(&storageheader.Metadata{
		Version:   3,
		BlockSize: blockSize,
		Size:      uint64(entries) * blockSize,
	}, mappings)
	require.NoError(t, err)

	return h
}

func TestHeaderSource_Stream_Rootfs(t *testing.T) {
	t.Parallel()

	h, err := storageheader.NewHeader(&storageheader.Metadata{Version: 3, BlockSize: 4096, Size: 4096}, nil)
	require.NoError(t, err)

	dev := blockmocks.NewMockReadonlyDevice(t)
	dev.EXPECT().Header().Return(h)

	tmplMock := templatemocks.NewMockTemplate(t)
	tmplMock.EXPECT().Rootfs().Return(dev, nil)

	cache := peerservermocks.NewMockCache(t)
	cache.EXPECT().LookupPinned(mock.Anything, "build-1").Return(tmplMock, func() {}, true)

	src, release, err := ResolveBlob(t.Context(), cache, "build-1", storage.RootfsName+storage.HeaderSuffix)
	defer release()
	require.NoError(t, err)

	sender := &collectSender{}

	require.NoError(t, src.Stream(t.Context(), sender))
	assert.NotEmpty(t, sender.data)
}
