//go:build linux

package peerserver

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	templatemocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/mocks"
	peerservermocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerserver/mocks"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

func TestFileSource_Exists_FileOnDisk(t *testing.T) {
	t.Parallel()

	path := writeTempFile(t, "blob content")

	f := templatemocks.NewMockFile(t)
	f.EXPECT().Path().Return(path)

	tmplMock := templatemocks.NewMockTemplate(t)
	tmplMock.EXPECT().Snapfile().Return(f, nil)

	cache := peerservermocks.NewMockCache(t)
	cache.EXPECT().LookupPinned(mock.Anything, "build-1").Return(tmplMock, func() {}, true)

	src, release, err := ResolveBlob(t.Context(), cache, "build-1", storage.SnapfileName)
	defer release()
	require.NoError(t, err)

	exists, err := src.Exists(t.Context())
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestFileSource_Exists_FileNotOnDisk(t *testing.T) {
	t.Parallel()

	f := templatemocks.NewMockFile(t)
	f.EXPECT().Path().Return(filepath.Join(t.TempDir(), "missing"))

	tmplMock := templatemocks.NewMockTemplate(t)
	tmplMock.EXPECT().Snapfile().Return(f, nil)

	cache := peerservermocks.NewMockCache(t)
	cache.EXPECT().LookupPinned(mock.Anything, "build-1").Return(tmplMock, func() {}, true)

	src, release, err := ResolveBlob(t.Context(), cache, "build-1", storage.SnapfileName)
	defer release()
	require.NoError(t, err)

	exists, err := src.Exists(t.Context())
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestFileSource_Stream_FileOnDisk(t *testing.T) {
	t.Parallel()

	content := "snapfile content"
	path := writeTempFile(t, content)

	f := templatemocks.NewMockFile(t)
	f.EXPECT().Path().Return(path)

	tmplMock := templatemocks.NewMockTemplate(t)
	tmplMock.EXPECT().Snapfile().Return(f, nil)

	cache := peerservermocks.NewMockCache(t)
	cache.EXPECT().LookupPinned(mock.Anything, "build-1").Return(tmplMock, func() {}, true)

	src, release, err := ResolveBlob(t.Context(), cache, "build-1", storage.SnapfileName)
	defer release()
	require.NoError(t, err)

	sender := &collectSender{}

	require.NoError(t, src.Stream(t.Context(), sender))
	assert.Equal(t, content, string(sender.data))
}

// A snapfile larger than the bound has to reach the peer as several bounded
// messages. This covers chunkWriter's call site, which the in-memory sources
// bypass.
func TestFileSource_Stream_ChunksLargeFile(t *testing.T) {
	t.Parallel()

	content := bytes.Repeat([]byte("s"), 2*sendChunkSize+7)
	path := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(path, content, 0o600))

	f := templatemocks.NewMockFile(t)
	f.EXPECT().Path().Return(path)

	tmplMock := templatemocks.NewMockTemplate(t)
	tmplMock.EXPECT().Snapfile().Return(f, nil)

	cache := peerservermocks.NewMockCache(t)
	cache.EXPECT().LookupPinned(mock.Anything, "build-1").Return(tmplMock, func() {}, true)

	src, release, err := ResolveBlob(t.Context(), cache, "build-1", storage.SnapfileName)
	defer release()
	require.NoError(t, err)

	sender := &collectSender{}
	require.NoError(t, src.Stream(t.Context(), sender))

	assert.Equal(t, []int{sendChunkSize, sendChunkSize, 7}, sender.sends)
	assert.Equal(t, content, sender.data)
}

func TestFileSource_Stream_FileNotOnDisk(t *testing.T) {
	t.Parallel()

	f := templatemocks.NewMockFile(t)
	f.EXPECT().Path().Return(filepath.Join(t.TempDir(), "missing"))

	tmplMock := templatemocks.NewMockTemplate(t)
	tmplMock.EXPECT().Snapfile().Return(f, nil)

	cache := peerservermocks.NewMockCache(t)
	cache.EXPECT().LookupPinned(mock.Anything, "build-1").Return(tmplMock, func() {}, true)

	src, release, err := ResolveBlob(t.Context(), cache, "build-1", storage.SnapfileName)
	defer release()
	require.NoError(t, err)

	err = src.Stream(t.Context(), &collectSender{})
	assert.ErrorIs(t, err, ErrNotAvailable)
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	return path
}
