package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helper to create a FileSystemStorageProvider rooted in a temp directory.
func newTempProvider(t *testing.T) *fsStorage {
	t.Helper()

	base := t.TempDir()
	p := newFileSystemStorage(base, "", nil)

	return p
}

func TestOpenObject_Write_Exists_WriteTo(t *testing.T) {
	t.Parallel()
	p := newTempProvider(t)
	ctx := t.Context()

	obj, err := p.OpenBlob(ctx, filepath.Join("sub", "file.txt"))
	require.NoError(t, err)

	contents := []byte("hello world")
	// write via Write
	err = obj.Put(t.Context(), contents)
	require.NoError(t, err)

	// check Size
	exists, err := obj.Exists(t.Context())
	require.NoError(t, err)
	require.True(t, exists)

	// read the entire file back via WriteTo
	data, err := GetBlob(t.Context(), obj)
	require.NoError(t, err)
	require.Equal(t, contents, data)
}

func TestFSPut(t *testing.T) {
	t.Parallel()
	p := newTempProvider(t)
	ctx := t.Context()

	// create a separate source file on disk
	srcPath := filepath.Join(t.TempDir(), "src.txt")
	const payload = "copy me please"
	require.NoError(t, os.WriteFile(srcPath, []byte(payload), 0o600))

	obj, err := p.OpenBlob(ctx, "copy/dst.txt")
	require.NoError(t, err)

	require.NoError(t, obj.Put(t.Context(), []byte(payload)))

	data, err := GetBlob(t.Context(), obj)
	require.NoError(t, err)
	require.Equal(t, payload, string(data))
}

func TestFSPutTruncatesExistingObject(t *testing.T) {
	t.Parallel()
	p := newTempProvider(t)
	ctx := t.Context()

	obj, err := p.OpenBlob(ctx, "overwrite/dst.txt")
	require.NoError(t, err)

	require.NoError(t, obj.Put(ctx, []byte("a longer initial payload")))
	require.NoError(t, obj.Put(ctx, []byte("short")))

	seekable, ok := obj.(Seekable)
	require.True(t, ok)
	size, err := seekable.Size(ctx)
	require.NoError(t, err)
	require.EqualValues(t, len("short"), size)

	data, err := GetBlob(ctx, obj)
	require.NoError(t, err)
	require.Equal(t, []byte("short"), data)
}

func TestFSStoreFileTruncatesExistingObject(t *testing.T) {
	t.Parallel()
	p := newTempProvider(t)
	ctx := t.Context()

	srcPath := filepath.Join(t.TempDir(), "src.txt")
	require.NoError(t, os.WriteFile(srcPath, []byte("a longer initial payload"), 0o600))

	obj, err := p.OpenSeekable(ctx, "overwrite/dst.txt")
	require.NoError(t, err)

	_, _, err = obj.StoreFile(ctx, srcPath)
	require.NoError(t, err)

	const replacement = "short"
	require.NoError(t, os.WriteFile(srcPath, []byte(replacement), 0o600))
	_, _, err = obj.StoreFile(ctx, srcPath)
	require.NoError(t, err)

	size, err := obj.Size(ctx)
	require.NoError(t, err)
	require.EqualValues(t, len(replacement), size)

	blob, ok := obj.(Blob)
	require.True(t, ok)
	data, err := GetBlob(ctx, blob)
	require.NoError(t, err)
	require.Equal(t, []byte(replacement), data)
}

func TestDelete(t *testing.T) {
	t.Parallel()
	p := newTempProvider(t)
	ctx := t.Context()

	obj, err := p.OpenBlob(ctx, "to/delete.txt")
	require.NoError(t, err)

	err = obj.Put(t.Context(), []byte("bye"))
	require.NoError(t, err)

	exists, err := obj.Exists(t.Context())
	require.NoError(t, err)
	assert.True(t, exists)

	err = p.DeleteObjectsWithPrefix(t.Context(), "to/delete.txt")
	require.NoError(t, err)

	// subsequent Size call should fail with ErrorObjectNotExist
	exists, err = obj.Exists(t.Context())
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestDeleteObjectsWithPrefix(t *testing.T) {
	t.Parallel()
	p := newTempProvider(t)
	ctx := t.Context()

	paths := []string{
		"data/a.txt",
		"data/b.txt",
		"data/sub/c.txt",
	}
	for _, pth := range paths {
		obj, err := p.OpenBlob(ctx, pth)
		require.NoError(t, err)
		err = obj.Put(t.Context(), []byte("x"))
		require.NoError(t, err)
	}

	// remove the entire "data" prefix
	require.NoError(t, p.DeleteObjectsWithPrefix(ctx, "data"))

	for _, pth := range paths {
		full := filepath.Join(p.GetDetails()[len("[Local file storage, base path set to "):len(p.GetDetails())-1], pth) // derive basePath
		_, err := os.Stat(full)
		require.True(t, os.IsNotExist(err))
	}
}

func TestWriteToNonExistentObject(t *testing.T) {
	t.Parallel()
	p := newTempProvider(t)

	ctx := t.Context()
	obj, err := p.OpenBlob(ctx, "missing/file.txt")
	require.NoError(t, err)

	_, err = GetBlob(t.Context(), obj)
	require.ErrorIs(t, err, ErrObjectNotExist)
}
