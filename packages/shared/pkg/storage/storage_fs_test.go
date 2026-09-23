package storage

import (
	"context"
	"errors"
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

func TestFSPutReplacesExistingObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		initial     string
		replacement string
	}{
		{"shorter", "a longer initial payload", "short"},
		{"longer", "short", "a longer replacement payload"},
		{"same_length", "first", "other"},
		{"empty", "a non-empty payload", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := newTempProvider(t)
			ctx := t.Context()

			obj, err := p.OpenBlob(ctx, filepath.Join("overwrite", tc.name+".txt"))
			require.NoError(t, err)

			require.NoError(t, obj.Put(ctx, []byte(tc.initial)))
			require.NoError(t, obj.Put(ctx, []byte(tc.replacement)))

			seekable, ok := obj.(Seekable)
			require.True(t, ok)
			size, err := seekable.Size(ctx)
			require.NoError(t, err)
			require.EqualValues(t, len(tc.replacement), size)

			data, err := GetBlob(ctx, obj)
			require.NoError(t, err)
			require.Equal(t, []byte(tc.replacement), data)
		})
	}
}

func TestFSStoreFileReplacesExistingObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		initial     string
		replacement string
	}{
		{"shorter", "a longer initial payload", "short"},
		{"longer", "short", "a longer replacement payload"},
		{"same_length", "first", "other"},
		{"empty", "a non-empty payload", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := newTempProvider(t)
			ctx := t.Context()

			srcPath := filepath.Join(t.TempDir(), "src.txt")
			require.NoError(t, os.WriteFile(srcPath, []byte(tc.initial), 0o600))

			obj, err := p.OpenSeekable(ctx, filepath.Join("overwrite", tc.name+".txt"))
			require.NoError(t, err)

			_, _, err = obj.StoreFile(ctx, srcPath)
			require.NoError(t, err)

			require.NoError(t, os.WriteFile(srcPath, []byte(tc.replacement), 0o600))
			_, _, err = obj.StoreFile(ctx, srcPath)
			require.NoError(t, err)

			size, err := obj.Size(ctx)
			require.NoError(t, err)
			require.EqualValues(t, len(tc.replacement), size)

			blob, ok := obj.(Blob)
			require.True(t, ok)
			data, err := GetBlob(ctx, blob)
			require.NoError(t, err)
			require.Equal(t, []byte(tc.replacement), data)
		})
	}
}

func TestFSStoreFileSameInodeDoesNotEmptyObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source func(t *testing.T, objectPath string) string
	}{
		{
			name: "same_path",
			source: func(t *testing.T, objectPath string) string {
				t.Helper()

				return objectPath
			},
		},
		{
			name: "hard_link",
			source: func(t *testing.T, objectPath string) string {
				t.Helper()

				linkPath := filepath.Join(t.TempDir(), "link.txt")
				require.NoError(t, os.Link(objectPath, linkPath))

				return linkPath
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := newTempProvider(t)
			ctx := t.Context()

			objectKey := filepath.Join("overwrite", "same-inode-"+tc.name+".txt")
			obj, err := p.OpenSeekable(ctx, objectKey)
			require.NoError(t, err)

			payload := []byte("must survive a same-inode store")
			srcPath := filepath.Join(t.TempDir(), "src.txt")
			require.NoError(t, os.WriteFile(srcPath, payload, 0o600))
			_, _, err = obj.StoreFile(ctx, srcPath)
			require.NoError(t, err)

			_, _, err = obj.StoreFile(ctx, tc.source(t, p.getPath(objectKey)))
			require.NoError(t, err)

			size, err := obj.Size(ctx)
			require.NoError(t, err)
			require.EqualValues(t, len(payload), size)

			blob, ok := obj.(Blob)
			require.True(t, ok)
			data, err := GetBlob(ctx, blob)
			require.NoError(t, err)
			require.Equal(t, payload, data)
		})
	}
}

type errAfterReader struct {
	prefix []byte
	err    error
	sent   bool
}

func (r *errAfterReader) Read(p []byte) (int, error) {
	if r.sent {
		return 0, r.err
	}

	r.sent = true

	return copy(p, r.prefix), r.err
}

func TestFSReplaceFailureLeavesExistingObject(t *testing.T) {
	t.Parallel()
	p := newTempProvider(t)
	ctx := t.Context()

	obj, err := p.OpenBlob(ctx, "keep.txt")
	require.NoError(t, err)
	require.NoError(t, obj.Put(ctx, []byte("keep me")))

	fsObj, ok := obj.(*fsObject)
	require.True(t, ok)

	_, err = fsObj.replaceFrom(&errAfterReader{prefix: []byte("xx"), err: errors.New("nope")})
	require.Error(t, err)

	data, err := GetBlob(ctx, obj)
	require.NoError(t, err)
	require.Equal(t, []byte("keep me"), data)
}

func TestFSUncompressedReplacementClearsCompressedSizeSidecar(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		replace func(t *testing.T, ctx context.Context, obj Seekable, replacement []byte)
	}{
		{
			name: "put",
			replace: func(t *testing.T, ctx context.Context, obj Seekable, replacement []byte) {
				t.Helper()

				blob, ok := obj.(Blob)
				require.True(t, ok)
				require.NoError(t, blob.Put(ctx, replacement))
			},
		},
		{
			name: "store_file",
			replace: func(t *testing.T, ctx context.Context, obj Seekable, replacement []byte) {
				t.Helper()

				replacementPath := filepath.Join(t.TempDir(), "replacement.txt")
				require.NoError(t, os.WriteFile(replacementPath, replacement, 0o600))
				_, _, err := obj.StoreFile(ctx, replacementPath)
				require.NoError(t, err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := newTempProvider(t)
			ctx := t.Context()

			objectPath := filepath.Join("overwrite", "compressed-object-"+tc.name)
			obj, err := p.OpenSeekable(ctx, objectPath)
			require.NoError(t, err)

			compressedSrcPath := filepath.Join(t.TempDir(), "compressed-src.txt")
			compressedPayload := []byte("a longer payload written through the compressed filesystem path")
			require.NoError(t, os.WriteFile(compressedSrcPath, compressedPayload, 0o600))

			compression := CompressConfig{
				Enabled:            true,
				Type:               CompressionLZ4.String(),
				FrameSizeKB:        1,
				MinPartSizeMB:      1,
				FrameEncodeWorkers: 1,
			}
			_, _, err = obj.StoreFile(ctx, compressedSrcPath, WithCompressConfig(compression))
			require.NoError(t, err)

			sidecarPath := SizeSidecar(p.getPath(objectPath))
			_, err = os.Stat(sidecarPath)
			require.NoError(t, err)

			replacement := []byte("short")
			tc.replace(t, ctx, obj, replacement)

			size, err := obj.Size(ctx)
			require.NoError(t, err)
			require.EqualValues(t, len(replacement), size)

			blob, ok := obj.(Blob)
			require.True(t, ok)
			data, err := GetBlob(ctx, blob)
			require.NoError(t, err)
			require.Equal(t, replacement, data)

			_, err = os.Stat(sidecarPath)
			require.True(t, os.IsNotExist(err))
		})
	}
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
