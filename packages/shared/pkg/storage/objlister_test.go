package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFSStorage_ListObjects(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	s := newFileSystemStorage(base, "", nil)

	// Two build prefixes, one with two objects.
	require.NoError(t, os.MkdirAll(filepath.Join(base, "build-a"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(base, "build-b"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(base, "build-a", "memfile"), []byte("hello"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(base, "build-a", "rootfs.ext4.header"), []byte("hdr"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(base, "build-b", "memfile"), []byte("x"), 0o644))

	objs, err := s.ListObjects(context.Background(), "")
	require.NoError(t, err)

	paths := make(map[string]int64)
	for _, o := range objs {
		paths[o.Path] = o.Size
		assert.False(t, o.LastModified.IsZero(), "LastModified must be populated for grace checks")
	}

	assert.Equal(t, int64(5), paths["build-a/memfile"])
	assert.Equal(t, int64(3), paths["build-a/rootfs.ext4.header"])
	assert.Equal(t, int64(1), paths["build-b/memfile"])
	assert.Len(t, objs, 3)
}

func TestFSStorage_ListObjects_PrefixScoped(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	s := newFileSystemStorage(base, "", nil)
	require.NoError(t, os.MkdirAll(filepath.Join(base, "build-a"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(base, "build-b"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(base, "build-a", "memfile"), []byte("a"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(base, "build-b", "memfile"), []byte("b"), 0o644))

	objs, err := s.ListObjects(context.Background(), "build-a")
	require.NoError(t, err)
	require.Len(t, objs, 1)
	assert.Equal(t, "build-a/memfile", objs[0].Path)
}

// ListObjects helper returns ErrListUnsupported for a provider without the
// capability, so callers fail closed instead of seeing an empty bucket.
func TestListObjects_UnsupportedFailsClosed(t *testing.T) {
	t.Parallel()

	_, err := ListObjects(context.Background(), noListProvider{}, "")
	assert.ErrorIs(t, err, ErrListUnsupported)
}

type noListProvider struct{ StorageProvider }
