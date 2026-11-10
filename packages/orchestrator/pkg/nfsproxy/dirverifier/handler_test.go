package dirverifier

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-git/go-billy/v5/memfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willscott/go-nfs"
)

// newVolume creates a volume holding src/main.go of the given size.
func newVolume(t *testing.T, size int) string {
	t.Helper()

	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "src"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "src", "main.go"), make([]byte, size), 0o644))

	return root
}

func list(t *testing.T, h nfs.CachingHandler, volume string, path string) uint64 {
	t.Helper()

	entries, err := os.ReadDir(filepath.Join(volume, path))
	require.NoError(t, err)

	contents := make([]fs.FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		require.NoError(t, err)
		contents = append(contents, info)
	}

	return h.VerifierFor(path, contents)
}

func TestVolumesWithTheSameTreeDoNotShareListings(t *testing.T) {
	t.Parallel()

	h, err := Wrap(nil, 16)
	require.NoError(t, err)

	volumeA, volumeB := newVolume(t, 10), newVolume(t, 20)
	verifierA := list(t, h, volumeA, "src")
	verifierB := list(t, h, volumeB, "src")

	assert.NotEqual(t, verifierA, verifierB)

	pageA := h.DataForVerifier("src", verifierA)
	require.Len(t, pageA, 1)
	assert.Equal(t, int64(10), pageA[0].Size())

	pageB := h.DataForVerifier("src", verifierB)
	require.Len(t, pageB, 1)
	assert.Equal(t, int64(20), pageB[0].Size())
}

// A listing that fell out of the cache is read again; the verifier must come
// out the same or the client's next page fails with NFS3ERR_BAD_COOKIE.
func TestVerifierIsStableAcrossEviction(t *testing.T) {
	t.Parallel()

	h, err := Wrap(nil, 1)
	require.NoError(t, err)

	volume := newVolume(t, 10)
	before := list(t, h, volume, "src")
	list(t, h, volume, ".")
	assert.Nil(t, h.DataForVerifier("src", before), "listing should have been evicted")

	after := list(t, h, volume, "src")
	assert.Equal(t, before, after)
}

func TestDataForVerifierMisses(t *testing.T) {
	t.Parallel()

	t.Run("other path", func(t *testing.T) {
		t.Parallel()

		h, err := Wrap(nil, 16)
		require.NoError(t, err)

		verifier := list(t, h, newVolume(t, 10), "src")
		assert.Nil(t, h.DataForVerifier("other", verifier))
	})

	t.Run("entries without an inode", func(t *testing.T) {
		t.Parallel()

		h, err := Wrap(nil, 16)
		require.NoError(t, err)

		volume := memfs.New()
		require.NoError(t, volume.MkdirAll("src/pkg", 0o755))
		contents, err := volume.ReadDir("src")
		require.NoError(t, err)

		verifier := h.VerifierFor("src", contents)
		assert.NotZero(t, verifier)
		assert.Nil(t, h.DataForVerifier("src", verifier))
	})
}

func TestWrapRejectsInvalidLimit(t *testing.T) {
	t.Parallel()

	_, err := Wrap(nil, 0)
	assert.Error(t, err)
}
