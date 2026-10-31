package chroot

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRemoveNFSOrphans verifies that removeNFSOrphans deletes NFS silly-rename
// orphans (.nfs*) in the volume directory while leaving real files, and that a
// missing directory is a no-op rather than an error.
func TestRemoveNFSOrphans(t *testing.T) {
	t.Parallel()

	h := &NFSHandler{}
	ctx := context.Background()

	t.Run("removes .nfs* and keeps real files", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()

		orphans := []string{
			".nfs000000019000ee9800000001",
			".nfs0000000a",
			".nfsZZZ",
		}
		keep := []string{
			"ws_123.img",
			"data.bin",
			".hidden",        // dotfile but not .nfs
			".nfs",           // exactly ".nfs" has the prefix -> treated as orphan? HasPrefix(".nfs",".nfs")=true
		}
		// ".nfs" exactly matches the prefix and is removed; adjust expectations.
		for _, n := range append(append([]string{}, orphans...), keep...) {
			require.NoError(t, os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o644))
		}
		// a subdirectory named like an orphan must NOT be removed (IsDir guard)
		require.NoError(t, os.Mkdir(filepath.Join(dir, ".nfsdir"), 0o755))

		h.removeNFSOrphans(ctx, "sbx-test", dir)

		got, err := os.ReadDir(dir)
		require.NoError(t, err)
		var names []string
		for _, e := range got {
			names = append(names, e.Name())
		}
		sort.Strings(names)

		want := []string{".hidden", ".nfsdir", "data.bin", "ws_123.img"}
		sort.Strings(want)
		require.Equal(t, want, names,
			"only non-.nfs* files and the .nfsdir subdirectory must remain")
	})

	t.Run("missing directory is a no-op", func(t *testing.T) {
		t.Parallel()
		require.NotPanics(t, func() {
			h.removeNFSOrphans(ctx, "sbx-test", filepath.Join(t.TempDir(), "does-not-exist"))
		})
	})

	t.Run("empty directory is a no-op", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		h.removeNFSOrphans(ctx, "sbx-test", dir)
		got, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Empty(t, got)
	})
}
