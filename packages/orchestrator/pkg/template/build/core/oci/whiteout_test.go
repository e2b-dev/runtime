//go:build linux

package oci

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	containerregistry "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/filesystem"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func requireWhiteoutPrivileges(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("whiteout extraction requires root and CAP_MKNOD/CAP_SYS_ADMIN")
	}
}

func readWhiteoutXattr(t *testing.T, path, name string) []byte {
	t.Helper()
	value := make([]byte, 256)
	n, err := unix.Lgetxattr(path, name, value)
	require.NoError(t, err)

	return value[:n]
}

func TestMarkWhiteoutParents(t *testing.T) {
	t.Parallel()
	requireWhiteoutPrivileges(t)

	root := t.TempDir()
	outside := t.TempDir()
	for _, dir := range []string{"whiteouts", "existing", "opaque", "ordinary", "hardlink"} {
		require.NoError(t, os.Mkdir(filepath.Join(root, dir), 0o755))
	}
	for _, name := range []string{"whiteouts/one", "whiteouts/two", "existing/gone", "opaque/gone", "root-whiteout"} {
		require.NoError(t, unix.Mknod(filepath.Join(root, name), unix.S_IFCHR|0o600, 0))
	}
	require.NoError(t, os.Link(filepath.Join(root, "whiteouts/one"), filepath.Join(root, "hardlink/gone")))
	require.NoError(t, unix.Mknod(filepath.Join(root, "ordinary/null"), unix.S_IFCHR|0o600, int(unix.Mkdev(1, 3))))
	require.NoError(t, os.WriteFile(filepath.Join(root, "ordinary/empty"), nil, 0o600))
	require.NoError(t, unix.Mknod(filepath.Join(outside, "gone"), unix.S_IFCHR|0o600, 0))
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "outside")))
	require.NoError(t, os.Symlink("missing", filepath.Join(root, "dangling")))

	origin := []byte("existing-origin")
	require.NoError(t, unix.Lsetxattr(filepath.Join(root, "existing"), "trusted.overlay.origin", origin, 0))
	require.NoError(t, unix.Lsetxattr(filepath.Join(root, "opaque"), "trusted.overlay.opaque", []byte("y"), 0))
	require.NoError(t, markWhiteoutParents(t.Context(), root))
	require.NoError(t, markWhiteoutParents(t.Context(), root), "marking is idempotent")

	for _, dir := range []string{"", "whiteouts", "opaque", "hardlink"} {
		assert.Empty(t, readWhiteoutXattr(t, filepath.Join(root, dir), "trusted.overlay.origin"))
	}
	assert.Equal(t, origin, readWhiteoutXattr(t, filepath.Join(root, "existing"), "trusted.overlay.origin"))
	assert.Equal(t, []byte("y"), readWhiteoutXattr(t, filepath.Join(root, "opaque"), "trusted.overlay.opaque"))
	for _, dir := range []string{outside, filepath.Join(root, "ordinary")} {
		_, err := unix.Lgetxattr(dir, "trusted.overlay.origin", nil)
		require.ErrorIs(t, err, unix.ENODATA, "must not mark non-whiteout directories or follow links")
	}
}

func TestMarkWhiteoutParentsErrors(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, markWhiteoutParents(ctx, t.TempDir()), context.Canceled)
	require.ErrorIs(t, markWhiteoutParents(t.Context(), filepath.Join(t.TempDir(), "missing")), os.ErrNotExist)
}

func TestMarkWhiteoutParentsReadOnly(t *testing.T) {
	t.Parallel()
	requireWhiteoutPrivileges(t)
	source := t.TempDir()
	require.NoError(t, unix.Mknod(filepath.Join(source, "gone"), unix.S_IFCHR|0o600, 0))
	mount := t.TempDir()
	require.NoError(t, unix.Mount(source, mount, "", unix.MS_BIND, ""))
	t.Cleanup(func() { require.NoError(t, unix.Unmount(mount, 0)) })
	require.NoError(t, unix.Mount("", mount, "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, ""))
	require.ErrorIs(t, markWhiteoutParents(t.Context(), mount), unix.EROFS)
}

// whiteoutImage builds layers entirely in memory. Regular files contain their
// own path, making unexpected deletion or replacement observable after copying.
func whiteoutImage(t *testing.T, layers ...[]tar.Header) containerregistry.Image {
	t.Helper()
	img := empty.Image
	for _, headers := range layers {
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)
		for _, header := range headers {
			if header.Mode == 0 {
				header.Mode = 0o755
			}
			var content []byte
			if header.Typeflag == tar.TypeReg || header.Typeflag == tar.TypeRegA {
				content = []byte(header.Name)
				if strings.HasPrefix(filepath.Base(header.Name), ".wh.") {
					content = nil
				}
				header.Size = int64(len(content))
			}
			require.NoError(t, tw.WriteHeader(&header))
			_, err := tw.Write(content)
			require.NoError(t, err)
		}
		require.NoError(t, tw.Close())
		layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(buf.Bytes())), nil
		})
		require.NoError(t, err)
		img, err = mutate.AppendLayers(img, layer)
		require.NoError(t, err)
	}

	return img
}

func mountWhiteoutTestLayers(t *testing.T, layers []string) (string, func()) {
	t.Helper()
	mount := t.TempDir()
	err := filesystem.MountOverlayFS(t.Context(), layers, mount)
	legacyAllowed := os.Getenv("E2B_TEST_ALLOW_LEGACY_OVERLAY") == "1"
	if legacyAllowed && (errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP)) {
		// The production fsconfig lowerdir+ interface requires Linux >=6.8.
		// Explicitly opt in with E2B_TEST_ALLOW_LEGACY_OVERLAY=1 only for
		// component coverage on older kernels. Default tests must exercise
		// the production mount interface and propagate its errors.
		t.Logf("production mount interface unavailable; testing legacy lowerdir mount: %v", err)
		err = unix.Mount("overlay", mount, "overlay", unix.MS_RDONLY, "lowerdir="+strings.Join(layers, ":"))
	}
	require.NoError(t, err)
	mounted := true
	unmount := func() {
		if mounted {
			require.NoError(t, unix.Unmount(mount, 0))
			mounted = false
		}
	}
	t.Cleanup(unmount)

	return mount, unmount
}

func TestCreateExportCopiesWhiteoutDirectories(t *testing.T) {
	t.Parallel()
	requireWhiteoutPrivileges(t)
	_, err := exec.LookPath("rsync")
	require.NoError(t, err)

	img := whiteoutImage(t, []tar.Header{
		{Name: "base.txt"},
		{Name: "merged/", Typeflag: tar.TypeDir},
		{Name: "merged/keep"},
		{Name: "merged/deleted"},
		{Name: "opaque/", Typeflag: tar.TypeDir},
		{Name: "opaque/hidden"},
		{Name: "recreated/", Typeflag: tar.TypeDir},
		{Name: "recreated/file"},
	}, []tar.Header{
		{Name: "unique/", Typeflag: tar.TypeDir},
		{Name: "unique/.wh.absent"},
		{Name: "only-whiteouts/", Typeflag: tar.TypeDir},
		{Name: "only-whiteouts/.wh.first"},
		{Name: "only-whiteouts/.wh.second"},
		{Name: "unique/kept"},
		{Name: "unique/hardlink", Typeflag: tar.TypeLink, Linkname: "unique/kept"},
		{Name: "unique/symlink", Typeflag: tar.TypeSymlink, Linkname: "kept"},
		{Name: "same-layer/", Typeflag: tar.TypeDir},
		{Name: "same-layer/.wh.recreated"},
		{Name: "same-layer/recreated"},
		{Name: "merged/", Typeflag: tar.TypeDir},
		{Name: "merged/.wh.deleted"},
		{Name: "opaque/", Typeflag: tar.TypeDir},
		{Name: "opaque/.wh..wh..opq"},
		{Name: "opaque/.wh.absent"},
		{Name: "opaque/kept"},
		{Name: "recreated/", Typeflag: tar.TypeDir},
		{Name: "recreated/.wh.file"},
	}, []tar.Header{
		{Name: "recreated/", Typeflag: tar.TypeDir},
		{Name: "recreated/file"},
	})
	layers, err := createExport(t.Context(), logger.NewNopLogger(), img, t.TempDir())
	require.NoError(t, err)
	_, err = unix.Lgetxattr(filepath.Join(layers[1], "same-layer"), "trusted.overlay.origin", nil)
	require.ErrorIs(t, err, unix.ENODATA, "a replaced whiteout must not mark its parent")
	mount, _ := mountWhiteoutTestLayers(t, layers)
	entries, err := os.ReadDir(filepath.Join(mount, "only-whiteouts"))
	require.NoError(t, err)
	assert.Empty(t, entries, "no ghost whiteouts may be enumerated")
	dest := t.TempDir()
	require.NoError(t, copyFiles(t.Context(), mount, dest))
	for _, name := range []string{"base.txt", "unique/kept", "merged/keep", "opaque/kept", "recreated/file", "same-layer/recreated"} {
		content, err := os.ReadFile(filepath.Join(dest, name))
		require.NoError(t, err)
		assert.Equal(t, name, string(content))
	}
	for _, name := range []string{"unique/absent", "only-whiteouts/first", "only-whiteouts/second", "merged/deleted", "opaque/hidden", "opaque/absent"} {
		_, err := os.Lstat(filepath.Join(dest, name))
		require.ErrorIs(t, err, os.ErrNotExist)
	}
	kept, err := os.Stat(filepath.Join(dest, "unique/kept"))
	require.NoError(t, err)
	linked, err := os.Stat(filepath.Join(dest, "unique/hardlink"))
	require.NoError(t, err)
	assert.True(t, os.SameFile(kept, linked), "rsync must preserve hardlinks")
	target, err := os.Readlink(filepath.Join(dest, "unique/symlink"))
	require.NoError(t, err)
	assert.Equal(t, "kept", target)
}

func TestWhiteoutOriginPreventsRsyncVanishedFiles(t *testing.T) {
	t.Parallel()
	requireWhiteoutPrivileges(t)
	img := whiteoutImage(t, []tar.Header{{Name: "base.txt"}}, []tar.Header{
		{Name: "unique/", Typeflag: tar.TypeDir},
		{Name: "unique/.wh.absent"},
		{Name: "unique/kept"},
	})
	layers, err := createExport(t.Context(), logger.NewNopLogger(), img, t.TempDir())
	require.NoError(t, err)
	parent := filepath.Join(layers[0], "unique")
	require.NoError(t, unix.Lremovexattr(parent, "trusted.overlay.origin"))
	mount, unmount := mountWhiteoutTestLayers(t, layers)
	entries, err := os.ReadDir(filepath.Join(mount, "unique"))
	require.NoError(t, err)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	_, err = os.Lstat(filepath.Join(mount, "unique/absent"))
	require.ErrorIs(t, err, os.ErrNotExist)
	err = copyFiles(t.Context(), mount, t.TempDir())
	if slices.Contains(names, "absent") {
		var exitErr *exec.ExitError
		require.ErrorAs(t, err, &exitErr)
		assert.Equal(t, 24, exitErr.ExitCode(), "vanished sources must remain fatal")
	} else {
		// A kernel that filters unmarked whiteouts already must also pass.
		t.Log("kernel filters non-merged whiteouts without an origin marker")
		require.NoError(t, err)
	}
	unmount()

	require.NoError(t, markWhiteoutParents(t.Context(), layers[0]))
	mount, _ = mountWhiteoutTestLayers(t, layers)
	entries, err = os.ReadDir(filepath.Join(mount, "unique"))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "kept", entries[0].Name())
	require.NoError(t, copyFiles(t.Context(), mount, t.TempDir()))
}

func TestCopyFilesPropagatesFailure(t *testing.T) {
	t.Parallel()
	_, err := exec.LookPath("rsync")
	require.NoError(t, err)
	err = copyFiles(t.Context(), filepath.Join(t.TempDir(), "missing"), t.TempDir())
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.NotZero(t, exitErr.ExitCode())
}
