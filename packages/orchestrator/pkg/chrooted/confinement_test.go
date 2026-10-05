//go:build linux

package chrooted

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestChroot(t *testing.T) (*Chrooted, string) {
	t.Helper()

	dir := t.TempDir()

	fs, err := Chroot(dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := fs.Close()
		assert.NoError(t, err)
	})

	return fs, dir
}

func writeHostFile(t *testing.T, path, content string) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

func readChrootFile(t *testing.T, fs *Chrooted, name string) string {
	t.Helper()

	f, err := fs.Open(name)
	require.NoError(t, err)
	defer f.Close()

	data, err := io.ReadAll(f)
	require.NoError(t, err)

	return string(data)
}

func TestPathsAreRelativeToTheRoot(t *testing.T) {
	t.Parallel()

	fs, dir := newTestChroot(t)
	writeHostFile(t, filepath.Join(dir, "etc", "passwd"), "inside")

	for _, name := range []string{"/etc/passwd", "etc/passwd", "/../etc/passwd", "/../../../etc/passwd", "/etc/../etc/passwd", "//etc//passwd"} {
		assert.Equal(t, "inside", readChrootFile(t, fs, name), "path %q", name)
	}

	// The kernel resolves ".." against a directory that must exist, as it
	// does for any process; nothing collapses it lexically first.
	_, err := fs.Open("/missing/../etc/passwd")
	require.ErrorIs(t, err, os.ErrNotExist)

	info, err := fs.Stat("/")
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	info, err = fs.Stat("/..")
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	entries, err := fs.ReadDir("/")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "etc", entries[0].Name())
}

func TestSymlinksResolveInsideTheRoot(t *testing.T) {
	t.Parallel()

	fs, dir := newTestChroot(t)

	// A file the host has that the tree also has, to tell the two apart.
	outside := t.TempDir()
	writeHostFile(t, filepath.Join(outside, "secret"), "outside")
	writeHostFile(t, filepath.Join(dir, outside, "secret"), "inside")
	writeHostFile(t, filepath.Join(dir, "data", "file"), "data")

	require.NoError(t, fs.Mkdir("/dir", 0o755))
	require.NoError(t, fs.Symlink(filepath.Join(outside, "secret"), "/absolute"))
	require.NoError(t, fs.Symlink("../data/file", "/dir/relative"))
	require.NoError(t, fs.Symlink("../../../../data/file", "/dir/climbing"))
	require.NoError(t, fs.Symlink("/absolute", "/chain"))
	require.NoError(t, fs.Symlink("/data", "/dirlink"))
	require.NoError(t, fs.Symlink("/loop2", "/loop1"))
	require.NoError(t, fs.Symlink("/loop1", "/loop2"))
	require.NoError(t, fs.Symlink("/nowhere", "/dangling"))

	t.Run("absolute target is anchored at the root", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "inside", readChrootFile(t, fs, "/absolute"))

		resolved, err := fs.EvalSymlinks("/absolute")
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(outside, "secret"), resolved)
	})

	t.Run("relative target", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "data", readChrootFile(t, fs, "/dir/relative"))
	})

	t.Run("climbing target is clamped at the root", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "data", readChrootFile(t, fs, "/dir/climbing"))

		resolved, err := fs.EvalSymlinks("/dir/climbing")
		require.NoError(t, err)
		assert.Equal(t, "/data/file", resolved)
	})

	t.Run("chain of links", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "inside", readChrootFile(t, fs, "/chain"))
	})

	t.Run("link to a directory in the middle of a path", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "data", readChrootFile(t, fs, "/dirlink/file"))

		info, err := fs.Stat("/dirlink/file")
		require.NoError(t, err)
		assert.Equal(t, "file", info.Name())

		require.NoError(t, fs.Chmod("/dirlink/file", 0o600))
		hostInfo, err := os.Stat(filepath.Join(dir, "data", "file"))
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), hostInfo.Mode().Perm())
	})

	t.Run("operations that do not follow the final link", func(t *testing.T) {
		t.Parallel()

		info, err := fs.Lstat("/absolute")
		require.NoError(t, err)
		assert.NotZero(t, info.Mode()&os.ModeSymlink)

		target, err := fs.Readlink("/absolute")
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(outside, "secret"), target)

		_, err = fs.OpenFile("/dangling", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		require.ErrorIs(t, err, os.ErrExist)
	})

	t.Run("loops fail", func(t *testing.T) {
		t.Parallel()

		_, err := fs.Stat("/loop1")
		require.Error(t, err)

		_, err = fs.EvalSymlinks("/loop1")
		require.ErrorIs(t, err, syscall.ELOOP)
	})

	t.Run("dangling link", func(t *testing.T) {
		t.Parallel()

		_, err := fs.Stat("/dangling")
		require.ErrorIs(t, err, os.ErrNotExist)

		// Creating through a dangling link creates its target, as the kernel does.
		f, err := fs.Create("/dangling")
		require.NoError(t, err)
		require.NoError(t, f.Close())
		assert.FileExists(t, filepath.Join(dir, "nowhere"))
	})

	t.Run("host is untouched", func(t *testing.T) {
		t.Parallel()

		data, err := os.ReadFile(filepath.Join(outside, "secret"))
		require.NoError(t, err)
		assert.Equal(t, "outside", string(data))
	})
}

// TestParentComponentsResolveAfterSymlinks checks that ".." is resolved by
// the kernel against the directory a symlink led to, not collapsed
// lexically against the link's own parent.
func TestParentComponentsResolveAfterSymlinks(t *testing.T) {
	t.Parallel()

	fs, dir := newTestChroot(t)

	writeHostFile(t, filepath.Join(dir, "a", "b", "inner"), "inner")
	writeHostFile(t, filepath.Join(dir, "a", "victim"), "sibling of the target")
	writeHostFile(t, filepath.Join(dir, "victim"), "sibling of the link")
	require.NoError(t, fs.Symlink("/a/b", "/link"))

	assert.Equal(t, "sibling of the target", readChrootFile(t, fs, "/link/../victim"))

	resolved, err := fs.EvalSymlinks("/link/../victim")
	require.NoError(t, err)
	assert.Equal(t, "/a/victim", resolved)

	require.NoError(t, fs.MkdirAll("/link/../x/y", 0o755))
	assert.DirExists(t, filepath.Join(dir, "a", "x", "y"))
	assert.NoDirExists(t, filepath.Join(dir, "x"))

	require.NoError(t, fs.Rename("/link/../victim", "/link/../moved"))
	assert.FileExists(t, filepath.Join(dir, "a", "moved"))
	assert.FileExists(t, filepath.Join(dir, "victim"))

	// Above the root, ".." stays at the root.
	assert.Equal(t, "sibling of the link", readChrootFile(t, fs, "/../../victim"))
	assert.Equal(t, "sibling of the link", readChrootFile(t, fs, "/link/../../../victim"))
}

// TestMkdirAllWithTrailingDots creates the directory a path names even when
// the path ends in "." or "..", as os.MkdirAll does.
func TestMkdirAllWithTrailingDots(t *testing.T) {
	t.Parallel()

	fs, dir := newTestChroot(t)

	require.NoError(t, fs.MkdirAll("/x/y/.", 0o755))
	assert.DirExists(t, filepath.Join(dir, "x", "y"))

	require.NoError(t, fs.MkdirAll("/x/z/..", 0o755))
	assert.DirExists(t, filepath.Join(dir, "x", "z"))

	require.NoError(t, fs.MkdirAll("/.", 0o755))
	require.NoError(t, fs.MkdirAll("/..", 0o755))
	require.NoError(t, fs.MkdirAll("/x/y/./", 0o755))

	writeHostFile(t, filepath.Join(dir, "file"), "")
	require.ErrorIs(t, fs.MkdirAll("/file/.", 0o755), syscall.ENOTDIR)
}

func TestCreateThroughAbsoluteDirectoryLink(t *testing.T) {
	t.Parallel()

	fs, dir := newTestChroot(t)

	require.NoError(t, fs.Mkdir("/real", 0o755))
	require.NoError(t, fs.Symlink("/real", "/link"))

	require.NoError(t, fs.MkdirAll("/link/a/b", 0o755))
	assert.DirExists(t, filepath.Join(dir, "real", "a", "b"))

	f, err := fs.Create("/link/a/b/file")
	require.NoError(t, err)
	assert.Equal(t, "/link/a/b/file", f.Name())
	require.NoError(t, f.Close())
	assert.FileExists(t, filepath.Join(dir, "real", "a", "b", "file"))

	require.NoError(t, fs.Rename("/link/a/b/file", "/link/a/renamed"))
	assert.FileExists(t, filepath.Join(dir, "real", "a", "renamed"))

	require.NoError(t, fs.Remove("/link/a/renamed"))
	require.NoError(t, fs.RemoveAll("/link/a"))
	assert.NoDirExists(t, filepath.Join(dir, "real", "a"))
}

func TestFileNameIsInsideTheRoot(t *testing.T) {
	t.Parallel()

	fs, _ := newTestChroot(t)

	require.NoError(t, fs.MkdirAll("/dir", 0o755))

	f, err := fs.Create("dir//file")
	require.NoError(t, err)
	defer f.Close()
	assert.Equal(t, "/dir/file", f.Name())

	tmp, err := fs.TempFile("/dir", "tmp-")
	require.NoError(t, err)
	defer tmp.Close()
	assert.Equal(t, "/dir", filepath.Dir(tmp.Name()))
	assert.Contains(t, filepath.Base(tmp.Name()), "tmp-")

	_, err = fs.Stat(tmp.Name())
	require.NoError(t, err)
}

// TestOpenFileWithoutCreate passes a mode to an open that cannot create,
// which openat2 rejects unless the mode is dropped.
func TestOpenFileWithoutCreate(t *testing.T) {
	t.Parallel()

	fs, dir := newTestChroot(t)
	writeHostFile(t, filepath.Join(dir, "file"), "content")

	f, err := fs.OpenFile("/file", os.O_RDWR, 0o666)
	require.NoError(t, err)
	defer f.Close()

	_, err = f.WriteString("CONTENT")
	require.NoError(t, err)
	assert.Equal(t, "CONTENT", readChrootFile(t, fs, "/file"))

	_, err = fs.OpenFile("/missing", os.O_RDWR, 0o666)
	require.ErrorIs(t, err, os.ErrNotExist)

	_, err = fs.OpenFile("/file", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	require.ErrorIs(t, err, os.ErrExist)
}

func TestReadDirDoesNotFollowLinks(t *testing.T) {
	t.Parallel()

	fs, dir := newTestChroot(t)

	writeHostFile(t, filepath.Join(dir, "d", "file"), "content")
	require.NoError(t, fs.Symlink("file", "/d/link"))
	require.NoError(t, fs.Symlink("/nowhere", "/d/dangling"))
	require.NoError(t, fs.Mkdir("/d/sub", 0o755))

	entries, err := fs.ReadDir("/d")
	require.NoError(t, err)

	byName := make(map[string]os.FileInfo, len(entries))
	for _, entry := range entries {
		byName[entry.Name()] = entry
	}
	require.Len(t, byName, 4)

	assert.True(t, byName["file"].Mode().IsRegular())
	assert.Equal(t, int64(len("content")), byName["file"].Size())
	assert.NotZero(t, byName["link"].Mode()&os.ModeSymlink)
	assert.NotZero(t, byName["dangling"].Mode()&os.ModeSymlink)
	assert.True(t, byName["sub"].IsDir())

	_, ok := byName["file"].Sys().(*syscall.Stat_t)
	assert.True(t, ok, "entries carry the raw stat for inode and ownership")
}

func TestGetEntry(t *testing.T) {
	t.Parallel()

	fs, dir := newTestChroot(t)

	writeHostFile(t, filepath.Join(dir, "target"), "content")
	require.NoError(t, fs.Symlink("/target", "/link"))
	require.NoError(t, fs.Symlink("/nowhere", "/dangling"))

	entry, err := fs.GetEntry("/target")
	require.NoError(t, err)
	assert.Nil(t, entry.SymlinkTarget)
	assert.Equal(t, int64(len("content")), entry.Size)

	entry, err = fs.GetEntry("/link")
	require.NoError(t, err)
	require.NotNil(t, entry.SymlinkTarget)
	assert.Equal(t, "/target", *entry.SymlinkTarget)
	assert.Equal(t, "link", entry.Name)

	entry, err = fs.GetEntry("/dangling")
	require.NoError(t, err)
	require.NotNil(t, entry.SymlinkTarget)
	assert.Equal(t, "/dangling", *entry.SymlinkTarget)

	_, err = fs.GetEntry("/missing")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestClose(t *testing.T) {
	t.Parallel()

	fs, err := Chroot(t.TempDir())
	require.NoError(t, err)

	require.NoError(t, fs.Close())
	require.ErrorIs(t, fs.Close(), ErrClosed)

	_, err = fs.Stat("/")
	require.ErrorIs(t, err, ErrClosed)

	_, err = fs.EvalSymlinks("/")
	require.ErrorIs(t, err, ErrClosed)
}

// TestCloseWaitsForOperations closes a tree while other goroutines use it:
// every operation must either complete or fail with ErrClosed, none may run
// against a closed, possibly reused, descriptor, and nothing may deadlock.
// EvalSymlinks is included because it takes several steps under one hold of
// the root, which is where a nested acquire would wedge against Close.
func TestCloseWaitsForOperations(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeHostFile(t, filepath.Join(dir, "d", "file"), "content")

	fs, err := Chroot(dir)
	require.NoError(t, err)
	require.NoError(t, fs.Symlink("/d", "/link1"))
	require.NoError(t, fs.Symlink("/link1/file", "/link2"))

	operations := []func() error{
		func() error {
			_, err := fs.Stat("/link2")

			return err
		},
		func() error {
			_, err := fs.EvalSymlinks("/link2")

			return err
		},
		func() error {
			_, err := fs.GetEntry("/link2")

			return err
		},
		func() error {
			_, err := fs.ReadDir("/link1")

			return err
		},
	}

	const workersPerOperation = 4

	var wg sync.WaitGroup
	errs := make(chan error, workersPerOperation*len(operations))
	for _, operation := range operations {
		for range workersPerOperation {
			wg.Go(func() {
				for {
					if err := operation(); err != nil {
						errs <- err

						return
					}
				}
			})
		}
	}

	// Let the workers get going before pulling the root from under them.
	_, err = fs.Stat("/link2")
	require.NoError(t, err)

	closed := make(chan error, 1)
	go func() {
		closed <- fs.Close()
	}()

	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return: an operation holds the root while waiting for it")
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.ErrorIs(t, err, ErrClosed)
	}
}

func TestChrootRequiresADirectory(t *testing.T) {
	t.Parallel()

	file := filepath.Join(t.TempDir(), "file")
	writeHostFile(t, file, "")

	_, err := Chroot(file)
	require.Error(t, err)

	_, err = Chroot(filepath.Join(t.TempDir(), "missing"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestConcurrentOperations(t *testing.T) {
	t.Parallel()

	fs, dir := newTestChroot(t)

	const workers = 8
	const filesPerWorker = 50

	writeFiles := func(worker int) error {
		base := filepath.Join("/", "w", string(rune('a'+worker)))
		if err := fs.MkdirAll(base, 0o755); err != nil {
			return err
		}

		for i := range filesPerWorker {
			name := filepath.Join(base, string(rune('a'+i%26))+string(rune('a'+i/26)))

			f, err := fs.Create(name)
			if err != nil {
				return err
			}
			if _, err := f.WriteString(name); err != nil {
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}

			info, err := fs.Stat(name)
			if err != nil {
				return err
			}
			if info.Size() != int64(len(name)) {
				return fmt.Errorf("%s: got size %d, want %d", name, info.Size(), len(name))
			}
		}

		return nil
	}

	errs := make(chan error, workers)

	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			errs <- writeFiles(w)
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	var count int
	err := filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			count++
		}

		return err
	})
	require.NoError(t, err)
	assert.Equal(t, workers*filesPerWorker, count)
}

// TestFinalDotComponentsCannotEscape targets the operations that address an
// entry by name relative to its parent's descriptor: a final "." or ".."
// there is not resolved by openat2 and would name the root or the host
// directory above it. Files beside the volume on the host must survive.
func TestFinalDotComponentsCannotEscape(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	root := filepath.Join(parent, "volume")
	require.NoError(t, os.Mkdir(root, 0o755))
	writeHostFile(t, filepath.Join(parent, "sibling", "file"), "host")
	writeHostFile(t, filepath.Join(root, "a", "file"), "inside")

	fs, err := Chroot(root)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, fs.Close())
	})

	for _, name := range []string{"/..", "/a/../..", "/a/..", "/.", "/a/.", "/", "..", "."} {
		require.Error(t, fs.RemoveAll(name), "RemoveAll(%q)", name)
		require.Error(t, fs.Remove(name), "Remove(%q)", name)
		require.Error(t, fs.Mkdir(name, 0o755), "Mkdir(%q)", name)
		require.Error(t, fs.Symlink("target", name), "Symlink(%q)", name)
		require.Error(t, fs.Rename(name, "/renamed"), "Rename(%q, ...)", name)
		require.Error(t, fs.Rename("/a/file", name), "Rename(..., %q)", name)
	}

	assert.FileExists(t, filepath.Join(parent, "sibling", "file"))
	assert.FileExists(t, filepath.Join(root, "a", "file"))

	// ".." anywhere but last is resolved by the kernel inside the root.
	writeHostFile(t, filepath.Join(root, "b", "file"), "inside")
	require.NoError(t, fs.RemoveAll("/a/../b"))
	assert.NoDirExists(t, filepath.Join(root, "b"))
	assert.FileExists(t, filepath.Join(root, "a", "file"))
}

func TestRenameOverDirectory(t *testing.T) {
	t.Parallel()

	fs, root := newTestChroot(t)
	require.NoError(t, os.Mkdir(filepath.Join(root, "src"), 0o755))
	writeHostFile(t, filepath.Join(root, "src", "file"), "x")
	require.NoError(t, os.Mkdir(filepath.Join(root, "empty"), 0o755))
	require.NoError(t, os.Mkdir(filepath.Join(root, "full"), 0o755))
	writeHostFile(t, filepath.Join(root, "full", "file"), "y")
	writeHostFile(t, filepath.Join(root, "file"), "z")

	// An empty directory is replaced, as rename(2) does.
	require.NoError(t, fs.Rename("/src", "/empty"))
	assert.NoDirExists(t, filepath.Join(root, "src"))
	assert.FileExists(t, filepath.Join(root, "empty", "file"))

	err := fs.Rename("/empty", "/full")
	require.ErrorIs(t, err, syscall.ENOTEMPTY)

	err = fs.Rename("/file", "/full")
	require.ErrorIs(t, err, syscall.EISDIR)
	assert.FileExists(t, filepath.Join(root, "file"))
}

func TestTrailingSlashRequiresADirectory(t *testing.T) {
	t.Parallel()

	fs, root := newTestChroot(t)
	writeHostFile(t, filepath.Join(root, "file"), "x")
	require.NoError(t, os.Mkdir(filepath.Join(root, "dir"), 0o755))
	require.NoError(t, os.Symlink("dir", filepath.Join(root, "ld")))

	require.ErrorIs(t, fs.Rename("/file/", "/other"), syscall.ENOTDIR)
	assert.FileExists(t, filepath.Join(root, "file"))

	require.ErrorIs(t, fs.Rename("/file", "/other/"), syscall.ENOTDIR)
	assert.FileExists(t, filepath.Join(root, "file"))

	// A symlink to a directory is still not a directory itself.
	require.ErrorIs(t, fs.Remove("/ld/"), syscall.ENOTDIR)
	assert.FileExists(t, filepath.Join(root, "ld"))
	require.ErrorIs(t, fs.Remove("/file/"), syscall.ENOTDIR)

	require.NoError(t, fs.Remove("/dir/"))
	assert.NoDirExists(t, filepath.Join(root, "dir"))

	require.NoError(t, fs.Mkdir("/new/", 0o755))
	assert.DirExists(t, filepath.Join(root, "new"))

	require.ErrorIs(t, fs.Symlink("target", "/new/"), os.ErrExist)
	require.ErrorIs(t, fs.Symlink("target", "/missing/"), os.ErrNotExist)
	assert.NoFileExists(t, filepath.Join(root, "missing"))
}

func TestReadlinkOfARegularFile(t *testing.T) {
	t.Parallel()

	fs, root := newTestChroot(t)
	writeHostFile(t, filepath.Join(root, "regular"), "x")

	_, err := fs.Readlink("/regular")
	require.ErrorIs(t, err, syscall.EINVAL)
	require.NotErrorIs(t, err, os.ErrNotExist)

	_, err = fs.Readlink("/missing")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestResolveThroughAFileIsNotADirectory(t *testing.T) {
	t.Parallel()

	fs, root := newTestChroot(t)
	writeHostFile(t, filepath.Join(root, "file"), "x")
	writeHostFile(t, filepath.Join(root, "x"), "y")
	require.NoError(t, os.Symlink("/file", filepath.Join(root, "link")))

	_, err := fs.EvalSymlinks("/link/../x")
	require.ErrorIs(t, err, syscall.ENOTDIR)

	_, err = fs.EvalSymlinks("/file/../x")
	require.ErrorIs(t, err, syscall.ENOTDIR)

	_, err = fs.GetEntry("/link/../x")
	require.ErrorIs(t, err, syscall.ENOTDIR)
}

func TestResolveStepBudget(t *testing.T) {
	t.Parallel()

	fs, root := newTestChroot(t)
	require.NoError(t, os.Mkdir(filepath.Join(root, "d"), 0o755))

	// Dot components cost a step each without moving anywhere.
	name := "/d" + strings.Repeat("/.", maxResolveSteps+1)

	_, err := fs.EvalSymlinks(name)
	require.ErrorIs(t, err, syscall.ENAMETOOLONG)

	// Within the budget the same shape resolves.
	resolved, err := fs.EvalSymlinks("/d" + strings.Repeat("/.", 16))
	require.NoError(t, err)
	assert.Equal(t, "/d", resolved)
}

func TestRemoveAllLargeDirectory(t *testing.T) {
	t.Parallel()

	fs, root := newTestChroot(t)
	dir := filepath.Join(root, "big")
	require.NoError(t, os.Mkdir(dir, 0o755))

	const entries = 3*removeAllBatch + 7
	for i := range entries {
		require.NoError(t, os.WriteFile(filepath.Join(dir, strconv.Itoa(i)), nil, 0o644))
	}
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "nested", "deeper"), 0o755))
	writeHostFile(t, filepath.Join(dir, "nested", "deeper", "file"), "x")

	require.NoError(t, fs.RemoveAll("/big"))
	assert.NoDirExists(t, dir)

	// Missing paths are not an error, like os.RemoveAll.
	require.NoError(t, fs.RemoveAll("/big"))
	require.NoError(t, fs.RemoveAll("/never/existed"))
}

func TestOpenDirectoryWithMode(t *testing.T) {
	t.Parallel()

	fs, root := newTestChroot(t)
	require.NoError(t, os.Mkdir(filepath.Join(root, "d"), 0o755))

	// Without O_CREAT the mode is ignored, even when O_DIRECTORY shares bits
	// with O_TMPFILE.
	f, err := fs.OpenFile("/d", os.O_RDONLY|syscall.O_DIRECTORY, 0o755)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = fs.OpenFile("/missing", os.O_RDONLY|syscall.O_DIRECTORY, 0o755)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestCheckSupport(t *testing.T) {
	t.Parallel()

	require.NoError(t, CheckSupport())
}
