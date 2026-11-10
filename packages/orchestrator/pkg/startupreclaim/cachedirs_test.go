//go:build linux

package startupreclaim

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResetCacheDirsMovesContentsAsideForBackgroundRemoval(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	sandboxDir := filepath.Join(root, "sandbox")
	templateDir := filepath.Join(root, "template")
	templatesDir := filepath.Join(root, "build-templates")
	outside := filepath.Join(root, "outside")

	writeFile(t, filepath.Join(sandboxDir, "rootfs-sbx-1.cow"))
	writeFile(t, filepath.Join(templateDir, "build-1", "cache", "id-1", "snapfile"))
	require.NoError(t, os.MkdirAll(templatesDir, 0o700))
	writeFile(t, filepath.Join(outside, "kept"))
	require.NoError(t, os.Symlink(outside, filepath.Join(templateDir, "link")))
	// Mkdir alone would come back with the umask applied.
	require.NoError(t, os.Chmod(templateDir, 0o777))
	leftover := templateDir + staleSuffix + "1"
	writeFile(t, filepath.Join(leftover, "build-0", "snapfile"))

	dirs := []string{sandboxDir, templateDir, templatesDir, templateDir, filepath.Join(root, "missing")}
	reset, stale, failures := resetCacheDirs(dirs, []string{outside})

	require.Empty(t, failures)
	assert.Equal(t, 2, reset, "only the non-empty dirs are reset")
	for _, dir := range []string{sandboxDir, templateDir, templatesDir} {
		entries, err := os.ReadDir(dir)
		require.NoError(t, err, "the directory itself must survive")
		assert.Empty(t, entries, dir)
	}
	info, err := os.Stat(templateDir)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o777), info.Mode().Perm())

	assert.Contains(t, stale, leftover, "an unfinished earlier removal is picked up")
	movedTemplate := ""
	for _, path := range stale {
		if strings.HasPrefix(path, templateDir+staleSuffix) && path != leftover {
			movedTemplate = path
		}
	}
	require.NotEmpty(t, movedTemplate, "the template dir must have been moved aside")
	assert.FileExists(t, filepath.Join(movedTemplate, "build-1", "cache", "id-1", "snapfile"),
		"the old contents wait in the moved-aside tree")

	removeStale(t.Context(), stale)

	for _, path := range stale {
		assert.NoDirExists(t, path)
	}
	assert.FileExists(t, filepath.Join(outside, "kept"), "a symlink's target must survive")
}

func TestResetCacheDirsEmptiesSymlinkInPlace(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	target := filepath.Join(root, "disk", "template")
	link := filepath.Join(root, "template")
	writeFile(t, filepath.Join(target, "build-1", "snapfile"))
	require.NoError(t, os.Symlink(target, link))

	reset, stale, failures := resetCacheDirs([]string{link}, nil)

	require.Empty(t, failures)
	assert.Equal(t, 1, reset)
	assert.Empty(t, stale)
	info, err := os.Lstat(link)
	require.NoError(t, err)
	assert.Equal(t, os.ModeSymlink, info.Mode().Type(), "the symlink must not be replaced by a directory")
	entries, err := os.ReadDir(target)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestResetCacheDirsSkipsDirsHoldingOtherPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dirs    func(root string) []string
		keep    func(root string) []string
		kept    []string
		emptied []string
		errText string
	}{
		{
			name:    "cache dir set to the base dir",
			dirs:    func(root string) []string { return []string{root} },
			keep:    func(root string) []string { return []string{filepath.Join(root, "shared-store", "chunks-cache")} },
			kept:    []string{"shared-store/chunks-cache/chunk", "swapfile"},
			errText: "holds",
		},
		{
			name:    "cache dir equal to a kept path",
			dirs:    func(root string) []string { return []string{filepath.Join(root, "build")} },
			keep:    func(root string) []string { return []string{filepath.Join(root, "build")} },
			kept:    []string{"build/diff"},
			errText: "holds",
		},
		{
			name: "cache dir holding another cache dir",
			dirs: func(root string) []string {
				return []string{filepath.Join(root, "outer"), filepath.Join(root, "outer", "inner")}
			},
			keep:    func(string) []string { return nil },
			kept:    []string{"outer/file"},
			emptied: []string{"outer/inner"},
			errText: "holds",
		},
		{
			name:    "filesystem root",
			dirs:    func(string) []string { return []string{"/"} },
			keep:    func(root string) []string { return []string{root} },
			errText: "filesystem root",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			for _, rel := range tt.kept {
				writeFile(t, filepath.Join(root, rel))
			}
			for _, rel := range tt.emptied {
				writeFile(t, filepath.Join(root, rel, "entry"))
			}

			_, _, failures := resetCacheDirs(tt.dirs(root), tt.keep(root))

			require.Len(t, failures, 1)
			require.ErrorContains(t, failures[0], tt.errText)
			for _, rel := range tt.kept {
				assert.FileExists(t, filepath.Join(root, rel))
			}
			for _, rel := range tt.emptied {
				entries, err := os.ReadDir(filepath.Join(root, rel))
				require.NoError(t, err)
				assert.Empty(t, entries, rel)
			}
		})
	}
}

// A cache dir that is a symlink is emptied through it, so the guard must see
// what the link resolves to: here a persistent volume behind the cache path.
func TestResetCacheDirsSkipsSymlinkIntoKeptPath(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	volume := filepath.Join(root, "persistent")
	writeFile(t, filepath.Join(volume, "data"))
	cacheDir := filepath.Join(root, "template")
	require.NoError(t, os.Symlink(volume, cacheDir))

	reset, stale, failures := resetCacheDirs([]string{cacheDir}, []string{volume})

	require.Len(t, failures, 1)
	require.ErrorContains(t, failures[0], "it holds "+volume)
	assert.Zero(t, reset)
	assert.Empty(t, stale)
	assert.FileExists(t, filepath.Join(volume, "data"))
}

func TestResetCacheDirsSkipsDirHoldingASymlinkedKeepPath(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cacheDir := filepath.Join(root, "template")
	writeFile(t, filepath.Join(cacheDir, "pv", "data"))
	keep := filepath.Join(root, "pv-link")
	require.NoError(t, os.Symlink(filepath.Join(cacheDir, "pv"), keep))

	reset, _, failures := resetCacheDirs([]string{cacheDir}, []string{keep})

	require.Len(t, failures, 1)
	require.ErrorContains(t, failures[0], "it holds "+keep)
	assert.Zero(t, reset)
	assert.FileExists(t, filepath.Join(cacheDir, "pv", "data"))
}

// Two names for one cache, a symlink and its target, are the same dir rather
// than dirs holding each other, so the cache is still emptied.
func TestResetCacheDirsEmptiesACacheConfiguredUnderTwoNames(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dir := filepath.Join(root, "template")
	writeFile(t, filepath.Join(dir, "build-1", "snapfile"))
	alias := filepath.Join(root, "template-link")
	require.NoError(t, os.Symlink(dir, alias))

	reset, stale, failures := resetCacheDirs([]string{dir, alias}, nil)

	require.Empty(t, failures)
	assert.Equal(t, 1, reset, "the alias finds the cache already reset")
	require.Len(t, stale, 1)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

// Checked on the guard alone, never through resetCacheDirs: a regression there
// would empty the test host's root through the link.
func TestCacheDirGuardTreatsASymlinkToRootAsRoot(t *testing.T) {
	t.Parallel()

	link := filepath.Join(t.TempDir(), "cache")
	require.NoError(t, os.Symlink("/", link))

	guard := withResolved([]string{link}, nil)[0]

	assert.Equal(t, "/", guard.resolved)
	assert.True(t, guard.isRoot())
}

// Only names moveAside creates are earlier leftovers, and never one that is a
// protected path: a shared cache can be named like a moved-aside tree.
func TestResetCacheDirsRemovesOnlyItsOwnLeftovers(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dir := filepath.Join(root, "template")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	own := dir + staleSuffix + "123"
	writeFile(t, filepath.Join(own, "snapfile"))
	sharedCache := dir + staleSuffix + "chunks"
	writeFile(t, filepath.Join(sharedCache, "chunk"))
	keptLookalike := dir + staleSuffix + "456"
	writeFile(t, filepath.Join(keptLookalike, "chunk"))
	unknown := dir + staleSuffix + "12x"
	writeFile(t, filepath.Join(unknown, "entry"))

	_, stale, failures := resetCacheDirs([]string{dir}, []string{sharedCache, keptLookalike})

	assert.Equal(t, []string{own}, stale)
	require.Len(t, failures, 1)
	require.ErrorContains(t, failures[0], "not removing "+keptLookalike)
	for _, path := range []string{sharedCache, keptLookalike, unknown} {
		assert.DirExists(t, path)
	}
}

func writeFile(t *testing.T, path string) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o600))
}
