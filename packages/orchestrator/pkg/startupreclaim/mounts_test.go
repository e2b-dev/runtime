//go:build linux

package startupreclaim

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A bind mount shows a directory under a second name that neither the path nor
// its symlinks reveal, so the guard also compares where paths live on disk.
func TestCacheDirGuardSeesBindMountedAliases(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cacheDir := filepath.Join(root, "template")
	volume := filepath.Join(root, "pv")
	alias := filepath.Join(root, "template-alias")
	other := filepath.Join(root, "other")
	for _, dir := range []string{cacheDir, volume, alias, other} {
		require.NoError(t, os.MkdirAll(dir, 0o700))
	}
	mounts := mountTable{
		{device: "8:1", root: "/", target: "/"},
		{device: "9:1", root: "/srv/template", target: cacheDir},
		{device: "9:1", root: "/srv/template/pv", target: volume},
		{device: "9:1", root: "/srv/template", target: alias},
		{device: "9:1", root: "/srv/other", target: other},
	}
	guards := withResolved([]string{cacheDir, volume, alias, other}, mounts)
	cache, pv, cacheAlias, sibling := guards[0], guards[1], guards[2], guards[3]

	held, ok := heldPath(cache, nil, []guardPath{pv})
	assert.True(t, ok, "a bind mount of a directory inside the cache is held by it")
	assert.Equal(t, volume, held)

	_, ok = heldPath(cache, nil, []guardPath{cacheAlias})
	assert.True(t, ok, "a second mount of the cache itself is held by it")

	_, ok = heldPath(cache, []guardPath{cache, cacheAlias}, nil)
	assert.False(t, ok, "two mounts of one cache dir are one cache, not dirs holding each other")

	_, ok = heldPath(cache, nil, []guardPath{sibling})
	assert.False(t, ok, "another directory of the same filesystem is not held")
}

// A protected filesystem mounted below a cache dir and kept under another name
// is reachable through the cache tree, though it is another device.
func TestCacheDirGuardSeesProtectedFilesystemsMountedBelowIt(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cacheDir := filepath.Join(root, "cache")
	volume := filepath.Join(root, "volume")
	for _, dir := range []string{filepath.Join(cacheDir, "pv"), filepath.Join(cacheDir, "scratch"), volume} {
		require.NoError(t, os.MkdirAll(dir, 0o700))
	}
	mounts := mountTable{
		{device: "8:1", root: "/", target: "/"},
		{device: "9:1", root: "/srv/cache", target: cacheDir},
		{device: "9:2", root: "/", target: filepath.Join(cacheDir, "pv")},
		{device: "9:2", root: "/", target: volume},
		{device: "7:0", root: "/", target: filepath.Join(cacheDir, "scratch")},
	}
	guards := withResolved([]string{cacheDir, volume}, mounts)
	cache, kept := guards[0], guards[1]

	held, ok := heldPath(cache, nil, []guardPath{kept})
	assert.True(t, ok, "the kept filesystem is mounted inside the cache tree")
	assert.Equal(t, volume, held)

	unrelated := withResolved([]string{filepath.Join(root, "unrelated")}, append(mounts, mountEntry{device: "9:3", root: "/", target: filepath.Join(root, "unrelated")}))[0]
	_, ok = heldPath(cache, nil, []guardPath{unrelated})
	assert.False(t, ok, "a filesystem mounted only outside the cache is not held")
}

func TestMountTableLocateUsesTheDeepestVisibleMount(t *testing.T) {
	t.Parallel()

	mounts := mountTable{
		{device: "8:1", root: "/", target: "/"},
		{device: "9:1", root: "/", target: "/orchestrator"},
		{device: "9:2", root: "/", target: "/orchestrator"},
	}

	device, fsPath := mounts.locate("/orchestrator/template/a")
	assert.Equal(t, "9:2", device, "of mounts stacked on one target, the last is visible")
	assert.Equal(t, "/template/a", fsPath)

	device, fsPath = mounts.locate("/var/lib")
	assert.Equal(t, "8:1", device)
	assert.Equal(t, "/var/lib", fsPath)
}

func TestReadMountsDecodesEscapedPaths(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "mountinfo")
	require.NoError(t, os.WriteFile(path, []byte(
		"22 1 8:1 / / rw,relatime shared:1 - xfs /dev/sda1 rw\n"+
			`40 22 9:1 /srv/a\040b /mnt/x\040y rw - xfs /dev/sdb rw`+"\n"), 0o600))

	mounts, err := readMounts(path)
	require.NoError(t, err)
	assert.Equal(t, mountTable{
		{device: "8:1", root: "/", target: "/"},
		{device: "9:1", root: "/srv/a b", target: "/mnt/x y"},
	}, mounts)
}
