//go:build unix

package api

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A destination that is a symlink to a source is replaced by the composed
// file, as the rename over the link name does; it is not refused as an alias.
func TestCompose_DestinationLinkToSourceIsReplaced(t *testing.T) {
	t.Parallel()

	api, currentUser := newComposeTestAPI(t)
	dir := t.TempDir()
	part1 := writeSourceFile(t, dir, "part1", []byte("a"))
	part2 := writeSourceFile(t, dir, "part2", []byte("b"))
	destPath := filepath.Join(dir, "out")
	require.NoError(t, os.Symlink(part1, destPath))

	w := callCompose(t, api, ComposeRequest{
		SourcePaths: []string{part1, part2},
		Destination: destPath,
		Username:    &currentUser.Username,
	})

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	info, err := os.Lstat(destPath)
	require.NoError(t, err)
	assert.True(t, info.Mode().IsRegular())
	got, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, "ab", string(got))
}

// A source that is a symlink reaching the destination through the destination
// link is removed after composing, as any source is: the composed file has
// replaced the link it pointed through, and only the link is removed.
func TestCompose_SourceLinkThroughDestinationIsRemoved(t *testing.T) {
	t.Parallel()

	api, currentUser := newComposeTestAPI(t)
	dir := t.TempDir()
	target := writeSourceFile(t, dir, "target", []byte("a"))
	part2 := writeSourceFile(t, dir, "part2", []byte("b"))
	destPath := filepath.Join(dir, "out")
	require.NoError(t, os.Symlink(target, destPath))
	src := filepath.Join(dir, "src")
	require.NoError(t, os.Symlink(destPath, src))

	w := callCompose(t, api, ComposeRequest{
		SourcePaths: []string{src, part2},
		Destination: destPath,
		Username:    &currentUser.Username,
	})

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	got, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, "ab", string(got))
	assert.NoFileExists(t, src)
	assert.NoFileExists(t, part2)
	assert.FileExists(t, target)
}
