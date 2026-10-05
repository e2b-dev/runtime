//go:build linux

package volumes

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// TestListDir_SymlinksResolveInsideTheVolume gives a symlink the name of a
// directory that exists on the host: its type and target must come from
// the volume, and ListDir must agree with StatPath.
func TestListDir_SymlinksResolveInsideTheVolume(t *testing.T) {
	t.Parallel()

	s, basePath, volumeInfo := setupTestService(t)

	require.NoError(t, os.WriteFile(filepath.Join(basePath, "target.txt"), []byte("target"), 0o644))
	require.NoError(t, os.Symlink("/target.txt", filepath.Join(basePath, "etc")))
	require.NoError(t, os.Symlink("/nowhere", filepath.Join(basePath, "dangling")))
	require.DirExists(t, "/etc", "the test needs a host directory sharing the link's name")

	listed, err := s.ListDir(t.Context(), &orchestrator.ListDirRequest{Volume: volumeInfo, Path: "/"})
	require.NoError(t, err)

	byPath := make(map[string]*orchestrator.EntryInfo, len(listed.GetFiles()))
	for _, item := range listed.GetFiles() {
		byPath[item.GetEntry().GetPath()] = item.GetEntry()
	}
	require.Len(t, byPath, 3)

	require.Equal(t, orchestrator.FileType_FILE_TYPE_FILE, byPath["/etc"].GetType())
	require.Equal(t, "/target.txt", byPath["/etc"].GetSymlinkTarget())
	require.Equal(t, orchestrator.FileType_FILE_TYPE_UNSPECIFIED, byPath["/dangling"].GetType())

	stat, err := s.StatPath(t.Context(), &orchestrator.StatPathRequest{Volume: volumeInfo, Path: "/etc"})
	require.NoError(t, err)
	require.Equal(t, byPath["/etc"].GetType(), stat.GetEntry().GetType())
	require.Equal(t, byPath["/etc"].GetSymlinkTarget(), stat.GetEntry().GetSymlinkTarget())
}

func TestListDir_Depth(t *testing.T) {
	t.Parallel()

	s, basePath, volumeInfo := setupTestService(t)

	// Prepare directory structure:
	// /mnt/shared/team-<teamID>/vol-<volumeID>/dir/test.txt
	// /mnt/shared/team-<teamID>/vol-<volumeID>/dir/deep/test.txt

	err := os.MkdirAll(filepath.Join(basePath, "dir", "deep"), 0o755)
	require.NoError(t, err)

	err = os.WriteFile(filepath.Join(basePath, "dir", "test.txt"), []byte("test"), 0o644)
	require.NoError(t, err)

	err = os.WriteFile(filepath.Join(basePath, "dir", "deep", "test.txt"), []byte("deep test"), 0o644)
	require.NoError(t, err)

	err = os.MkdirAll(filepath.Join(basePath, "dir", "deep", "deeper"), 0o755)
	require.NoError(t, err)

	err = os.WriteFile(filepath.Join(basePath, "dir", "deep", "deeper", "test.txt"), []byte("deeper test"), 0o644)
	require.NoError(t, err)

	ctx := t.Context()

	t.Run("depth 0", func(t *testing.T) {
		t.Parallel()

		req := &orchestrator.ListDirRequest{
			Volume: volumeInfo,
			Path:   "dir",
			Depth:  0,
		}
		resp, err := s.ListDir(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		paths := getPaths(t, resp.GetFiles())
		require.ElementsMatch(t, []string{"/dir/test.txt", "/dir/deep"}, paths)
	})

	t.Run("depth 1", func(t *testing.T) {
		t.Parallel()

		req := &orchestrator.ListDirRequest{
			Volume: volumeInfo,
			Path:   "dir",
			Depth:  1,
		}
		resp, err := s.ListDir(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		paths := getPaths(t, resp.GetFiles())
		require.ElementsMatch(t, []string{"/dir/test.txt", "/dir/deep"}, paths)
	})

	t.Run("depth 2", func(t *testing.T) {
		t.Parallel()

		req := &orchestrator.ListDirRequest{
			Volume: volumeInfo,
			Path:   "dir",
			Depth:  2,
		}
		resp, err := s.ListDir(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		paths := getPaths(t, resp.GetFiles())
		require.ElementsMatch(t, []string{"/dir/test.txt", "/dir/deep", "/dir/deep/test.txt", "/dir/deep/deeper"}, paths)
	})

	t.Run("list non-existent dir", func(t *testing.T) {
		t.Parallel()

		req := &orchestrator.ListDirRequest{
			Volume: volumeInfo,
			Path:   "non-existent-dir",
		}
		_, err := s.ListDir(ctx, req)
		requireGRPCError(t, err, codes.NotFound, orchestrator.UserErrorCode_PATH_NOT_FOUND)
	})

	t.Run("list depth out of range", func(t *testing.T) {
		t.Parallel()

		req := &orchestrator.ListDirRequest{
			Volume: volumeInfo,
			Path:   "dir",
			Depth:  11,
		}
		_, err := s.ListDir(ctx, req)
		requireGRPCError(t, err, codes.InvalidArgument, orchestrator.UserErrorCode_DEPTH_OUT_OF_RANGE)
	})
}

func getPaths(t *testing.T, items []*orchestrator.VolumeDirectoryItem) []string {
	t.Helper()

	paths := make([]string, 0, len(items))
	for _, item := range items {
		paths = append(paths, item.GetEntry().GetPath())
	}

	return paths
}
