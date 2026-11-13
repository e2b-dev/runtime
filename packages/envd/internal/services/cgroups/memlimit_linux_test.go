//go:build linux

package cgroups

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// No root needed: cgroupPaths is just a map, so a temp dir holding the two interface
// files stands in for the cgroup.
func TestCgroup2Manager_SetMemoryLimit(t *testing.T) {
	t.Parallel()

	t.Run("writes memory.high and memory.max", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		for _, name := range []string{"memory.high", "memory.max"} {
			require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("max\n"), 0o644))
		}
		mgr := Cgroup2Manager{cgroupPaths: map[ProcessType]string{ProcessTypeUser: dir}}

		require.NoError(t, mgr.SetMemoryLimit(ProcessTypeUser, 3087007744))

		for _, name := range []string{"memory.high", "memory.max"} {
			b, err := os.ReadFile(filepath.Join(dir, name))
			require.NoError(t, err)
			assert.Equal(t, "3087007744", string(b), name)
		}
	})

	t.Run("an unknown process type is an error", func(t *testing.T) {
		t.Parallel()
		mgr := Cgroup2Manager{cgroupPaths: map[ProcessType]string{}}

		require.Error(t, mgr.SetMemoryLimit(ProcessTypeUser, 1))
	})

	t.Run("a missing interface file is an error, not a created file", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		mgr := Cgroup2Manager{cgroupPaths: map[ProcessType]string{ProcessTypeUser: dir}}

		require.Error(t, mgr.SetMemoryLimit(ProcessTypeUser, 1))
		assert.NoFileExists(t, filepath.Join(dir, "memory.high"))
	})
}
