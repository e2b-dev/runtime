package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
)

type fakePathManager struct {
	cgroups.Manager

	paths map[cgroups.ProcessType]string
}

func (f fakePathManager) PathOf(t cgroups.ProcessType) (string, bool) {
	p, ok := f.paths[t]

	return p, ok
}

func writeLimits(t *testing.T, dir, high, maxValue string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.high"), []byte(high+"\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte(maxValue+"\n"), 0o644))
}

func TestMemoryLimitApplied(t *testing.T) {
	t.Parallel()
	setup := func(t *testing.T, userHigh, userMax, ptyHigh, ptyMax string) cgroups.Manager {
		t.Helper()
		user, pty := t.TempDir(), t.TempDir()
		writeLimits(t, user, userHigh, userMax)
		writeLimits(t, pty, ptyHigh, ptyMax)

		return fakePathManager{Manager: cgroups.NewNoopManager(), paths: map[cgroups.ProcessType]string{
			cgroups.ProcessTypeUser: user, cgroups.ProcessTypePTY: pty,
		}}
	}

	require.True(t, memoryLimitApplied(setup(t, "1000", "1000", "1000", "1000"), 1000))
	// A write createCgroup was not permitted to make leaves the kernel default.
	require.False(t, memoryLimitApplied(setup(t, "max", "max", "1000", "1000"), 1000))
	require.False(t, memoryLimitApplied(setup(t, "1000", "1000", "1000", "max"), 1000))
	require.False(t, memoryLimitApplied(setup(t, "999", "1000", "1000", "1000"), 1000))
	require.False(t, memoryLimitApplied(cgroups.NewNoopManager(), 1000))
}
