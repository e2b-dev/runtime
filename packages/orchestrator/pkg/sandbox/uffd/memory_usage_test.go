//go:build linux

package uffd

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/memory"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func TestMemoryUsageBeforeReadyAndAfterClose(t *testing.T) {
	t.Parallel()
	log, err := logger.NewDevelopmentLogger()
	require.NoError(t, err)
	backend := New(nil, "", log)
	_, ok := backend.MemoryUsage()
	require.False(t, ok)
	var fd [2]int
	require.NoError(t, syscall.Pipe(fd[:]))
	t.Cleanup(func() { _ = syscall.Close(fd[1]) })
	handler, err := userfaultfd.NewUserfaultfdFromFd(uintptr(fd[0]), nil, memory.NewMapping([]memory.Region{{Size: 4096, PageSize: 4096}}), 0, log)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, handler.Close()) })
	backend.handler.SetValue(handler)
	_, ok = backend.MemoryUsage()
	require.True(t, ok)
	require.NoError(t, handler.Close())
	final, ok := backend.MemoryUsage()
	require.True(t, ok)
	require.Equal(t, handler.MemoryUsage(), final)
}
