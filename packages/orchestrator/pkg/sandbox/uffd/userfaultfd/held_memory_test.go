//go:build linux

package userfaultfd

import (
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/memory"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

func TestHeldMemoryRealFaultAndRemove(t *testing.T) {
	t.Parallel()
	cfg := testConfig{pagesize: header.PageSize, numberOfPages: 4, removeEnabled: true, syncWP: true, alwaysWP: true}
	h, err := configureCrossProcessTest(t.Context(), t, cfg)
	require.NoError(t, err)
	// The second page: the first is what most tests in this package fault.
	const offset = header.PageSize
	page := (*h.memoryArea)[offset : 2*offset]
	initial, err := h.client.MemoryUsage()
	require.NoError(t, err)
	require.Zero(t, initial.HeldBytes)
	require.NoError(t, h.executeOperation(t.Context(), operation{offset: offset, mode: operationModeRead}))
	_, err = h.awaitPageState(t.Context(), offset, block.Clean)
	require.NoError(t, err)
	installed, err := h.client.MemoryUsage()
	require.NoError(t, err)
	require.Equal(t, uint64(header.PageSize), installed.HeldBytes)
	require.NoError(t, h.executeOperation(t.Context(), operation{offset: offset, mode: operationModeWrite}))
	_, err = h.awaitPageState(t.Context(), offset, block.Dirty)
	require.NoError(t, err)
	// One 4 KiB page adds a MiB-millisecond every 256 ms.
	require.Eventually(t, func() bool {
		usage, err := h.client.MemoryUsage()

		return err == nil && usage.HeldMiBMillis > installed.HeldMiBMillis
	}, 5*time.Second, time.Millisecond)
	require.NoError(t, h.executeOperation(t.Context(), operation{offset: offset, mode: operationModeRemove, pages: 2}))
	_, err = h.awaitPageState(t.Context(), offset, block.Removed)
	require.NoError(t, err)
	removed, err := h.client.MemoryUsage()
	require.NoError(t, err)
	require.Zero(t, removed.HeldBytes)
	require.Greater(t, removed.HeldMiBMillis, installed.HeldMiBMillis)
	again, err := h.client.MemoryUsage()
	require.NoError(t, err)
	require.Equal(t, removed, again)
	require.NoError(t, unix.Madvise(page, unix.MADV_POPULATE_READ))
	require.Equal(t, make([]byte, header.PageSize), page)
	_, err = h.awaitPageState(t.Context(), offset, block.Zero)
	require.NoError(t, err)
	reinstalled, err := h.client.MemoryUsage()
	require.NoError(t, err)
	require.Equal(t, uint64(header.PageSize), reinstalled.HeldBytes)
}

func TestHeldMemoryFrozenOnClose(t *testing.T) {
	t.Parallel()
	var fd [2]int
	require.NoError(t, syscall.Pipe(fd[:]))
	t.Cleanup(func() { _ = syscall.Close(fd[1]) })
	log, err := logger.NewDevelopmentLogger()
	require.NoError(t, err)
	u, err := NewUserfaultfdFromFd(uintptr(fd[0]), nil, memory.NewMapping([]memory.Region{{Size: 1 << 40, PageSize: header.PageSize}}), 0, log)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, u.Close()) })
	u.pageTracker.SetRange(0, 1<<28, block.Dirty)
	require.NoError(t, u.Close())
	final := u.MemoryUsage()
	require.Equal(t, uint64(1<<40), final.HeldBytes())
	for range 100 {
		require.Equal(t, final, u.MemoryUsage())
	}
}
