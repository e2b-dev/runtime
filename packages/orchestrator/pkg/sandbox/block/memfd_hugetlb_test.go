//go:build linux

package block

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// An empty 1 GiB pool makes the reservation failure deterministic without
// consuming physical memory or changing a pool used by running guests.
// Never access either mapping: there is deliberately no backing huge page.
func TestNewFromFd_HugetlbNoReservation(t *testing.T) { //nolint:paralleltest // observes host hugepage pool state
	const pool = "/sys/kernel/mm/hugepages/hugepages-1048576kB"
	for _, name := range []string{"nr_hugepages", "nr_overcommit_hugepages"} {
		value, err := os.ReadFile(filepath.Join(pool, name))
		if err != nil || strings.TrimSpace(string(value)) != "0" {
			t.Skip("requires a supported, empty 1 GiB hugetlb pool with no overcommit")
		}
	}

	fd, err := unix.MemfdCreate("e2b-noreserve-test", unix.MFD_CLOEXEC|unix.MFD_HUGETLB|unix.MFD_HUGE_1GB)
	if errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOSYS) {
		t.Skipf("hugetlb memfd unavailable: %v", err)
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fd) })

	const size = 1 << 30
	require.NoError(t, unix.Ftruncate(fd, size))

	// Match Firecracker's owner mapping. No huge pages are allocated or read.
	owner, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_NORESERVE)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, unix.Munmap(owner)) })

	// Verify that the fixture really exercises the reservation failure.
	reserving, err := unix.Mmap(fd, 0, size, unix.PROT_READ, unix.MAP_SHARED)
	if err == nil {
		require.NoError(t, unix.Munmap(reserving))
	}
	require.ErrorIs(t, err, unix.ENOMEM)

	observerFD, err := unix.Dup(fd)
	require.NoError(t, err)
	observer, err := NewFromFd(observerFD) // consumes observerFD on both paths
	require.NoError(t, err, "observing guest memory must not reserve additional huge pages")
	t.Cleanup(func() { require.NoError(t, observer.Close()) })
	require.Len(t, observer.mmap, size)
}
