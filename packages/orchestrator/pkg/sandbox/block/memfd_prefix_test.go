//go:build linux

package block

import (
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestMemfdPrefixMapsOnlyTheUsableFile(t *testing.T) {
	t.Parallel()

	const size, tail = 8 << 20, 6 << 20
	fd, err := unix.MemfdCreate("test", 0)
	require.NoError(t, err)
	require.NoError(t, unix.Ftruncate(fd, size))
	data := make([]byte, size)
	_, err = rand.Read(data)
	require.NoError(t, err)
	_, err = unix.Pwrite(fd, data, 0)
	require.NoError(t, err)

	m, err := NewFromFdPrefix(fd, tail)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	assert.Equal(t, int64(size-tail), m.MappedBytes())

	got, err := m.Slice(0, size-tail)
	require.NoError(t, err)
	assert.Equal(t, data[:size-tail], got)
	_, err = m.Slice(size-tail, 1)
	require.Error(t, err, "the unused tail is not readable")

	require.NoError(t, m.Extend(tail+1<<20), "a larger tail never shrinks the mapping")
	assert.Equal(t, int64(size-tail), m.MappedBytes())

	require.NoError(t, m.Extend(2<<20))
	assert.Equal(t, int64(size-2<<20), m.MappedBytes())
	got, err = m.Slice(0, size-2<<20)
	require.NoError(t, err)
	assert.Equal(t, data[:size-2<<20], got)
}

func TestMemfdPrefixMapsTheWholeFileWithoutATail(t *testing.T) {
	t.Parallel()

	for _, tail := range []int64{0, -1, 8 << 20, 9 << 20} {
		fd, err := unix.MemfdCreate("test", 0)
		require.NoError(t, err)
		require.NoError(t, unix.Ftruncate(fd, 8<<20))
		m, err := NewFromFdPrefix(fd, tail)
		require.NoError(t, err)
		assert.Equal(t, int64(8<<20), m.MappedBytes(), "tail %d", tail)
		require.NoError(t, m.Close())
	}
}

func TestMemfdExtendAfterCloseIsRefused(t *testing.T) {
	t.Parallel()

	fd, err := unix.MemfdCreate("test", 0)
	require.NoError(t, err)
	require.NoError(t, unix.Ftruncate(fd, 8<<20))
	m, err := NewFromFdPrefix(fd, 4<<20)
	require.NoError(t, err)
	require.NoError(t, m.Close())
	require.ErrorIs(t, m.Extend(0), ErrMemfdClosed)
	require.NoError(t, m.Close(), "a second Close does nothing")
}

func TestMemfdExtendAfterSealIsRefused(t *testing.T) {
	t.Parallel()

	fd, err := unix.MemfdCreate("test", 0)
	require.NoError(t, err)
	require.NoError(t, unix.Ftruncate(fd, 8<<20))
	m, err := NewFromFdPrefix(fd, 4<<20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Extend(2<<20), "extending before the seal works")
	m.Seal()
	require.ErrorIs(t, m.Extend(0), ErrMemfdSealed)
	assert.Equal(t, int64(6<<20), m.MappedBytes(), "a refused Extend keeps the mapping")
}

func hugePagesRsvd(t *testing.T) (rsvd, free int64) {
	t.Helper()

	b, err := os.ReadFile("/proc/meminfo")
	require.NoError(t, err)
	for line := range strings.SplitSeq(string(b), "\n") {
		var v int64
		if _, err := fmt.Sscanf(line, "HugePages_Rsvd: %d", &v); err == nil {
			rsvd = v
		}
		if _, err := fmt.Sscanf(line, "HugePages_Free: %d", &v); err == nil {
			free = v
		}
	}

	return rsvd, free
}

// The mapping is what reserves a hugetlb memfd's pages: an unmapped tail reserves none, Extend
// reserves the pages it adds, and Close gives them all back. HugePages_Rsvd counts every process
// on the host, so it runs only when asked, on a host with a free 2 MiB pool and no other hugepage
// user: HUGEPAGE_RESERVATION_TEST=1 go test -run TestMemfdPrefixReservesOnlyTheMappedHugePages.
//
//nolint:paralleltest // reads the host-wide hugepage counters
func TestMemfdPrefixReservesOnlyTheMappedHugePages(t *testing.T) {
	const page = 2 << 20
	if os.Getenv("HUGEPAGE_RESERVATION_TEST") == "" {
		t.Skip("set HUGEPAGE_RESERVATION_TEST=1 on a host with no other hugepage users")
	}
	// HugePages_Free still counts reserved pages; the test reserves 6.
	if rsvd, free := hugePagesRsvd(t); free-rsvd < 6 {
		t.Skip("needs 6 unreserved free 2 MiB huge pages")
	}
	fd, err := unix.MemfdCreate("test", unix.MFD_HUGETLB|unix.MFD_HUGE_2MB)
	require.NoError(t, err)
	require.NoError(t, unix.Ftruncate(fd, 8*page))

	before, _ := hugePagesRsvd(t)
	m, err := NewFromFdPrefix(fd, 6*page)
	require.NoError(t, err)
	rsvd, _ := hugePagesRsvd(t)
	assert.Equal(t, int64(2), rsvd-before, "only the mapped prefix is reserved")

	require.NoError(t, m.Extend(2*page))
	rsvd, _ = hugePagesRsvd(t)
	assert.Equal(t, int64(6), rsvd-before, "Extend reserves the pages it adds")

	require.NoError(t, m.Close())
	rsvd, _ = hugePagesRsvd(t)
	assert.Equal(t, before, rsvd, "Close gives the reservation back")
}
