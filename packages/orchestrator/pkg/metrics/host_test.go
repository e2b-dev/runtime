//go:build linux

package metrics

import (
	"errors"
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fakeSysctls(values map[string]string) func(string) ([]byte, error) {
	return func(path string) ([]byte, error) {
		v, ok := values[path]
		if !ok {
			return nil, os.ErrNotExist
		}

		return []byte(v), nil
	}
}

func TestReadHugePagesPoolReportsTheBaseAndAddsTheOvercommitAllowance(t *testing.T) {
	t.Parallel()

	pool, err := readHugePagesPool(fakeSysctls(map[string]string{
		nrHugePagesPath:           "1600\n",
		nrOvercommitHugePagesPath: "400\n",
	}))

	require.NoError(t, err)
	assert.Equal(t, hugePagePool{Base: 1600, BaseRead: true, Capacity: 2000}, pool)
}

func TestReadHugePagesPoolCountsAPoolThatIsAllOvercommit(t *testing.T) {
	t.Parallel()

	pool, err := readHugePagesPool(fakeSysctls(map[string]string{
		nrHugePagesPath:           "0\n",
		nrOvercommitHugePagesPath: "54097\n",
	}))

	require.NoError(t, err)
	assert.Equal(t, hugePagePool{Base: 0, BaseRead: true, Capacity: 54097}, pool)
}

// A missing sysctl means the kernel has no hugetlb; any other failure is
// returned so the caller can warn that the node is left unlimited. A base that
// was read is kept even when the overcommit allowance cannot be: only the
// ceiling is unknown then.
func TestReadHugePagesPoolIsUnknownWhenASysctlCannotBeRead(t *testing.T) {
	t.Parallel()

	missing, missingErr := readHugePagesPool(fakeSysctls(map[string]string{nrHugePagesPath: "1600"}))
	garbage, garbageErr := readHugePagesPool(fakeSysctls(map[string]string{nrHugePagesPath: "1600", nrOvercommitHugePagesPath: "lots"}))
	failing, failingErr := readHugePagesPool(func(string) ([]byte, error) { return nil, errors.New("permission denied") })

	assert.Equal(t, hugePagePool{Base: 1600, BaseRead: true}, missing)
	require.ErrorIs(t, missingErr, fs.ErrNotExist)
	assert.Equal(t, hugePagePool{Base: 1600, BaseRead: true}, garbage)
	require.Error(t, garbageErr)
	require.NotErrorIs(t, garbageErr, fs.ErrNotExist)
	assert.Zero(t, failing)
	require.Error(t, failingErr)
	require.NotErrorIs(t, failingErr, fs.ErrNotExist)
}

// Placement gates on these counters, so they must reflect the host when asked,
// not the last poll: no poll has run here.
func TestGetMemoryMetricsReadsTheHostOnRequest(t *testing.T) {
	t.Parallel()

	memory, err := NewHostMetrics().GetMemoryMetrics(t.Context())
	require.NoError(t, err)

	assert.NotZero(t, memory.TotalBytes)
}

func TestMemoryReadReplacesTheRememberedSample(t *testing.T) {
	t.Parallel()

	h := NewHostMetrics()
	older := &MemoryMetrics{TotalBytes: 1 << 30, HugePagesCapacity: 1600}
	fresh := &MemoryMetrics{TotalBytes: 1 << 30, HugePagesCapacity: 2000}

	_, err := h.keepLastGood(older, nil)
	require.NoError(t, err)

	memory, err := h.keepLastGood(fresh, nil)
	require.NoError(t, err)
	assert.Equal(t, fresh, memory)

	memory, err = h.keepLastGood(nil, errors.New("read /proc/meminfo: input/output error"))
	require.NoError(t, err)
	assert.Equal(t, fresh, memory)
}

func TestMemoryReadFailureServesTheLastGoodSample(t *testing.T) {
	t.Parallel()

	h := NewHostMetrics()
	good := &MemoryMetrics{TotalBytes: 1 << 30, HugePagesCapacity: 2000}

	_, err := h.keepLastGood(good, nil)
	require.NoError(t, err)

	memory, err := h.keepLastGood(nil, errors.New("read /proc/meminfo: input/output error"))
	require.NoError(t, err)
	assert.Equal(t, good, memory)
}

func TestMemoryReadFailureBeforeAnyGoodReadReturnsTheError(t *testing.T) {
	t.Parallel()

	readErr := errors.New("read /proc/meminfo: input/output error")

	memory, err := NewHostMetrics().keepLastGood(nil, readErr)
	require.ErrorIs(t, err, readErr)
	assert.Nil(t, memory)
}
