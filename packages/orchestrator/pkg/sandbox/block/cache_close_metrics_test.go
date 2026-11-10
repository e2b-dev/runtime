//go:build linux

package block

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

//nolint:paralleltest // swaps the package-level cache instruments
func TestCacheCloseRecordsEachStep(t *testing.T) {
	bs := int64(os.Getpagesize())

	for name, tc := range map[string]struct {
		size       int64
		failUnmap  bool
		wantCounts map[string]uint64
	}{
		"mapped":       {size: 16 * bs, wantCounts: map[string]uint64{"unmap/ok": 1, "remove/ok": 1}},
		"size zero":    {size: 0, wantCounts: map[string]uint64{"remove/ok": 1}},
		"failed unmap": {size: 16 * bs, failUnmap: true, wantCounts: map[string]uint64{"unmap/error": 1, "remove/ok": 1}},
	} {
		t.Run(name, func(t *testing.T) {
			reader := swapCacheInstruments(t)

			cache, err := NewCache(tc.size, bs, t.TempDir()+"/cache", false)
			require.NoError(t, err)
			if tc.failUnmap {
				// Unmapping early leaves an empty mapping, which unix.Munmap rejects with EINVAL.
				require.NoError(t, cache.mmap.Unmap())
			}

			err = cache.Close()
			if tc.failUnmap {
				require.ErrorIs(t, err, unix.EINVAL)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantCounts, histogramCounts(t, reader, telemetry.OrchestratorBlockCacheCloseDuration, "op", "result"))
		})
	}
}
