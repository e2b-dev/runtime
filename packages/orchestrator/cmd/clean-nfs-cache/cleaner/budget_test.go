package cleaner

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// buildBytes is the sized cost of one fixture build from writeFourColdBuilds:
// 4 chunks × 4 MiB plus the flat non-chunk charge.
const buildBytes = 16<<20 + otherFilesBytesEstimate

// writeFourColdBuilds lays down the standard fixture: four builds of 16 MiB each,
// warmest chunk atime 40d / 30d / 2d / 1d ago (cold-a, cold-b, warm-a, warm-b).
func writeFourColdBuilds(t *testing.T, root string) {
	t.Helper()
	now := time.Now()
	builds := map[string]time.Duration{
		"cold-a": 40 * 24 * time.Hour,
		"cold-b": 30 * 24 * time.Hour,
		"warm-a": 2 * 24 * time.Hour,
		"warm-b": 1 * 24 * time.Hour,
	}
	for name, age := range builds {
		for i := range 4 {
			dataDir := storage.MemfileName
			if i%2 == 1 {
				dataDir = storage.RootfsName
			}
			writeChunk(t, root, name, dataDir, i, 4<<20, now.Add(-age))
		}
	}
}

// newBudgetCleaner is newTestCleaner without the unbounded byte target, so the
// byte budget under test is the only thing that selects deletions.
func newBudgetCleaner(root string, mutate func(o *Options)) *Cleaner {
	return newTestCleaner(root, func(o *Options) {
		o.TargetBytesToDelete = 0
		mutate(o)
	})
}

// TestByteBudgetEvictsToMax: cache is 4 × 17 MiB = 68 MiB, budget 40 MiB → must
// free 28 MiB → the two coldest builds (34 MiB) go, the two warm ones stay.
func TestByteBudgetEvictsToMax(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeFourColdBuilds(t, root)

	c := newBudgetCleaner(root, func(o *Options) { o.MaxCacheBytes = 40 << 20 })
	require.NoError(t, c.Clean(t.Context()))

	require.Equal(t, uint64(4*buildBytes), c.CacheBytes.Load())
	require.Equal(t, uint64(4*buildBytes), c.cacheBytesEstimate)
	require.Equal(t, uint64(4*buildBytes-40<<20), c.TargetBytesToDelete)
	require.NoDirExists(t, filepath.Join(root, "cold-a"))
	require.NoDirExists(t, filepath.Join(root, "cold-b"))
	require.DirExists(t, filepath.Join(root, "warm-a"))
	require.DirExists(t, filepath.Join(root, "warm-b"))
	require.Equal(t, int64(2), c.Deleted.Load())
}

// TestByteBudgetUnderMaxDeletesNothing: a cache under the budget is left alone.
func TestByteBudgetUnderMaxDeletesNothing(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeFourColdBuilds(t, root)

	c := newBudgetCleaner(root, func(o *Options) { o.MaxCacheBytes = 100 << 20 })
	require.NoError(t, c.Clean(t.Context()))

	require.Equal(t, uint64(4*buildBytes), c.cacheBytesEstimate)
	require.Equal(t, uint64(0), c.TargetBytesToDelete)
	require.Equal(t, int64(0), c.Deleted.Load())
	for _, b := range []string{"cold-a", "cold-b", "warm-a", "warm-b"} {
		require.DirExists(t, filepath.Join(root, b))
	}
}

// TestByteBudgetDryRunSelectsWithoutDeleting: dry run resolves the budget and
// selects the same builds, but removes nothing.
func TestByteBudgetDryRunSelectsWithoutDeleting(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeFourColdBuilds(t, root)

	c := newBudgetCleaner(root, func(o *Options) {
		o.DryRun = true
		o.MaxCacheBytes = 40 << 20
	})
	require.NoError(t, c.Clean(t.Context()))

	require.Equal(t, uint64(4*buildBytes-40<<20), c.TargetBytesToDelete)
	require.Equal(t, int64(2), c.Deleted.Load(), "dry run counts selections")
	for _, b := range []string{"cold-a", "cold-b", "warm-a", "warm-b"} {
		require.DirExists(t, filepath.Join(root, b))
	}
}

// TestByteBudgetCountsGracedBuilds: builds created within Grace cannot be
// evicted but must still count toward the cache size, or the budget leaks by the
// whole fresh set. btime cannot be backdated, so every fixture build is graced:
// the estimate must equal their full size and the target must be resolved, while
// nothing is deleted and the run is reported under target.
func TestByteBudgetCountsGracedBuilds(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeFourColdBuilds(t, root)

	c := newBudgetCleaner(root, func(o *Options) {
		o.Grace = time.Hour
		o.MaxCacheBytes = 20 << 20
	})
	require.NoError(t, c.Clean(t.Context()))

	require.Equal(t, uint64(4*buildBytes), c.CacheBytes.Load(), "graced builds are sized")
	require.Equal(t, uint64(4*buildBytes-20<<20), c.TargetBytesToDelete)
	require.Equal(t, int64(0), c.Deleted.Load(), "graced builds are never evicted")
	require.Equal(t, int64(4), c.StatxC.Load(), "sizing a graced build costs no chunk statx (only the 4 btime checks)")
}

// TestByteBudgetOffDoesNotSizeGracedBuilds: without a budget the Grace filter
// keeps skipping the data-dir readdir for fresh builds (no new I/O on GCP).
func TestByteBudgetOffDoesNotSizeGracedBuilds(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeChunk(t, root, "fresh", storage.MemfileName, 0, 4096, time.Now())

	c := newTestCleaner(root, func(o *Options) { o.Grace = time.Hour })
	require.NoError(t, c.Clean(t.Context()))

	require.Equal(t, uint64(0), c.CacheBytes.Load())
	require.Equal(t, int64(1), c.ReadDirC.Load(), "root listing only")
}

// TestByteBudgetScalesSampledScan: with build sampling on, the sampled bytes are
// scaled by rootBuilds/scanned to estimate the whole cache.
func TestByteBudgetScalesSampledScan(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeFourColdBuilds(t, root)

	c := newBudgetCleaner(root, func(o *Options) {
		o.DryRun = true
		o.MaxCacheBytes = 1 << 40
		o.BuildSampleMin = 2
		o.BuildSamplePercent = 50
		o.BuildSampleMax = 2
	})
	require.NoError(t, c.Clean(t.Context()))

	require.Equal(t, int64(2), c.BuildsScanned.Load())
	require.Equal(t, uint64(2*buildBytes), c.CacheBytes.Load())
	require.Equal(t, uint64(4*buildBytes), c.cacheBytesEstimate, "2 of 4 scanned → ×2")
}

func TestValidateBudgetOptions(t *testing.T) {
	t.Parallel()
	base := Options{
		Path:                "/cache",
		SampleMinFiles:      8,
		SamplePercent:       10,
		SampleMaxFiles:      64,
		MaxConcurrentScan:   1,
		MaxConcurrentStat:   1,
		MaxConcurrentDelete: 1,
	}
	cases := []struct {
		name   string
		mutate func(o *Options)
		errMsg string
	}{
		{"no policy at all", func(*Options) {}, "must be set"},
		{"budget alone is enough", func(o *Options) { o.MaxCacheBytes = 1 }, ""},
		{"budget with bytes-to-delete", func(o *Options) { o.MaxCacheBytes = 1; o.TargetBytesToDelete = 1 }, "mutually exclusive"},
		{"budget with percent is fine", func(o *Options) { o.MaxCacheBytes = 1; o.TargetDiskUsagePercent = 90 }, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := base
			tc.mutate(&o)
			err := o.Validate()
			if tc.errMsg == "" {
				require.NoError(t, err)

				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestParseDiskInfo(t *testing.T) {
	t.Parallel()

	t.Run("real disk", func(t *testing.T) {
		t.Parallel()
		out := "Filesystem     1024-blocks      Used Available Capacity Mounted on\n" +
			"/dev/sdb        1055762868 950186580  51884024      95% /mnt/disks/e2b\n"
		d, err := parseDiskInfo(out)
		require.NoError(t, err)
		require.Equal(t, uint64(1055762868)*1024, d.Total)
		require.Equal(t, uint64(950186580)*1024, d.Used)
		require.False(t, d.Elastic())
	})

	t.Run("amazon efs reports 8 EiB", func(t *testing.T) {
		t.Parallel()
		// 2^53 KiB = 2^63 B: overflows int64 to a negative total; must stay exact.
		// `df -Pk`: the 48-character source stays on the data line. Plain `df`
		// would wrap it onto its own line and shift every number by a field.
		out := "Filesystem                                        1024-blocks  Used        Available Capacity Mounted on\n" +
			"fs-0123456789abcdef0.efs.us-east-1.amazonaws.com:/ 9007199254740992 0 9007199254740992       0% /cache\n"
		d, err := parseDiskInfo(out)
		require.NoError(t, err)
		require.Equal(t, uint64(1)<<63, d.Total)
		require.Equal(t, uint64(0), d.Used)
		require.True(t, d.Elastic())
	})

	t.Run("header only", func(t *testing.T) {
		t.Parallel()
		_, err := parseDiskInfo("Filesystem 1024-blocks Used Available Capacity Mounted on\n")
		require.Error(t, err)
	})
}
