package cleaner

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

func TestCleanDeletesColdestBuilds(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	now := time.Now()

	// Four builds, each 4 chunks of 4 MiB (16 MiB). warmest atime per build:
	//   cold-a: 40d, cold-b: 30d, warm-a: 2d, warm-b: 1d.
	builds := map[string]time.Duration{
		"cold-a": 40 * 24 * time.Hour,
		"cold-b": 30 * 24 * time.Hour,
		"warm-a": 2 * 24 * time.Hour,
		"warm-b": 1 * 24 * time.Hour,
	}
	for name, age := range builds {
		// Split chunks across memfile/ and rootfs.ext4/ so sizing and the warmest
		// sample span both data dirs.
		for i := range 4 {
			dataDir := storage.MemfileName
			if i%2 == 1 {
				dataDir = storage.RootfsName
			}
			writeChunk(t, root, name, dataDir, i, 4<<20, now.Add(-age))
		}
	}

	// Target ~24 MiB → deletes the two coldest whole builds (cold-a, cold-b =
	// 32 MiB) and stops; the two warm builds survive.
	c := NewCleaner(Options{
		Path:                root,
		TargetBytesToDelete: 24 << 20,
		SampleMinFiles:      8,
		SamplePercent:       10,
		SampleMaxFiles:      64,
		MaxConcurrentScan:   4,
		MaxConcurrentStat:   4,
		MaxConcurrentDelete: 4,
	}, logger.NewNopLogger(), nil)

	require.NoError(t, c.Clean(t.Context()))

	require.NoDirExists(t, filepath.Join(root, "cold-a"))
	require.NoDirExists(t, filepath.Join(root, "cold-b"))
	require.DirExists(t, filepath.Join(root, "warm-a"))
	require.DirExists(t, filepath.Join(root, "warm-b"))
	require.Equal(t, int64(2), c.Deleted.Load())
}

// TestDeleteColdestRespectsGraceFloor exercises the atime floor directly on
// deleteColdest (the create-time filter that skips fresh-btime builds runs
// earlier in the scan, so an end-to-end fixture can't reach the floor — btime
// can't be backdated). A build whose warmest chunk was accessed within Grace is
// never queued, even with an unbounded byte target.
func TestDeleteColdestRespectsGraceFloor(t *testing.T) {
	t.Parallel()
	now := time.Now()
	c := NewCleaner(Options{
		TargetBytesToDelete: 1 << 40, // huge: only the Grace floor limits deletion
		Grace:               24 * time.Hour,
		DryRun:              true, // count selections without touching the filesystem
		MaxConcurrentDelete: 2,
	}, logger.NewNopLogger(), nil)

	builds := []build{
		{uuid: "cold", timestamp: now.Add(-30 * 24 * time.Hour).Unix(), size: 1 << 20},
		{uuid: "hot", timestamp: now.Add(-1 * time.Hour).Unix(), size: 1 << 20},
	}

	_, _, stop := c.deleteColdest(t.Context(), builds, c.TargetBytesToDelete, time.Time{}, false)

	require.Equal(t, int64(1), c.Deleted.Load(), "cold build past the floor is deleted; hot build within Grace is not")
	assert.Equal(t, stopAgeFloor, stop)
}

// TestDeleteColdestStopsAtDeadline: once the deadline has passed no build is
// queued, however far the target is. Clean cannot reach this deterministically:
// it lifts a deadline that is already spent before the first delete.
func TestDeleteColdestStopsAtDeadline(t *testing.T) {
	t.Parallel()
	c := NewCleaner(Options{
		TargetBytesToDelete: 1 << 40,
		DryRun:              true,
		MaxConcurrentDelete: 2,
	}, logger.NewNopLogger(), nil)

	builds := []build{{uuid: "cold", timestamp: time.Now().Add(-30 * 24 * time.Hour).Unix(), size: 1 << 20}}

	queued, _, stop := c.deleteColdest(t.Context(), builds, c.TargetBytesToDelete, time.Now().Add(-time.Second), false)

	assert.Equal(t, uint64(0), queued)
	assert.Equal(t, int64(0), c.Deleted.Load())
	assert.Equal(t, stopBudget, stop)
}

func TestClampSample(t *testing.T) {
	t.Parallel()
	// min 8, pct 10, max 64
	require.Equal(t, 3, clampSample(3, 8, 10, 64))     // fewer chunks than min → all
	require.Equal(t, 8, clampSample(50, 8, 10, 64))    // 10% = 5 < min → min
	require.Equal(t, 50, clampSample(500, 8, 10, 64))  // 10% = 50
	require.Equal(t, 64, clampSample(5000, 8, 10, 64)) // 10% = 500 > max → max
}

func TestChunkOnDiskBytes(t *testing.T) {
	t.Parallel()
	// uncompressed .bin: decimal on-disk size
	require.Equal(t, uint64(4194304), chunkOnDiskBytes("000000000905-4194304.bin"))
	// compressed .frm: hex on-disk frame length (0x100000 = 1 MiB)
	require.Equal(t, uint64(0x100000), chunkOnDiskBytes("0000000003c00000-100000.frm"))
	// non-chunk and unparseable files contribute nothing
	require.Equal(t, uint64(0), chunkOnDiskBytes("size.txt"))
	require.Equal(t, uint64(0), chunkOnDiskBytes("garbage"))
}
