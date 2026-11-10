package cleaner

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRunBudget runs Clean over the four-build fixture with a two-build sample.
// Without a budget a run deletes from one sample only; with one it scans further
// samples only while the target is unmet. A budget already spent by the first
// delete does not stop the first batch, and byte-budget mode never batches.
func TestRunBudget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		budget      time.Duration
		sampleMax   int
		samplePct   int
		target      uint64
		maxCache    uint64
		wantScanned int64
		wantDeleted int64
	}{
		{
			name:        "no budget deletes from one sample only",
			budget:      0,
			sampleMax:   2,
			samplePct:   100,
			target:      3 * buildBytes,
			wantScanned: 2,
			wantDeleted: 2,
		},
		{
			name:        "budget scans further samples and stops at the target",
			budget:      time.Hour,
			sampleMax:   2,
			samplePct:   100,
			target:      3 * buildBytes,
			wantScanned: 4,
			wantDeleted: 3,
		},
		{
			name:        "budget scans no further once the first sample meets the target",
			budget:      time.Hour,
			sampleMax:   2,
			samplePct:   100,
			target:      buildBytes,
			wantScanned: 2,
			wantDeleted: 1,
		},
		{
			name:        "budget spent before the first delete still deletes the first batch",
			budget:      time.Nanosecond,
			sampleMax:   0,
			samplePct:   100,
			target:      1 << 50,
			wantScanned: 4,
			wantDeleted: 4,
		},
		{
			name:        "byte budget never batches",
			budget:      time.Hour,
			sampleMax:   2,
			samplePct:   100,
			maxCache:    buildBytes,
			wantScanned: 2,
			wantDeleted: 2,
		},
		{
			name:        "spent budget deletes the first sample and starts no second",
			budget:      time.Nanosecond,
			sampleMax:   2,
			samplePct:   100,
			target:      3 * buildBytes,
			wantScanned: 2,
			wantDeleted: 2,
		},
		{
			name:        "zero-size sample scans nothing",
			budget:      time.Hour,
			sampleMax:   2,
			samplePct:   0,
			target:      1 << 50,
			wantScanned: 0,
			wantDeleted: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			writeFourColdBuilds(t, root)

			c := newTestCleaner(root, func(o *Options) {
				o.TargetBytesToDelete = tt.target
				o.MaxCacheBytes = tt.maxCache
				o.BuildSampleMax = tt.sampleMax
				o.BuildSamplePercent = tt.samplePct
				o.RunBudget = tt.budget
			})
			require.NoError(t, c.Clean(t.Context()))

			remaining, err := os.ReadDir(root)
			require.NoError(t, err)
			assert.Equal(t, tt.wantScanned, c.BuildsScanned.Load())
			assert.Equal(t, tt.wantDeleted, c.Deleted.Load())
			assert.Len(t, remaining, 4-int(tt.wantDeleted))
		})
	}
}
