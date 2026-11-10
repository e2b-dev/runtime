package prefetch

import "sync/atomic"

// Node-wide prefetch activity, sampled by a resume to label its trace with the
// neighbours it competed with: runs are mappings being replayed, workers are
// live fetch/copy goroutines, including ones waiting for work.
var (
	activeRuns         atomic.Int64
	activeFetchWorkers atomic.Int64
	activeCopyWorkers  atomic.Int64
)

// Activity is a point-in-time sample of the node's prefetch work.
type Activity struct {
	Runs         int64
	FetchWorkers int64
	CopyWorkers  int64
}

// SampleActivity reads the node's prefetch activity now.
func SampleActivity() Activity {
	return Activity{
		Runs:         activeRuns.Load(),
		FetchWorkers: activeFetchWorkers.Load(),
		CopyWorkers:  activeCopyWorkers.Load(),
	}
}
