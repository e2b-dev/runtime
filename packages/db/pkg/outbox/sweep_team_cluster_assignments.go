package outbox

import (
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/e2b-dev/infra/packages/shared/pkg/sharedriver"
)

// SweepTeamClusterAssignments enqueues a SyncTeamClusterAssignment for every
// live team whose cluster the cluster service does not record.
type SweepTeamClusterAssignments struct{}

func (SweepTeamClusterAssignments) Kind() string { return "sweep_team_cluster_assignments" }

// InsertOpts keeps one sweep open at a time: while one is still retrying, the
// next period's is not enqueued behind it.
func (SweepTeamClusterAssignments) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       river.QueueDefault,
		Priority:    sharedriver.PriorityLow,
		MaxAttempts: 100,
		UniqueOpts:  UniqueWhileOpen(),
	}
}

// UniqueWhileOpen makes enqueueing a job a no-op while one with the same
// arguments, enqueued with these options too, has not finished.
func UniqueWhileOpen() river.UniqueOpts {
	return river.UniqueOpts{
		ByArgs: true,
		ByState: []rivertype.JobState{
			rivertype.JobStateAvailable,
			rivertype.JobStatePending,
			rivertype.JobStateRetryable,
			rivertype.JobStateRunning,
			rivertype.JobStateScheduled,
		},
	}
}
