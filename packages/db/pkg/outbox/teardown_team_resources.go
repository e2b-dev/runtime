// Package outbox holds the River job arguments shared by the service that
// enqueues a job and the API, which works it, through the shared database.
package outbox

import (
	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/e2b-dev/infra/packages/shared/pkg/sharedriver"
)

// TeardownTeamResources releases a deleted team's compute and external
// resources.
type TeardownTeamResources struct {
	TeamID uuid.UUID `json:"team_id"`
}

func (TeardownTeamResources) Kind() string { return "teardown_team_resources" }

// InsertOpts makes a repeat enqueue for a team a no-op while its teardown is
// still open; once it finishes, a new one can be enqueued.
func (TeardownTeamResources) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:    river.QueueDefault,
		Priority: sharedriver.PriorityNormal,
		// At a 15-minute retry cap this keeps a teardown retrying for about a
		// day before River discards it.
		MaxAttempts: 100,
		UniqueOpts: river.UniqueOpts{
			ByArgs: true,
			ByState: []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRetryable,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			},
		},
	}
}
