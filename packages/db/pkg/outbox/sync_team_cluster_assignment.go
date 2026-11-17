package outbox

import (
	"github.com/google/uuid"
	"github.com/riverqueue/river"

	"github.com/e2b-dev/infra/packages/shared/pkg/sharedriver"
)

// SyncTeamClusterAssignment brings the cluster service's record of a team's
// cluster in line with the team's row.
type SyncTeamClusterAssignment struct {
	TeamID uuid.UUID `json:"team_id"`
}

func (SyncTeamClusterAssignment) Kind() string { return "sync_team_cluster_assignment" }

// InsertOpts leaves the job non-unique: one enqueued while another is running
// has to run after it, or the change that enqueued it is never read.
func (SyncTeamClusterAssignment) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       river.QueueDefault,
		Priority:    sharedriver.PriorityNormal,
		MaxAttempts: 100,
	}
}
