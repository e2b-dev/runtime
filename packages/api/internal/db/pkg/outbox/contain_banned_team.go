package outbox

import (
	"github.com/google/uuid"
	"github.com/riverqueue/river"

	"github.com/e2b-dev/infra/packages/shared/pkg/sharedriver"
)

type ContainBannedTeam struct {
	TeamID uuid.UUID `json:"team_id"`
}

func (ContainBannedTeam) Kind() string { return "contain_banned_team" }

func (ContainBannedTeam) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       river.QueueDefault,
		Priority:    sharedriver.PriorityHigh,
		MaxAttempts: 100,
	}
}
