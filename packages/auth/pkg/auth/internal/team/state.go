package team

import (
	"fmt"

	"github.com/e2b-dev/infra/packages/auth/pkg/teamdata"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

func CheckTeamBanned(team teamdata.Team) error {
	if team.IsBanned {
		return &ForbiddenError{Message: "team is banned"}
	}

	return nil
}

// CheckTeamBlocked returns *BlockedError if the team is blocked.
// Called inline at any handler that creates or mutates a billable resource.
// Each service decides for itself which endpoints need it.
//
// Handlers without team context are allowed through.
func CheckTeamBlocked(team *types.Team) error {
	if team == nil || team.Team == nil || !team.IsBlocked {
		return nil
	}

	msg := "team is blocked"
	if team.BlockedReason != nil && *team.BlockedReason != "" {
		msg = fmt.Sprintf("%s: %s", msg, *team.BlockedReason)
	}

	return &BlockedError{Message: msg}
}
