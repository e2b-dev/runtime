package team

import (
	"fmt"

	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
)

// CheckTeamBanned returns *ForbiddenError if the team is banned.
// Called inside the shared auth store so every service rejects banned teams
// at auth time without per-handler plumbing.
func CheckTeamBanned(team dbtypes.Team) error {
	if team.IsBanned {
		return &ForbiddenError{Message: "team is banned"}
	}

	return nil
}

// CheckTeamDeleted returns *ForbiddenError if the team was deleted. A deleted
// team keeps its memberships, so this is what stops them from granting access.
func CheckTeamDeleted(team dbtypes.Team) error {
	if team.DeletedAt != nil {
		return &ForbiddenError{Message: "team is deleted"}
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
