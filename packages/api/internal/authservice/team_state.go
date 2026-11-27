package authservice

import (
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/teamdata"
)

// checkTeamDeleted returns *auth.TeamForbiddenError if the team was deleted. A deleted
// team keeps its memberships, so this is what stops them from granting access.
func checkTeamDeleted(team teamdata.Team) error {
	if team.DeletedAt != nil {
		return &auth.TeamForbiddenError{Message: "team is deleted"}
	}

	return nil
}
