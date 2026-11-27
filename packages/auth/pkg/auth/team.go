package auth

import (
	"github.com/gin-gonic/gin"

	internalauthteam "github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/team"
	"github.com/e2b-dev/infra/packages/auth/pkg/teamdata"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

type BlockedTeamAllowlist = internalauthteam.BlockedTeamAllowlist

func CheckTeamBlocked(team *types.Team) error {
	return internalauthteam.CheckTeamBlocked(team)
}

func CheckTeamAccess(c *gin.Context, team *types.Team, allowlist BlockedTeamAllowlist) error {
	return internalauthteam.CheckTeamAccess(c, team, allowlist)
}

type ReadOnlyDeniedReads = internalauthteam.ReadOnlyDeniedReads

func CheckReadOnlyMemberForRoute(c *gin.Context, team *types.Team, deniedReads ReadOnlyDeniedReads) error {
	return internalauthteam.CheckReadOnlyMemberForRoute(c, team, deniedReads)
}

func EnforceReadOnlyMember(deniedReads ReadOnlyDeniedReads) gin.HandlerFunc {
	return internalauthteam.EnforceReadOnlyMember(deniedReads)
}

func EnforceBlockedTeam(allowlist BlockedTeamAllowlist) gin.HandlerFunc {
	return internalauthteam.EnforceBlockedTeam(allowlist)
}

func CheckTeamBanned(team teamdata.Team) error {
	return internalauthteam.CheckTeamBanned(team)
}
