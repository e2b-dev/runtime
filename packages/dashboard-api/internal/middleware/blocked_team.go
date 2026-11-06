package middleware

import (
	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
)

// blockedTeamAllowlist enumerates routes a blocked team MAY still reach on
// the dashboard-api service: none. User-only and admin routes are omitted:
// they have no team on the gin context so the middleware short-circuits.
var blockedTeamAllowlist = auth.BlockedTeamAllowlist{}

// EnforceBlockedTeam returns the gin middleware for the dashboard-api
// service, configured with the dashboard-specific blocked-team allowlist.
func EnforceBlockedTeam() gin.HandlerFunc {
	return auth.EnforceBlockedTeam(blockedTeamAllowlist)
}
