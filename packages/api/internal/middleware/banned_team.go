package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
)

// EnforceBannedTeam returns the gin middleware that limits a banned team to
// reads and deletes. Only an admin request resolves a banned team; every other
// way to authenticate refuses it outright. Must run after auth.
//
// A read is what a read-only member may reach, so a route that hands out
// something that writes stays closed. Nothing that starts a workload is a
// read or a delete, so a ban's containment is not undone.
func EnforceBannedTeam() gin.HandlerFunc {
	return func(c *gin.Context) {
		team, ok := auth.GetTeamInfo(c)
		if !ok || team == nil || team.Team == nil {
			c.Next()

			return
		}

		if err := auth.CheckTeamBanned(*team.Team); err != nil && !bannedTeamMayReach(c) {
			apierrors.SendAPIStoreError(c, http.StatusForbidden, err.Error())
			c.Abort()

			return
		}

		c.Next()
	}
}

func bannedTeamMayReach(c *gin.Context) bool {
	switch c.Request.Method {
	case http.MethodDelete:
		return true
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		_, denied := readOnlyDeniedReads[c.Request.Method][c.FullPath()]

		return !denied
	default:
		return false
	}
}
