package team

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/authcontext"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
)

// BlockedTeamAllowlist maps HTTP method to gin route patterns (c.FullPath())
// that blocked teams MAY still reach.
type BlockedTeamAllowlist map[string]map[string]struct{}

// Allows reports whether the matched route + method on c is in the
// allowlist. Nil-safe.
func (a BlockedTeamAllowlist) Allows(c *gin.Context) bool {
	if a == nil {
		return false
	}

	_, ok := a[c.Request.Method][c.FullPath()]

	return ok
}

// CheckBlockedTeamForRoute returns *TeamBlockedError if team is blocked
// and the matched route is not in allowlist.
func CheckBlockedTeamForRoute(c *gin.Context, team *types.Team, allowlist BlockedTeamAllowlist) error {
	err := CheckTeamBlocked(team)
	if err == nil {
		return nil
	}

	if allowlist.Allows(c) {
		return nil
	}

	return err
}

// CheckTeamAccess returns an error if team is banned, or if team is
// blocked and the matched route is not in allowlist.
func CheckTeamAccess(c *gin.Context, team *types.Team, allowlist BlockedTeamAllowlist) error {
	if team == nil || team.Team == nil {
		return nil
	}

	if err := CheckTeamBanned(*team.Team); err != nil {
		return err
	}

	return CheckBlockedTeamForRoute(c, team, allowlist)
}

// ReadOnlyDeniedReads maps HTTP method to gin route patterns (c.FullPath())
// that a read-only member may NOT reach although the method is a read: the
// route hands out something that writes.
type ReadOnlyDeniedReads map[string]map[string]struct{}

// CheckReadOnlyMemberForRoute returns *ForbiddenError if the user's role on
// team is read-only and the matched route changes something. Every method but
// GET, HEAD and OPTIONS does, and so do the routes in deniedReads.
func CheckReadOnlyMemberForRoute(c *gin.Context, team *types.Team, deniedReads ReadOnlyDeniedReads) error {
	if team == nil || !team.ReadOnly() {
		return nil
	}

	switch c.Request.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		if _, denied := deniedReads[c.Request.Method][c.FullPath()]; !denied {
			return nil
		}
	}

	return &ForbiddenError{Message: "your role on this team is read-only"}
}

// EnforceReadOnlyMember returns a gin middleware that denies a read-only
// member with 403 on every route that changes something. Must run after auth.
func EnforceReadOnlyMember(deniedReads ReadOnlyDeniedReads) gin.HandlerFunc {
	return func(c *gin.Context) {
		team, ok := authcontext.GetTeamInfo(c)
		if !ok {
			c.Next()

			return
		}

		if err := CheckReadOnlyMemberForRoute(c, team, deniedReads); err != nil {
			apierrors.SendAPIStoreError(c, http.StatusForbidden, err.Error())
			c.Abort()

			return
		}

		c.Next()
	}
}

// EnforceBlockedTeam returns a gin middleware that denies blocked teams
// with 403 unless the matched route is in allowlist. Must run after auth.
func EnforceBlockedTeam(allowlist BlockedTeamAllowlist) gin.HandlerFunc {
	return func(c *gin.Context) {
		team, ok := authcontext.GetTeamInfo(c)
		if !ok || team == nil {
			c.Next()

			return
		}

		if err := CheckBlockedTeamForRoute(c, team, allowlist); err != nil {
			apierrors.SendAPIStoreError(c, http.StatusForbidden, err.Error())
			c.Abort()

			return
		}

		c.Next()
	}
}
