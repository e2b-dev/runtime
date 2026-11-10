package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func (a *APIStore) GetUnstableTeamsTeamIDStatus(c *gin.Context, teamID api.TeamID) {
	row, ok := a.getPathTeam(c, teamID)
	if !ok {
		return
	}

	c.JSON(http.StatusOK, api.TeamStatusResponse{
		IsBanned:      row.IsBanned,
		IsBlocked:     row.IsBlocked,
		BlockedReason: row.BlockedReason,
	})
}

// getPathTeam reads the team the path names. A team-authenticated request may
// name only its own team; an admin request may name any team, banned included.
func (a *APIStore) getPathTeam(c *gin.Context, pathTeamID string) (authqueries.GetTeamByTeamIDRow, bool) {
	ctx := c.Request.Context()

	teamID, err := id.ParseTeamID(pathTeamID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid team ID")

		return authqueries.GetTeamByTeamIDRow{}, false
	}

	if team, ok := auth.GetTeamInfo(c); ok && team.ID != teamID {
		a.sendAPIStoreError(c, http.StatusForbidden, "Team path parameter does not match authenticated team")

		return authqueries.GetTeamByTeamIDRow{}, false
	}

	row, err := a.authDB.GetTeamByTeamID(ctx, teamID)
	if dberrors.IsNotFoundError(err) {
		a.sendAPIStoreError(c, http.StatusNotFound, "Team not found")

		return authqueries.GetTeamByTeamIDRow{}, false
	}
	if err != nil {
		logger.L().Error(ctx, "Failed to get team", zap.Error(err), logger.WithTeamID(teamID.String()))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get team")

		return authqueries.GetTeamByTeamIDRow{}, false
	}

	return row, true
}
