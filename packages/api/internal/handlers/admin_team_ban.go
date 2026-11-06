package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/team"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const banTeamFailedMessage = "Failed to ban team"

// banRequestBudget bounds a ban request from the moment it is detached from
// the caller: deletion checks, ban writes, cache evictions, kills and
// cancellations. A build delete already under way keeps its own 30 seconds, so
// the request ends by about 55 seconds: inside the 60 seconds callers wait and
// the server's write timeout.
const banRequestBudget = 25 * time.Second

func (a *APIStore) AdminBanTeam(c *gin.Context, teamID uuid.UUID) {
	ctx, cancel := a.teamService.Detach(c.Request.Context())
	defer cancel()

	if a.refuseDeletedTeam(ctx, c, teamID) {
		return
	}

	containment, err := a.teamService.Ban(ctx, teamID)
	if errors.Is(err, team.ErrTeamNotFound) {
		a.sendAPIStoreError(c, http.StatusNotFound, "Team not found")

		return
	}
	if err != nil {
		logger.L().Error(ctx, "Failed to ban team", logger.WithTeamID(teamID.String()), zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, banTeamFailedMessage)

		return
	}

	c.JSON(http.StatusOK, adminTeamBanResult(teamID, containment))
}

func (a *APIStore) AdminUnbanTeam(c *gin.Context, teamID uuid.UUID) {
	ctx := c.Request.Context()

	if a.refuseDeletedTeam(ctx, c, teamID) {
		return
	}

	err := a.teamService.Unban(ctx, teamID)
	if errors.Is(err, team.ErrTeamNotFound) {
		a.sendAPIStoreError(c, http.StatusNotFound, "Team not found")

		return
	}
	if err != nil {
		logger.L().Error(ctx, "Failed to unban team", logger.WithTeamID(teamID.String()), zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to unban team")

		return
	}

	c.Status(http.StatusNoContent)
}

func (a *APIStore) AdminBanTeams(c *gin.Context) {
	// A caller that hangs up must not leave the later teams unchecked and
	// unbanned while the earlier ones are banned.
	ctx, cancel := a.teamService.Detach(c.Request.Context())
	defer cancel()

	body, err := ginutils.ParseBody[api.AdminTeamsBanRequest](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		return
	}

	result := api.AdminTeamsBanResult{
		Banned:   []api.AdminTeamBanResult{},
		NotFound: []uuid.UUID{},
		Failed:   []api.AdminTeamBanFailure{},
	}
	live := make([]uuid.UUID, 0, len(body.TeamIDs))
	for _, teamID := range body.TeamIDs {
		deleted, err := a.IsProjectDeleted(ctx, teamID)
		switch {
		case err != nil:
			logger.L().Error(ctx, "Failed to check team deletion", logger.WithTeamID(teamID.String()), zap.Error(err))
			result.Failed = append(result.Failed, api.AdminTeamBanFailure{TeamID: teamID, Error: banTeamFailedMessage})
		case deleted:
			result.NotFound = append(result.NotFound, teamID)
		default:
			live = append(live, teamID)
		}
	}

	for _, outcome := range a.teamService.BanTeams(ctx, live) {
		switch {
		case outcome.Err == nil:
			result.Banned = append(result.Banned, adminTeamBanResult(outcome.TeamID, outcome.Containment))
		case errors.Is(outcome.Err, team.ErrTeamNotFound):
			result.NotFound = append(result.NotFound, outcome.TeamID)
		default:
			logger.L().Error(ctx, "Failed to ban team", logger.WithTeamID(outcome.TeamID.String()), zap.Error(outcome.Err))
			result.Failed = append(result.Failed, api.AdminTeamBanFailure{TeamID: outcome.TeamID, Error: banTeamFailedMessage})
		}
	}

	c.JSON(http.StatusOK, result)
}

// refuseDeletedTeam answers 404 for a deleted team, which keeps its row but is
// gone to every caller.
func (a *APIStore) refuseDeletedTeam(ctx context.Context, c *gin.Context, teamID uuid.UUID) bool {
	deleted, err := a.IsProjectDeleted(ctx, teamID)
	if err != nil {
		logger.L().Error(ctx, "Failed to check team deletion", logger.WithTeamID(teamID.String()), zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to check team")

		return true
	}
	if deleted {
		a.sendAPIStoreError(c, http.StatusNotFound, "Team not found")

		return true
	}

	return false
}

// The service logs what failed; callers get a fixed message.
func adminTeamBanResult(teamID uuid.UUID, containment team.Containment) api.AdminTeamBanResult {
	result := api.AdminTeamBanResult{
		TeamID:                teamID,
		SandboxesKilled:       containment.SandboxesKilled,
		SandboxesFailedToKill: containment.SandboxesFailedToKill,
		BuildsCancelled:       containment.BuildsCancelled,
		BuildsFailedToCancel:  containment.BuildsFailedToCancel,
	}
	if containment.SandboxKillErr != nil {
		result.SandboxKillError = new("Failed to kill the team's sandboxes")
	}
	if containment.BuildCancelErr != nil {
		result.BuildCancelError = new("Failed to cancel the team's builds")
	}

	return result
}
