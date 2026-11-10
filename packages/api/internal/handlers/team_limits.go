package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/api/internal/api"
)

func (a *APIStore) GetUnstableTeamsTeamIDLimits(c *gin.Context, teamID api.TeamID) {
	row, ok := a.getPathTeam(c, teamID)
	if !ok {
		return
	}

	c.JSON(http.StatusOK, api.TeamLimitsResponse{
		Limits: api.TeamLimits{
			MaxLengthHours:           row.MaxLengthHours,
			ConcurrentSandboxes:      row.ConcurrentSandboxes,
			ConcurrentTemplateBuilds: row.ConcurrentTemplateBuilds,
			MaxVcpu:                  row.MaxVcpu,
			MaxRamMb:                 row.MaxRamMb,
			DiskMb:                   row.DiskMb,
			EventsTtlDays:            row.EventsTtlDays,
			DefaultFreeDiskSizeMb:    row.DefaultFreeDiskSizeMb,
			MaxFreeDiskSizeMb:        row.MaxFreeDiskSizeMb,
			ApiTeamRpsList:           row.ApiTeamRpsList,
			ApiTeamRpsDelete:         row.ApiTeamRpsDelete,
			MaxSecrets:               row.MaxSecrets,
		},
	})
}
