package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/events"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// monitoringRetention is how long monitoring and logs data outlives a
// sandbox, independent of the team's events retention.
const monitoringRetention = 7 * 24 * time.Hour

func (a *APIStore) GetUnstableSandboxesSandboxIDRecord(c *gin.Context, sandboxID api.SandboxID) {
	ctx := c.Request.Context()

	sandboxID, err := utils.ShortID(sandboxID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sandbox ID")

		return
	}

	team := auth.MustGetTeamInfo(c)
	teamID := team.ID
	telemetry.SetAttributes(ctx, telemetry.WithTeamID(teamID.String()), telemetry.WithSandboxID(sandboxID))

	row, err := a.sqlcDB.GetSandboxRecordByTeamAndSandboxID(ctx, queries.GetSandboxRecordByTeamAndSandboxIDParams{
		TeamID:    teamID,
		SandboxID: sandboxID,
	})
	// A database without the stopped-sandbox log has no record of any sandbox.
	if dberrors.IsNotFoundError(err) || dberrors.IsUndefinedTable(err) {
		a.sendAPIStoreError(c, http.StatusNotFound, "Sandbox not found or you don't have access to it")

		return
	}
	if err != nil {
		logger.L().Error(ctx, "Error getting sandbox record", zap.Error(err), logger.WithTeamID(teamID.String()), logger.WithSandboxID(sandboxID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting sandbox record")

		return
	}

	var alias *string
	if row.Alias != "" {
		alias = &row.Alias
	}

	eventsRetention := time.Duration(events.RetentionDays(team.Limits.EventsTTLDays)) * 24 * time.Hour

	c.JSON(http.StatusOK, api.SandboxRecord{
		TemplateID:             row.TemplateID,
		Alias:                  alias,
		SandboxID:              row.SandboxID,
		StartedAt:              row.StartedAt,
		StoppedAt:              row.StoppedAt,
		Domain:                 row.Domain,
		CpuCount:               int32(row.Vcpu),
		MemoryMB:               int32(row.RamMb),
		DiskSizeMB:             int32(row.TotalDiskSizeMb),
		RetentionExpired:       stoppedLongerThan(row.StoppedAt, monitoringRetention),
		EventsRetentionExpired: stoppedLongerThan(row.StoppedAt, eventsRetention),
	})
}

func stoppedLongerThan(stoppedAt *time.Time, d time.Duration) bool {
	return stoppedAt != nil && time.Since(*stoppedAt) > d
}
