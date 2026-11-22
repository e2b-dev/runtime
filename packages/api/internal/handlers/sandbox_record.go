package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/events"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/postgres/dberrors"
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

	// The team's current cluster decides the domain; the local cluster and a
	// cluster without its own domain leave the default.
	domain, err := a.teamClusterDomain(ctx, row.ClusterID)
	if err != nil {
		logger.L().Error(ctx, "Error reading the team's cluster", zap.Error(err), logger.WithTeamID(teamID.String()), logger.WithSandboxID(sandboxID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting sandbox record")

		return
	}

	eventsRetention := time.Duration(events.RetentionDays(team.Limits.EventsTTLDays)) * 24 * time.Hour

	c.JSON(http.StatusOK, api.SandboxRecord{
		TemplateID:             row.TemplateID,
		Alias:                  alias,
		SandboxID:              row.SandboxID,
		StartedAt:              row.StartedAt,
		StoppedAt:              row.StoppedAt,
		Domain:                 domain,
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

// teamClusterDomain reads the sandbox domain from the cluster pool, which the
// sync keeps current, and asks the cluster service only for a cluster the pool
// has not picked up yet.
func (a *APIStore) teamClusterDomain(ctx context.Context, clusterID *uuid.UUID) (*string, error) {
	if clusterID == nil {
		return nil, nil
	}
	if cluster, ok := a.clusters.GetClusterById(*clusterID); ok {
		return cluster.SandboxDomain, nil
	}

	cluster, err := a.lookUpCluster(ctx, clusterID)
	if err != nil || cluster == nil {
		return nil, err
	}

	return optionalOf(cluster.HasSandboxProxyDomain(), cluster.GetSandboxProxyDomain()), nil
}
