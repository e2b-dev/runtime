package handlers

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	clickhouseUtils "github.com/e2b-dev/infra/packages/clickhouse/pkg/utils"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const defaultTimeRange = 7 * 24 * time.Hour // 7 days

func (a *APIStore) GetTeamsTeamIDMetrics(c *gin.Context, teamID string, params api.GetTeamsTeamIDMetricsParams) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "team-metrics")
	defer span.End()

	authTeamID := auth.MustGetTeamID(c)

	if !teamIDMatches(authTeamID, teamID) {
		telemetry.ReportError(ctx, "team ids mismatch", fmt.Errorf("you (%s) are not authorized to access this team's (%s) metrics", authTeamID, teamID), telemetry.WithTeamID(authTeamID.String()))
		a.sendAPIStoreError(c, http.StatusForbidden, fmt.Sprintf("You (%s) are not authorized to access this team's (%s) metrics", authTeamID, teamID))

		return
	}

	// Default time range is the last 7 days
	start, end := time.Now().Add(-defaultTimeRange), time.Now()
	if params.Start != nil {
		start = time.Unix(*params.Start, 0)
	}

	if params.End != nil {
		end = time.Unix(*params.End, 0)
	}

	start, end, err := clickhouseUtils.ValidateRange(start, end)
	if err != nil {
		telemetry.ReportError(ctx, "error validating dates", err, telemetry.WithTeamID(authTeamID.String()))
		a.sendAPIStoreError(c, http.StatusBadRequest, err.Error())

		return
	}

	step := clickhouseUtils.CalculateStep(start, end)

	metrics, err := a.clickhouseStore.QueryTeamMetrics(ctx, authTeamID.String(), start, end, step)
	if err != nil {
		telemetry.ReportError(ctx, "error fetching team metrics", err, telemetry.WithTeamID(authTeamID.String()))
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("error querying team metrics: %s", err))

		return
	}

	apiMetrics := make([]api.TeamMetric, len(metrics))
	for i, m := range metrics {
		apiMetrics[i] = api.TeamMetric{
			Timestamp:           m.Timestamp,
			TimestampUnix:       m.Timestamp.Unix(),
			ConcurrentSandboxes: int32(m.ConcurrentSandboxes),
			SandboxStartRate:    float32(m.SandboxStartedRate),
		}
	}

	c.JSON(http.StatusOK, apiMetrics)
}
