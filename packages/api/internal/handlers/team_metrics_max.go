package handlers

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/metrics"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	clickhouse "github.com/e2b-dev/infra/packages/clickhouse/pkg"
	clickhouseUtils "github.com/e2b-dev/infra/packages/clickhouse/pkg/utils"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) GetTeamsTeamIDMetricsMax(c *gin.Context, teamID string, params api.GetTeamsTeamIDMetricsMaxParams) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "team-metrics-max")
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

	var maxMetric clickhouse.MaxTeamMetric
	switch params.Metric {
	case api.ConcurrentSandboxes:
		maxMetric, err = a.clickhouseStore.QueryMaxConcurrentTeamMetrics(ctx, authTeamID.String(), start, end)

	case api.SandboxStartRate:
		maxMetric, err = a.clickhouseStore.QueryMaxStartRateTeamMetrics(ctx, authTeamID.String(), start, end, metrics.ExportPeriod)
	default:
		telemetry.ReportError(ctx, "invalid metric", fmt.Errorf("invalid metric: %s", params.Metric), telemetry.WithTeamID(authTeamID.String()))
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("invalid metric: %s", params.Metric))

		return
	}
	if err != nil {
		telemetry.ReportError(ctx, "error querying max team metrics", err, telemetry.WithTeamID(authTeamID.String()))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error querying max team metrics")

		return
	}

	apiMetrics := api.MaxTeamMetric{
		Timestamp:     maxMetric.Timestamp,
		TimestampUnix: maxMetric.Timestamp.Unix(),
		Value:         float32(maxMetric.Value),
	}

	c.JSON(http.StatusOK, apiMetrics)
}
