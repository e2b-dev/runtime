package middleware

import (
	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/middleware/otel/metrics"
)

// LabelMetricsWithTeam puts the authenticated team on the request's metrics
// through the gin key the metrics middleware reads once the chain has run, so
// the team the auth middleware resolves further down still labels the request.
func LabelMetricsWithTeam() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		if team, ok := auth.GetTeamInfo(c); ok && team != nil && team.Team != nil {
			c.Set(metrics.MetricPrefix+"team.id", team.TeamID())
		}
	}
}
