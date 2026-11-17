package middleware

import (
	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/middleware/otel/metrics"
)

// LabelMetricsWithTeam puts the authenticated team, its cluster and byoc (true
// for every cluster but the local one) on the request's metrics through the
// gin keys the metrics middleware reads once the chain has run, so the team
// the auth middleware resolves further down still labels the request.
func LabelMetricsWithTeam() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		if team, ok := auth.GetTeamInfo(c); ok && team != nil && team.Team != nil {
			clusterID := clusters.WithClusterFallback(team.ClusterID)

			c.Set(metrics.MetricPrefix+"team.id", team.TeamID())
			c.Set(metrics.MetricPrefix+"cluster.id", clusterID.String())
			c.Set(metrics.MetricPrefix+"byoc", clusterID != consts.LocalClusterID)
		}
	}
}
