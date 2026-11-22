package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	dbtypes "github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/middleware/otel/metrics"
)

// The team is resolved inside the chain, after the metrics middleware has
// started timing the request, and still labels the request's histogram with
// itself and its cluster.
func TestLabelMetricsWithTeam(t *testing.T) {
	t.Parallel()

	teamID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	byocCluster := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	for name, tc := range map[string]struct {
		team *types.Team
		want map[attribute.Key]attribute.Value
	}{
		"local cluster team": {
			team: &types.Team{Team: &dbtypes.Team{ID: teamID}},
			want: map[attribute.Key]attribute.Value{
				"team.id":    attribute.StringValue(teamID.String()),
				"cluster.id": attribute.StringValue(consts.LocalClusterID.String()),
				"byoc":       attribute.BoolValue(false),
			},
		},
		"byoc cluster team": {
			team: &types.Team{Team: &dbtypes.Team{ID: teamID, ClusterID: &byocCluster}},
			want: map[attribute.Key]attribute.Value{
				"team.id":    attribute.StringValue(teamID.String()),
				"cluster.id": attribute.StringValue(byocCluster.String()),
				"byoc":       attribute.BoolValue(true),
			},
		},
		"no team":               {team: nil, want: map[attribute.Key]attribute.Value{}},
		"team without a record": {team: &types.Team{}, want: map[attribute.Key]attribute.Value{}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			reader := sdkmetric.NewManualReader()
			router := gin.New()
			router.Use(metrics.Middleware(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)), "test"), LabelMetricsWithTeam())
			router.GET("/sandboxes", func(c *gin.Context) {
				if tc.team != nil {
					auth.SetTeamInfoForTest(t, c, tc.team)
				}
				c.Status(http.StatusOK)
			})

			router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/sandboxes", nil))

			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &rm))

			var points []metricdata.HistogramDataPoint[float64]
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					if hist, ok := m.Data.(metricdata.Histogram[float64]); ok {
						points = append(points, hist.DataPoints...)
					}
				}
			}
			require.Len(t, points, 1)

			got := map[attribute.Key]attribute.Value{}
			for _, key := range []attribute.Key{"team.id", "cluster.id", "byoc"} {
				if v, ok := points[0].Attributes.Value(key); ok {
					got[key] = v
				}
			}

			assert.Equal(t, tc.want, got)
		})
	}
}
