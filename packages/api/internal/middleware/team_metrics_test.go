package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/middleware/otel/metrics"
)

// The team is resolved inside the chain, after the metrics middleware has
// started timing the request, and still labels the request's histogram.
func TestLabelMetricsWithTeam(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		team *types.Team
		want []string
	}{
		"authenticated":         {team: &types.Team{Team: &dbtypes.Team{ID: uuid.MustParse("11111111-1111-1111-1111-111111111111")}}, want: []string{"11111111-1111-1111-1111-111111111111"}},
		"no team":               {team: nil, want: nil},
		"team without a record": {team: &types.Team{}, want: nil},
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

			var teams []string
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					hist, ok := m.Data.(metricdata.Histogram[float64])
					if !ok {
						continue
					}
					for _, dp := range hist.DataPoints {
						if v, ok := dp.Attributes.Value("team.id"); ok {
							teams = append(teams, v.AsString())
						}
					}
				}
			}

			assert.Equal(t, tc.want, teams)
		})
	}
}
