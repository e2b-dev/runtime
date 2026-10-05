package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	clickhouse "github.com/e2b-dev/infra/packages/clickhouse/pkg"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
)

type recordingTeamMetricsStore struct {
	*clickhouse.NoopClient

	mu      sync.Mutex
	teamIDs []string
}

func (s *recordingTeamMetricsStore) record(teamID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.teamIDs = append(s.teamIDs, teamID)
}

func (s *recordingTeamMetricsStore) QueryTeamMetrics(_ context.Context, teamID string, _, _ time.Time, _ time.Duration) ([]clickhouse.TeamMetrics, error) {
	s.record(teamID)

	return nil, nil
}

func (s *recordingTeamMetricsStore) QueryMaxConcurrentTeamMetrics(_ context.Context, teamID string, _, _ time.Time) (clickhouse.MaxTeamMetric, error) {
	s.record(teamID)

	return clickhouse.MaxTeamMetric{}, nil
}

func TestTeamMetricsAcceptPublicProjectID(t *testing.T) {
	t.Parallel()

	teamID := uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479")
	otherTeamID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	for _, tc := range []struct {
		name     string
		pathID   string
		wantCode int
	}{
		{name: "team UUID", pathID: teamID.String(), wantCode: http.StatusOK},
		{name: "public project ID", pathID: id.ProjectID(teamID).String(), wantCode: http.StatusOK},
		{name: "another project's public ID", pathID: id.ProjectID(otherTeamID).String(), wantCode: http.StatusForbidden},
		{name: "workspace ID with the same UUID", pathID: id.WorkspaceID(teamID).String(), wantCode: http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			for endpoint, call := range map[string]func(*APIStore, *gin.Context){
				"metrics": func(store *APIStore, c *gin.Context) {
					store.GetTeamsTeamIDMetrics(c, tc.pathID, api.GetTeamsTeamIDMetricsParams{})
				},
				"max metrics": func(store *APIStore, c *gin.Context) {
					store.GetTeamsTeamIDMetricsMax(c, tc.pathID, api.GetTeamsTeamIDMetricsMaxParams{Metric: api.ConcurrentSandboxes})
				},
			} {
				t.Run(endpoint, func(t *testing.T) {
					t.Parallel()

					recorder := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(recorder)
					c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/teams/"+tc.pathID+"/metrics", nil)
					auth.SetTeamInfoForTest(t, c, &types.Team{Team: &authqueries.Team{ID: teamID}})

					metricsStore := &recordingTeamMetricsStore{NoopClient: clickhouse.NewNoopClient()}
					call(&APIStore{clickhouseStore: metricsStore}, c)

					require.Equal(t, tc.wantCode, recorder.Code, recorder.Body.String())
					if tc.wantCode == http.StatusOK {
						require.Equal(t, []string{teamID.String()}, metricsStore.teamIDs)
					} else {
						require.Empty(t, metricsStore.teamIDs)
					}
				})
			}
		})
	}
}
