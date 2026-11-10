package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestSandboxCreateRetryTemplateLookup(t *testing.T) {
	t.Parallel()

	for _, route := range []struct{ name, path string }{
		{name: "v1", path: "/sandboxes"},
		{name: "v2", path: "/v2/sandboxes"},
	} {
		t.Run(route.name, func(t *testing.T) {
			t.Parallel()
			db := testutils.SetupDatabase(t)
			cache := templatecache.NewTemplateCache(db.SqlcClient, redisutils.SetupInstance(t))
			t.Cleanup(func() { require.NoError(t, cache.Close(t.Context())) })
			store := &APIStore{templateCache: cache}
			router := gin.New()
			router.Use(func(c *gin.Context) {
				auth.SetTeamInfoForTest(t, c, &types.Team{Team: &dbtypes.Team{ID: uuid.New(), Slug: "test-team"}})
			})
			router.POST("/sandboxes", store.PostSandboxes)
			router.POST("/v2/sandboxes", func(c *gin.Context) { store.PostV2Sandboxes(c, api.PostV2SandboxesParams{}) })

			for _, tc := range []struct {
				body, wantRetry string
				wantCode        int
				closeDB         bool
			}{
				{body: `{"templateID":"base","timeout":300.0}`, wantCode: http.StatusBadRequest, wantRetry: "false"},
				{body: `{"templateID":"/"}`, wantCode: http.StatusBadRequest, wantRetry: "false"},
				{body: `{"templateID":"missing"}`, wantCode: http.StatusNotFound, wantRetry: "false"},
				{body: `{"templateID":"uncached"}`, wantCode: http.StatusInternalServerError, wantRetry: "true", closeDB: true},
			} {
				if tc.closeDB {
					require.NoError(t, db.SqlcClient.Close())
				}
				req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, route.path, strings.NewReader(tc.body))
				req.Header.Set("Content-Type", "application/json")
				recorder := httptest.NewRecorder()
				router.ServeHTTP(recorder, req)
				require.Equal(t, tc.wantCode, recorder.Code, recorder.Body.String())
				if route.path == "/sandboxes" {
					assert.NotContains(t, recorder.Result().Header, "Should-Retry")
				} else {
					assert.Equal(t, tc.wantRetry, recorder.Result().Header.Get("Should-Retry"))
				}
			}
		})
	}
}
