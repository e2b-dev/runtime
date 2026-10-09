package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/middleware/idempotency"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	sharedmiddleware "github.com/e2b-dev/infra/packages/shared/pkg/middleware"
	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestPostV2SandboxesSuppliesIdempotencyFields(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	source := ldtestdata.DataSource()
	source.Update(source.Flag(featureflags.APIIdempotencyRoutes.Key()).ValueForAll(ldvalue.CopyArbitraryValue(map[string]bool{"POST /v2/sandboxes": true})))
	flags, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, flags.Close(context.WithoutCancel(t.Context()))) })
	team := &types.Team{Team: &authqueries.Team{ID: uuid.New()}}
	r := gin.New()
	r.Use(func(c *gin.Context) { auth.SetTeamInfoForTest(t, c, team) })
	r.Use(sharedmiddleware.RequestTimeout(time.Minute), idempotency.Middleware(client, flags))
	store := &APIStore{}
	calls := 0
	r.POST("/v2/sandboxes", func(c *gin.Context) {
		calls++
		store.PostV2Sandboxes(c, api.PostV2SandboxesParams{})
	})

	var original string
	for _, body := range []string{`{"templateID":"/"}`, `{"templateID":"/","ignored":true}`} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v2/sandboxes", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "samerequest")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), "Invalid template reference")
		if original == "" {
			original = w.Body.String()
		} else {
			require.Equal(t, original, w.Body.String())
		}
	}
	require.Equal(t, 2, calls)
	keys, _, err := client.Scan(t.Context(), 0, "api:idempotency:"+team.ID.String()+":*", 10).Result()
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.True(t, client.HExists(t.Context(), keys[0], "response").Val())
}
