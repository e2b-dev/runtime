package idempotency

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	sharedmiddleware "github.com/e2b-dev/infra/packages/shared/pkg/middleware"
	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestGenericExecutor(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	flags, source := testFlags(t, true, 86400)
	source.Update(source.Flag(featureflags.APIIdempotencyRoutes.Key()).ValueForAll(ldvalue.CopyArbitraryValue(map[string]bool{
		"POST /widgets/:id":   true,
		"PUT /widgets/:id":    true,
		"POST /blobs":         true,
		"DELETE /widgets/:id": true,
	})))
	team := &types.Team{Team: &authqueries.Team{ID: uuid.New()}}
	r := gin.New()
	r.Use(func(c *gin.Context) {
		auth.SetTeamInfoForTest(t, c, team)
		c.Header("X-Request-ID", c.GetHeader("X-Request-ID"))
		c.Header("RateLimit-Remaining", c.GetHeader("RateLimit-Remaining"))
	})
	r.Use(sharedmiddleware.RequestTimeout(time.Minute), Middleware(client, flags))
	calls := 0
	widget := func(c *gin.Context) {
		var body struct {
			Value string `json:"value"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.AbortWithStatus(http.StatusBadRequest)

			return
		}
		if c.GetHeader("X-Deny") == "true" {
			c.AbortWithStatus(http.StatusForbidden)
		}
		Execute(c, body, func() {
			calls++
			c.Header("Location", "/widgets/"+c.Param("id"))
			c.Header("ETag", `"created"`)
			c.Header("X-Request-ID", "handler-request-id")
			c.Header("RateLimit-Remaining", "handler-limit")
			c.JSON(http.StatusCreated, body)
		})
	}
	r.POST("/widgets/:id", widget)
	r.PUT("/widgets/:id", widget)
	first := methodRequest(t.Context(), r, "POST", "/widgets/one?mode=a", `{"value":"a"}`, []string{"key"})
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	require.Equal(t, "handler-request-id", first.Header().Get("X-Request-ID"))
	require.Equal(t, "handler-limit", first.Header().Get("RateLimit-Remaining"))
	require.Equal(t, "/widgets/one", first.Header().Get("Location"))
	require.Equal(t, `"created"`, first.Header().Get("ETag"))
	require.Equal(t, "application/json; charset=utf-8", first.Header().Get("Content-Type"))
	second := methodRequest(t.Context(), r, "POST", "/widgets/one?mode=a", `{ "value": "a" }`, []string{"key"}, "Content-Type", "application/json; charset=utf-8", "X-Request-ID", "retry", "RateLimit-Remaining", "10")
	require.Equal(t, first.Code, second.Code)
	require.Equal(t, first.Body.String(), second.Body.String())
	require.Empty(t, second.Header().Get("Location"))
	require.Empty(t, second.Header().Get("ETag"))
	require.Equal(t, first.Header().Get("Content-Type"), second.Header().Get("Content-Type"))
	require.Equal(t, "retry", second.Header().Get("X-Request-ID"))
	require.Equal(t, "10", second.Header().Get("RateLimit-Remaining"))
	for _, change := range []struct{ method, target, body string }{
		{"PUT", "/widgets/one?mode=a", `{"value":"a"}`},
		{"POST", "/widgets/two?mode=a", `{"value":"a"}`},
		{"POST", "/widgets/one?mode=b", `{"value":"a"}`},
		{"POST", "/widgets/one?mode=a", `{"value":"b"}`},
	} {
		w := methodRequest(t.Context(), r, change.method, change.target, change.body, []string{"key"})
		require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), `"error_code":"idempotency_request_mismatch"`)
	}
	denied := methodRequest(t.Context(), r, "POST", "/widgets/one?mode=a", `{"value":"a"}`, []string{"key"}, "X-Deny", "true")
	require.Equal(t, http.StatusForbidden, denied.Code)
	require.Equal(t, 1, calls)

	r.POST("/blobs", func(c *gin.Context) {
		body, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		Execute(c, body, func() {
			calls++
			_, err := c.Writer.Write(body)
			require.NoError(t, err)
		})
	})
	blob := string([]byte{0, 255, 'a'})
	first = methodRequest(t.Context(), r, "POST", "/blobs", blob, []string{"blob"}, "Content-Type", "application/octet-stream")
	second = methodRequest(t.Context(), r, "POST", "/blobs", blob, []string{"blob"}, "Content-Type", "text/plain")
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, first.Code, second.Code)
	require.Equal(t, blob, second.Body.String())
	require.NotContains(t, first.Header(), "Content-Type")
	require.NotContains(t, second.Header(), "Content-Type")
	require.Equal(t, 2, calls)
	changed := methodRequest(t.Context(), r, "POST", "/blobs", "different", []string{"blob"}, "Content-Type", "application/octet-stream")
	require.Equal(t, http.StatusConflict, changed.Code)

	r.DELETE("/widgets/:id", func(c *gin.Context) {
		Execute(c, nil, func() {
			calls++
			c.Status(http.StatusNoContent)
		})
	})
	for range 2 {
		w := methodRequest(t.Context(), r, "DELETE", "/widgets/one", "", []string{"delete"}, "Content-Type", "")
		require.Equal(t, http.StatusNoContent, w.Code)
		require.Empty(t, w.Body.String())
		require.NotContains(t, w.Header(), "Content-Type")
	}
	require.Equal(t, 3, calls)
}

func TestExecutorRouteEnablement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		routes    any
		anonymous bool
		status    int
	}{
		{"null disables all", nil, false, 204},
		{"empty disables all", map[string]bool{}, false, 204},
		{"different method", map[string]bool{"PUT /widgets/:id": true}, false, 204},
		{"concrete path is not pattern", map[string]bool{"POST /widgets/one": true}, false, 204},
		{"false disables route", map[string]bool{"POST /widgets/:id": false}, false, 204},
		{"invalid value bypasses", map[string]string{"POST /widgets/:id": "true"}, false, 204},
		{"true enables route", map[string]bool{"POST /widgets/:id": true}, false, 503},
		{"no team has no scope", map[string]bool{"POST /widgets/:id": true}, true, 204},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			flags, source := testFlags(t, true, 86400)
			source.Update(source.Flag(featureflags.APIIdempotencyRoutes.Key()).ValueForAll(ldvalue.CopyArbitraryValue(tc.routes)))
			r := gin.New()
			r.Use(func(c *gin.Context) {
				if !tc.anonymous {
					auth.SetTeamInfoForTest(t, c, &types.Team{Team: &authqueries.Team{ID: uuid.New()}})
				}
			})
			r.Use(sharedmiddleware.RequestTimeout(time.Minute), middleware(faultStore{reserveErr: errors.New("redis unavailable")}, flags))
			calls := 0
			r.POST("/widgets/:id", func(c *gin.Context) {
				Execute(c, nil, func() {
					calls++
					c.Status(http.StatusNoContent)
				})
			})
			w := request(t.Context(), r, "/widgets/one", "", []string{"key"})
			require.Equal(t, tc.status, w.Code, w.Body.String())
			require.Equal(t, tc.status == 204, calls == 1)
			invalidKeys := [][]string{{""}, {"a", "b"}, {strings.Repeat("a", 256)}, {"\xff"}, {"ABC"}, {"abc-123"}, {" a"}, {"\u754c"}, {"😀"}}
			for _, keys := range invalidKeys {
				w := request(t.Context(), r, "/widgets/one", "", keys)
				if tc.status == http.StatusServiceUnavailable {
					require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
				} else {
					require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
				}
			}
			wantCalls := 0
			if tc.status == http.StatusNoContent {
				wantCalls = 1 + len(invalidKeys)
			}
			require.Equal(t, wantCalls, calls)
		})
	}
}

func TestExecuteWithoutMiddleware(t *testing.T) {
	t.Parallel()
	r := gin.New()
	r.POST("/widgets", func(c *gin.Context) {
		Execute(c, nil, func() { c.Status(http.StatusCreated) })
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/widgets", strings.NewReader("")))
	require.Equal(t, http.StatusCreated, w.Code)
}

func TestExecutorRequiresRequestDeadline(t *testing.T) {
	t.Parallel()
	flags, _ := testFlags(t, false, 86400)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		auth.SetTeamInfoForTest(t, c, &types.Team{Team: &authqueries.Team{ID: uuid.New()}})
	})
	r.Use(middleware(nil, flags))
	r.POST("/v2/sandboxes", func(c *gin.Context) {
		Execute(c, nil, func() { t.Error("executed without a request deadline") })
	})
	w := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestPanicBeforeResponsePreservesReservation(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	flags, _ := testFlags(t, false, 86400)
	team := uuid.New()
	key := recordKey(team.String(), "key")
	sideEffects := 0
	r := testRouter(t, flags, &redisStore{client: client}, team, time.Minute, func(c *gin.Context) {
		sideEffects++
		c.Header("X-Partial-Response", "set before panic")
		require.False(t, c.Writer.Written())
		panic("failed after creation, before writing a response")
	})
	first := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
	require.Equal(t, http.StatusInternalServerError, first.Code)
	require.Equal(t, "set before panic", first.Header().Get("X-Partial-Response"))
	require.True(t, client.HExists(t.Context(), key, "owner").Val())
	require.False(t, client.HExists(t.Context(), key, "response").Val())
	retry := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
	require.Equal(t, http.StatusConflict, retry.Code)
	require.NoError(t, client.HSet(t.Context(), key, "expected_response_at", 1).Err())
	retry = request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
	require.Equal(t, http.StatusUnprocessableEntity, retry.Code)
	require.Equal(t, 1, sideEffects)
}

func TestExecuteUsesRequestContent(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	flags, source := testFlags(t, true, 86400)
	source.Update(source.Flag(featureflags.APIIdempotencyRoutes.Key()).ValueForAll(ldvalue.CopyArbitraryValue(map[string]bool{"POST /operations": true})))
	r := gin.New()
	team := &types.Team{Team: &authqueries.Team{ID: uuid.New()}}
	r.Use(func(c *gin.Context) { auth.SetTeamInfoForTest(t, c, team) })
	r.Use(sharedmiddleware.RequestTimeout(time.Minute), Middleware(client, flags))
	calls := 0
	r.POST("/operations", func(c *gin.Context) {
		var requestContent any = struct{ Mode string }{c.GetHeader("X-Operation-Mode")}
		if c.GetHeader("X-Unserializable") == "true" {
			requestContent = func() {}
		}
		Execute(c, requestContent, func() {
			calls++
			c.Status(http.StatusAccepted)
		})
	})
	first := request(t.Context(), r, "/operations", "", []string{"key"}, "X-Operation-Mode", "one")
	require.Equal(t, http.StatusAccepted, first.Code)
	retry := request(t.Context(), r, "/operations", "", []string{"key"}, "X-Operation-Mode", "one", "X-Request-ID", "new")
	require.Equal(t, first.Code, retry.Code)
	changed := request(t.Context(), r, "/operations", "", []string{"key"}, "X-Operation-Mode", "two")
	require.Equal(t, http.StatusConflict, changed.Code)
	invalid := request(t.Context(), r, "/operations", "", []string{"invalid"}, "X-Unserializable", "true")
	require.Equal(t, http.StatusBadRequest, invalid.Code)
	require.Zero(t, client.Exists(t.Context(), recordKey(team.ID.String(), "invalid")).Val())
	require.Equal(t, 1, calls)
}
