package ratelimit

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis_rate/v10"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	ginmiddleware "github.com/oapi-codegen/gin-middleware"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/middleware"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// newTestFF creates a feature flags client with optional route config overrides.
func newTestFF(t *testing.T, routeConfigs ...map[string]map[string]int) *featureflags.Client {
	t.Helper()

	td := ldtestdata.DataSource()

	if len(routeConfigs) > 0 && routeConfigs[0] != nil {
		td.Update(td.Flag(featureflags.RateLimitConfigFlag.Key()).ValueForAll(ldvalue.CopyArbitraryValue(routeConfigs[0])))
	}

	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = ff.Close(context.WithoutCancel(t.Context()))
	})

	return ff
}

const testRoute = "/sandboxes/:sandboxID/connect"

// routeConfig returns a route config map for the test route with the given rate and burst.
func routeConfig(rate, burst int) map[string]map[string]int {
	return map[string]map[string]int{
		testRoute: {"rate": rate, "burst": burst},
	}
}

// doRequest performs a POST /sandboxes/test-sbx/connect.
func doRequest(t *testing.T, r *gin.Engine) *httptest.ResponseRecorder {
	t.Helper()

	return groupRequest(t, r, http.MethodPost, "/sandboxes/test-sbx/connect")
}

// newRouterWithTeam creates a Gin engine that injects a team then applies rate limiting.
func newRouterWithTeam(t *testing.T, limiter *redis_rate.Limiter, cfg Config, ff *featureflags.Client, teamID uuid.UUID) *gin.Engine {
	t.Helper()

	r := gin.New()
	r.Use(func(c *gin.Context) {
		auth.SetTeamInfoForTest(t, c, &types.Team{
			Team: &authqueries.Team{ID: teamID},
		})
		c.Next()
	})
	r.Use(newTestMiddleware(t, limiter, cfg, ff))
	r.POST("/sandboxes/:sandboxID/connect", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	return r
}

func newTestMiddleware(t *testing.T, limiter requestLimiter, cfg Config, ff *featureflags.Client) gin.HandlerFunc {
	t.Helper()

	handler, err := Middleware(limiter, cfg, ff, noop.NewMeterProvider(), logger.NewNopLogger())
	require.NoError(t, err)

	return handler
}

// --- Unit tests ---

func TestMiddleware_SkipsUnauthenticated(t *testing.T) {
	t.Parallel()

	ff := newTestFF(t)
	// Unreachable Redis — shouldn't matter since no team is set.
	badClient := redis.NewClient(&redis.Options{Addr: "localhost:1"})
	defer badClient.Close()

	limiter := redis_rate.NewLimiter(badClient)

	r := gin.New()
	// No team set — unauthenticated.
	r.Use(newTestMiddleware(t, limiter, Config{FailOpen: true}, ff))
	r.POST("/sandboxes/:sandboxID/connect", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	w := doRequest(t, r)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestMiddleware_FailOpen(t *testing.T) {
	t.Parallel()

	ff := newTestFF(t, routeConfig(10, 10))
	// Unreachable Redis.
	badClient := redis.NewClient(&redis.Options{
		Addr:        "localhost:1",
		DialTimeout: 10 * time.Millisecond,
	})
	defer badClient.Close()

	limiter := redis_rate.NewLimiter(badClient)
	r := newRouterWithTeam(t, limiter, Config{FailOpen: true}, ff, uuid.New())

	w := doRequest(t, r)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestMiddleware_FailClosed(t *testing.T) {
	t.Parallel()

	ff := newTestFF(t, routeConfig(10, 10))
	badClient := redis.NewClient(&redis.Options{
		Addr:        "localhost:1",
		DialTimeout: 10 * time.Millisecond,
	})
	defer badClient.Close()

	limiter := redis_rate.NewLimiter(badClient)
	r := newRouterWithTeam(t, limiter, Config{FailOpen: false}, ff, uuid.New())

	w := doRequest(t, r)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestMiddleware_UnconfiguredRouteAllowsThrough(t *testing.T) {
	t.Parallel()

	// Rate limiting enabled, but no route config — all routes should pass through.
	ff := newTestFF(t)
	badClient := redis.NewClient(&redis.Options{Addr: "localhost:1"})
	defer badClient.Close()

	limiter := redis_rate.NewLimiter(badClient)
	r := newRouterWithTeam(t, limiter, Config{FailOpen: true}, ff, uuid.New())

	w := doRequest(t, r)
	assert.Equal(t, http.StatusOK, w.Code)
	// No rate limit headers should be set for unconfigured routes.
	assert.Empty(t, w.Header().Get("RateLimit-Limit"))
}

// --- Integration tests (real Redis) ---

func TestIntegration_AllowedRequestSetsHeaders(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	redisClient := redis_utils.SetupInstance(t)
	limiter := redis_rate.NewLimiter(redisClient)
	ff := newTestFF(t, routeConfig(10, 20))

	r := newRouterWithTeam(t, limiter, Config{FailOpen: true}, ff, uuid.New())

	w := doRequest(t, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "20", w.Header().Get("RateLimit-Limit"))
	assert.NotEmpty(t, w.Header().Get("RateLimit-Remaining"))
	assert.NotEmpty(t, w.Header().Get("RateLimit-Reset"))
}

func TestIntegration_BurstThenDeny(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	redisClient := redis_utils.SetupInstance(t)
	limiter := redis_rate.NewLimiter(redisClient)
	ff := newTestFF(t, routeConfig(1, 3))

	r := newRouterWithTeam(t, limiter, Config{FailOpen: true}, ff, uuid.New())

	// First 3 requests should succeed (burst).
	for i := range 3 {
		w := doRequest(t, r)
		assert.Equal(t, http.StatusOK, w.Code, "request %d should be allowed", i+1)
	}

	// 4th should be denied.
	w := doRequest(t, r)
	assert.Equal(t, http.StatusTooManyRequests, w.Code)
	assert.NotEmpty(t, w.Header().Get("Retry-After"))

	var body struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	err := json.NewDecoder(w.Body).Decode(&body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusTooManyRequests, body.Code)
	assert.Equal(t, "Rate limit exceeded", body.Message)
}

func TestIntegration_Refill(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	redisClient := redis_utils.SetupInstance(t)
	limiter := redis_rate.NewLimiter(redisClient)
	// One token every 2s: slow enough that a loaded runner cannot refill a
	// token mid-drain (a 100ms refill raced the three requests below and the
	// deny assertion saw 200), fast enough for the refill poll to finish.
	ff := newTestFF(t, map[string]map[string]int{
		testRoute: {"rate": 1, "burst": 2, "period_s": 2},
	})

	r := newRouterWithTeam(t, limiter, Config{FailOpen: true}, ff, uuid.New())

	// Exhaust burst.
	for range 2 {
		w := doRequest(t, r)
		assert.Equal(t, http.StatusOK, w.Code)
	}
	w := doRequest(t, r)
	assert.Equal(t, http.StatusTooManyRequests, w.Code)

	// A denied request consumes no token, so poll until one refills instead
	// of sleeping through a fixed window.
	require.Eventually(t, func() bool {
		return doRequest(t, r).Code == http.StatusOK
	}, 10*time.Second, 50*time.Millisecond, "no token was refilled")
}

func TestIntegration_IndependentTeams(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	redisClient := redis_utils.SetupInstance(t)
	limiter := redis_rate.NewLimiter(redisClient)
	ff := newTestFF(t, routeConfig(1, 1))

	cfg := Config{FailOpen: true}

	teamA := uuid.New()
	teamB := uuid.New()

	rA := newRouterWithTeam(t, limiter, cfg, ff, teamA)
	rB := newRouterWithTeam(t, limiter, cfg, ff, teamB)

	// Team A uses its quota.
	w := doRequest(t, rA)
	assert.Equal(t, http.StatusOK, w.Code)
	w = doRequest(t, rA)
	assert.Equal(t, http.StatusTooManyRequests, w.Code)

	// Team B should still have quota.
	w = doRequest(t, rB)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestIntegration_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	redisClient := redis_utils.SetupInstance(t)
	limiter := redis_rate.NewLimiter(redisClient)
	ff := newTestFF(t, map[string]map[string]int{
		testRoute: {"rate": 1, "burst": 10, "period_s": 600}, // slow refill so burst is the effective limit
	})

	burst := 10
	r := newRouterWithTeam(t, limiter, Config{FailOpen: true}, ff, uuid.New())

	// Fire 20 concurrent requests; only `burst` should be allowed.
	total := 20
	results := make([]int, total)

	var wg sync.WaitGroup
	for i := range total {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			w := doRequest(t, r)
			results[idx] = w.Code
		}(i)
	}
	wg.Wait()

	allowed := 0
	denied := 0
	for _, code := range results {
		switch code {
		case http.StatusOK:
			allowed++
		case http.StatusTooManyRequests:
			denied++
		default:
			t.Errorf("unexpected status code: %d", code)
		}
	}

	assert.Equal(t, burst, allowed, "exactly burst requests should be allowed")
	assert.Equal(t, total-burst, denied, "remaining requests should be denied")
}

type groupLimiterFunc func(context.Context, string, redis_rate.Limit) (*redis_rate.Result, error)

func (f groupLimiterFunc) Allow(ctx context.Context, key string, limit redis_rate.Limit) (*redis_rate.Result, error) {
	return f(ctx, key, limit)
}

func groupTeam(rate int64) *types.Team {
	return &types.Team{
		Team:   &authqueries.Team{ID: uuid.New()},
		Limits: &types.TeamLimits{APITeamRPSList: rate},
	}
}

type groupFixture struct {
	router *gin.Engine
	reader *sdkmetric.ManualReader
	logs   *observer.ObservedLogs
	flags  *ldtestdata.TestDataSource
}

func groupRouter(t *testing.T, limiter requestLimiter, mode string, team *types.Team) groupFixture {
	t.Helper()

	td := ldtestdata.DataSource()
	if mode != "" {
		td.Update(td.Flag(featureflags.RateLimitV2Mode.Key()).ValueForAll(ldvalue.String(mode)))
	}
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ff.Close(context.WithoutCancel(t.Context()))) })

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
	core, logs := observer.New(zapcore.InfoLevel)
	l, err := logger.NewLogger(logger.LoggerConfig{Cores: []zapcore.Core{core}})
	require.NoError(t, err)

	spec, err := api.GetSpec()
	require.NoError(t, err)
	spec.Servers = nil
	spec.Security = openapi3.SecurityRequirements{{"ApiKeyAuth": {}}}
	spec.Paths.Set("/unmapped-group", &openapi3.PathItem{
		Get: &openapi3.Operation{Extensions: map[string]any{middleware.APIGroupExtension: "unmapped"}},
	})
	spec.Paths.Set("/unlimited", &openapi3.PathItem{Get: &openapi3.Operation{}})

	r := gin.New()
	r.Use(ginmiddleware.OapiRequestValidatorWithOptions(spec, &ginmiddleware.Options{
		Options: openapi3filter.Options{
			ExcludeRequestBody: true,
			AuthenticationFunc: middleware.WithAPIGroup(func(ctx context.Context, _ *openapi3filter.AuthenticationInput) error {
				if team != nil {
					auth.SetTeamInfoForTest(t, ginmiddleware.GetGinContext(ctx), team)
				}

				return nil
			}),
		},
	}))
	cfg := Config{FailOpen: true}
	handler, err := Middleware(limiter, cfg, ff, provider, l)
	require.NoError(t, err)
	r.Use(handler)
	ok := func(c *gin.Context) {
		c.Header("X-Test-Handler", "called")
		c.Status(http.StatusNoContent)
	}
	r.POST("/sandboxes", ok)
	for _, path := range []string{
		"/sandboxes", "/v2/sandboxes", "/sandboxes/metrics", "/snapshots",
		"/templates", "/v2/templates", "/templates/:templateID", "/templates/:templateID/tags",
		"/api-keys", "/volumes", "/secrets",
		"/unmapped-group", "/unlimited", "/teams", "/nodes", "/sandboxes/:sandboxID",
	} {
		r.GET(path, ok)
	}

	return groupFixture{router: r, reader: reader, logs: logs, flags: td}
}

func groupRequest(t *testing.T, r *gin.Engine, method, path string) *httptest.ResponseRecorder {
	t.Helper()

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), method, path, nil))

	return w
}

func groupDecisions(t *testing.T, reader *sdkmetric.ManualReader, mode string, teamID uuid.UUID) map[string]int64 {
	t.Helper()

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	decisions := make(map[string]int64)
	for _, scope := range metrics.ScopeMetrics {
		assert.Equal(t, "github.com/e2b-dev/infra/packages/api/internal/middleware/ratelimit", scope.Scope.Name)
		for _, m := range scope.Metrics {
			assert.Equal(t, "api.rate_limit.requests", m.Name)
			assert.Equal(t, "Number of API group rate-limit checks, by team, group, mode, and decision", m.Description)
			assert.Equal(t, "{request}", m.Unit)
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			assert.True(t, sum.IsMonotonic)
			for _, point := range sum.DataPoints {
				labelTeamID, present := point.Attributes.Value("team.id")
				require.True(t, present, "every decision must carry the authenticated team ID")
				assert.Equal(t, teamID.String(), labelTeamID.AsString())
				group, _ := point.Attributes.Value("api_group")
				gotMode, _ := point.Attributes.Value("mode")
				decision, _ := point.Attributes.Value("decision")
				assert.Equal(t, "list", group.AsString())
				assert.Equal(t, mode, gotMode.AsString())
				decisions[decision.AsString()] += point.Value
			}
		}
	}

	return decisions
}

func TestAPIGroupRPSFieldsCoverOpenAPIGroups(t *testing.T) {
	t.Parallel()

	spec, err := api.GetSpec()
	require.NoError(t, err)
	for path, item := range spec.Paths.Map() {
		for method, operation := range item.Operations() {
			extension, exists := operation.Extensions[middleware.APIGroupExtension]
			if !exists {
				continue
			}
			t.Run(method+" "+path, func(t *testing.T) {
				t.Parallel()

				group, ok := extension.(string)
				require.True(t, ok, "x-api-group must be a string")
				require.NotEmpty(t, group)
				require.Contains(t, apiGroupRPSFields, group, "x-api-group must have a rate-limit field")
				require.NotNil(t, apiGroupRPSFields[group])
			})
		}
	}
}

func TestAPIGroupBypass(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, mode, method, path string
		team                     *types.Team
	}{
		{"disabled", "disabled", http.MethodGet, "/sandboxes", groupTeam(1)},
		{"missing flag", "", http.MethodGet, "/sandboxes", groupTeam(1)},
		{"invalid mode", "enabeld", http.MethodGet, "/sandboxes", groupTeam(1)},
		{"unauthenticated", "enabled", http.MethodGet, "/sandboxes", nil},
		{"missing cached limits", "enabled", http.MethodGet, "/sandboxes", &types.Team{Team: &authqueries.Team{ID: uuid.New()}}},
		{"unconfigured", "enabled", http.MethodGet, "/sandboxes", groupTeam(0)},
		{"negative", "enabled", http.MethodGet, "/sandboxes", groupTeam(-1)},
		{"sandbox creation", "enabled", http.MethodPost, "/sandboxes", groupTeam(1)},
		{"individual sandbox", "enabled", http.MethodGet, "/sandboxes/sbx-a", groupTeam(1)},
		{"user-wide teams", "enabled", http.MethodGet, "/teams", groupTeam(1)},
		{"admin node list", "enabled", http.MethodGet, "/nodes", groupTeam(1)},
		{"ungrouped route", "enabled", http.MethodGet, "/unlimited?api-group=list", groupTeam(1)},
		{"unmapped group", "enabled", http.MethodGet, "/unmapped-group", groupTeam(1)},
		{"unmapped group in shadow", "shadow", http.MethodGet, "/unmapped-group", groupTeam(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			limiter := groupLimiterFunc(func(context.Context, string, redis_rate.Limit) (*redis_rate.Result, error) {
				t.Fatal("bypassed request reached Redis")

				return nil, nil
			})
			f := groupRouter(t, limiter, tc.mode, tc.team)
			w := groupRequest(t, f.router, tc.method, tc.path)
			assert.Equal(t, http.StatusNoContent, w.Code)
			assert.Empty(t, w.Header().Get("RateLimit-Limit"))
			assert.Empty(t, w.Header().Get("Retry-After"))
			assert.Empty(t, groupDecisions(t, f.reader, tc.mode, uuid.Nil))
			assert.Zero(t, f.logs.Len())
		})
	}
}

func TestAPIGroupDecisions(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, mode, decision string
		allowed, status      int
		err                  error
	}{
		{"enabled allowed", "enabled", "allowed", 1, http.StatusNoContent, nil},
		{"enabled limited", "enabled", "limited", 0, http.StatusTooManyRequests, nil},
		{"shadow allowed", "shadow", "allowed", 1, http.StatusNoContent, nil},
		{"shadow limited", "shadow", "limited", 0, http.StatusNoContent, nil},
		{"enabled Redis error", "enabled", "error", 0, http.StatusNoContent, errors.New("Redis unavailable")},
		{"shadow Redis error", "shadow", "error", 0, http.StatusNoContent, errors.New("Redis unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			team := groupTeam(7)
			calls := 0
			limiter := groupLimiterFunc(func(_ context.Context, key string, limit redis_rate.Limit) (*redis_rate.Result, error) {
				calls++
				assert.Equal(t, rateLimitKeyV2(team.ID, http.MethodGet, "/sandboxes"), key)
				assert.Equal(t, redis_rate.Limit{Rate: 7, Burst: 7, Period: time.Second}, limit)

				return &redis_rate.Result{Allowed: tc.allowed, Remaining: 3, RetryAfter: 1500 * time.Millisecond, ResetAfter: 2500 * time.Millisecond}, tc.err
			})
			f := groupRouter(t, limiter, tc.mode, team)
			w := groupRequest(t, f.router, http.MethodGet, "/sandboxes")
			assert.Equal(t, tc.status, w.Code)
			assert.Equal(t, 1, calls)
			assert.Equal(t, map[string]int64{tc.decision: 1}, groupDecisions(t, f.reader, tc.mode, team.ID))
			if tc.mode == "enabled" && tc.err == nil {
				assert.Equal(t, "7", w.Header().Get("RateLimit-Limit"))
				assert.Equal(t, "3", w.Header().Get("RateLimit-Remaining"))
				assert.Equal(t, "3", w.Header().Get("RateLimit-Reset"))
			} else {
				assert.Empty(t, w.Header().Get("RateLimit-Limit"))
				assert.Empty(t, w.Header().Get("RateLimit-Remaining"))
				assert.Empty(t, w.Header().Get("RateLimit-Reset"))
			}
			if tc.status == http.StatusTooManyRequests {
				assert.Equal(t, "2", w.Header().Get("Retry-After"))
				assert.JSONEq(t, `{"code":429,"message":"Rate limit exceeded"}`, w.Body.String())
			} else {
				assert.Empty(t, w.Header().Get("Retry-After"))
			}
			if tc.mode == "shadow" || tc.allowed == 0 {
				require.Equal(t, 1, f.logs.Len())
				fields := f.logs.All()[0].ContextMap()
				assert.Equal(t, tc.mode, fields["mode"])
				assert.Equal(t, tc.decision, fields["decision"])
				assert.Equal(t, "list", fields["api_group"])
			}
		})
	}
}

func TestAPIGroupPreservesInt64RPS(t *testing.T) {
	t.Parallel()

	const rps int64 = 1<<32 + 17
	calls := 0
	limiter := groupLimiterFunc(func(_ context.Context, _ string, limit redis_rate.Limit) (*redis_rate.Result, error) {
		calls++
		assert.Equal(t, rps, int64(limit.Rate))
		assert.Equal(t, rps, int64(limit.Burst))

		return &redis_rate.Result{Allowed: 0, RetryAfter: time.Second}, nil
	})
	f := groupRouter(t, limiter, "enabled", groupTeam(rps))
	w := groupRequest(t, f.router, http.MethodGet, "/sandboxes")
	assert.Equal(t, 1, calls)
	assert.Equal(t, http.StatusTooManyRequests, w.Code)
	assert.Equal(t, "4294967313", w.Header().Get("RateLimit-Limit"))
	require.Equal(t, 1, f.logs.Len())
	assert.Equal(t, rps, f.logs.All()[0].ContextMap()["rate_limit_rate"])
}

func TestRateLimitV2ModeSelectsEnforcement(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, mode, path     string
		rps                  int64
		v1Allowed, v2Allowed int
		v1Error, v2Error     error
		status               int
		wantV2               bool
		wantHeader, decision string
	}{
		{"disabled uses V1", "disabled", "/sandboxes", 7, 0, 1, nil, nil, http.StatusTooManyRequests, false, "13", ""},
		{"missing flag uses V1", "", "/sandboxes", 7, 0, 1, nil, nil, http.StatusTooManyRequests, false, "13", ""},
		{"unknown mode uses V1", "unknown", "/sandboxes", 7, 0, 1, nil, nil, http.StatusTooManyRequests, false, "13", ""},
		{"enabled enforces V1 first", "enabled", "/sandboxes", 7, 0, 1, nil, nil, http.StatusTooManyRequests, false, "13", ""},
		{"enabled enforces V2 after V1", "enabled", "/sandboxes", 7, 1, 0, nil, nil, http.StatusTooManyRequests, true, "7", "limited"},
		{"enabled both allow", "enabled", "/sandboxes", 7, 1, 1, nil, nil, http.StatusNoContent, true, "7", "allowed"},
		{"enabled unconfigured preserves V1", "enabled", "/sandboxes", 0, 0, 0, nil, nil, http.StatusTooManyRequests, false, "13", ""},
		{"enabled ungrouped preserves V1", "enabled", "/unlimited", 7, 0, 0, nil, nil, http.StatusTooManyRequests, false, "13", ""},
		{"shadow ignores V2 rejection", "shadow", "/sandboxes", 7, 1, 0, nil, nil, http.StatusNoContent, true, "13", "limited"},
		{"shadow enforces V1 first", "shadow", "/sandboxes", 7, 0, 1, nil, nil, http.StatusTooManyRequests, false, "13", ""},
		{"shadow error preserves V1 headers", "shadow", "/sandboxes", 7, 1, 0, nil, errors.New("Redis unavailable"), http.StatusNoContent, true, "13", "error"},
		{"enabled error preserves V1 headers", "enabled", "/sandboxes", 7, 1, 0, nil, errors.New("Redis unavailable"), http.StatusNoContent, true, "13", "error"},
		{"V1 error still enforces V2", "enabled", "/sandboxes", 7, 0, 0, errors.New("Redis unavailable"), nil, http.StatusTooManyRequests, true, "7", "limited"},
		{"both errors fail open", "enabled", "/sandboxes", 7, 0, 0, errors.New("Redis unavailable"), errors.New("Redis unavailable"), http.StatusNoContent, true, "", "error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			team := groupTeam(tc.rps)
			v1Key := redis_utils.CreateKey("ratelimit", team.ID.String(), tc.path)
			v2Key := rateLimitKeyV2(team.ID, http.MethodGet, tc.path)
			var calls []string
			limiter := groupLimiterFunc(func(_ context.Context, key string, limit redis_rate.Limit) (*redis_rate.Result, error) {
				calls = append(calls, key)
				if key == v2Key {
					assert.Equal(t, redis_rate.Limit{Rate: int(tc.rps), Burst: int(tc.rps), Period: time.Second}, limit)

					return &redis_rate.Result{Allowed: tc.v2Allowed, Remaining: 2, RetryAfter: 2500 * time.Millisecond, ResetAfter: 2500 * time.Millisecond}, tc.v2Error
				}
				assert.Equal(t, v1Key, key)
				assert.Equal(t, redis_rate.Limit{Rate: 5, Burst: 13, Period: time.Minute}, limit)

				return &redis_rate.Result{Allowed: tc.v1Allowed, Remaining: 5, RetryAfter: 6500 * time.Millisecond, ResetAfter: 2500 * time.Millisecond}, tc.v1Error
			})
			f := groupRouter(t, limiter, tc.mode, team)
			f.flags.Update(f.flags.Flag(featureflags.RateLimitConfigFlag.Key()).ValueForAll(ldvalue.CopyArbitraryValue(
				map[string]map[string]int{tc.path: {"rate": 5, "burst": 13, "period_s": 60}},
			)))
			w := groupRequest(t, f.router, http.MethodGet, tc.path)
			assert.Equal(t, tc.status, w.Code)
			assert.Equal(t, tc.wantHeader, w.Header().Get("RateLimit-Limit"))
			wantCalls := []string{v1Key}
			if tc.wantV2 {
				wantCalls = append(wantCalls, v2Key)
			}
			assert.Equal(t, wantCalls, calls)
			decisions := groupDecisions(t, f.reader, tc.mode, team.ID)
			if tc.wantV2 {
				assert.Equal(t, map[string]int64{tc.decision: 1}, decisions)
			} else {
				assert.Empty(t, decisions)
			}
			if tc.wantHeader != "" {
				assert.Equal(t, "3", w.Header().Get("RateLimit-Reset"))
				remaining := "2"
				if tc.wantHeader == "13" {
					remaining = "5"
				}
				assert.Equal(t, remaining, w.Header().Get("RateLimit-Remaining"))
			}
			if tc.status == http.StatusTooManyRequests {
				assert.Empty(t, w.Header().Get("X-Test-Handler"))
				assert.JSONEq(t, `{"code":429,"message":"Rate limit exceeded"}`, w.Body.String())
				retryAfter := "3"
				if tc.wantHeader == "13" {
					retryAfter = "6"
				}
				assert.Equal(t, retryAfter, w.Header().Get("Retry-After"))
			} else {
				assert.Equal(t, "called", w.Header().Get("X-Test-Handler"))
				assert.Empty(t, w.Header().Get("Retry-After"))
			}
		})
	}
}

func TestRateLimitV2KeysUseTeamHashTag(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	base := rateLimitKeyV2(teamID, http.MethodGet, "/sandboxes")
	assert.Equal(t, "ratelimit:v2:{"+teamID.String()+"}:GET:%2Fsandboxes", base)
	keys := []string{
		base,
		rateLimitKeyV2(teamID, http.MethodPost, "/sandboxes"),
		rateLimitKeyV2(teamID, http.MethodGet, "/v2/sandboxes"),
		rateLimitKeyV2(teamID, http.MethodGet, "/templates/:templateID/tags"),
		rateLimitKeyV2(teamID, http.MethodGet, "/templates/{other-slot}/tags"),
	}
	seen := make(map[string]bool)
	for _, key := range keys {
		assert.False(t, seen[key], "distinct methods and routes must not share a key: %s", key)
		seen[key] = true
		_, afterOpen, found := strings.Cut("rate:"+key, "{")
		require.True(t, found)
		tag, afterClose, found := strings.Cut(afterOpen, "}")
		require.True(t, found)
		assert.Equal(t, teamID.String(), tag)
		assert.NotContains(t, afterClose, "{")
		assert.NotContains(t, afterClose, "}")
	}
	assert.NotEqual(t, base, rateLimitKeyV2(uuid.New(), http.MethodGet, "/sandboxes"))
}

func TestTeamListEndpointsUseConfiguredLimit(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"/sandboxes", "/v2/sandboxes", "/sandboxes/metrics?sandbox_ids=sbx-a", "/snapshots",
		"/templates", "/v2/templates", "/templates/template-a", "/templates/template-a/tags",
		"/api-keys", "/volumes", "/secrets",
	} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			calls := 0
			limiter := groupLimiterFunc(func(_ context.Context, _ string, limit redis_rate.Limit) (*redis_rate.Result, error) {
				calls++
				assert.Equal(t, redis_rate.PerSecond(7), limit)

				return &redis_rate.Result{Allowed: 0, RetryAfter: time.Second}, nil
			})
			team := groupTeam(7)
			f := groupRouter(t, limiter, "enabled", team)
			response := groupRequest(t, f.router, http.MethodGet, path)
			assert.Equal(t, http.StatusTooManyRequests, response.Code)
			assert.Equal(t, 1, calls)
			assert.Equal(t, map[string]int64{"limited": 1}, groupDecisions(t, f.reader, "enabled", team.ID))
		})
	}
}

func TestIntegration_APIGroupUsesIndependentRouteBudgets(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping Redis integration test")
	}
	limiter := redis_rate.NewLimiter(redis_utils.SetupInstance(t))
	team := groupTeam(1)
	f := groupRouter(t, limiter, "enabled", team)
	otherTeam := groupRouter(t, limiter, "enabled", groupTeam(1))
	assert.Equal(t, http.StatusNoContent, groupRequest(t, f.router, http.MethodGet, "/sandboxes").Code)
	assert.Equal(t, http.StatusTooManyRequests, groupRequest(t, f.router, http.MethodGet, "/sandboxes").Code)
	assert.Equal(t, http.StatusNoContent, groupRequest(t, f.router, http.MethodGet, "/v2/sandboxes").Code)
	assert.Equal(t, http.StatusTooManyRequests, groupRequest(t, f.router, http.MethodGet, "/v2/sandboxes").Code)
	assert.Equal(t, http.StatusNoContent, groupRequest(t, f.router, http.MethodPost, "/sandboxes").Code)
	assert.Equal(t, http.StatusNoContent, groupRequest(t, otherTeam.router, http.MethodGet, "/sandboxes").Code)
}

func TestIntegration_APIGroupUsesRouteTemplate(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping Redis integration test")
	}
	limiter := redis_rate.NewLimiter(redis_utils.SetupInstance(t))
	f := groupRouter(t, limiter, "enabled", groupTeam(1))
	assert.Equal(t, http.StatusNoContent, groupRequest(t, f.router, http.MethodGet, "/templates/template-a/tags?value=1").Code)
	assert.Equal(t, http.StatusTooManyRequests, groupRequest(t, f.router, http.MethodGet, "/templates/template-b/tags?value=2").Code)
}

func TestIntegration_APIGroupRolloutModes(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping Redis integration test")
	}
	limiter := redis_rate.NewLimiter(redis_utils.SetupInstance(t))
	team := groupTeam(1)
	f := groupRouter(t, limiter, "disabled", team)
	for range 3 {
		assert.Equal(t, http.StatusNoContent, groupRequest(t, f.router, http.MethodGet, "/sandboxes").Code)
	}
	assert.Empty(t, groupDecisions(t, f.reader, "disabled", team.ID))
	f.flags.Update(f.flags.Flag(featureflags.RateLimitV2Mode.Key()).ValueForAll(ldvalue.String("shadow")))
	for range 2 {
		w := groupRequest(t, f.router, http.MethodGet, "/sandboxes")
		assert.Equal(t, http.StatusNoContent, w.Code)
		assert.Empty(t, w.Header().Get("RateLimit-Limit"))
	}
	assert.Equal(t, map[string]int64{"allowed": 1, "limited": 1}, groupDecisions(t, f.reader, "shadow", team.ID))
	assert.Equal(t, 2, f.logs.Len())
	f.flags.Update(f.flags.Flag(featureflags.RateLimitV2Mode.Key()).ValueForAll(ldvalue.String("enabled")))
	assert.Equal(t, http.StatusTooManyRequests, groupRequest(t, f.router, http.MethodGet, "/sandboxes").Code)
	f.flags.Update(f.flags.Flag(featureflags.RateLimitV2Mode.Key()).ValueForAll(ldvalue.String("disabled")))
	assert.Equal(t, http.StatusNoContent, groupRequest(t, f.router, http.MethodGet, "/sandboxes").Code)
}

func TestIntegration_V1BudgetSurvivesV2ModeChanges(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping Redis integration test")
	}
	limiter := redis_rate.NewLimiter(redis_utils.SetupInstance(t))
	f := groupRouter(t, limiter, "disabled", groupTeam(7))
	f.flags.Update(f.flags.Flag(featureflags.RateLimitConfigFlag.Key()).ValueForAll(ldvalue.CopyArbitraryValue(
		map[string]map[string]int{"/sandboxes": {"rate": 1, "burst": 3, "period_s": 600}},
	)))

	// Each allowed request consumes the same V1 budget across rollout modes.
	for _, step := range []struct {
		mode, header string
		status       int
	}{
		{"disabled", "3", http.StatusNoContent},
		{"shadow", "3", http.StatusNoContent},
		{"enabled", "7", http.StatusNoContent},
		{"enabled", "3", http.StatusTooManyRequests},
		{"shadow", "3", http.StatusTooManyRequests},
		{"disabled", "3", http.StatusTooManyRequests},
	} {
		f.flags.Update(f.flags.Flag(featureflags.RateLimitV2Mode.Key()).ValueForAll(ldvalue.String(step.mode)))
		w := groupRequest(t, f.router, http.MethodGet, "/sandboxes")
		assert.Equal(t, step.status, w.Code, "mode %s", step.mode)
		assert.Equal(t, step.header, w.Header().Get("RateLimit-Limit"), "mode %s", step.mode)
	}
}
