package idempotency

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	ginmiddleware "github.com/oapi-codegen/gin-middleware"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	custommiddleware "github.com/e2b-dev/infra/packages/api/internal/middleware"
	"github.com/e2b-dev/infra/packages/api/internal/middleware/ratelimit"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sharedmiddleware "github.com/e2b-dev/infra/packages/shared/pkg/middleware"
	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

const validBody = `{"templateID":"base"}`

type createHandler struct {
	api.ServerInterface

	create gin.HandlerFunc
}

func (h createHandler) PostV2Sandboxes(c *gin.Context, _ api.PostV2SandboxesParams) {
	var body api.NewSandboxV2
	if err := c.ShouldBind(&body); err != nil {
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ClientMsg: "Invalid create request", Retryable: apierrors.NoRetry})

		return
	}
	c.Set("parsedCreateBody", body)
	Execute(c, body, func() *apierrors.APIError {
		h.create(c)

		return nil
	})
}

func (h createHandler) PostSandboxes(c *gin.Context) { h.create(c) }

func (h createHandler) GetSandboxes(c *gin.Context, _ api.GetSandboxesParams) { h.create(c) }

func (h createHandler) DeleteSandboxesSandboxID(c *gin.Context, _ api.SandboxID) { h.create(c) }

func testFlags(t *testing.T, disabled bool, seconds int) (*featureflags.Client, *ldtestdata.TestDataSource) {
	t.Helper()
	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.APIIdempotencyRoutes.Key()).ValueForAll(ldvalue.CopyArbitraryValue(map[string]bool{"POST /v2/sandboxes": !disabled})))
	td.Update(td.Flag(featureflags.APIIdempotencyTTLSeconds.Key()).ValueForAll(ldvalue.Int(seconds)))
	flags, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, flags.Close(context.WithoutCancel(t.Context()))) })

	return flags, td
}

func testRouter(t *testing.T, flags *featureflags.Client, store store, teamID uuid.UUID, requestTimeout time.Duration, create gin.HandlerFunc, before ...gin.HandlerFunc) *gin.Engine {
	t.Helper()
	swagger, err := api.GetSwagger()
	require.NoError(t, err)
	swagger.Servers = nil
	r := gin.New()
	r.Use(gin.CustomRecoveryWithWriter(io.Discard, func(c *gin.Context, _ any) { c.AbortWithStatus(http.StatusInternalServerError) }))
	r.Use(sharedmiddleware.RequestTimeout(requestTimeout))
	r.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(featureflags.AddToContext(c.Request.Context(), featureflags.UserContext(c.GetHeader("X-API-Key"))))
	})
	r.Use(ginmiddleware.OapiRequestValidatorWithOptions(swagger, &ginmiddleware.Options{
		Options: openapi3filter.Options{AuthenticationFunc: func(ctx context.Context, input *openapi3filter.AuthenticationInput) error {
			token := input.RequestValidationInput.Request.Header.Get("X-API-Key")
			if input.SecuritySchemeName != "ApiKeyAuth" || (token != "a" && token != "b" && token != "other") {
				return errors.New("invalid credentials")
			}
			id := teamID
			if token == "other" {
				id = uuid.NewSHA1(teamID, []byte("other"))
			}
			c := ginmiddleware.GetGinContext(ctx)
			auth.SetTeamInfoForTest(t, c, &types.Team{Team: &dbtypes.Team{ID: id}})

			return nil
		}},
	}))
	r.Use(func(c *gin.Context) {
		c.Header("X-Request-ID", c.GetHeader("X-Request-ID"))
		c.Header("RateLimit-Remaining", c.GetHeader("RateLimit-Remaining"))
	})
	r.Use(before...)
	r.Use(custommiddleware.EnforceBlockedTeam())
	r.Use(middleware(store, flags))
	api.RegisterHandlers(r, createHandler{create: create})

	return r
}

// untilTestDeadline is a request budget for tests that do not assert on deadlines, so a slow runner cannot expire it.
func untilTestDeadline(t *testing.T) time.Duration {
	t.Helper()
	deadline, ok := t.Deadline()
	if !ok {
		return 24 * time.Hour
	}

	return time.Until(deadline)
}

func request(ctx context.Context, r http.Handler, path, body string, keys []string, headers ...string) *httptest.ResponseRecorder {
	return methodRequest(ctx, r, http.MethodPost, path, body, keys, headers...)
}

func methodRequest(ctx context.Context, r http.Handler, method, path, body string, keys []string, headers ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(ctx, method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "a")
	if keys != nil {
		req.Header[headerName] = keys
	}
	for i := 0; i < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	return w
}

func validatedRequest(t *testing.T, flags *featureflags.Client, body string) api.NewSandboxV2 {
	t.Helper()
	var parsed api.NewSandboxV2
	r := testRouter(t, flags, nil, uuid.New(), time.Second, func(c *gin.Context) {
		parsed = c.MustGet("parsedCreateBody").(api.NewSandboxV2)
		c.Status(http.StatusCreated)
	})
	w := request(t.Context(), r, "/v2/sandboxes", body, nil)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())

	return parsed
}

func TestBypassAndKeyValidation(t *testing.T) {
	t.Parallel()
	// The schema and the generated binding reject malformed or repeated keys,
	// so those 400s carry no hint. The 503s are reserve failures, a transient
	// condition.
	for _, tc := range []struct {
		name, path string
		disabled   bool
		keys       []string
		want       int
		wantRetry  string
	}{
		{"absent", "/v2/sandboxes", false, nil, 201, ""},
		{"disabled absent", "/v2/sandboxes", true, nil, 201, ""},
		{"disabled valid", "/v2/sandboxes", true, []string{"key"}, 201, ""},
		{"disabled empty", "/v2/sandboxes", true, []string{""}, 400, ""},
		{"disabled repeated", "/v2/sandboxes", true, []string{"a", "b"}, 400, ""},
		{"disabled overlong", "/v2/sandboxes", true, []string{strings.Repeat("a", 256)}, 400, ""},
		{"disabled invalid utf8", "/v2/sandboxes", true, []string{"\xff"}, 400, ""},
		{"disabled 255 characters", "/v2/sandboxes", true, []string{strings.Repeat("a", 255)}, 201, ""},
		{"disabled unicode", "/v2/sandboxes", true, []string{"\u754c"}, 400, ""},
		{"disabled uppercase", "/v2/sandboxes", true, []string{"ABC"}, 400, ""},
		{"disabled uuid", "/v2/sandboxes", true, []string{uuid.NewString()}, 201, ""},
		{"v1 valid", "/sandboxes", false, []string{"key"}, 201, ""},
		{"v1 repeated", "/sandboxes", false, []string{"a", "b"}, 201, ""},
		{"empty", "/v2/sandboxes", false, []string{""}, 400, ""},
		{"repeated", "/v2/sandboxes", false, []string{"a", "a"}, 400, ""},
		{"overlong", "/v2/sandboxes", false, []string{strings.Repeat("\u754c", 256)}, 400, ""},
		{"invalid utf8", "/v2/sandboxes", false, []string{"\xff"}, 400, ""},
		{"255 characters", "/v2/sandboxes", false, []string{strings.Repeat("a", 255)}, 503, "true"},
		{"lowercase alphanumeric", "/v2/sandboxes", false, []string{"abc0123456789xyz"}, 503, "true"},
		{"uuid", "/v2/sandboxes", false, []string{uuid.NewString()}, 503, "true"},
		{"uppercase", "/v2/sandboxes", false, []string{"ABC"}, 400, ""},
		{"uppercase uuid", "/v2/sandboxes", false, []string{strings.ToUpper(uuid.NewString())}, 400, ""},
		{"punctuation", "/v2/sandboxes", false, []string{"abc_123"}, 400, ""},
		{"whitespace", "/v2/sandboxes", false, []string{" abc "}, 400, ""},
		{"unicode", "/v2/sandboxes", false, []string{"\u754c"}, 400, ""},
		{"non BMP", "/v2/sandboxes", false, []string{strings.Repeat("😀", 128)}, 400, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			flags, _ := testFlags(t, tc.disabled, 86400)
			calls := 0
			r := testRouter(t, flags, faultStore{reserveErr: errors.New("redis unavailable")}, uuid.New(), time.Second, func(c *gin.Context) {
				calls++
				c.Status(http.StatusCreated)
			})
			w := request(t.Context(), r, tc.path, validBody, tc.keys)
			require.Equal(t, tc.want, w.Code, w.Body.String())
			require.Equal(t, tc.wantRetry, w.Result().Header.Get("Should-Retry"))
			require.Equal(t, tc.want == 201, calls == 1)
		})
	}
}

func TestDisabledCreatePreservesIdempotencyHeader(t *testing.T) {
	t.Parallel()
	flags, _ := testFlags(t, true, 86400)
	keys := []string{"key"}
	var captured http.Header
	r := testRouter(t, flags, faultStore{reserveErr: errors.New("unexpected reservation")}, uuid.New(), time.Second, func(c *gin.Context) {
		captured = c.Request.Header
		require.Equal(t, keys, captured.Values(headerName))
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusServiceUnavailable, ClientMsg: "transient create failure", Retryable: apierrors.CanRetry})
	})
	w := request(t.Context(), r, "/v2/sandboxes", validBody, keys)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "true", w.Result().Header.Get("Should-Retry"))
	require.Equal(t, keys, captured.Values(headerName))
	require.Equal(t, "a", captured.Get("X-API-Key"))
}

func TestUnrelatedRoutesIgnoreIdempotencyHeader(t *testing.T) {
	t.Parallel()
	for _, disabled := range []bool{true, false} {
		for _, route := range []struct{ method, path string }{
			{http.MethodGet, "/sandboxes"},
			{http.MethodDelete, "/sandboxes/sandbox"},
		} {
			for _, header := range []struct {
				name string
				keys []string
			}{
				{"empty", []string{""}},
				{"repeated", []string{"a", "b"}},
				{"overlong", []string{strings.Repeat("a", 256)}},
				{"invalid utf8", []string{"\xff"}},
			} {
				t.Run(fmt.Sprintf("disabled=%t/%s/%s", disabled, route.method, header.name), func(t *testing.T) {
					t.Parallel()
					flags, _ := testFlags(t, disabled, 86400)
					calls := 0
					r := testRouter(t, flags, nil, uuid.New(), time.Second, func(c *gin.Context) {
						calls++
						require.Equal(t, header.keys, c.Request.Header.Values(headerName))
						_, enabled := c.Get(executorKey{})
						require.False(t, enabled)
						c.Status(http.StatusOK)
					})
					w := methodRequest(t.Context(), r, route.method, route.path, "", header.keys)
					require.Equal(t, http.StatusOK, w.Code, w.Body.String())
					require.Empty(t, w.Result().Header.Get("Should-Retry"))
					require.Equal(t, 1, calls)
				})
			}
		}
	}
}

func TestMalformedCreateDoesNotReserve(t *testing.T) {
	t.Parallel()
	flags, _ := testFlags(t, false, 86400)
	for _, tc := range []struct{ body, wantRetry string }{
		{body: ""},
		{body: "{"},
		{body: `{"templateID":}`},
		{body: `{"templateID":"base","timeout":"bad"}`},
		{body: `{"templateID":"base","timeout":300.0}`, wantRetry: "false"},
		{body: `{"templateID":"base","network":null}`},
	} {
		t.Run(tc.body, func(t *testing.T) {
			t.Parallel()
			r := testRouter(t, flags, faultStore{reserveErr: errors.New("unexpected reservation")}, uuid.New(), time.Second, func(_ *gin.Context) {
				t.Error("malformed create executed")
			})
			w := request(t.Context(), r, "/v2/sandboxes", tc.body, []string{"key"})
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			assert.Equal(t, tc.wantRetry, w.Result().Header.Get("Should-Retry"))
		})
	}
}

func TestRedisMiddleware(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	s := &redisStore{client: client}

	t.Run("pending replicas fail immediately and later replay", func(t *testing.T) {
		t.Parallel()
		flags, _ := testFlags(t, false, 86400)
		team := uuid.New()
		started := make(chan struct{})
		release := make(chan struct{})
		releaseOwner := sync.OnceFunc(func() { close(release) })
		var calls atomic.Int64
		handler := func(c *gin.Context) {
			if calls.Add(1) == 1 {
				close(started)
				<-release
			}
			c.Data(201, "application/json", []byte("{\"sandboxID\":\"original\"}\n"))
		}
		routers := []*gin.Engine{
			testRouter(t, flags, &redisStore{client: client}, team, untilTestDeadline(t), handler),
			testRouter(t, flags, &redisStore{client: client}, team, untilTestDeadline(t), handler),
		}
		owner := make(chan *httptest.ResponseRecorder, 1)
		ownerDone := make(chan struct{})
		go func() {
			defer close(ownerDone)
			owner <- request(t.Context(), routers[0], "/v2/sandboxes", validBody, []string{"key"})
		}()
		defer func() {
			releaseOwner()
			<-ownerDone
		}()
		<-started
		key := recordKey(team.String(), "key")
		before, err := client.HGetAll(t.Context(), key).Result()
		require.NoError(t, err)
		ttl := client.PTTL(t.Context(), key).Val()
		responses := make([]*httptest.ResponseRecorder, 23)
		var wg sync.WaitGroup
		for i := range responses {
			wg.Go(func() {
				responses[i] = request(t.Context(), routers[i%2], "/v2/sandboxes", "{ \"templateID\": \"base\" }", []string{"key"},
					"X-API-Key", "b", "X-Request-ID", fmt.Sprint(i), "RateLimit-Remaining", "10", "traceparent", fmt.Sprint(i))
			})
		}
		// The owner stays blocked until releaseOwner, so these return only if they do not wait for it.
		wg.Wait()
		require.EqualValues(t, 1, calls.Load())
		for i, result := range responses {
			require.Equal(t, http.StatusConflict, result.Code, result.Body.String())
			require.Equal(t, "true", result.Result().Header.Get("Should-Retry"))
			require.JSONEq(t, `{"code":409,"message":"The original request has not recorded a response yet. Retry with the same Idempotency-Key.","error_code":"idempotency_in_progress"}`, result.Body.String())
			require.Equal(t, fmt.Sprint(i), result.Header().Get("X-Request-ID"))
			require.Equal(t, "10", result.Header().Get("RateLimit-Remaining"))
		}
		after, err := client.HGetAll(t.Context(), key).Result()
		require.NoError(t, err)
		require.Equal(t, before, after)
		require.Positive(t, client.PTTL(t.Context(), key).Val())
		require.LessOrEqual(t, client.PTTL(t.Context(), key).Val(), ttl)
		releaseOwner()
		original := <-owner
		require.Equal(t, http.StatusCreated, original.Code, original.Body.String())
		assert.NotContains(t, original.Result().Header, "Should-Retry")
		replayed := request(t.Context(), routers[1], "/v2/sandboxes", validBody, []string{"key"},
			"X-API-Key", "b", "X-Request-ID", "retry", "RateLimit-Remaining", "9")
		require.Equal(t, original.Code, replayed.Code, replayed.Body.String())
		require.Equal(t, original.Body.String(), replayed.Body.String())
		assert.NotContains(t, replayed.Result().Header, "Should-Retry")
		require.Equal(t, "retry", replayed.Header().Get("X-Request-ID"))
		require.Equal(t, "9", replayed.Header().Get("RateLimit-Remaining"))
		require.EqualValues(t, 1, calls.Load())
		w := request(t.Context(), routers[0], "/v2/sandboxes", validBody, []string{"key"}, "X-API-Key", "other")
		require.Equal(t, 201, w.Code)
		require.EqualValues(t, 2, calls.Load())
	})

	t.Run("terminal errors mismatch and admission", func(t *testing.T) {
		t.Parallel()
		flags, td := testFlags(t, false, 86400)
		team := uuid.New()
		var blocked atomic.Bool
		var calls atomic.Int64
		limiter, err := ratelimit.Middleware(ratelimit.NewLimiter(client), ratelimit.Config{FailOpen: true}, flags, noop.NewMeterProvider(), logger.NewNopLogger())
		require.NoError(t, err)
		r := testRouter(t, flags, s, team, untilTestDeadline(t), func(c *gin.Context) {
			calls.Add(1)
			apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusInternalServerError, ClientMsg: "original error", Retryable: apierrors.CanRetry})
		}, limiter, func(c *gin.Context) { auth.MustGetTeamInfo(c).IsBlocked = blocked.Load() })
		w := request(t.Context(), r, "/v2/sandboxes", `{}`, []string{"key"})
		require.Equal(t, 400, w.Code)
		require.Zero(t, client.Exists(t.Context(), recordKey(team.String(), "key")).Val())
		original := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
		require.Equal(t, 500, original.Code)
		require.Equal(t, "true", original.Result().Header.Get("Should-Retry"))
		w = request(t.Context(), r, "/v2/sandboxes", `{"templateID":"other"}`, []string{"key"})
		require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
		require.JSONEq(t, `{"code":409,"message":"Idempotency-Key was already used with a different request","error_code":"idempotency_request_mismatch"}`, w.Body.String())
		require.Equal(t, "false", w.Result().Header.Get("Should-Retry"))
		w = request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"}, "Content-Type", "application/json; charset=utf-8")
		require.Equal(t, original.Code, w.Code, w.Body.String())
		require.Equal(t, original.Body.String(), w.Body.String())
		blocked.Store(true)
		require.Equal(t, "true", w.Result().Header.Get("Should-Retry"))
		w = request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
		require.Equal(t, 403, w.Code, w.Body.String())
		require.Empty(t, w.Result().Header.Get("Should-Retry"))
		blocked.Store(false)
		w = request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"}, "X-API-Key", "revoked")
		require.NotEqual(t, 500, w.Code)
		w = request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"}, "X-API-Key", "b", "package_version", "changed")
		require.Equal(t, original.Body.String(), w.Body.String())
		require.Equal(t, original.Code, w.Code)
		require.EqualValues(t, 1, calls.Load())
		td.Update(td.Flag(featureflags.RateLimitConfigFlag.Key()).ValueForAll(ldvalue.CopyArbitraryValue(map[string]map[string]int{"/v2/sandboxes": {"rate": 1, "burst": 1, "period_s": 600}})))
		request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
		w = request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
		require.Equal(t, 429, w.Code, w.Body.String())
		require.Equal(t, "true", w.Result().Header.Get("Should-Retry"))
		require.EqualValues(t, 1, calls.Load())
	})

	for _, tc := range []struct {
		name      string
		retryable apierrors.Retryability
		wantRetry string
	}{
		{name: "undefined", retryable: apierrors.UndefinedRetry},
		{name: "no retry", retryable: apierrors.NoRetry, wantRetry: "false"},
		{name: "can retry", retryable: apierrors.CanRetry, wantRetry: "true"},
	} {
		t.Run("retry metadata survives replay/"+tc.name, func(t *testing.T) {
			t.Parallel()
			flags, _ := testFlags(t, false, 86400)
			calls := 0
			r := testRouter(t, flags, s, uuid.New(), time.Second, func(c *gin.Context) {
				calls++
				apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusServiceUnavailable, ClientMsg: "unavailable", Retryable: tc.retryable})
			})
			for range 2 {
				w := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
				require.Equal(t, http.StatusServiceUnavailable, w.Code)
				if tc.wantRetry == "" {
					assert.NotContains(t, w.Result().Header, "Should-Retry")
				} else {
					assert.Equal(t, tc.wantRetry, w.Result().Header.Get("Should-Retry"))
				}
				assert.JSONEq(t, `{"code":503,"message":"unavailable"}`, w.Body.String())
			}
			assert.Equal(t, 1, calls)
		})
	}

	t.Run("equivalent parsed requests replay", func(t *testing.T) {
		t.Parallel()
		flags, _ := testFlags(t, false, 86400)
		calls := 0
		r := testRouter(t, flags, s, uuid.New(), untilTestDeadline(t), func(c *gin.Context) {
			calls++
			c.Status(201)
		})
		for _, pair := range []struct{ first, second string }{
			{validBody, `{"templateID":"base","timeout":300}`},
			{`{"templateID":"base","extra":9007199254740992}`, `{"templateID":"base","extra":9007199254740993}`},
			{`{"templateID":"base","extra":null}`, validBody},
			{`{"templateID":"base","extra":[1,2]}`, `{"templateID":"base","extra":[2,1]}`},
			{`{"templateID":"base","autoPause":false,"autoPauseMemory":true}`, validBody},
		} {
			key := []string{uuid.NewString()}
			w := request(t.Context(), r, "/v2/sandboxes", pair.first, key)
			require.Equal(t, 201, w.Code, "%s: %s", pair.first, w.Body.String())
			w = request(t.Context(), r, "/v2/sandboxes", pair.second, key)
			require.Equal(t, 201, w.Code, "%s: %s", pair.second, w.Body.String())
		}
		require.Equal(t, 5, calls)
	})

	t.Run("changed parsed parameters conflict", func(t *testing.T) {
		t.Parallel()
		flags, _ := testFlags(t, false, 86400)
		calls := 0
		r := testRouter(t, flags, s, uuid.New(), untilTestDeadline(t), func(c *gin.Context) {
			calls++
			c.Status(201)
		})
		for _, pair := range []struct{ first, second string }{
			{validBody, `{"templateID":"base","timeout":301}`},
			{`{"templateID":"base","network":{"allowOut":["a","b"]}}`, `{"templateID":"base","network":{"allowOut":["b","a"]}}`},
			{`{"templateID":"base","metadata":{"a":"one"}}`, `{"templateID":"base","metadata":{"a":"two"}}`},
		} {
			key := []string{uuid.NewString()}
			w := request(t.Context(), r, "/v2/sandboxes", pair.first, key)
			require.Equal(t, 201, w.Code, w.Body.String())
			w = request(t.Context(), r, "/v2/sandboxes", pair.second, key)
			require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
		}
		require.Equal(t, 3, calls)
	})

	t.Run("case sensitive map reordering replays", func(t *testing.T) {
		t.Parallel()
		flags, _ := testFlags(t, false, 86400)
		calls := 0
		r := testRouter(t, flags, s, uuid.New(), untilTestDeadline(t), func(c *gin.Context) {
			calls++
			c.Data(201, "application/json", []byte(`{"sandboxID":"original"}`))
		})
		first := request(t.Context(), r, "/v2/sandboxes", `{"templateID":"base","envVars":{"FOO":"1","foo":"2"}}`, []string{"key"})
		require.Equal(t, 201, first.Code, first.Body.String())
		second := request(t.Context(), r, "/v2/sandboxes", `{"envVars":{"foo":"2","FOO":"1"},"templateID":"base"}`, []string{"key"})
		require.Equal(t, first.Code, second.Code, second.Body.String())
		require.Equal(t, first.Body.String(), second.Body.String())
		require.Equal(t, 1, calls)
	})

	t.Run("cancellation and pending mismatch preserve owner completion", func(t *testing.T) {
		t.Parallel()
		flags, _ := testFlags(t, false, 60)
		team := uuid.New()
		started := make(chan struct{})
		release := make(chan struct{})
		releaseOwner := sync.OnceFunc(func() { close(release) })
		var calls atomic.Int64
		r := testRouter(t, flags, s, team, untilTestDeadline(t), func(c *gin.Context) {
			if calls.Add(1) == 1 {
				close(started)
				<-release
			}
			c.Data(201, "application/json", []byte(`{"sandboxID":"original"}`))
		})
		ownerCtx, cancelOwner := context.WithCancel(t.Context())
		defer cancelOwner()
		owner := make(chan *httptest.ResponseRecorder, 1)
		ownerDone := make(chan struct{})
		go func() {
			defer close(ownerDone)
			owner <- request(ownerCtx, r, "/v2/sandboxes", validBody, []string{"key"})
		}()
		defer func() {
			releaseOwner()
			<-ownerDone
		}()
		select {
		case <-started:
		case <-ownerDone:
			t.Fatalf("owner returned before executing: %s", (<-owner).Body.String())
		}
		cancelOwner()
		key := recordKey(team.String(), "key")
		ttl := client.PTTL(t.Context(), key).Val()
		require.Positive(t, ttl)
		require.LessOrEqual(t, ttl, time.Minute)
		w := request(t.Context(), r, "/v2/sandboxes", `{"templateID":"other"}`, []string{"key"})
		require.Equal(t, http.StatusConflict, w.Code)
		require.Contains(t, w.Body.String(), `"error_code":"idempotency_request_mismatch"`)
		w = request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
		require.Equal(t, http.StatusConflict, w.Code)
		require.Contains(t, w.Body.String(), `"error_code":"idempotency_in_progress"`)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		w = request(ctx, r, "/v2/sandboxes", validBody, []string{"key"})
		require.Equal(t, 503, w.Code)
		require.EqualValues(t, 1, calls.Load())
		require.Empty(t, client.HGet(t.Context(), key, "response").Val())
		releaseOwner()
		require.Equal(t, 201, (<-owner).Code)
		w = request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
		require.Equal(t, 201, w.Code)
	})

	t.Run("redis failure paths retain uncertain reservations", func(t *testing.T) {
		t.Parallel()
		for _, phase := range []string{"reserve", "abandoned", "complete", "panic", "panic after retryable error"} {
			t.Run(phase, func(t *testing.T) {
				t.Parallel()
				flags, _ := testFlags(t, false, 86400)
				team := uuid.New()
				key := recordKey(team.String(), "key")
				fault := faultStore{store: s}
				failure := errors.New("injected redis failure")
				switch phase {
				case "reserve":
					fault.reserveErr = failure
				case "abandoned":
					digest, err := fingerprint(jsonRequest(t), validatedRequest(t, flags, validBody))
					require.NoError(t, err)
					_, err = s.reserve(t.Context(), key, digest, "previous", featureflags.APIIdempotencyTTLSeconds.Fallback(), time.Now().Add(untilTestDeadline(t)))
					require.NoError(t, err)
				case "complete":
					fault.completeErr = failure
				}
				calls := 0
				r := testRouter(t, flags, fault, team, untilTestDeadline(t), func(c *gin.Context) {
					calls++
					if phase == "panic after retryable error" {
						apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusInternalServerError, ClientMsg: "buffered error", Retryable: apierrors.CanRetry})
					} else {
						c.Data(201, "application/json", []byte(`{"sandboxID":"original"}`))
					}
					c.Writer.Flush()
					if strings.HasPrefix(phase, "panic") {
						panic("uncertain create")
					}
				})
				w := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
				switch phase {
				case "panic", "panic after retryable error":
					require.Equal(t, 500, w.Code)
					assert.Empty(t, w.Body.String())
				case "complete":
					require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
					require.JSONEq(t, `{"sandboxID":"original"}`, w.Body.String())
				case "abandoned":
					require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
				default:
					require.Equal(t, 503, w.Code, w.Body.String())
				}
				switch phase {
				case "reserve", "abandoned":
					require.Equal(t, "true", w.Result().Header.Get("Should-Retry"))
				default:
					// A panic discards the handler's buffered hint with its response.
					require.NotContains(t, w.Result().Header, "Should-Retry")
				}
				if phase == "reserve" {
					require.Zero(t, calls)
					require.Zero(t, client.Exists(t.Context(), key).Val())

					return
				}
				ttl := client.PTTL(t.Context(), key).Val()
				require.Positive(t, ttl)
				require.LessOrEqual(t, ttl, 24*time.Hour)
				require.Empty(t, client.HGet(t.Context(), key, "response").Val())
				before, err := client.HGetAll(t.Context(), key).Result()
				require.NoError(t, err)
				previousCalls := calls
				r = testRouter(t, flags, s, team, untilTestDeadline(t), func(_ *gin.Context) { calls++ })
				for range 3 {
					w = request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
					require.Equal(t, http.StatusConflict, w.Code)
					require.JSONEq(t, `{"code":409,"message":"The original request has not recorded a response yet. Retry with the same Idempotency-Key.","error_code":"idempotency_in_progress"}`, w.Body.String())
				}
				require.Equal(t, previousCalls, calls)
				after, err := client.HGetAll(t.Context(), key).Result()
				require.NoError(t, err)
				require.Equal(t, before, after)
				require.LessOrEqual(t, client.PTTL(t.Context(), key).Val(), ttl)
			})
		}
	})

	t.Run("persist before flush after caller cancellation", func(t *testing.T) {
		t.Parallel()
		flags, _ := testFlags(t, false, 86400)
		ctx, cancel := context.WithCancel(context.WithValue(t.Context(), requestContextKey{}, "request"))
		defer cancel()
		w := httptest.NewRecorder()
		team := uuid.New()
		key := recordKey(team.String(), "key")
		var reservedExpiry time.Duration
		fault := faultStore{store: s, beforeComplete: func(ctx context.Context) {
			require.NoError(t, ctx.Err())
			require.Equal(t, "request", ctx.Value(requestContextKey{}))
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.LessOrEqual(t, time.Until(deadline), completionTimeout)
			require.Empty(t, w.Body.String())
			require.False(t, w.Flushed)
			var err error
			reservedExpiry, err = client.PExpireTime(ctx, key).Result()
			require.NoError(t, err)
			require.Positive(t, reservedExpiry)
		}}
		r := testRouter(t, flags, fault, team, untilTestDeadline(t), func(c *gin.Context) {
			c.Data(400, "application/json", []byte("{\"code\":400}\n"))
			c.Writer.Flush()
			cancel()
		})
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v2/sandboxes", strings.NewReader(validBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-Key", "a")
		req.Header.Set(headerName, "key")
		r.ServeHTTP(w, req)
		require.Equal(t, 400, w.Code)
		completedExpiry, err := client.PExpireTime(t.Context(), key).Result()
		require.NoError(t, err)
		require.Equal(t, reservedExpiry, completedExpiry)
		replayed := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
		require.Equal(t, w.Code, replayed.Code)
		require.Equal(t, w.Body.String(), replayed.Body.String())
		replayedExpiry, err := client.PExpireTime(t.Context(), key).Result()
		require.NoError(t, err)
		require.Equal(t, reservedExpiry, replayedExpiry)
	})
}

type scriptedRedisClient struct {
	redis.UniversalClient

	evalSha func(context.Context, string, []string, ...any) *redis.Cmd
}

func (c scriptedRedisClient) EvalSha(ctx context.Context, sha string, keys []string, args ...any) *redis.Cmd {
	return c.evalSha(ctx, sha, keys, args...)
}

func TestResponseDeadlineFromOriginalRequest(t *testing.T) {
	t.Parallel()
	flags, _ := testFlags(t, false, 60)
	synctest.Test(t, func(t *testing.T) {
		team := uuid.New()
		key := recordKey(team.String(), "key")
		responseDeadline := time.Now().Add(time.Second + completionTimeout)
		var fingerprint, reservationOwner, response string
		var reserveCalls, completeCalls int
		client := scriptedRedisClient{evalSha: func(ctx context.Context, sha string, keys []string, args ...any) *redis.Cmd {
			if err := ctx.Err(); err != nil {
				t.Errorf("Redis called with canceled context: %v", err)

				return redis.NewCmdResult(nil, err)
			}
			assert.Equal(t, []string{key}, keys)
			switch sha {
			case reserveScript.Hash():
				reserveCalls++
				assert.Equal(t, 60, args[2])
				if reserveCalls == 1 {
					// Verify the real store sends the original caller's budget to Redis.
					assert.Equal(t, (time.Second + completionTimeout).Milliseconds(), args[3])
					fingerprint = args[0].(string)
					reservationOwner = args[1].(string)

					return redis.NewCmdResult([]any{"true", "", "", "", "", ""}, nil)
				}
				assert.Equal(t, fingerprint, args[0])
				assert.Equal(t, (time.Minute + completionTimeout).Milliseconds(), args[3])
				// Redis keeps the original deadline even when a retry has a longer budget.
				return redis.NewCmdResult([]any{"false", fingerprint, response, fmt.Sprint(responseDeadline.UnixMilli()), fmt.Sprint(time.Now().UnixMilli()), ""}, nil)
			case completeScript.Hash():
				completeCalls++
				assert.Equal(t, reservationOwner, args[0])
				response = string(args[1].([]byte))

				return redis.NewCmdResult(int64(1), nil)
			default:
				return redis.NewCmdResult(nil, fmt.Errorf("unexpected Redis script: %s", sha))
			}
		}}
		release := make(chan struct{})
		releaseOwner := sync.OnceFunc(func() { close(release) })
		defer releaseOwner()
		calls := 0
		handler := func(c *gin.Context) {
			calls++
			<-release
			c.JSON(http.StatusCreated, gin.H{"sandboxID": "original"})
		}
		ownerRouter := testRouter(t, flags, &redisStore{client: client}, team, time.Minute, handler)
		retryRouter := testRouter(t, flags, &redisStore{client: client}, team, time.Minute, func(_ *gin.Context) {
			t.Error("retry executed the operation")
		})
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		owner := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			owner <- request(ctx, ownerRouter, "/v2/sandboxes", validBody, []string{"key"})
		}()
		synctest.Wait()
		require.Equal(t, 1, calls, "owner must be blocked in the handler")
		w := request(t.Context(), retryRouter, "/v2/sandboxes", validBody, []string{"key"})
		require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), `"error_code":"idempotency_in_progress"`)

		time.Sleep(time.Until(responseDeadline) - time.Millisecond)
		synctest.Wait()
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
		w = request(t.Context(), retryRouter, "/v2/sandboxes", validBody, []string{"key"})
		require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), `"error_code":"idempotency_in_progress"`)

		time.Sleep(time.Millisecond)
		w = request(t.Context(), retryRouter, "/v2/sandboxes", validBody, []string{"key"})
		require.Equal(t, http.StatusUnprocessableEntity, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), `"error_code":"idempotency_outcome_unknown"`)
		require.Equal(t, "false", w.Result().Header.Get("Should-Retry"))
		require.Equal(t, 1, calls)
		require.Zero(t, completeCalls)
		releaseOwner()
		original := <-owner
		require.Equal(t, http.StatusCreated, original.Code, original.Body.String())
		w = request(t.Context(), retryRouter, "/v2/sandboxes", validBody, []string{"key"})
		require.Equal(t, original.Code, w.Code)
		require.Equal(t, original.Body.String(), w.Body.String())
		require.Equal(t, 1, calls)
		require.Equal(t, 5, reserveCalls)
		require.Equal(t, 1, completeCalls)
	})
}

func TestMissingResponseAfterDeadline(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	s := &redisStore{client: client}
	flags, _ := testFlags(t, false, 86400)
	digest, err := fingerprint(jsonRequest(t), validatedRequest(t, flags, validBody))
	require.NoError(t, err)
	for _, deadline := range []string{"1", ""} {
		t.Run("deadline="+deadline, func(t *testing.T) {
			t.Parallel()
			team := uuid.New()
			key := recordKey(team.String(), "key")
			fields := map[string]any{"fingerprint": digest, "owner": "original"}
			if deadline != "" {
				fields["expected_response_at"] = deadline
			}
			require.NoError(t, client.HSet(t.Context(), key, fields).Err())
			require.NoError(t, client.Expire(t.Context(), key, time.Hour).Err())
			before, err := client.HGetAll(t.Context(), key).Result()
			require.NoError(t, err)
			expiresAt, err := client.PExpireTime(t.Context(), key).Result()
			require.NoError(t, err)
			r := testRouter(t, flags, s, team, time.Minute, func(_ *gin.Context) {
				t.Error("request with an unknown outcome was executed again")
			})
			for range 2 {
				w := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
				require.Equal(t, http.StatusUnprocessableEntity, w.Code, w.Body.String())
				require.JSONEq(t, `{"code":422,"message":"The original request has no recorded response and its outcome is unknown. This request was not executed again.","error_code":"idempotency_outcome_unknown"}`, w.Body.String())
			}
			mismatch := request(t.Context(), r, "/v2/sandboxes", `{"templateID":"other"}`, []string{"key"})
			require.Equal(t, http.StatusConflict, mismatch.Code)
			require.Contains(t, mismatch.Body.String(), `"error_code":"idempotency_request_mismatch"`)
			after, err := client.HGetAll(t.Context(), key).Result()
			require.NoError(t, err)
			require.Equal(t, before, after)
			afterExpiry, err := client.PExpireTime(t.Context(), key).Result()
			require.NoError(t, err)
			require.Equal(t, expiresAt, afterExpiry)
			original := cachedResponse{Status: http.StatusCreated, Body: []byte(`{"sandboxID":"original"}`), Headers: http.Header{"Content-Type": {"application/json"}}}
			require.NoError(t, s.complete(t.Context(), key, "original", original, false))
			w := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
			require.Equal(t, original.Status, w.Code)
			require.Equal(t, original.Body, w.Body.Bytes())
		})
	}
}

func TestRetentionFlagValidation(t *testing.T) {
	t.Parallel()
	require.True(t, featureflags.APIIdempotencyRoutes.Fallback().IsNull())
	require.Equal(t, 86400, featureflags.APIIdempotencyTTLSeconds.Fallback())
	client := redisutils.SetupInstance(t)
	for _, tc := range []struct {
		name    string
		value   *ldvalue.Value
		status  int
		seconds int64
	}{
		{"unavailable flag", nil, http.StatusCreated, 86400},
		{"minimum", new(ldvalue.Int(1)), http.StatusCreated, 1},
		{"explicit default", new(ldvalue.Int(86400)), http.StatusCreated, 86400},
		{"above former maximum", new(ldvalue.Int(2592001)), http.StatusCreated, 2592001},
		{"beyond time duration", new(ldvalue.Int(1 << 34)), http.StatusCreated, 1 << 34},
		{"zero", new(ldvalue.Int(0)), http.StatusServiceUnavailable, 0},
		{"negative", new(ldvalue.Int(-1)), http.StatusServiceUnavailable, 0},
		{"string", new(ldvalue.String("86400")), http.StatusServiceUnavailable, 0},
		{"fraction", new(ldvalue.Float64(1.5)), http.StatusServiceUnavailable, 0},
		{"null", new(ldvalue.Null()), http.StatusServiceUnavailable, 0},
		{"redis expiry overflow", new(ldvalue.Int(1 << 54)), http.StatusServiceUnavailable, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := ldtestdata.DataSource()
			source.Update(source.Flag(featureflags.APIIdempotencyRoutes.Key()).ValueForAll(ldvalue.CopyArbitraryValue(map[string]bool{
				"POST /v2/sandboxes": true,
			})))
			if tc.value != nil {
				source.Update(source.Flag(featureflags.APIIdempotencyTTLSeconds.Key()).ValueForAll(*tc.value))
			}
			flags, err := featureflags.NewClientWithDatasource(source)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, flags.Close(context.WithoutCancel(t.Context()))) })
			team := uuid.New()
			calls := 0
			r := testRouter(t, flags, &redisStore{client: client}, team, time.Minute, func(c *gin.Context) {
				calls++
				c.Status(http.StatusCreated)
			})
			before := redisMillis(t, client)
			w := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
			require.Equal(t, tc.status, w.Code, w.Body.String())
			key := recordKey(team.String(), "key")
			if tc.status != http.StatusCreated {
				require.Zero(t, calls)
				require.Zero(t, client.Exists(t.Context(), key).Val())

				return
			}
			require.Equal(t, 1, calls)
			// Bounds come from the Redis clock, so a slow runner only widens the window the key may have aged by.
			ttl, err := client.Do(t.Context(), "PTTL", key).Int64()
			require.NoError(t, err)
			elapsed := redisMillis(t, client) - before
			retention := tc.seconds * 1000
			if ttl == -2 {
				require.GreaterOrEqual(t, elapsed, retention, "key expired before its retention")

				return
			}
			require.NotEqual(t, int64(-1), ttl, "key has no expiry")
			require.LessOrEqual(t, ttl, retention)
			require.GreaterOrEqual(t, ttl, retention-elapsed)
		})
	}
}

func TestInvalidRetentionRejectsKeyedRequestsAsPermanent(t *testing.T) {
	t.Parallel()
	flags, _ := testFlags(t, false, 0)
	r := testRouter(t, flags, faultStore{reserveErr: errors.New("unexpected reservation")}, uuid.New(), time.Second, func(_ *gin.Context) {
		t.Error("misconfigured idempotency executed the request")
	})
	w := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
	require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
	// A configuration fault is not cleared by retrying.
	require.Equal(t, "false", w.Result().Header.Get("Should-Retry"))
}

func redisMillis(t *testing.T, client redis.UniversalClient) int64 {
	t.Helper()
	now, err := client.Time(t.Context()).Result()
	require.NoError(t, err)

	return now.UnixMilli()
}

func TestInvalidRetentionDoesNotAffectBypassedRequests(t *testing.T) {
	t.Parallel()
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprint(disabled), func(t *testing.T) {
			t.Parallel()
			flags, _ := testFlags(t, disabled, 0)
			r := testRouter(t, flags, faultStore{reserveErr: errors.New("unexpected reservation")}, uuid.New(), time.Second, func(c *gin.Context) {
				c.Status(http.StatusCreated)
			})
			var keys []string
			if disabled {
				keys = []string{"key"}
			}
			w := request(t.Context(), r, "/v2/sandboxes", validBody, keys)
			require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
		})
	}
}

func TestKeyedCreatePreservesSchemaDefaults(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	s := &redisStore{client: client}
	flags, _ := testFlags(t, false, 86400)
	var bodies []api.NewSandboxV2
	r := testRouter(t, flags, s, uuid.New(), time.Minute, func(c *gin.Context) {
		body := c.MustGet("parsedCreateBody").(api.NewSandboxV2)
		bodies = append(bodies, body)
		c.JSON(201, body)
	})
	body := `{"templateID":"base","autoResume":{}}`
	ordinary := request(t.Context(), r, "/v2/sandboxes", body, nil)
	require.Equal(t, 201, ordinary.Code, ordinary.Body.String())
	keyed := request(t.Context(), r, "/v2/sandboxes", body, []string{"key"})
	require.Equal(t, 201, keyed.Code, keyed.Body.String())
	require.JSONEq(t, ordinary.Body.String(), keyed.Body.String())
	parsed := bodies[1]
	require.NotNil(t, parsed.AutoResume)
	require.False(t, parsed.AutoResume.Enabled)
	require.Equal(t, bodies[0], parsed)
	require.Equal(t, new(int32(300)), parsed.Timeout)
	require.Equal(t, new(true), parsed.AutoPauseMemory)
	replayed := request(t.Context(), r, "/v2/sandboxes", body, []string{"key"})
	require.Equal(t, keyed.Body.String(), replayed.Body.String())
	require.Len(t, bodies, 2)
	explicit := request(t.Context(), r, "/v2/sandboxes", `{"templateID":"base","autoResume":{"enabled":false}}`, []string{"key"})
	require.Equal(t, keyed.Code, explicit.Code)
	require.Equal(t, keyed.Body.String(), explicit.Body.String())
	require.Len(t, bodies, 2)
}

type delayedReserveStore struct {
	store

	delay         time.Duration
	beforeReserve func(context.Context, time.Time)
}

func (s delayedReserveStore) reserve(ctx context.Context, key, digest, owner string, retentionSeconds int, responseDeadline time.Time) (record, error) {
	if s.beforeReserve != nil {
		s.beforeReserve(ctx, responseDeadline)
	}
	select {
	case <-ctx.Done():
		return record{}, ctx.Err()
	case <-time.After(s.delay):
		return s.store.reserve(ctx, key, digest, owner, retentionSeconds, responseDeadline)
	}
}

func TestReservationHonorsRequestTimeout(t *testing.T) {
	t.Parallel()
	flags, _ := testFlags(t, false, 86400)
	synctest.Test(t, func(t *testing.T) {
		s := &ownerStore{}
		r := testRouter(t, flags, delayedReserveStore{store: s, delay: time.Second}, uuid.New(), 50*time.Millisecond, func(_ *gin.Context) {
			t.Error("expired request executed")
		})
		start := time.Now()
		w := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
		require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
		require.Equal(t, 50*time.Millisecond, time.Since(start))
		require.Zero(t, s.reserveCalls, "timed-out reservation must not reach the store")
	})
}

type ownerStore struct {
	store

	reserveCalls int
}

func (s *ownerStore) reserve(ctx context.Context, _, _, _ string, _ int, _ time.Time) (record, error) {
	s.reserveCalls++

	return record{state: stateOwner}, ctx.Err()
}

func (*ownerStore) complete(ctx context.Context, _, _ string, _ cachedResponse, _ bool) error {
	return ctx.Err()
}

func TestOwnerUsesSameDeadlineAsReservation(t *testing.T) {
	t.Parallel()
	flags, _ := testFlags(t, false, 86400)
	synctest.Test(t, func(t *testing.T) {
		var reservationDeadline time.Time
		s := delayedReserveStore{
			store: &ownerStore{},
			// Advance fake time so restarting the timeout after reservation is observable.
			delay: 40 * time.Millisecond,
			beforeReserve: func(ctx context.Context, responseDeadline time.Time) {
				var ok bool
				reservationDeadline, ok = ctx.Deadline()
				require.True(t, ok)
				require.Equal(t, reservationDeadline.Add(completionTimeout), responseDeadline)
			},
		}
		r := testRouter(t, flags, s, uuid.New(), 500*time.Millisecond, func(c *gin.Context) {
			deadline, ok := c.Request.Context().Deadline()
			require.True(t, ok)
			require.Equal(t, reservationDeadline, deadline)
			select {
			case <-c.Request.Context().Done():
				c.Status(http.StatusServiceUnavailable)
			case <-time.After(150 * time.Millisecond):
				c.Status(http.StatusCreated)
			}
		})
		w := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
		require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	})
}

func TestReservationHonorsEarlierCallerDeadline(t *testing.T) {
	t.Parallel()
	flags, _ := testFlags(t, false, 86400)
	synctest.Test(t, func(t *testing.T) {
		s := &ownerStore{}
		r := testRouter(t, flags, delayedReserveStore{store: s, delay: time.Second}, uuid.New(), time.Second, func(_ *gin.Context) {
			t.Error("expired request executed")
		})
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		started := time.Now()
		w := request(ctx, r, "/v2/sandboxes", validBody, []string{"key"})
		require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
		require.Equal(t, 100*time.Millisecond, time.Since(started))
		require.Zero(t, s.reserveCalls, "timed-out reservation must not reach the store")
	})
}
