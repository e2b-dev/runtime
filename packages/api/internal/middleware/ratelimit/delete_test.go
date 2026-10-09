package ratelimit

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestTeamDeleteEndpointsUseConfiguredLimit(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"/sandboxes/sbx-a", "/templates/template-a",
	} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			team := groupTeam(13)
			team.Limits.APITeamRPSDelete = 1<<32 + 7
			calls := 0
			limiter := groupLimiterFunc(func(_ context.Context, _ string, limit redis_rate.Limit) (*redis_rate.Result, error) {
				calls++
				assert.Equal(t, int64(1<<32+7), int64(limit.Rate))
				assert.Equal(t, limit.Rate, limit.Burst)
				assert.Equal(t, time.Second, limit.Period)

				return &redis_rate.Result{Allowed: 0, RetryAfter: 1500 * time.Millisecond}, nil
			})
			f := groupRouter(t, limiter, "enabled", team)
			f.flags.Update(f.flags.Flag(featureflags.RateLimitDeleteMode.Key()).ValueForAll(ldvalue.String("enabled")))
			response := groupRequest(t, f.router, http.MethodDelete, path)
			assert.Equal(t, http.StatusTooManyRequests, response.Code)
			assert.Equal(t, 1, calls)
			assert.Empty(t, response.Header().Get("X-Test-Handler"))
			assert.Equal(t, "4294967303", response.Header().Get("RateLimit-Limit"))
			assert.Equal(t, "2", response.Header().Get("Retry-After"))
			assert.Equal(t, map[string]int64{"limited": 1}, decisionsForGroup(t, f.reader, "enabled", team.ID, "delete"))
		})
	}
}

func TestDeleteGroupRolloutAndV1Precedence(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, mode, decision   string
		rate                   int64
		v1, v1Reject, v2Reject bool
		redisError             bool
		status                 int
		header                 string
	}{
		{"disabled", "disabled", "", 7, false, false, false, false, 204, ""},
		{"missing mode", "", "", 7, false, false, false, false, 204, ""},
		{"unknown mode", "unknown", "", 7, false, false, false, false, 204, ""},
		{"zero rate", "enabled", "", 0, false, false, false, false, 204, ""},
		{"allowed", "enabled", "allowed", 7, false, false, false, false, 204, "7"},
		{"limited", "enabled", "limited", 7, false, false, true, false, 429, "7"},
		{"shadow limited", "shadow", "limited", 7, false, false, true, false, 204, ""},
		{"enabled error", "enabled", "error", 7, true, false, false, true, 204, "13"},
		{"shadow error", "shadow", "error", 7, true, false, false, true, 204, "13"},
		{"V1 rejects enabled", "enabled", "", 7, true, true, false, false, 429, "13"},
		{"V1 rejects shadow", "shadow", "", 7, true, true, false, false, 429, "13"},
		{"V1 rejects disabled", "disabled", "", 7, true, true, false, false, 429, "13"},
		{"V1 rejects zero", "enabled", "", 0, true, true, false, false, 429, "13"},
		{"V2 rejects after V1", "enabled", "limited", 7, true, false, true, false, 429, "7"},
		{"shadow preserves V1", "shadow", "limited", 7, true, false, true, false, 204, "13"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			team := groupTeam(13)
			team.Limits.APITeamRPSDelete = tc.rate
			v1Key := redisutils.CreateKey("ratelimit", team.ID.String(), "/sandboxes/:sandboxID")
			v2Key := rateLimitKeyV2(team.ID, http.MethodDelete, "/sandboxes/:sandboxID")
			var calls []string
			limiter := groupLimiterFunc(func(_ context.Context, key string, limit redis_rate.Limit) (*redis_rate.Result, error) {
				calls = append(calls, key)
				reject := tc.v2Reject
				if key == v1Key {
					assert.True(t, tc.v1)
					reject = tc.v1Reject
				} else {
					assert.Equal(t, v2Key, key)
					assert.Equal(t, int(tc.rate), limit.Rate)
					if tc.redisError {
						return nil, errors.New("Redis unavailable")
					}
				}
				allowed := 1
				if reject {
					allowed = 0
				}

				return &redis_rate.Result{Allowed: allowed, Remaining: 3, RetryAfter: 1500 * time.Millisecond}, nil
			})
			f := groupRouter(t, limiter, "enabled", team)
			if tc.mode != "" {
				f.flags.Update(f.flags.Flag(featureflags.RateLimitDeleteMode.Key()).ValueForAll(ldvalue.String(tc.mode)))
			}
			var wantCalls []string
			if tc.v1 {
				f.flags.Update(f.flags.Flag(featureflags.RateLimitConfigFlag.Key()).ValueForAll(ldvalue.CopyArbitraryValue(
					map[string]map[string]int{"/sandboxes/:sandboxID": {"rate": 13, "burst": 13}},
				)))
				wantCalls = append(wantCalls, v1Key)
			}
			response := groupRequest(t, f.router, http.MethodDelete, "/sandboxes/sbx-a")
			assert.Equal(t, tc.status, response.Code)
			assert.Equal(t, tc.header, response.Header().Get("RateLimit-Limit"))
			decisions := decisionsForGroup(t, f.reader, tc.mode, team.ID, "delete")
			if tc.decision != "" {
				wantCalls = append(wantCalls, v2Key)
				assert.Equal(t, map[string]int64{tc.decision: 1}, decisions)
			} else {
				assert.Empty(t, decisions)
			}
			assert.Equal(t, wantCalls, calls)
			if tc.status == http.StatusTooManyRequests {
				assert.Empty(t, response.Header().Get("X-Test-Handler"))
				assert.JSONEq(t, `{"code":429,"message":"Rate limit exceeded"}`, response.Body.String())
			} else {
				assert.Equal(t, "called", response.Header().Get("X-Test-Handler"))
				assert.Empty(t, response.Header().Get("Retry-After"))
			}
		})
	}
}

func TestDeleteGroupExcludesOtherDeleteOperations(t *testing.T) {
	t.Parallel()

	team := groupTeam(13)
	team.Limits.APITeamRPSDelete = 1
	limiter := groupLimiterFunc(func(context.Context, string, redis_rate.Limit) (*redis_rate.Result, error) {
		t.Fatal("ungrouped delete reached Redis")

		return nil, nil
	})
	f := groupRouter(t, limiter, "enabled", team)
	f.flags.Update(f.flags.Flag(featureflags.RateLimitDeleteMode.Key()).ValueForAll(ldvalue.String("enabled")))
	for _, path := range []string{
		"/templates/tags",
		"/api-keys/00000000-0000-0000-0000-000000000001",
		"/volumes/00000000-0000-0000-0000-000000000001",
		"/secrets/secret-a",
		"/events/webhooks/00000000-0000-0000-0000-000000000001",
		"/admin/teams/00000000-0000-0000-0000-000000000001/api-keys/00000000-0000-0000-0000-000000000002",
		"/clusters/00000000-0000-0000-0000-000000000001/rigs/instances/instance-a?decrementDesired=false",
	} {
		response := groupRequest(t, f.router, http.MethodDelete, path)
		assert.Equal(t, http.StatusNoContent, response.Code, path)
		assert.Equal(t, "called", response.Header().Get("X-Test-Handler"), path)
		assert.Empty(t, response.Header().Get("RateLimit-Limit"), path)
		assert.Empty(t, response.Header().Get("Retry-After"), path)
	}
	assert.Empty(t, decisionsForGroup(t, f.reader, "enabled", team.ID, "delete"))
}

func TestIntegration_DeleteBudgetsAreSharedByRouteAndIsolatedByMethodAndTeam(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping Redis integration test")
	}
	limiter := slowRefillRedisLimiter(t)
	team := groupTeam(2)
	team.Limits.APITeamRPSDelete = 1
	f := groupRouter(t, limiter, "enabled", team)
	replica := groupRouter(t, limiter, "enabled", team)
	otherTeam := groupTeam(2)
	otherTeam.Limits.APITeamRPSDelete = 1
	other := groupRouter(t, limiter, "enabled", otherTeam)
	for _, fixture := range []groupFixture{f, replica, other} {
		fixture.flags.Update(fixture.flags.Flag(featureflags.RateLimitDeleteMode.Key()).ValueForAll(ldvalue.String("enabled")))
	}
	assert.Equal(t, 204, groupRequest(t, f.router, http.MethodDelete, "/templates/template-a").Code)
	assert.Equal(t, 429, groupRequest(t, replica.router, http.MethodDelete, "/templates/template-b").Code)
	assert.Equal(t, 204, groupRequest(t, f.router, http.MethodGet, "/templates/template-a").Code)
	assert.Equal(t, 204, groupRequest(t, f.router, http.MethodGet, "/templates/template-b").Code)
	assert.Equal(t, 429, groupRequest(t, f.router, http.MethodGet, "/templates/template-c").Code)
	assert.Equal(t, 204, groupRequest(t, f.router, http.MethodDelete, "/sandboxes/sbx-a").Code)
	assert.Equal(t, 429, groupRequest(t, replica.router, http.MethodDelete, "/sandboxes/sbx-b").Code)
	assert.Equal(t, 204, groupRequest(t, other.router, http.MethodDelete, "/sandboxes/sbx-a").Code)
}

func TestDeleteRateModeDefaultsDisabledWithoutAffectingLists(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"shadow", "enabled"} {
		for name, value := range map[string]*ldvalue.Value{
			"missing":    nil,
			"disabled":   new(ldvalue.String("disabled")),
			"unknown":    new(ldvalue.String("unknown")),
			"wrong type": new(ldvalue.Bool(true)),
		} {
			t.Run(mode+"/"+name, func(t *testing.T) {
				t.Parallel()

				team := groupTeam(13)
				team.Limits.APITeamRPSDelete = 7
				calls := 0
				limiter := groupLimiterFunc(func(_ context.Context, key string, _ redis_rate.Limit) (*redis_rate.Result, error) {
					calls++
					assert.Equal(t, rateLimitKeyV2(team.ID, http.MethodGet, "/templates/:templateID"), key)

					return &redis_rate.Result{Allowed: 0, RetryAfter: time.Second}, nil
				})
				f := groupRouter(t, limiter, mode, team)
				if value != nil {
					f.flags.Update(f.flags.Flag(featureflags.RateLimitDeleteMode.Key()).ValueForAll(*value))
				}
				response := groupRequest(t, f.router, http.MethodDelete, "/templates/template-a")
				assert.Equal(t, http.StatusNoContent, response.Code)
				assert.Equal(t, "called", response.Header().Get("X-Test-Handler"))
				assert.Empty(t, response.Header().Get("RateLimit-Limit"))
				assert.Empty(t, response.Header().Get("Retry-After"))
				assert.Zero(t, calls)
				assert.Empty(t, decisionsForGroup(t, f.reader, mode, team.ID, "delete"))

				response = groupRequest(t, f.router, http.MethodGet, "/templates/template-a")
				status := http.StatusTooManyRequests
				if mode == "shadow" {
					status = http.StatusNoContent
				}
				assert.Equal(t, status, response.Code)
				assert.Equal(t, 1, calls)
				assert.Equal(t, map[string]int64{"limited": 1}, groupDecisions(t, f.reader, mode, team.ID))
			})
		}
	}
}

func TestEitherDisabledRateModePreservesV1(t *testing.T) {
	t.Parallel()

	for _, modes := range []struct{ global, delete string }{
		{"disabled", "disabled"},
		{"shadow", "disabled"},
		{"enabled", "disabled"},
		{"disabled", "shadow"},
		{"disabled", "enabled"},
		{"", "enabled"},
		{"unknown", "enabled"},
	} {
		for _, allowed := range []int{0, 1} {
			t.Run(modes.global+"/"+modes.delete+"/"+strconv.Itoa(allowed), func(t *testing.T) {
				t.Parallel()

				team := groupTeam(13)
				team.Limits.APITeamRPSDelete = 7
				calls := 0
				limiter := groupLimiterFunc(func(_ context.Context, key string, _ redis_rate.Limit) (*redis_rate.Result, error) {
					calls++
					assert.Equal(t, redisutils.CreateKey("ratelimit", team.ID.String(), "/sandboxes/:sandboxID"), key)

					return &redis_rate.Result{Allowed: allowed, Remaining: 3, RetryAfter: time.Second}, nil
				})
				f := groupRouter(t, limiter, modes.global, team)
				f.flags.Update(f.flags.Flag(featureflags.RateLimitDeleteMode.Key()).ValueForAll(ldvalue.String(modes.delete)))
				f.flags.Update(f.flags.Flag(featureflags.RateLimitConfigFlag.Key()).ValueForAll(ldvalue.CopyArbitraryValue(
					map[string]map[string]int{"/sandboxes/:sandboxID": {"rate": 13, "burst": 13}},
				)))
				response := groupRequest(t, f.router, http.MethodDelete, "/sandboxes/sbx-a")
				assert.Equal(t, 1, calls)
				assert.Equal(t, "13", response.Header().Get("RateLimit-Limit"))
				assert.Empty(t, decisionsForGroup(t, f.reader, "disabled", team.ID, "delete"))
				if allowed == 0 {
					assert.Equal(t, http.StatusTooManyRequests, response.Code)
					assert.Empty(t, response.Header().Get("X-Test-Handler"))
				} else {
					assert.Equal(t, http.StatusNoContent, response.Code)
					assert.Equal(t, "called", response.Header().Get("X-Test-Handler"))
				}
			})
		}
	}
}

func TestIntegration_DeleteRateModesPreserveBudgetsAcrossRolloutChanges(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping Redis integration test")
	}
	limiter := slowRefillRedisLimiter(t)
	team := groupTeam(1)
	team.Limits.APITeamRPSDelete = 1
	f := groupRouter(t, limiter, "enabled", team)
	for range 3 {
		assert.Equal(t, http.StatusNoContent, groupRequest(t, f.router, http.MethodDelete, "/templates/template-a").Code)
	}
	assert.Equal(t, http.StatusNoContent, groupRequest(t, f.router, http.MethodGet, "/templates/template-a").Code)
	f.flags.Update(f.flags.Flag(featureflags.RateLimitDeleteMode.Key()).ValueForAll(ldvalue.String("shadow")))
	assert.Equal(t, http.StatusNoContent, groupRequest(t, f.router, http.MethodDelete, "/templates/template-a").Code)
	shadowResponse := groupRequest(t, f.router, http.MethodDelete, "/templates/template-b")
	assert.Equal(t, http.StatusNoContent, shadowResponse.Code)
	assert.Empty(t, shadowResponse.Header().Get("Retry-After"))
	assert.Empty(t, shadowResponse.Header().Get("RateLimit-Limit"))
	f.flags.Update(f.flags.Flag(featureflags.RateLimitDeleteMode.Key()).ValueForAll(ldvalue.String("enabled")))
	assert.Equal(t, http.StatusTooManyRequests, groupRequest(t, f.router, http.MethodDelete, "/templates/template-b").Code)
	f.flags.Update(f.flags.Flag(featureflags.RateLimitDeleteMode.Key()).ValueForAll(ldvalue.String("disabled")))
	assert.Equal(t, http.StatusNoContent, groupRequest(t, f.router, http.MethodDelete, "/templates/template-b").Code)
	assert.Equal(t, http.StatusTooManyRequests, groupRequest(t, f.router, http.MethodGet, "/templates/template-b").Code)
	f.flags.Update(f.flags.Flag(featureflags.RateLimitDeleteMode.Key()).ValueForAll(ldvalue.String("enabled")))
	assert.Equal(t, http.StatusTooManyRequests, groupRequest(t, f.router, http.MethodDelete, "/templates/template-b").Code)
	f.flags.Update(f.flags.Flag(featureflags.RateLimitV2Mode.Key()).ValueForAll(ldvalue.String("disabled")))
	assert.Equal(t, http.StatusNoContent, groupRequest(t, f.router, http.MethodDelete, "/templates/template-b").Code)
	assert.Equal(t, http.StatusNoContent, groupRequest(t, f.router, http.MethodGet, "/templates/template-b").Code)
	f.flags.Update(f.flags.Flag(featureflags.RateLimitV2Mode.Key()).ValueForAll(ldvalue.String("shadow")))
	assert.Equal(t, http.StatusNoContent, groupRequest(t, f.router, http.MethodDelete, "/templates/template-b").Code)
	f.flags.Update(f.flags.Flag(featureflags.RateLimitV2Mode.Key()).ValueForAll(ldvalue.String("enabled")))
	assert.Equal(t, http.StatusTooManyRequests, groupRequest(t, f.router, http.MethodDelete, "/templates/template-b").Code)
	assert.Equal(t, http.StatusTooManyRequests, groupRequest(t, f.router, http.MethodGet, "/templates/template-b").Code)
}

func TestGlobalRateLimitModeGatesDeleteMode(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ global, delete, effective string }{
		{"disabled", "disabled", "disabled"},
		{"disabled", "shadow", "disabled"},
		{"disabled", "enabled", "disabled"},
		{"shadow", "disabled", "disabled"},
		{"shadow", "shadow", "shadow"},
		{"shadow", "enabled", "shadow"},
		{"enabled", "disabled", "disabled"},
		{"enabled", "shadow", "shadow"},
		{"enabled", "enabled", "enabled"},
		{"", "shadow", "disabled"},
		{"", "enabled", "disabled"},
		{"unknown", "shadow", "disabled"},
		{"unknown", "enabled", "disabled"},
		{"wrong type", "shadow", "disabled"},
		{"wrong type", "enabled", "disabled"},
	} {
		t.Run(tc.global+"/"+tc.delete, func(t *testing.T) {
			t.Parallel()

			team := groupTeam(13)
			team.Limits.APITeamRPSDelete = 7
			deleteKey := rateLimitKeyV2(team.ID, http.MethodDelete, "/templates/:templateID")
			listKey := rateLimitKeyV2(team.ID, http.MethodGet, "/templates/:templateID")
			var calls []string
			limiter := groupLimiterFunc(func(_ context.Context, key string, limit redis_rate.Limit) (*redis_rate.Result, error) {
				calls = append(calls, key)
				if key == deleteKey {
					assert.Equal(t, 7, limit.Rate)
				} else {
					assert.Equal(t, listKey, key)
					assert.Equal(t, 13, limit.Rate)
				}

				return &redis_rate.Result{Allowed: 0, RetryAfter: time.Second}, nil
			})
			f := groupRouter(t, limiter, tc.global, team)
			if tc.global == "wrong type" {
				f.flags.Update(f.flags.Flag(featureflags.RateLimitV2Mode.Key()).ValueForAll(ldvalue.Bool(true)))
			}
			f.flags.Update(f.flags.Flag(featureflags.RateLimitDeleteMode.Key()).ValueForAll(ldvalue.String(tc.delete)))
			response := groupRequest(t, f.router, http.MethodDelete, "/templates/template-a")
			var wantCalls []string
			if tc.effective == "enabled" {
				assert.Equal(t, http.StatusTooManyRequests, response.Code)
				assert.Equal(t, "7", response.Header().Get("RateLimit-Limit"))
				assert.Equal(t, "1", response.Header().Get("Retry-After"))
				assert.Empty(t, response.Header().Get("X-Test-Handler"))
			} else {
				assert.Equal(t, http.StatusNoContent, response.Code)
				assert.Empty(t, response.Header().Get("RateLimit-Limit"))
				assert.Empty(t, response.Header().Get("Retry-After"))
				assert.Equal(t, "called", response.Header().Get("X-Test-Handler"))
			}
			decisions := decisionsForGroup(t, f.reader, tc.effective, team.ID, "delete")
			if tc.effective == "disabled" {
				assert.Empty(t, decisions)
			} else {
				wantCalls = append(wantCalls, deleteKey)
				assert.Equal(t, map[string]int64{"limited": 1}, decisions)
			}
			response = groupRequest(t, f.router, http.MethodGet, "/templates/template-a")
			if tc.global == "enabled" {
				assert.Equal(t, http.StatusTooManyRequests, response.Code)
				assert.Equal(t, "13", response.Header().Get("RateLimit-Limit"))
			} else {
				assert.Equal(t, http.StatusNoContent, response.Code)
				assert.Empty(t, response.Header().Get("RateLimit-Limit"))
			}
			if tc.global == "enabled" || tc.global == "shadow" {
				wantCalls = append(wantCalls, listKey)
			}
			assert.Equal(t, wantCalls, calls)
		})
	}
}

func slowRefillRedisLimiter(t *testing.T) groupLimiterFunc {
	t.Helper()

	limiter := redis_rate.NewLimiter(redisutils.SetupInstance(t))

	return func(ctx context.Context, key string, limit redis_rate.Limit) (*redis_rate.Result, error) {
		require.Equal(t, time.Second, limit.Period)
		limit.Period = time.Hour
		result, err := limiter.Allow(ctx, key, limit)
		require.NoError(t, err)

		return result, err
	}
}
