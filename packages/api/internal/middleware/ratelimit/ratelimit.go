package ratelimit

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis_rate/v10"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/middleware"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const rateLimitPefix = "ratelimit"

const (
	decisionAllowed = "allowed"
	decisionLimited = "limited"
	decisionError   = "error"
)

type requestLimiter interface {
	Allow(ctx context.Context, key string, limit redis_rate.Limit) (*redis_rate.Result, error)
}

// Config defines the rate limit parameters.
type Config struct {
	// FailOpen allows requests through when Redis is unavailable.
	FailOpen bool
}

// NewLimiter creates a redis_rate.Limiter from a Redis client.
func NewLimiter(redisClient redis.UniversalClient) *redis_rate.Limiter {
	return redis_rate.NewLimiter(redisClient)
}

// resolveLimit returns the rate limit for the current request, checking the
// RateLimitConfigFlag. The flag JSON format is:
//
//	{
//	  "/sandboxes/": {"rate": 50, "burst": 100, "period_s": 1},
//	  "/sandboxes/:sandboxID/pause": {"rate": 10, "burst": 20, "period_s": 60}
//	}
//
// period_s is optional and defaults to 1 (second).
// The route is the Gin route pattern (c.FullPath()). If no config exists
// for the route (or the flag is null), returns false (no limit applied).
func resolveLimit(ctx context.Context, ff *featureflags.Client, route string) (redis_rate.Limit, bool) {
	flagValue := ff.JSONFlag(ctx, featureflags.RateLimitConfigFlag)
	if flagValue.IsNull() {
		return redis_rate.Limit{}, false
	}

	override := flagValue.GetByKey(route)
	if override.IsNull() {
		return redis_rate.Limit{}, false
	}

	rate := override.GetByKey("rate")
	burst := override.GetByKey("burst")

	if !rate.IsInt() || !burst.IsInt() {
		return redis_rate.Limit{}, false
	}

	period := time.Second
	if v := override.GetByKey("period_s"); v.IsInt() {
		period = time.Duration(v.IntValue()) * time.Second
	}

	return redis_rate.Limit{
		Rate:   rate.IntValue(),
		Burst:  burst.IntValue(),
		Period: period,
	}, true
}

func Middleware(limiter requestLimiter, cfg Config, ff *featureflags.Client, meterProvider metric.MeterProvider, l logger.Logger) (gin.HandlerFunc, error) {
	meter := meterProvider.Meter("github.com/e2b-dev/infra/packages/api/internal/middleware/ratelimit")
	requests, err := telemetry.GetCounter(meter, telemetry.ApiRateLimitRequests)
	if err != nil {
		return nil, fmt.Errorf("create rate-limit requests counter: %w", err)
	}

	return func(c *gin.Context) {
		// Keep existing route limits active in every V2 rollout mode.
		checkLimitV1(c, limiter, cfg, ff, l)
		if c.IsAborted() {
			return
		}

		mode := ff.StringFlag(c.Request.Context(), featureflags.RateLimitV2Mode)
		switch mode {
		case featureflags.APIGroupRateLimitEnabled, featureflags.APIGroupRateLimitShadow:
			checkLimitV2(c, limiter, requests, l, mode)
		}
		if !c.IsAborted() {
			c.Next()
		}
	}, nil
}

func checkLimitV1(c *gin.Context, limiter requestLimiter, cfg Config, ff *featureflags.Client, log logger.Logger) {
	ctx := c.Request.Context()

	// Skip unauthenticated requests
	team, ok := auth.GetTeamInfo(c)
	if !ok {
		return
	}

	route := c.FullPath()

	// Resolve per-team limit overrides from feature flag.
	// If the route is not configured, skip V1 limiting.
	limit, ok := resolveLimit(ctx, ff, route)
	if !ok {
		return
	}

	// Build a logger with rate limit context for reuse.
	teamID := team.ID.String()
	l := log.With(
		logger.WithTeamID(teamID),
		zap.String("route", route),
		zap.Int("rate_limit_rate", limit.Rate),
		zap.Int("rate_limit_burst", limit.Burst),
	)

	key := redis_utils.CreateKey(rateLimitPefix, teamID, route)
	res, err := limiter.Allow(ctx, key, limit)
	if err != nil {
		l.Warn(ctx, "rate limiter Redis error", zap.Error(err))

		if cfg.FailOpen {
			return
		}

		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
			"code":    http.StatusInternalServerError,
			"message": "Rate limiter unavailable",
		})

		return
	}

	retryAfterSecs := max(int(res.RetryAfter.Seconds()), 1)
	writeRateLimitResult(c, int64(limit.Burst), res, retryAfterSecs)
	if res.Allowed == 0 {
		l.Warn(ctx, "rate limit exceeded",
			zap.Int("remaining", res.Remaining),
			zap.Int("retry_after_s", retryAfterSecs),
		)
	}
}

var apiGroupRPSFields = map[string]func(*types.TeamLimits) int64{
	"list": func(limits *types.TeamLimits) int64 { return limits.APITeamRPSList },
}

// Keys combine a team hash tag with an escaped method and route template to keep
// budgets distinct while co-locating the team's keys in one Redis Cluster slot.
func rateLimitKeyV2(teamID uuid.UUID, method, route string) string {
	return redis_utils.CreateKey(
		"ratelimit", "v2", redis_utils.SameSlot(teamID.String()),
		url.QueryEscape(method), url.QueryEscape(route),
	)
}

func checkLimitV2(c *gin.Context, limiter requestLimiter, rateLimitCounter metric.Int64Counter, l logger.Logger, mode string) {
	ctx := c.Request.Context()
	group := middleware.APIGroupFromContext(c)
	limitField, hasLimit := apiGroupRPSFields[group]
	team, ok := auth.GetTeamInfo(c)
	if !hasLimit || !ok || team.Limits == nil {
		return
	}

	rate := limitField(team.Limits)
	if rate <= 0 {
		return
	}

	limit := redis_rate.Limit{Rate: int(rate), Burst: int(rate), Period: time.Second}
	method, route := c.Request.Method, c.FullPath()
	key := rateLimitKeyV2(team.ID, method, route)
	result, err := limiter.Allow(ctx, key, limit)
	decision := decisionAllowed
	if err != nil {
		decision = decisionError
	} else if result.Allowed == 0 {
		decision = decisionLimited
	}

	rateLimitCounter.Add(ctx, 1, metric.WithAttributes(
		telemetry.WithTeamID(team.ID.String()),
		attribute.String("api_group", group),
		attribute.String("mode", mode),
		attribute.String("decision", decision),
	))
	fields := []zap.Field{
		logger.WithTeamID(team.ID.String()),
		zap.String("api_group", group),
		zap.String("method", method),
		zap.String("route", route),
		zap.String("mode", mode),
		zap.String("decision", decision),
		zap.Int64("rate_limit_rate", rate),
	}
	if err != nil {
		l.Warn(ctx, "API group rate limiter unavailable", append(fields, zap.Error(err))...)

		return
	}

	if mode == featureflags.APIGroupRateLimitShadow {
		l.Info(ctx, "API group rate limit shadow decision", fields...)

		return
	}

	retryAfterSecs := max(int(math.Ceil(result.RetryAfter.Seconds())), 1)
	writeRateLimitResult(c, rate, result, retryAfterSecs)
	if result.Allowed == 0 {
		l.Warn(ctx, "API group rate limit exceeded", fields...)
	}
}

func writeRateLimitResult(c *gin.Context, burst int64, result *redis_rate.Result, retryAfterSecs int) {
	c.Header("RateLimit-Limit", strconv.FormatInt(burst, 10))
	c.Header("RateLimit-Remaining", strconv.Itoa(result.Remaining))
	c.Header("RateLimit-Reset", strconv.FormatInt(int64(math.Ceil(result.ResetAfter.Seconds())), 10))
	if result.Allowed > 0 {
		return
	}

	c.Header("Retry-After", strconv.Itoa(retryAfterSecs))
	c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
		"code":    http.StatusTooManyRequests,
		"message": "Rate limit exceeded",
	})
}
