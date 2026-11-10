package pauselimit

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// MaxPeriod bounds how long a limiter key stays in Redis.
const MaxPeriod = 10 * time.Minute

type Limit struct {
	Pauses int
	Period time.Duration
}

type Decision string

const (
	DecisionAllowed Decision = "allowed"
	// DecisionDeduplicated: the execution's pause was already counted, so the
	// request joins that pause without using another slot.
	DecisionDeduplicated Decision = "deduplicated"
	DecisionLimited      Decision = "limited"
	decisionError        Decision = "error"
)

type Result struct {
	Decision   Decision
	RetryAfter time.Duration
}

// allowScript is GCRA with burst = limit: each counted pause moves the
// theoretical arrival time (tat) forward by period/limit, and a pause is
// allowed while tat stays within one period of now. The key expires when tat
// is reached, so it never outlives the period.
var allowScript = redis.NewScript(`
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local limit = tonumber(ARGV[1])
local period_ms = tonumber(ARGV[2])
local execution_id = ARGV[3]

local state = redis.call('HMGET', KEYS[1], 'tat', 'execution_id')
if execution_id ~= '' and state[2] == execution_id then
    return {'deduplicated', '0'}
end

local tat = math.max(tonumber(state[1]) or now_ms, now_ms)
local new_tat = math.ceil(tat + period_ms / limit)
local retry_ms = new_tat - period_ms - now_ms
if retry_ms > 0 then
    return {'limited', string.format('%.0f', retry_ms)}
end

redis.call('HSET', KEYS[1], 'tat', string.format('%.0f', new_tat), 'execution_id', execution_id)
redis.call('PEXPIREAT', KEYS[1], new_tat)
return {'allowed', '0'}
`)

type Limiter struct {
	client    redis.UniversalClient
	ff        *featureflags.Client
	decisions metric.Int64Counter
}

func New(client redis.UniversalClient, ff *featureflags.Client, meterProvider metric.MeterProvider) (*Limiter, error) {
	meter := meterProvider.Meter("github.com/e2b-dev/infra/packages/api/internal/pauselimit")
	decisions, err := telemetry.GetCounter(meter, telemetry.ApiSandboxPauseRateLimit)
	if err != nil {
		return nil, fmt.Errorf("create pause rate-limit counter: %w", err)
	}

	return &Limiter{client: client, ff: ff, decisions: decisions}, nil
}

// Limit returns the limit the flag sets for the request's context, and false
// when pauses are unlimited.
func (l *Limiter) Limit(ctx context.Context) (Limit, bool) {
	return parseLimit(l.ff.JSONFlag(ctx, featureflags.SandboxPauseRateLimitFlag))
}

func parseLimit(value ldvalue.Value) (Limit, bool) {
	pauses, periodSeconds := value.GetByKey("limit"), value.GetByKey("period_s")
	if !pauses.IsInt() || !periodSeconds.IsInt() {
		return Limit{}, false
	}

	limit := Limit{
		Pauses: pauses.IntValue(),
		Period: time.Duration(periodSeconds.IntValue()) * time.Second,
	}
	if limit.Pauses < 1 || limit.Period < time.Second || limit.Period > MaxPeriod {
		return Limit{}, false
	}

	return limit, true
}

// Allow counts a pause of the sandbox's current execution. Requests for an
// execution that was already counted share its slot, so concurrent pauses of
// one execution count once.
func (l *Limiter) Allow(ctx context.Context, limit Limit, teamID, sandboxID, executionID string) (Result, error) {
	result, err := l.allow(ctx, limit, sandboxID, executionID)
	decision := result.Decision
	if err != nil {
		decision = decisionError
	}
	l.decisions.Add(ctx, 1, metric.WithAttributes(
		telemetry.WithTeamID(teamID),
		attribute.String("decision", string(decision)),
	))

	return result, err
}

func (l *Limiter) allow(ctx context.Context, limit Limit, sandboxID, executionID string) (Result, error) {
	key := redis_utils.CreateKey("ratelimit", "sandbox-pause", sandboxID)
	reply, err := allowScript.Run(ctx, l.client, []string{key}, limit.Pauses, limit.Period.Milliseconds(), executionID).StringSlice()
	if err != nil {
		return Result{}, fmt.Errorf("run pause rate-limit script: %w", err)
	}
	if len(reply) != 2 {
		return Result{}, fmt.Errorf("unexpected pause rate-limit reply %q", reply)
	}

	retryAfterMs, err := strconv.ParseInt(reply[1], 10, 64)
	if err != nil {
		return Result{}, fmt.Errorf("parse pause rate-limit retry: %w", err)
	}

	switch decision := Decision(reply[0]); decision {
	case DecisionAllowed, DecisionDeduplicated, DecisionLimited:
		return Result{Decision: decision, RetryAfter: time.Duration(retryAfterMs) * time.Millisecond}, nil
	default:
		return Result{}, fmt.Errorf("unexpected pause rate-limit decision %q", decision)
	}
}
