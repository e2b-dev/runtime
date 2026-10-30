package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// Lua scripts for atomic operations.
// These scripts ensure true atomicity in Redis cluster mode
var (
	// Add bypasses the storage lock; compare the record atomically before restoring it.
	restoreSandboxScript = redis.NewScript(`
		local current = redis.call('GET', KEYS[1])
		if current == ARGV[2] then
			return 1
		end
		if current ~= ARGV[1] then
			return 0
		end
		redis.call('SET', KEYS[1], ARGV[2], 'KEEPTTL')
		return 1
	`)

	finishTransitionScript = redis.NewScript(`
		redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
		if ARGV[4] ~= '' then
			redis.call('SET', KEYS[3], ARGV[4], 'EX', ARGV[3])
		end
		if redis.call('GET', KEYS[1]) == ARGV[1] then
			return redis.call('DEL', KEYS[1])
		end
		return 0
	`)

	// addSandboxScript atomically stores a sandbox and adds it to the team index.
	// KEYS[1] = sandbox key, KEYS[2] = team index key
	// ARGV[1] = serialized sandbox data, ARGV[2] = sandbox ID
	addSandboxScript = redis.NewScript(`
		redis.call('SET', KEYS[1], ARGV[1])
		redis.call('SADD', KEYS[2], ARGV[2])
		return 1
	`)

	// removeSandboxScript atomically removes a sandbox and its team index entry.
	// It returns the stored JSON (or nil if the key was already gone) so the
	// caller knows exactly which execution it removed and can scope the
	// expiration-index cleanup to that execution.
	// KEYS[1] = sandbox key, KEYS[2] = team index key
	// ARGV[1] = sandbox ID
	removeSandboxScript = redis.NewScript(`
		local data = redis.call('GET', KEYS[1])
		redis.call('DEL', KEYS[1])
		redis.call('SREM', KEYS[2], ARGV[1])
		return data
	`)

	// startTransitionScript atomically updates sandbox and sets transition key with UUID.
	// This is called AFTER Go code has validated the transition and prepared the new sandbox data.
	//
	// When ARGV[5] is set, the write only happens if the stored record is still
	// that execution. This is the enforcement point for RemoveOpts.
	// ExpectExecutionID, and it has to live in here rather than in Go: Add is
	// lockless, so a resume can install a new incarnation between a Go-side
	// comparison and this write, and the SET below would then overwrite the new
	// live record with the stale one. Returns 0 without writing anything if the
	// record is gone or is a different execution.
	// KEYS[1] = sandbox key
	// KEYS[2] = transition key
	// KEYS[3] = transition result key
	// ARGV[1] = new sandbox JSON data
	// ARGV[2] = transition ID (UUID)
	// ARGV[3] = transition key TTL in seconds
	// ARGV[4] = result key TTL in seconds
	// ARGV[5] = expected execution ID, or "" to write unconditionally
	startTransitionScript = redis.NewScript(`
		if ARGV[5] ~= '' then
			local current = redis.call('GET', KEYS[1])
			if not current then
				return 0
			end
			local ok, decoded = pcall(cjson.decode, current)
			if not ok or decoded['executionID'] ~= ARGV[5] then
				return 0
			end
		end
		redis.call('SET', KEYS[1], ARGV[1])
		redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
		redis.call('SET', KEYS[3], '', 'EX', ARGV[4])
		return 1
	`)

	// pruneOrphanIndexScript removes a sandbox ID from the team index ONLY if the
	// sandbox key is still absent, atomically. The orphan sweep found the key gone
	// via MGET, but between that read and this call a resume can re-create the key
	// under the same (reused) sandbox ID and re-add it to the index; the EXISTS
	// guard makes sure we never unindex such a live sandbox. The team index is not
	// repaired by the healer, so a wrong SREM here would be permanent.
	// KEYS[1] = sandbox key, KEYS[2] = team index key, ARGV[1] = sandbox ID.
	pruneOrphanIndexScript = redis.NewScript(`
		if redis.call('EXISTS', KEYS[1]) == 0 then
			return redis.call('SREM', KEYS[2], ARGV[1])
		end
		return 0
	`)
)

const (
	scriptAttr         = "script"
	scriptOutcomeAttr  = "outcome"
	scriptOutcomeOK    = "ok"
	scriptOutcomeError = "error"
)

// scriptMetrics records every script run: a count by script and outcome,
// and the round trip by script.
type scriptMetrics struct {
	calls    metric.Int64Counter
	duration metric.Float64Histogram
}

func newScriptMetrics(meter metric.Meter) (scriptMetrics, error) {
	calls, err := telemetry.GetCounter(meter, telemetry.ApiRedisScriptCalls)
	if err != nil {
		return scriptMetrics{}, fmt.Errorf("script calls counter: %w", err)
	}

	duration, err := telemetry.GetFloatHistogram(meter, telemetry.ApiRedisScriptDuration)
	if err != nil {
		return scriptMetrics{}, fmt.Errorf("script duration histogram: %w", err)
	}

	return scriptMetrics{calls: calls, duration: duration}, nil
}

// On the global provider the API installs at startup: the reservation storage runs scripts too and has no meter.
var defaultScriptMetrics = utils.Must(newScriptMetrics(otel.Meter(meterScope)))

// RunScript runs script on client as Script.Run does and records the run
// under name. It returns the command unchanged, so callers read it as they
// would Script.Run's.
func RunScript(ctx context.Context, client redis.Scripter, name string, script *redis.Script, keys []string, args ...any) *redis.Cmd {
	return defaultScriptMetrics.run(ctx, client, name, script, keys, args...)
}

func (m scriptMetrics) run(ctx context.Context, client redis.Scripter, name string, script *redis.Script, keys []string, args ...any) *redis.Cmd {
	start := time.Now()
	cmd := script.Run(ctx, client, keys, args...)
	elapsed := time.Since(start)

	// A nil reply is the script's result (remove_sandbox on a missing key), not a failed run.
	outcome := scriptOutcomeOK
	if err := cmd.Err(); err != nil && !errors.Is(err, redis.Nil) {
		outcome = scriptOutcomeError
	}

	m.calls.Add(ctx, 1, metric.WithAttributes(
		attribute.String(scriptAttr, name),
		attribute.String(scriptOutcomeAttr, outcome),
	))
	// Float: a script round trip is usually under a millisecond.
	m.duration.Record(ctx, float64(elapsed)/float64(time.Millisecond), metric.WithAttributes(
		attribute.String(scriptAttr, name),
	))

	return cmd
}
