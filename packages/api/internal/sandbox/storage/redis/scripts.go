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

	// fencedSetScript writes a record, keeping its TTL, only while the lock
	// key still holds the writer's value. The keys share a slot: the lock key
	// is the record key with a prefix.
	// KEYS[1] = lock key, KEYS[2] = record key
	// ARGV[1] = lock value (token and metadata), ARGV[2] = serialized record
	fencedSetScript = redis.NewScript(`
		if redis.call('GET', KEYS[1]) ~= ARGV[1] then
			return 0
		end
		redis.call('SET', KEYS[2], ARGV[2], 'KEEPTTL')
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
	// Unless ARGV[4] is '1', the write is refused (nothing written) when it is
	// a late duplicate (see Add): 0 when its execution is already stored, 2
	// when its execution is in the gone set, which holds the executions
	// removed or replaced under this sandbox ID for ARGV[5] seconds. A write
	// that replaces another execution's record puts that execution in the
	// gone set.
	// KEYS[1] = sandbox key, KEYS[2] = team index key, KEYS[3] = gone set
	// ARGV[1] = serialized sandbox data, ARGV[2] = sandbox ID,
	// ARGV[3] = execution ID, ARGV[4] = '1' to replace a stored record of the
	// same execution, ARGV[5] = gone set TTL in seconds
	// Replies: 1 written, 0 already stored for this execution, 2 refused as gone.
	addSandboxScript = redis.NewScript(`
		local current = redis.call('GET', KEYS[1])
		local stored = nil
		if current then
			local ok, decoded = pcall(cjson.decode, current)
			if ok then
				stored = decoded['executionID']
			end
		end
		if ARGV[4] ~= '1' then
			if stored and stored == ARGV[3] then
				return 0
			end
			if redis.call('SISMEMBER', KEYS[3], ARGV[3]) == 1 then
				return 2
			end
		end
		if stored and stored ~= ARGV[3] then
			redis.call('SADD', KEYS[3], stored)
			redis.call('EXPIRE', KEYS[3], ARGV[5])
		end
		redis.call('SET', KEYS[1], ARGV[1])
		redis.call('SADD', KEYS[2], ARGV[2])
		return 1
	`)

	// removeSandboxScript atomically removes a record and, when KEYS[3] is
	// given, its team index entry, and returns the removed JSON so the caller
	// can scope the expiration-index cleanup to the execution it removed. The
	// sandbox form passes no index key: its entry lives in another slot. With
	// ARGV[2] set, a record of another execution is returned and left alone,
	// index entry included: a script re-executed after a lost reply, or
	// delayed past the lock, cannot remove a successor. An absent record is
	// nil and writes nothing; its index entry may be a successor's in the
	// other form. The removed execution goes into the gone set (KEYS[2], in
	// the record's slot), so a late duplicate of its create cannot bring it
	// back.
	// KEYS[1] = sandbox key, KEYS[2] = gone set, KEYS[3] = team index key, optional
	// ARGV[1] = sandbox ID
	// ARGV[2] = expected execution ID, or "" to remove any execution
	// ARGV[3] = gone set TTL in seconds
	// Replies, read by parseRemoveReply: nil, {0, JSON} refused, {1, JSON} removed.
	removeSandboxScript = redis.NewScript(`
		local data = redis.call('GET', KEYS[1])
		if not data then
			return nil
		end
		local ok, decoded = pcall(cjson.decode, data)
		if ARGV[2] ~= '' then
			if not ok or decoded['executionID'] ~= ARGV[2] then
				return {0, data}
			end
		end
		redis.call('DEL', KEYS[1])
		if KEYS[3] then
			redis.call('SREM', KEYS[3], ARGV[1])
		end
		if ok and decoded['executionID'] then
			redis.call('SADD', KEYS[2], decoded['executionID'])
			redis.call('EXPIRE', KEYS[2], ARGV[3])
		end
		return {1, data}
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
)

// parseRemoveReply reads a remove script's reply: absent when the record was
// already gone, else whether it was removed and the JSON the script found,
// the removed record's or the other execution's.
func parseRemoveReply(cmd *redis.Cmd) (deleted bool, json string, absent bool, err error) {
	reply, err := cmd.Result()
	if errors.Is(err, redis.Nil) {
		return false, "", true, nil
	}
	if err != nil {
		return false, "", false, err
	}

	if parts, ok := reply.([]any); ok && len(parts) == 2 {
		flag, flagOK := parts[0].(int64)
		data, dataOK := parts[1].(string)
		if flagOK && dataOK {
			return flag == 1, data, false, nil
		}
	}

	return false, "", false, fmt.Errorf("remove script reply %#v: want [deleted, json]", reply)
}

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
// would Script.Run's. In the package's tests it first panics if keys span
// hash slots.
func RunScript(ctx context.Context, client redis.Scripter, name string, script *redis.Script, keys []string, args ...any) *redis.Cmd {
	assertSameSlot(name, keys)

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
