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

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// isHeldLua defines is_held, which reports whether a decoded record is held
// (see Sandbox.Held): a hold whose deadline Redis's clock has reached is
// released. The scripts that write or remove a record without the record's
// lock, or whose write the lock alone does not cover, refuse a held one.
const isHeldLua = `
	local function is_held(record)
		if type(record) ~= 'table' or type(record['lifecycleHold']) ~= 'string' or record['lifecycleHold'] == '' then
			return false
		end
		local until_ms = record['lifecycleHoldUntil']
		if type(until_ms) ~= 'number' then
			return false
		end
		local now = redis.call('TIME')
		return until_ms > tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
	end
`

// Lua scripts for atomic operations.
// These scripts ensure true atomicity in Redis cluster mode
var (
	// A write past an expired lock can replace the record; compare it atomically before restoring it.
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

	// A write-form flip must preserve duplicate creates, gone executions and
	// held records with the same semantics as the destination write.
	clearOtherFormScript = redis.NewScript(isHeldLua + `
		local raw = redis.call('GET', KEYS[1])
		local ok, stored = pcall(cjson.decode, raw or '')
		if ARGV[2] == '0' then
			if ok and stored['executionID'] == ARGV[1] then return 0 end
			if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then return 2 end
		end
		if ok and is_held(stored) then return 3 end
		redis.call('DEL', KEYS[1])
		return 1
	`)

	// addSandboxScript atomically stores a record and, when KEYS[3] is given,
	// adds it to the team index; the sandbox form passes no index key, its
	// entry lives in another slot. Unless ARGV[4] is '1', the write is refused
	// (nothing written) when it is a late duplicate (see Add): 0 when its
	// execution is already stored, 2 when its execution is in the gone set
	// (KEYS[2], in the record's slot), which holds the executions removed or
	// replaced under this sandbox ID for ARGV[5] seconds. A write that
	// replaces another execution's record puts that execution in the gone set.
	// KEYS[1] = sandbox key, KEYS[2] = gone set, KEYS[3] = team index key, optional
	// ARGV[1] = serialized sandbox data, ARGV[2] = sandbox ID,
	// ARGV[3] = execution ID, ARGV[4] = '1' to replace a stored record of the
	// same execution, ARGV[5] = gone set TTL in seconds
	// Replies: 1 written, 0 already stored for this execution, 2 refused as
	// gone, 3 refused as held.
	addSandboxScript = redis.NewScript(isHeldLua + `
		local current = redis.call('GET', KEYS[1])
		local stored = nil
		local held = false
		if current then
			local ok, decoded = pcall(cjson.decode, current)
			if ok then
				stored = decoded['executionID']
				held = is_held(decoded)
			end
		end
		if ARGV[4] ~= '1' then
			if stored and stored == ARGV[3] then
				return 0
			end
			if redis.call('SISMEMBER', KEYS[2], ARGV[3]) == 1 then
				return 2
			end
		end
		if held then
			return 3
		end
		if stored and stored ~= ARGV[3] then
			redis.call('SADD', KEYS[2], stored)
			redis.call('EXPIRE', KEYS[2], ARGV[5])
		end
		redis.call('SET', KEYS[1], ARGV[1])
		if KEYS[3] then
			redis.call('SADD', KEYS[3], ARGV[2])
		end
		return 1
	`)

	// removeSandboxScript atomically removes a record and, when KEYS[4] is
	// given, its team index entry, and returns the removed JSON so the caller
	// can scope the expiration-index cleanup to the execution it removed. The
	// sandbox form passes no index key: its entry lives in another slot. With
	// ARGV[2] set, a record of another execution is returned and left alone,
	// index entry included: a script re-executed after a lost reply, or
	// delayed past the lock, cannot remove a successor. An absent record is
	// nil and writes nothing; its index entry may be a successor's in the
	// other form. The removed execution goes into the gone set (KEYS[2], in
	// the record's slot), so a late duplicate of its create cannot bring it
	// back. With ARGV[5] set, the record goes only while KEYS[3] still holds
	// that lock value: a removal delayed past its lock cannot remove what a
	// later holder wrote. Add's cleanup, which holds no lock, passes none and
	// relies on the execution guard. A held record is refused. With ARGV[4] set, only a record
	// under that live hold is removed: a holder whose hold lapsed finds the
	// record another operation's, or nobody's, and removes nothing.
	// KEYS[1] = sandbox key, KEYS[2] = gone set, both in the record's slot
	// KEYS[3] = the record's lock key, in the record's slot
	// KEYS[4] = team index key, optional
	// ARGV[1] = sandbox ID
	// ARGV[2] = expected execution ID, or "" to remove any execution
	// ARGV[3] = gone set TTL in seconds
	// ARGV[4] = the hold the record must be under, or "" for none
	// ARGV[5] = lock value (token and metadata), or "" for a caller without one
	// Replies, read by parseRemoveReply: nil, {0, JSON} refused, {1, JSON}
	// removed, {2, JSON} the lock no longer holds the caller's value.
	removeSandboxScript = redis.NewScript(isHeldLua + `
		local data = redis.call('GET', KEYS[1])
		if not data then
			return nil
		end
		if ARGV[5] ~= '' and redis.call('GET', KEYS[3]) ~= ARGV[5] then
			return {2, data}
		end
		local ok, decoded = pcall(cjson.decode, data)
		if ARGV[2] ~= '' then
			if not ok or decoded['executionID'] ~= ARGV[2] then
				return {0, data}
			end
		end
		local held = ok and is_held(decoded)
		if ARGV[4] ~= '' then
			if not held or decoded['lifecycleHold'] ~= ARGV[4] then
				return {0, data}
			end
		elseif held then
			return {0, data}
		end
		redis.call('DEL', KEYS[1])
		if KEYS[4] then
			redis.call('SREM', KEYS[4], ARGV[1])
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
	// ExpectExecutionID, and it has to live in here rather than in Go: once the
	// lock expires under a stalled removal, a resume can install a new
	// incarnation between a Go-side comparison and this write, and the SET below
	// would then overwrite the new live record with the stale one. Returns 0
	// without writing anything if the record is gone or is a different execution.
	// Returns 2 without writing if the record is held, and 3 if the record's
	// lock no longer holds the caller's value: a holder may have moved the
	// record since the caller read it.
	// KEYS[1] = sandbox key
	// KEYS[2] = transition key
	// KEYS[3] = transition result key
	// KEYS[4] = the record's lock key, in the record's slot
	// ARGV[1] = new sandbox JSON data
	// ARGV[2] = transition ID (UUID)
	// ARGV[3] = transition key TTL in seconds
	// ARGV[4] = result key TTL in seconds
	// ARGV[5] = expected execution ID, or "" to write unconditionally
	// ARGV[6] = lock value (token and metadata)
	startTransitionScript = redis.NewScript(isHeldLua + `
		if redis.call('GET', KEYS[4]) ~= ARGV[6] then
			return 3
		end
		local current = redis.call('GET', KEYS[1])
		local ok, decoded = false, nil
		if current then
			ok, decoded = pcall(cjson.decode, current)
		end
		if ok and is_held(decoded) then
			return 2
		end
		if ARGV[5] ~= '' then
			if not current then
				return 0
			end
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
// already gone, sandboxtypes.ErrLockLost when the caller's lock no longer
// held the record, else whether it was removed and the JSON the script
// found, the removed record's or the one left in place.
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
		switch {
		case flagOK && dataOK && flag == 2:
			return false, data, false, sandboxtypes.ErrLockLost
		case flagOK && dataOK:
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

// Error kinds, the errors counter's kind attribute: keys of one command in
// several slots; a MOVED or ASK redirect the client's retries did not
// resolve; a slot in migration; anything else, a lost connection included.
const (
	errorKindAttr           = "kind"
	errorKindCrossSlot      = "crossslot"
	errorKindMovedExhausted = "moved_exhausted"
	errorKindTryAgain       = "tryagain"
	errorKindOther          = "other"
)

// On the global provider like defaultScriptMetrics, so scripts and the record readers count into one instrument.
var defaultErrors = utils.Must(telemetry.GetCounter(otel.Meter(meterScope), telemetry.ApiRedisStorageErrors))

// redisErrorKind returns the kind a failed command is counted under, and
// false for a nil reply or a cancelled context, which are not failures.
func redisErrorKind(err error) (string, bool) {
	switch {
	case err == nil, errors.Is(err, redis.Nil), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "", false
	case redis.HasErrorPrefix(err, "CROSSSLOT"):
		return errorKindCrossSlot, true
	case redis.HasErrorPrefix(err, "MOVED"), redis.HasErrorPrefix(err, "ASK"):
		return errorKindMovedExhausted, true
	case redis.HasErrorPrefix(err, "TRYAGAIN"):
		return errorKindTryAgain, true
	default:
		return errorKindOther, true
	}
}

// recordRedisError counts a failed command by the error it returned.
func recordRedisError(ctx context.Context, err error) {
	kind, ok := redisErrorKind(err)
	if !ok {
		return
	}

	defaultErrors.Add(ctx, 1, metric.WithAttributes(attribute.String(errorKindAttr, kind)))
}

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
		recordRedisError(ctx, err)
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
