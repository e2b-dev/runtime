package redis

import "github.com/redis/go-redis/v9"

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
		if redis.call('GET', KEYS[1]) == ARGV[1] then
			return redis.call('DEL', KEYS[1])
		end
		return 0
	`)

	// addSandboxScript atomically stores a sandbox and adds it to the team index.
	// KEYS[1] = sandbox key, KEYS[2] = team index key, KEYS[3] = execution ID key
	// ARGV[1] = serialized sandbox data, ARGV[2] = sandbox ID, ARGV[3] = execution ID
	addSandboxScript = redis.NewScript(`
		redis.call('SET', KEYS[1], ARGV[1])
		redis.call('SET', KEYS[3], ARGV[3])
		redis.call('SADD', KEYS[2], ARGV[2])
		return 1
	`)

	// removeSandboxScript atomically removes a sandbox and its team index entry.
	// It returns the stored JSON (or nil if the key was already gone) so the
	// caller knows exactly which execution it removed and can scope the
	// expiration-index cleanup to that execution.
	// KEYS[1] = sandbox key, KEYS[2] = team index key, KEYS[3] = execution ID key
	// ARGV[1] = sandbox ID
	removeSandboxScript = redis.NewScript(`
		local data = redis.call('GET', KEYS[1])
		redis.call('DEL', KEYS[1])
		redis.call('DEL', KEYS[3])
		redis.call('SREM', KEYS[2], ARGV[1])
		return data
	`)

	// startTransitionScript atomically updates sandbox and sets transition key with UUID.
	// This is called AFTER Go code has validated the transition and prepared the new sandbox data.
	//
	// When ARGV[5] is set, the write only happens if the stored record is still
	// that execution. This is the enforcement point for RemoveOpts.ExpectExecutionID,
	// and it has to live in here rather than in Go: Add is lockless, so a resume
	// can install a new incarnation between a Go-side comparison and this write,
	// and the SET below would then overwrite the new live record with the stale one.
	// Returns 0 without writing anything if the record is gone or is a different execution.
	//
	// The check reads KEYS[4] (a dedicated execution-ID key) instead of decoding
	// the full sandbox JSON. This avoids running cjson.decode on the Redis event
	// loop for every concurrent eviction call, which would otherwise serialize
	// O(N-JSON) CPU work on the single-threaded Redis main thread.
	//
	// Migration note: KEYS[4] is absent for sandboxes created before this script
	// was deployed. The fallback branch re-decodes the JSON so that in-flight
	// sandboxes are still handled correctly. Once all pre-deploy sandboxes have
	// expired the fallback is unreachable and can be removed.
	//
	// KEYS[1] = sandbox key
	// KEYS[2] = transition key
	// KEYS[3] = transition result key
	// KEYS[4] = execution ID key
	// ARGV[1] = new sandbox JSON data
	// ARGV[2] = transition ID (UUID)
	// ARGV[3] = transition key TTL in seconds
	// ARGV[4] = result key TTL in seconds
	// ARGV[5] = expected execution ID, or "" to write unconditionally
	// ARGV[6] = new execution ID to persist in KEYS[4]
	startTransitionScript = redis.NewScript(`
		if ARGV[5] ~= '' then
			local eid = redis.call('GET', KEYS[4])
			if eid then
				-- Fast path: dedicated eid key present; O(1) string compare.
				if eid ~= ARGV[5] then
					return 0
				end
			else
				-- Migration fallback: eid key absent for pre-deploy sandboxes.
				-- Fall back to JSON decode so existing sandboxes are still handled.
				local current = redis.call('GET', KEYS[1])
				if not current then
					return 0
				end
				local ok, decoded = pcall(cjson.decode, current)
				if not ok or decoded['executionID'] ~= ARGV[5] then
					return 0
				end
			end
		end
		redis.call('SET', KEYS[1], ARGV[1])
		redis.call('SET', KEYS[4], ARGV[6])
		redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
		redis.call('SET', KEYS[3], '', 'EX', ARGV[4])
		return 1
	`)
)
