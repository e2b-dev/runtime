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
	// KEYS[1] = sandbox key, KEYS[2] = team index key
	// ARGV[1] = serialized sandbox data, ARGV[2] = sandbox ID
	addSandboxScript = redis.NewScript(`
		redis.call('SET', KEYS[1], ARGV[1])
		redis.call('SADD', KEYS[2], ARGV[2])
		return 1
	`)

	// removeSandboxScript atomically compares the stored execution, then removes
	// the sandbox and its team index entry. It returns {outcome, stored JSON},
	// where outcome is 0 for already absent, 1 for deleted, and 2 for an
	// execution mismatch or unreadable record.
	// KEYS[1] = sandbox key, KEYS[2] = team index key
	// ARGV[1] = sandbox ID, ARGV[2] = expected execution ID
	removeSandboxScript = redis.NewScript(`
		local data = redis.call('GET', KEYS[1])
		if not data then
			redis.call('SREM', KEYS[2], ARGV[1])
			return {0, false}
		end
		local ok, decoded = pcall(cjson.decode, data)
		if not ok or type(decoded) ~= 'table' or decoded['executionID'] ~= ARGV[2] then
			return {2, data}
		end
		redis.call('DEL', KEYS[1])
		redis.call('SREM', KEYS[2], ARGV[1])
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
