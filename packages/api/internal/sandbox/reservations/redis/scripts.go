package redis

import (
	"fmt"

	"github.com/redis/go-redis/v9"
)

const (
	// Reserve result codes
	reserveResultReserved         = 0
	reserveResultAlreadyInStorage = 1
	reserveResultAlreadyPending   = 2
	reserveResultLimitExceeded    = 3
	reserveResultKilled           = 4

	// ClaimKill result codes
	claimKillResultClaimed  = 0
	claimKillResultInFlight = 1
)

var (
	// reserveScript atomically checks limits and reserves a sandbox for creation.
	// The pending set is a ZSET where score = Unix timestamp of reservation.
	// Stale entries (older than staleTTL) are cleaned up before counting.
	//
	// KEYS[1] = storage index key (sandbox:storage:{teamID}:index)
	// KEYS[2] = pending zset key (sandbox:storage:{teamID}:reservations:pending)
	// KEYS[3] = result key (sandbox:storage:{teamID}:reservations:sandboxID:result)
	// KEYS[4] = kill-claim key (sandbox:storage:{teamID}:reservations:sandboxID:killed)
	// ARGV[1] = sandboxID
	// ARGV[2] = limit (-1 means no limit)
	// ARGV[3] = current Unix timestamp (seconds, float)
	// ARGV[4] = stale cutoff Unix timestamp (now - staleTTL)
	//
	// Returns:
	//   0 = RESERVED (sandbox added to pending zset)
	//   1 = ALREADY_IN_STORAGE (sandbox exists in storage index)
	//   2 = ALREADY_PENDING (sandbox already in pending zset)
	//   3 = LIMIT_EXCEEDED (total count >= limit)
	//   4 = KILLED (a DELETE claimed this sandbox ID for removal; see claimKillScript)
	reserveScript = redis.NewScript(fmt.Sprintf(`
		-- Clean up stale pending entries (score < cutoff)
		redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[4])

		-- Refuse if a concurrent DELETE has claimed this sandbox ID for removal.
		-- The claim is written atomically with the paused-delete snapshot removal
		-- (claimKillScript); refusing here is what makes an accepted kill
		-- irreversible against an in-flight or just-starting resume.
		if redis.call('EXISTS', KEYS[4]) == 1 then
			return %d
		end

		-- Check if sandbox already exists in storage index
		if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then
			return %d
		end

		-- Check if sandbox is already pending (has a score in the zset)
		if redis.call('ZSCORE', KEYS[2], ARGV[1]) then
			return %d
		end

		-- Check limit (ARGV[2] < 0 means no limit)
		local limit = tonumber(ARGV[2])
		if limit >= 0 then
			local storageCount = redis.call('SCARD', KEYS[1])
			local pendingCount = redis.call('ZCARD', KEYS[2])
			if storageCount + pendingCount >= limit then
				return %d
			end
		end

		-- Delete stale result key from a previous failed attempt
		redis.call('DEL', KEYS[3])
		-- Reserve: add to pending zset with current timestamp as score
		redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
		return %d
	`, reserveResultKilled, reserveResultAlreadyInStorage, reserveResultAlreadyPending, reserveResultLimitExceeded, reserveResultReserved))

	// claimKillScript is the rendezvous point that lets a DELETE of a paused
	// sandbox (one with no running-store record, only a snapshot) fence off any
	// concurrent resume before it soft-deletes the snapshot.
	//
	// A paused sandbox has no running record, so StartRemoving finds nothing to
	// lock or pin and the kill handler falls straight through to deleting the
	// snapshot. Meanwhile a resume's publication (storage.Add) is a lockless
	// SET+SADD with no delete-intent check. The two operations share no lock,
	// so a DELETE could soft-delete the snapshot and return 204 while an
	// in-flight resume republishes the sandbox as running.
	//
	// This script closes that gap using the reservation the resume already
	// holds for its whole lifecycle (Reserve..finishStart):
	//   - If the sandbox is pending (a resume is mid-flight) or already back in
	//     the storage index (a resume just finished), it refuses: the caller
	//     returns 409 and the client retries the kill against the running
	//     sandbox through the normal locked path.
	//   - Otherwise it writes a short-lived kill-claim that reserveScript
	//     rejects, so a resume that only starts after this point loses too. The
	//     claim just has to outlive the snapshot soft-delete becoming durable;
	//     once the snapshot is gone, any later resume fails when it fetches it.
	//
	// KEYS[1] = storage index key
	// KEYS[2] = pending zset key
	// KEYS[3] = kill-claim key
	// ARGV[1] = sandboxID
	// ARGV[2] = claim TTL in seconds
	//
	// Returns:
	//   0 = CLAIMED (safe to delete the snapshot)
	//   1 = IN_FLIGHT (a resume is pending or already running; refuse the kill)
	claimKillScript = redis.NewScript(fmt.Sprintf(`
		if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then
			return %d
		end
		if redis.call('ZSCORE', KEYS[2], ARGV[1]) then
			return %d
		end
		redis.call('SET', KEYS[3], '1', 'EX', tonumber(ARGV[2]))
		return %d
	`, claimKillResultInFlight, claimKillResultInFlight, claimKillResultClaimed))

	// releaseKillClaimScript drops a kill-claim written by claimKillScript. It
	// is best-effort cleanup for when the snapshot delete fails after the claim
	// was taken; the claim's TTL is the backstop if this never runs.
	// KEYS[1] = kill-claim key
	releaseKillClaimScript = redis.NewScript(`
		redis.call('DEL', KEYS[1])
		return 1
	`)

	// finishStartScript removes a sandbox from the pending zset and sets the result key.
	// KEYS[1] = pending zset key
	// KEYS[2] = result key
	// ARGV[1] = sandboxID
	// ARGV[2] = result JSON
	// ARGV[3] = TTL in seconds
	finishStartScript = redis.NewScript(`
		redis.call('ZREM', KEYS[1], ARGV[1])
		redis.call('SET', KEYS[2], ARGV[2], 'EX', tonumber(ARGV[3]))
		return 1
	`)

	// releaseScript removes a sandbox from the pending zset and deletes the result key.
	// KEYS[1] = pending zset key
	// KEYS[2] = result key
	// ARGV[1] = sandboxID
	releaseScript = redis.NewScript(`
		redis.call('ZREM', KEYS[1], ARGV[1])
		redis.call('DEL', KEYS[2])
		return 1
	`)
)
