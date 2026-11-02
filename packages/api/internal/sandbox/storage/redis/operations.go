package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// goneExecutionsTTL is how long an execution removed or replaced under a
// sandbox ID stays refused: longer than any retry or delayed segment of its
// create's write can take to land.
const goneExecutionsTTL = 10 * time.Minute

// Add stores a sandbox for its execution. The create's store write is
// lockless and can land twice (a retried command, a segment delayed past
// later writes), so it is idempotent per execution: a record of the same
// execution already stored is kept as it is, and the late attempt cannot
// take the record back to its create-time copy. An execution removed or
// replaced under the ID is refused for goneExecutionsTTL.
func (s *Storage) Add(ctx context.Context, sbx sandboxtypes.Sandbox) error {
	return s.add(ctx, sbx, false)
}

// Replace stores sbx over whatever is stored under its ID, this execution's
// record included.
func (s *Storage) Replace(ctx context.Context, sbx sandboxtypes.Sandbox) error {
	return s.add(ctx, sbx, true)
}

func (s *Storage) add(ctx context.Context, sbx sandboxtypes.Sandbox, replace bool) error {
	data, err := json.Marshal(sbx)
	if err != nil {
		return fmt.Errorf("failed to marshal sandbox: %w", err)
	}

	key := getSandboxKey(sbx.TeamID.String(), sbx.SandboxID)
	teamKey := GetSandboxStorageTeamIndexKey(sbx.TeamID.String())
	goneKey := getSandboxGoneKey(sbx.TeamID.String(), sbx.SandboxID)

	// Add to the index before adding to the cache, so there's no possibility of leaking
	// Index by EndTime so ExpiredItems can use ZRANGEBYSCORE instead of scanning all sandboxes.
	// The member is scoped to this execution: concurrent removals of older
	// executions of the same sandbox ID can never unindex this one. A create's
	// write never lowers the score a stored record of its execution has reached.
	member := redis.Z{Score: float64(sbx.EndTime.UnixMilli()), Member: sandboxExpirationMember(sbx)}
	if replace {
		err = s.redisClient.ZAdd(ctx, globalExpirationSet, member).Err()
	} else {
		err = s.redisClient.ZAddNX(ctx, globalExpirationSet, member).Err()
	}
	if err != nil {
		return fmt.Errorf("failed to add sandbox to global expiration index: %w", err)
	}

	replaceFlag := "0"
	if replace {
		replaceFlag = "1"
	}
	// Execute Lua script for atomic SET + SADD
	written, err := RunScript(ctx, s.redisClient, "add_sandbox", addSandboxScript, []string{key, teamKey, goneKey}, data, sbx.SandboxID, sbx.ExecutionID, replaceFlag, int(goneExecutionsTTL.Seconds())).Int64()
	if err != nil {
		return fmt.Errorf("failed to store sandbox in Redis: %w", err)
	}
	switch written {
	case 0:
		logger.L().Warn(ctx, "sandbox already stored for this execution, duplicate write kept out",
			logger.WithSandboxID(sbx.SandboxID), zap.String("execution_id", sbx.ExecutionID))
	case 2:
		// The member written above outlives the refused write, as a member of
		// any execution without a record does: ExpiredItems sweeps it once its
		// score passes, finding no record of its execution. Removing it here
		// would race a Replace of the same execution, which writes the same
		// member and relies on it.
		logger.L().Warn(ctx, "sandbox execution removed or replaced meanwhile, late write kept out",
			logger.WithSandboxID(sbx.SandboxID), zap.String("execution_id", sbx.ExecutionID))
	}

	// Assert the member again after the record's write, in one round trip with
	// the team's entry in the global teams index (neither key is in the
	// script's slot). ExpiredItems and Remove remove a member after reading
	// its record absent or another execution's; a removal that read the
	// record before the write above can land after the member's write and
	// take it, and this assertion puts it back, due now: the next sweep
	// reads the record and scores the member by it. The other order, this
	// write landing after the removal, is covered by their read of the
	// record again after the removal (see restoreReinstalled). NX: a score a
	// concurrent writer set stands. A refused write has no record to index.
	// A failure here leaves the record to the healer, which re-indexes a
	// stored record without a member.
	pipe := s.redisClient.Pipeline()
	var asserted *redis.IntCmd
	if written != 2 {
		asserted = pipe.ZAddNX(ctx, globalExpirationSet, redis.Z{Score: float64(time.Now().UnixMilli()), Member: member.Member})
	}
	teamIndexed := pipe.ZAdd(ctx, globalTeamsSet, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: sbx.TeamID.String(),
	})
	_, execErr := pipe.Exec(ctx)
	// A pipeline that failed as a whole answered no command: its error is
	// every command's. One that was answered carries each failure on its command.
	if execErr != nil && teamIndexed.Err() == nil && (asserted == nil || asserted.Err() == nil) {
		teamIndexed.SetErr(execErr)
		if asserted != nil {
			asserted.SetErr(execErr)
		}
	}
	if asserted != nil && asserted.Err() != nil {
		logger.L().Warn(ctx, "failed to assert the sandbox's expiration index member", zap.Error(asserted.Err()), logger.WithSandboxID(sbx.SandboxID))
	}
	if err := teamIndexed.Err(); err != nil {
		logger.L().Warn(ctx, "failed to add team to global teams index", zap.Error(err), logger.WithSandboxID(sbx.SandboxID))
	}

	return nil
}

// Get retrieves a sandbox from Redis
func (s *Storage) Get(ctx context.Context, teamID uuid.UUID, sandboxID string) (sandboxtypes.Sandbox, error) {
	key := getSandboxKey(teamID.String(), sandboxID)
	data, err := s.redisClient.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return sandboxtypes.Sandbox{}, fmt.Errorf("sandbox %q: %w", sandboxID, sandboxtypes.ErrNotFound)
	}
	if err != nil {
		return sandboxtypes.Sandbox{}, fmt.Errorf("failed to get sandbox from Redis: %w", err)
	}

	var sbx sandboxtypes.Sandbox
	err = json.Unmarshal(data, &sbx)
	if err != nil {
		return sandboxtypes.Sandbox{}, fmt.Errorf("failed to unmarshal sandbox: %w", err)
	}

	return sbx, nil
}

// Remove deletes a sandbox from Redis atomically with its team index entry.
func (s *Storage) Remove(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	key := getSandboxKey(teamID.String(), sandboxID)
	teamKey := GetSandboxStorageTeamIndexKey(teamID.String())

	lockKey := redis_utils.GetLockKey(key)
	lock, err := s.locker.Obtain(ctx, lockKey, lockTimeout)
	if err != nil {
		return fmt.Errorf("failed to obtain lock: %w", err)
	}

	defer func() {
		err := lock.Release(context.WithoutCancel(ctx))
		if err != nil {
			logger.L().Error(ctx, "Failed to release lock", zap.Error(err))
		}
	}()

	// Execute Lua script for atomic DEL + SREM; it returns the deleted JSON
	// so the expiration-index cleanup below is scoped to the execution we
	// actually removed. No expected execution yet: any execution is removed.
	// The removed execution goes into the gone set, so a late duplicate of
	// its create cannot bring it back.
	goneKey := getSandboxGoneKey(teamID.String(), sandboxID)
	_, raw, _, err := parseRemoveReply(RunScript(ctx, s.redisClient, "remove_sandbox", removeSandboxScript, []string{key, goneKey, teamKey}, sandboxID, "", int(goneExecutionsTTL.Seconds())))
	if err != nil {
		return fmt.Errorf("failed to remove sandbox from Redis: %w", err)
	}

	// Clean up from the global expiration index.
	// Do it after the removal to prevent leaking expired sandboxes.
	// Drop only the member of the execution we deleted. A concurrent lockless
	// Add for a newer execution wrote a different member, so it can never be
	// unindexed here. If the key was already gone, any leftover execution
	// member is swept by ExpiredItems once its score passes. A lockless
	// Replace of the deleted execution can land between the delete and this
	// removal and lose its member to it: the record is read again after.
	if raw != "" {
		var sbx sandboxtypes.Sandbox
		if unmarshalErr := json.Unmarshal([]byte(raw), &sbx); unmarshalErr == nil && sbx.ExecutionID != "" {
			ref := memberRef{member: expirationMember(teamID.String(), sandboxID, sbx.ExecutionID), teamID: teamID.String(), sandboxID: sandboxID, executionID: sbx.ExecutionID}
			if err := s.redisClient.ZRem(ctx, globalExpirationSet, ref.member).Err(); err != nil {
				// The removal may have applied without its reply.
				logger.L().Warn(ctx, "Failed to remove sandbox from global expiration index", zap.Error(err), logger.WithSandboxID(sandboxID))
			}
			s.restoreReinstalled(ctx, []memberRef{ref})
		}
	}

	return nil
}

// TeamItems retrieves sandboxes for a specific team, filtered by states (all
// states when empty).
//
// The team index is read with SSCAN and the records fetched in MGET batches of
// sandboxScanBatchSize, which caps each command's work and reply size
// regardless of how many sandboxes the team has.
//
// This is not a consistent snapshot, and neither was reading the whole index
// at once: records removed between the index read and the record fetch are
// skipped, and SSCAN may emit a member twice, which scannedItems drops.
// Callers already sort the result and treat it as best effort.
func (s *Storage) TeamItems(ctx context.Context, teamID uuid.UUID, states []sandboxtypes.State) ([]sandboxtypes.Sandbox, error) {
	items := newScannedItems(states)

	err := s.forEachTeamSandboxBatch(ctx, teamID.String(), func(_ string, batch []sandboxtypes.Sandbox) error {
		items.add(batch)

		return nil
	})
	if err != nil {
		return nil, err
	}

	if items.out == nil {
		return []sandboxtypes.Sandbox{}, nil
	}

	return items.out, nil
}

// Update modifies a sandbox atomically
func (s *Storage) Update(ctx context.Context, teamID uuid.UUID, sandboxID string, updateFunc func(sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error)) (sandboxtypes.Sandbox, error) {
	key := getSandboxKey(teamID.String(), sandboxID)

	lockKey := redis_utils.GetLockKey(key)
	lock, err := s.locker.Obtain(ctx, lockKey, lockTimeout)
	if err != nil {
		return sandboxtypes.Sandbox{}, fmt.Errorf("failed to obtain lock: %w", err)
	}

	defer func() {
		err := lock.Release(context.WithoutCancel(ctx))
		if err != nil {
			logger.L().Error(ctx, "Failed to release lock", zap.Error(err))
		}
	}()

	// Get current value
	data, err := s.redisClient.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return sandboxtypes.Sandbox{}, fmt.Errorf("sandbox %q: %w", sandboxID, sandboxtypes.ErrNotFound)
	}
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}

	var sbx sandboxtypes.Sandbox
	err = json.Unmarshal(data, &sbx)
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}

	// Apply update
	updatedSbx, err := updateFunc(sbx)
	if err != nil {
		return sandboxtypes.Sandbox{}, fmt.Errorf("failed to update sandbox: %w", err)
	}

	// Serialize updated sandbox
	newData, err := json.Marshal(updatedSbx)
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}

	// The update may have acted outside the store already (a network update
	// applies on the node first), so the write is retried, and fenced by the lock.
	err = s.setUnderLock(ctx, lock, key, newData)
	if err != nil {
		return sandboxtypes.Sandbox{}, fmt.Errorf("failed to store sandbox in Redis: %w", err)
	}

	// Re-score the expiration index if EndTime changed.
	if !updatedSbx.EndTime.Equal(sbx.EndTime) {
		if err := s.redisClient.ZAdd(ctx, globalExpirationSet, redis.Z{
			Score:  float64(updatedSbx.EndTime.UnixMilli()),
			Member: sandboxExpirationMember(updatedSbx),
		}).Err(); err != nil {
			return sandboxtypes.Sandbox{}, fmt.Errorf("failed to update sandbox in global expiration index: %w", err)
		}
	}

	return updatedSbx, nil
}

func (s *Storage) TeamsWithSandboxCount(ctx context.Context) (map[uuid.UUID]int64, error) {
	members, err := s.redisClient.ZRangeWithScores(ctx, globalTeamsSet, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get teams from global index: %w", err)
	}

	// Pipeline SCARD per team index key to get counts and filter stale entries
	type teamEntry struct {
		id    uuid.UUID
		score float64
		cmd   *redis.IntCmd
	}

	pipe := s.redisClient.Pipeline()
	entries := make([]teamEntry, 0, len(members))
	for _, m := range members {
		raw, ok := m.Member.(string)
		if !ok {
			continue
		}
		id, parseErr := uuid.Parse(raw)
		if parseErr != nil {
			logger.L().Warn(ctx, "Failed to parse team ID from global teams index", zap.Error(parseErr), zap.String("raw", raw))

			continue
		}
		cmd := pipe.SCard(ctx, GetSandboxStorageTeamIndexKey(raw))
		entries = append(entries, teamEntry{id: id, score: m.Score, cmd: cmd})
	}

	if len(entries) == 0 {
		return map[uuid.UUID]int64{}, nil
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("SCARD pipeline failed: %w", err)
	}

	nowSec := time.Now().Unix()
	cutoff := nowSec - int64(sandboxtypes.StaleCutoff.Seconds())

	teams := make(map[uuid.UUID]int64, len(entries))
	var stale []any
	for _, e := range entries {
		if count := e.cmd.Val(); count > 0 {
			teams[e.id] = count
		} else if int64(e.score) < cutoff {
			// Only prune if the entry is old enough — a fresh score means
			// an Add happened recently and SCARD==0 may be a transient race.
			stale = append(stale, e.id.String())
		}
	}

	// Prune stale entries from the global teams index
	if len(stale) > 0 {
		if err := s.redisClient.ZRem(ctx, globalTeamsSet, stale...).Err(); err != nil {
			logger.L().Warn(ctx, "Failed to prune stale teams from global index", zap.Error(err), zap.Int("count", len(stale)))
		}
	}

	return teams, nil
}

// updateWriteAttempts bounds the write of an update.
const updateWriteAttempts = 3

const updateWriteBackoff = 50 * time.Millisecond

// ErrLockLost refuses an update's write because the record's lock no longer
// holds this caller's token: it expired, and another caller may have written.
var ErrLockLost = errors.New("the record's lock was lost before the update was written")

// setUnderLock writes an updated record only while lock still holds this
// caller's token, checked and written in one script, so a write that lands
// late cannot overwrite what a later lock holder stored. A failed write is
// retried a bounded number of times; a lost lock is not.
func (s *Storage) setUnderLock(ctx context.Context, lock *storageLock, key string, data []byte) error {
	var err error
	for attempt := range updateWriteAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return errors.Join(err, ctx.Err())
			case <-time.After(time.Duration(attempt) * updateWriteBackoff):
			}
		}
		var written int64
		written, err = RunScript(ctx, s.redisClient, "fenced_set", fencedSetScript, []string{lock.Key(), key}, lock.Token()+lock.Metadata(), data).Int64()
		if err == nil {
			if written == 1 {
				return nil
			}

			return ErrLockLost
		}
	}

	return err
}
