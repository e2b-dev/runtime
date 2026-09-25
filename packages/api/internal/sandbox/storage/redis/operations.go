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

// Add stores a sandbox in Redis atomically with its team index entry.
func (s *Storage) Add(ctx context.Context, sbx sandboxtypes.Sandbox) error {
	data, err := json.Marshal(sbx)
	if err != nil {
		return fmt.Errorf("failed to marshal sandbox: %w", err)
	}

	key := getSandboxKey(sbx.TeamID.String(), sbx.SandboxID)
	teamKey := GetSandboxStorageTeamIndexKey(sbx.TeamID.String())

	// Add to the index before adding to the cache, so there's no possibility of leaking
	// Index by EndTime so ExpiredItems can use ZRANGEBYSCORE instead of scanning all sandboxes.
	// The member is scoped to this execution: concurrent removals of older
	// executions of the same sandbox ID can never unindex this one.
	if err := s.redisClient.ZAdd(ctx, globalExpirationSet, redis.Z{
		Score:  float64(sbx.EndTime.UnixMilli()),
		Member: sandboxExpirationMember(sbx),
	}).Err(); err != nil {
		return fmt.Errorf("failed to add sandbox to global expiration index: %w", err)
	}

	// Execute Lua script for atomic SET + SADD
	eidKey := getSandboxExecutionIDKey(sbx.TeamID.String(), sbx.SandboxID)
	err = addSandboxScript.Run(ctx, s.redisClient, []string{key, teamKey, eidKey}, data, sbx.SandboxID, sbx.ExecutionID).Err()
	if err != nil {
		return fmt.Errorf("failed to store sandbox in Redis: %w", err)
	}

	// We can't set the globalTeamsSet in Lua script as they can be in different shards
	if err := s.redisClient.ZAdd(ctx, globalTeamsSet, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: sbx.TeamID.String(),
	}).Err(); err != nil {
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
	// actually removed.
	eidKey := getSandboxExecutionIDKey(teamID.String(), sandboxID)
	raw, err := removeSandboxScript.Run(ctx, s.redisClient, []string{key, teamKey, eidKey}, sandboxID).Text()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("failed to remove sandbox from Redis: %w", err)
	}

	// Clean up from the global expiration index.
	// Do it after the removal to prevent leaking expired sandboxes.
	// Drop only the member of the execution we deleted. A concurrent lockless
	// Add for a newer execution wrote a different member, so it can never be
	// unindexed here. If the key was already gone, any leftover execution
	// member is swept by ExpiredItems once its score passes.
	if raw != "" {
		var sbx sandboxtypes.Sandbox
		if unmarshalErr := json.Unmarshal([]byte(raw), &sbx); unmarshalErr == nil && sbx.ExecutionID != "" {
			member := expirationMember(teamID.String(), sandboxID, sbx.ExecutionID)
			if err := s.redisClient.ZRem(ctx, globalExpirationSet, member).Err(); err != nil {
				logger.L().Warn(ctx, "Failed to remove sandbox from global expiration index", zap.Error(err), logger.WithSandboxID(sandboxID))
			}
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

	// Execute transaction
	err = s.redisClient.Set(ctx, key, newData, redis.KeepTTL).Err()
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
