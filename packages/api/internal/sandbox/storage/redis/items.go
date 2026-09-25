package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const expiredItemsBatchSize = 256

// ExpiredItems returns running sandboxes whose EndTime has passed.
// It bounds per-cycle work via LIMIT and cleans up orphaned ZSET entries.
func (s *Storage) ExpiredItems(ctx context.Context) ([]sandboxtypes.Sandbox, error) {
	sweepStart := time.Now()
	defer func() {
		s.metrics.sweepDuration.Record(ctx, time.Since(sweepStart).Milliseconds())
	}()

	now := sweepStart
	nowMs := float64(now.UnixMilli())

	// Fetch members whose score (EndTime in ms) is <= now, bounded to 256 per cycle.
	expiredMembers, err := s.redisClient.ZRangeByScore(ctx, globalExpirationSet, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%v", nowMs),
		Count: expiredItemsBatchSize,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to query global expiration index: %w", err)
	}

	if len(expiredMembers) == 0 {
		return nil, nil
	}

	// Group by team for per-team MGET (Redis Cluster slot compatibility).
	type memberRef struct {
		member      string
		sandboxID   string
		executionID string
	}
	type teamEntry struct {
		teamID string
		refs   []memberRef
	}
	// staleMembers collects ZSET entries to remove: unparseable members, orphans, and dead executions
	var staleMembers []any
	var invalidCount int64

	teamSandboxes := make(map[string]*teamEntry)
	for _, member := range expiredMembers {
		teamID, sandboxID, executionID, ok := parseExpirationMember(member)
		if !ok {
			// Unparseable: sweep it so it stops consuming the scan window on every tick
			logger.L().Warn(ctx, "Removing invalid expiration index member", zap.String("member", member))
			staleMembers = append(staleMembers, member)
			invalidCount++

			continue
		}

		entry, ok := teamSandboxes[teamID]
		if !ok {
			entry = &teamEntry{teamID: teamID}
			teamSandboxes[teamID] = entry
		}

		entry.refs = append(entry.refs, memberRef{member: member, sandboxID: sandboxID, executionID: executionID})
	}

	pipe := s.redisClient.Pipeline()
	type batchInfo struct {
		cmd    *redis.SliceCmd
		teamID string
		refs   []memberRef // aligned 1:1 with MGET keys
	}
	var batches []batchInfo
	for teamID, entry := range teamSandboxes {
		keys := make([]string, len(entry.refs))
		for i, ref := range entry.refs {
			keys[i] = getSandboxKey(teamID, ref.sandboxID)
		}

		cmd := pipe.MGet(ctx, keys...)
		batches = append(batches, batchInfo{cmd: cmd, teamID: teamID, refs: entry.refs})
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("MGET pipeline failed: %w", err)
	}

	// Deserialize and filter.
	var result []sandboxtypes.Sandbox
	var rescores []redis.Z // live members whose score drifted from EndTime
	var orphanCount, deadExecutionCount int64

	for _, batch := range batches {
		for i, raw := range batch.cmd.Val() {
			ref := batch.refs[i]

			// Sandbox key gone but ZSET entry remains — orphaned. Members are
			// execution-scoped, so this removal can never unindex a fresh
			// execution concurrently re-added under the same sandbox ID.
			if raw == nil {
				staleMembers = append(staleMembers, ref.member)
				orphanCount++

				continue
			}

			str, ok := raw.(string)
			if !ok {
				continue
			}

			var sbx sandboxtypes.Sandbox
			if err := json.Unmarshal([]byte(str), &sbx); err != nil {
				logger.L().Error(ctx, "Failed to unmarshal sandbox", zap.Error(err))

				continue
			}

			// Member names a dead execution; the live execution has its own member.
			if ref.executionID != sbx.ExecutionID {
				staleMembers = append(staleMembers, ref.member)
				deadExecutionCount++

				continue
			}

			// In case that index have failed to be updated
			if !sbx.IsExpired(now) {
				logger.L().Debug(ctx, "ExpiredItems: Sandbox marked as expried in index, but state say otherwise", logger.WithSandboxID(sbx.SandboxID), logger.Time("end_time", sbx.EndTime))

				// Re-score the drifted member to the stored EndTime so it
				// stops occupying the expired scan window on every tick.
				// XX: never resurrect a member a concurrent Remove deleted.
				rescores = append(rescores, redis.Z{
					Score:  float64(sbx.EndTime.UnixMilli()),
					Member: ref.member,
				})

				continue
			}

			// Only evict running sandboxes
			if sbx.State != sandboxtypes.StateRunning {
				// If the sandbox is in transitioning state for more than stale cutoff, it's likely failed removal. Let it be cleaned up by the regular expiration process.
				if time.Since(sbx.EndTime) <= sandboxtypes.StaleCutoff {
					// Let the current removal finish

					continue
				}

				logger.L().Debug(ctx, "ExpiredItems: Sandbox is in transition state for more than stale cutoff, removing", logger.WithSandboxID(sbx.SandboxID), logger.Time("end_time", sbx.EndTime))
			}

			result = append(result, sbx)
		}
	}

	// Remove orphaned ZSET entries so the set doesn't grow unboundedly.
	if len(staleMembers) > 0 {
		if err := s.redisClient.ZRem(ctx, globalExpirationSet, staleMembers...).Err(); err != nil {
			logger.L().Warn(ctx, "Failed to clean up stale expiration index entries", zap.Error(err), zap.Int("count", len(staleMembers)))
		} else {
			if orphanCount > 0 {
				s.metrics.indexSwept.Add(ctx, orphanCount, s.metrics.sweptOrphan)
			}
			if deadExecutionCount > 0 {
				s.metrics.indexSwept.Add(ctx, deadExecutionCount, s.metrics.sweptDeadExecution)
			}
			if invalidCount > 0 {
				s.metrics.indexSwept.Add(ctx, invalidCount, s.metrics.sweptInvalid)
			}
		}
	}

	if len(rescores) > 0 {
		if err := s.redisClient.ZAddXX(ctx, globalExpirationSet, rescores...).Err(); err != nil {
			logger.L().Warn(ctx, "Failed to re-score drifted expiration index entries", zap.Error(err), zap.Int("count", len(rescores)))
		} else {
			s.metrics.indexRescored.Add(ctx, int64(len(rescores)))
		}
	}

	return result, nil
}
