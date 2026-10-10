package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	healInterval = 5 * time.Minute

	// healGracePeriod skips recently started sandboxes:
	// this prevents the healer from clearing in-flight Add/Remove
	healGracePeriod = time.Minute
)

// startHealer restores "sandbox key exists => expiration index member exists".
// A sandbox missing from the global expiration ZSET is never seen by the evictor
// and would otherwise live forever.
//
// Runs on every API pod with jitter; ZADD NX makes concurrent passes idempotent and harmless.
func (s *Storage) startHealer(ctx context.Context) {
	timer := time.NewTimer(jitterBackoff(healInterval))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			healed, err := s.healExpirationIndex(ctx)
			if err != nil {
				logger.L().Warn(ctx, "Expiration index heal pass failed", zap.Error(err))
			}
			if healed > 0 {
				logger.L().Warn(ctx, "Healed sandboxes missing from expiration index", zap.Int("count", healed))
			}

			timer.Reset(jitterBackoff(healInterval))
		}
	}
}

// healerEnabled reports whether the heal pass should run. A nil feature flag
// client (tests) falls back to the flag's default.
func (s *Storage) healerEnabled(ctx context.Context) bool {
	if s.featureFlags == nil {
		return featureflags.ExpirationIndexHealerFlag.Fallback()
	}

	return s.featureFlags.BoolFlag(ctx, featureflags.ExpirationIndexHealerFlag)
}

// healExpirationIndex scans all stored sandboxes and re-adds expiration index
// members missing for live sandbox keys. Returns the number of healed members.
// Per-team failures are isolated by the shared scanner; other teams still get
// healed.
func (s *Storage) healExpirationIndex(ctx context.Context) (int, error) {
	// Re-evaluated every tick: acts as a kill switch without redeploy.
	if !s.healerEnabled(ctx) {
		return 0, nil
	}

	healed := 0
	err := s.forEachSandboxBatch(ctx, func(_ string, batch []sandboxtypes.Sandbox) error {
		n, err := s.healSandboxBatch(ctx, batch)
		if err != nil {
			return err
		}

		healed += n

		return nil
	})
	if err != nil {
		return healed, err
	}

	s.recordTeamIndexSizes(ctx)

	return healed, nil
}

// recordTeamIndexSizes pipelines SCARD over every team's sandbox index SET and
// records each result as a histogram observation. Called once per heal pass
// (every 5 min) so the cost is negligible. Use the p99/max to detect
// unbounded growth caused by stale entries that Remove() never cleaned up.
func (s *Storage) recordTeamIndexSizes(ctx context.Context) {
	teams, err := s.redisClient.ZRange(ctx, globalTeamsSet, 0, -1).Result()
	if err != nil {
		logger.L().Warn(ctx, "Failed to list teams for index-size metrics", zap.Error(err))
		return
	}
	if len(teams) == 0 {
		return
	}

	pipe := s.redisClient.Pipeline()
	cmds := make([]*redis.IntCmd, len(teams))
	for i, teamID := range teams {
		cmds[i] = pipe.SCard(ctx, GetSandboxStorageTeamIndexKey(teamID))
	}

	if _, err := pipe.Exec(ctx); err != nil {
		logger.L().Warn(ctx, "Failed to pipeline SCARD for index-size metrics", zap.Error(err))
		return
	}

	for _, cmd := range cmds {
		if n := cmd.Val(); n > 0 {
			s.metrics.teamIndexSize.Record(ctx, n)
		}
	}
}

// healSandboxBatch re-adds missing expiration index members for one bounded
// batch of sandbox records. Returns the number of healed members.
func (s *Storage) healSandboxBatch(ctx context.Context, batch []sandboxtypes.Sandbox) (int, error) {
	now := time.Now()
	type candidate struct {
		member string
		score  float64
	}
	var candidates []candidate
	for _, sbx := range batch {
		if now.Sub(sbx.StartTime) < healGracePeriod {
			continue
		}

		candidates = append(candidates, candidate{
			member: sandboxExpirationMember(sbx),
			score:  float64(sbx.EndTime.UnixMilli()),
		})
	}
	if len(candidates) == 0 {
		return 0, nil
	}

	// ZMSCORE returns 0 for absent members; legitimate scores are unix
	// milliseconds and can never be 0.
	members := make([]string, 0, len(candidates))
	for _, c := range candidates {
		members = append(members, c.member)
	}

	scores, err := s.redisClient.ZMScore(ctx, globalExpirationSet, members...).Result()
	if err != nil {
		return 0, fmt.Errorf("ZMSCORE failed: %w", err)
	}

	var missing []redis.Z
	for i, c := range candidates {
		if scores[i] != 0 {
			continue
		}

		missing = append(missing, redis.Z{Score: c.score, Member: c.member})
	}
	if len(missing) == 0 {
		return 0, nil
	}

	// NX: only fill holes. A concurrent Add/Update owns the member's score.
	// A TOCTOU with a concurrent Remove can only plant an orphan member,
	// which ExpiredItems sweeps once its score passes — garbage, never a
	// false eviction (eviction re-checks the stored JSON and re-validates
	// expiry under the lock in StartRemoving).
	if err := s.redisClient.ZAddNX(ctx, globalExpirationSet, missing...).Err(); err != nil {
		return 0, fmt.Errorf("ZADD NX failed: %w", err)
	}

	s.metrics.indexHealed.Add(ctx, int64(len(missing)))

	return len(missing), nil
}
