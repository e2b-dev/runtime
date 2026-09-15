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

	// Sample the ZSET cardinality once per heal pass. A single ZCARD piggybacked
	// here costs one Redis round-trip every 5 min — negligible next to the
	// forEachSandboxBatch scan that precedes it.
	if n, zErr := s.redisClient.ZCard(ctx, globalExpirationSet).Result(); zErr == nil {
		s.metrics.indexSize.Record(ctx, n)
	} else {
		logger.L().Warn(ctx, "Failed to sample expiration index size", zap.Error(zErr))
	}

	return healed, nil
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
