package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const expiredItemsBatchSize = 256

// transitionRecheckDelay is how long an expired sandbox that is still in a
// transition stays out of the expired window before ExpiredItems looks at it
// again. It bounds how late a sandbox restored to Running is seen.
const transitionRecheckDelay = 10 * time.Second

// memberRef is a parsed member of the global expiration index.
type memberRef struct {
	member      string
	teamID      string
	sandboxID   string
	executionID string
}

func parseMemberRef(member string) (memberRef, bool) {
	teamID, sandboxID, executionID, ok := parseExpirationMember(member)
	if !ok {
		return memberRef{}, false
	}

	return memberRef{member: member, teamID: teamID, sandboxID: sandboxID, executionID: executionID}, true
}

// ExpiredItems returns running sandboxes whose EndTime has passed.
// It bounds per-cycle work via LIMIT and cleans up orphaned ZSET entries.
func (s *Storage) ExpiredItems(ctx context.Context) ([]sandboxtypes.Sandbox, error) {
	now := time.Now()
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

	var refs []memberRef
	var invalid []any
	for _, member := range expiredMembers {
		ref, ok := parseMemberRef(member)
		if !ok {
			// Unparseable: sweep it so it stops consuming the scan window on every tick
			logger.L().Warn(ctx, "Removing invalid expiration index member", zap.String("member", member))
			invalid = append(invalid, member)

			continue
		}

		refs = append(refs, ref)
	}

	raws, err := s.readRecords(ctx, refs)
	if err != nil {
		return nil, err
	}

	// Deserialize and filter.
	var result []sandboxtypes.Sandbox
	var rescores []redis.Z  // live members whose score drifted from EndTime
	var deferrals []redis.Z // expired members whose sandbox is still in a transition
	var orphans, deadExecutions []memberRef

	for i, raw := range raws {
		ref := refs[i]

		// Sandbox key gone but ZSET entry remains — orphaned. Members are
		// execution-scoped, so this removal can never unindex a fresh
		// execution concurrently re-added under the same sandbox ID.
		if raw == nil {
			orphans = append(orphans, ref)

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
			deadExecutions = append(deadExecutions, ref)

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
				// Let the current removal finish, but move the member out of
				// the expired window for a while (never past the stale cutoff),
				// so transitions cannot fill the window.
				recheckAt := now.Add(transitionRecheckDelay)
				if staleAt := sbx.EndTime.Add(sandboxtypes.StaleCutoff); staleAt.Before(recheckAt) {
					recheckAt = staleAt
				}
				deferrals = append(deferrals, redis.Z{
					Score:  float64(recheckAt.UnixMilli()),
					Member: ref.member,
				})

				continue
			}

			logger.L().Debug(ctx, "ExpiredItems: Sandbox is in transition state for more than stale cutoff, removing", logger.WithSandboxID(sbx.SandboxID), logger.Time("end_time", sbx.EndTime))
		}

		result = append(result, sbx)
	}

	// Remove stale members so the set doesn't grow unboundedly. Each class is
	// one ZREM, whose reply is the count actually removed.
	for _, class := range []struct {
		members []any
		reason  metric.MeasurementOption
	}{
		{invalid, s.metrics.sweptInvalid},
		{membersOf(orphans), s.metrics.sweptOrphan},
		{membersOf(deadExecutions), s.metrics.sweptDeadExecution},
	} {
		if len(class.members) == 0 {
			continue
		}

		removed, err := s.redisClient.ZRem(ctx, globalExpirationSet, class.members...).Result()
		if err != nil {
			logger.L().Warn(ctx, "Failed to clean up stale expiration index entries", zap.Error(err), zap.Int("count", len(class.members)))

			continue
		}
		if removed > 0 {
			s.metrics.indexSwept.Add(ctx, removed, class.reason)
		}
	}

	// A Replace that reinstalled a member's execution between the read above
	// and the removal lost the member to it; its record is read again now.
	s.restoreReinstalled(ctx, append(orphans, deadExecutions...))

	if len(deferrals) > 0 {
		// XX: never resurrect a member a concurrent Remove deleted.
		if err := s.redisClient.ZAddXX(ctx, globalExpirationSet, deferrals...).Err(); err != nil {
			logger.L().Warn(ctx, "Failed to defer expiration index entries of sandboxes in transition", zap.Error(err), zap.Int("count", len(deferrals)))
		} else {
			s.metrics.indexDeferred.Add(ctx, int64(len(deferrals)))
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

func membersOf(refs []memberRef) []any {
	members := make([]any, len(refs))
	for i, ref := range refs {
		members[i] = ref.member
	}

	return members
}

// readRecords reads the records the members name, one MGET per team in one
// pipeline (a team's keys share a slot), and returns the raw values in the
// members' order, nil for an absent record.
func (s *Storage) readRecords(ctx context.Context, refs []memberRef) ([]any, error) {
	if len(refs) == 0 {
		return nil, nil
	}

	var teams []string
	byTeam := make(map[string][]int)
	for i, ref := range refs {
		if _, ok := byTeam[ref.teamID]; !ok {
			teams = append(teams, ref.teamID)
		}
		byTeam[ref.teamID] = append(byTeam[ref.teamID], i)
	}

	pipe := s.redisClient.Pipeline()
	cmds := make([]*redis.SliceCmd, len(teams))
	for t, teamID := range teams {
		keys := make([]string, 0, len(byTeam[teamID]))
		for _, i := range byTeam[teamID] {
			keys = append(keys, getSandboxKey(teamID, refs[i].sandboxID))
		}
		cmds[t] = pipe.MGet(ctx, keys...)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("MGET pipeline failed: %w", err)
	}

	raws := make([]any, len(refs))
	for t, teamID := range teams {
		for k, i := range byTeam[teamID] {
			raws[i] = cmds[t].Val()[k]
		}
	}

	return raws, nil
}

// restoreReinstalled puts back the member of each execution among refs whose
// record is stored, after a removal of those members by a caller that had
// read the record absent or another execution's. A Replace reinstalling the
// execution between that read and the removal wrote the same member before
// its record, and the removal took it; the member and the record live in
// different slots, so no one command can check the record as it removes the
// member. A Replace landing after this read asserts its member again after
// its record's write (see add), so the two reads together leave no stored
// record unindexed by an acknowledged removal. The member comes back due
// now, not scored by the record read here: that record can be older than
// the one a later write stored, and a future score would hold the sandbox
// past its end; the next sweep reads the record and scores the member by
// it, or returns the sandbox if it has expired. NX: a score a concurrent
// writer set stands. A failed read or write here, like a removal that
// executes after the repair that followed its own retry, leaves the record
// to the healer, as a failed index write does today.
func (s *Storage) restoreReinstalled(ctx context.Context, refs []memberRef) {
	if len(refs) == 0 {
		return
	}

	raws, err := s.readRecords(ctx, refs)
	if err != nil {
		logger.L().Warn(ctx, "Failed to read the records of removed expiration index members", zap.Error(err), zap.Int("count", len(refs)))

		return
	}

	dueNow := float64(time.Now().UnixMilli())
	var members []redis.Z
	for i, raw := range raws {
		str, ok := raw.(string)
		if !ok {
			continue
		}

		var sbx sandboxtypes.Sandbox
		if err := json.Unmarshal([]byte(str), &sbx); err != nil || sbx.ExecutionID != refs[i].executionID {
			continue
		}

		logger.L().Warn(ctx, "Expiration index member of an execution reinstalled during its removal put back", logger.WithSandboxID(sbx.SandboxID), zap.String("execution_id", sbx.ExecutionID))
		members = append(members, redis.Z{Score: dueNow, Member: refs[i].member})
	}

	if len(members) == 0 {
		return
	}

	if err := s.redisClient.ZAddNX(ctx, globalExpirationSet, members...).Err(); err != nil {
		logger.L().Warn(ctx, "Failed to put back expiration index members of reinstalled executions", zap.Error(err), zap.Int("count", len(members)))
	}
}
