package redis

import (
	"context"
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
// again. It bounds how late a sandbox restored to Running is seen. A member
// whose record the reader skipped waits the same delay, which bounds how
// often it is read and logged.
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

func (r memberRef) record() recordRef {
	return recordRef{TeamID: r.teamID, SandboxID: r.sandboxID}
}

func membersOf(refs []memberRef) []any {
	members := make([]any, len(refs))
	for i, ref := range refs {
		members[i] = ref.member
	}

	return members
}

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

	var invalid []any
	members := make([]memberRef, 0, len(expiredMembers))
	refs := make([]recordRef, 0, len(expiredMembers))
	for _, member := range expiredMembers {
		ref, ok := parseMemberRef(member)
		if !ok {
			// Unparseable: sweep it so it stops consuming the scan window on every tick
			logger.L().Warn(ctx, "Removing invalid expiration index member", zap.String("member", member))
			invalid = append(invalid, member)

			continue
		}

		members = append(members, ref)
		refs = append(refs, ref.record())
	}

	// A failed read leaves the batch untouched: a member is swept only once
	// its record is known to be absent.
	found, absent, err := s.fetchRecords(ctx, refs)
	if err != nil {
		return nil, fmt.Errorf("failed to read the expired sandboxes' records: %w", err)
	}
	gone := make(map[recordRef]struct{}, len(absent))
	for _, ref := range absent {
		gone[ref] = struct{}{}
	}

	var result []sandboxtypes.Sandbox
	var rescores []redis.Z  // live members whose score drifted from EndTime
	var deferrals []redis.Z // expired members whose sandbox is still in a transition or held
	var orphans, deadExecutions []memberRef

	for _, ref := range members {
		// Sandbox key gone but ZSET entry remains — orphaned. Members are
		// execution-scoped, so this removal can never unindex a fresh
		// execution concurrently re-added under the same sandbox ID.
		if _, ok := gone[ref.record()]; ok {
			orphans = append(orphans, ref)

			continue
		}

		// A record the reader skipped and logged: nothing is swept or killed
		// for it, and its member leaves the expired window for a while, so
		// the next ticks do not read and log it again.
		sbx, ok := found[ref.record()]
		if !ok {
			deferrals = append(deferrals, redis.Z{
				Score:  float64(now.Add(transitionRecheckDelay).UnixMilli()),
				Member: ref.member,
			})

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

		// Eviction refuses a held sandbox for as long as its operation runs,
		// so it leaves the window instead of being tried on every tick.
		if sbx.Held(now) {
			deferrals = append(deferrals, redis.Z{
				Score:  float64(now.Add(transitionRecheckDelay).UnixMilli()),
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
			logger.L().Warn(ctx, "Failed to defer expiration index entries of sandboxes in transition or held", zap.Error(err), zap.Int("count", len(deferrals)))
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

// restoreReinstalled puts back the member of each execution among refs whose
// record is stored, after a removal of those members by a caller that had
// read the record absent or another execution's. A Replace reinstalling the
// execution between that read and the removal wrote the same member before
// its record, and the removal took it; the member and the record live in
// different slots, so no one command can check the record as it removes the
// member. The sweep holds no lock, so a complete Replace can land inside it;
// a Remove holds the record's lock, so only a Replace written past an expired
// lock can land inside that one. A Replace landing after this read asserts
// its member again after its record's write (see add), so the two reads
// together leave no stored record unindexed by an acknowledged removal. The
// member comes back due now, not scored by the record read here: that
// record can be older than the one a later write stored, and a future score
// would hold the sandbox past its end; the next sweep reads the record and
// scores the member by it, or returns the sandbox if it has expired. NX: a
// score a concurrent writer set stands. A record the reader skips is left
// to the healer, as is a failed read or write here.
func (s *Storage) restoreReinstalled(ctx context.Context, refs []memberRef) {
	if len(refs) == 0 {
		return
	}

	records := make([]recordRef, len(refs))
	for i, ref := range refs {
		records[i] = ref.record()
	}
	found, _, err := s.fetchRecords(ctx, records)
	if err != nil {
		logger.L().Warn(ctx, "Failed to read the records of removed expiration index members", zap.Error(err), zap.Int("count", len(refs)))

		return
	}

	dueNow := float64(time.Now().UnixMilli())
	var members []redis.Z
	for _, ref := range refs {
		sbx, ok := found[ref.record()]
		if !ok || sbx.ExecutionID != ref.executionID {
			continue
		}

		logger.L().Warn(ctx, "Expiration index member of an execution reinstalled during its removal put back", logger.WithSandboxID(sbx.SandboxID), zap.String("execution_id", sbx.ExecutionID))
		members = append(members, redis.Z{Score: dueNow, Member: ref.member})
	}

	if len(members) == 0 {
		return
	}

	if err := s.redisClient.ZAddNX(ctx, globalExpirationSet, members...).Err(); err != nil {
		logger.L().Warn(ctx, "Failed to put back expiration index members of reinstalled executions", zap.Error(err), zap.Int("count", len(members)))
	}
}
