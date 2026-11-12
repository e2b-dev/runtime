package redis

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

const (
	healInterval = 5 * time.Minute

	// healGracePeriod skips recently started sandboxes:
	// this prevents the healer from clearing in-flight Add/Remove
	healGracePeriod = time.Minute
)

// startHealer restores "sandbox key exists => expiration index member exists".
// A sandbox missing from the global expiration ZSET is never seen by the evictor
// and would otherwise live forever. It then repairs what an interrupted Add or
// Remove leaves between a record and its index entry (healIndexConsistency),
// from the same walk of the team indexes.
//
// Runs on every API pod with jitter; ZADD NX makes concurrent passes idempotent
// and harmless, and the index repairs are made under the sandbox's lock.
func (s *Storage) startHealer(ctx context.Context) {
	timer := time.NewTimer(jitterBackoff(healInterval))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			healed, walk, err := s.healExpirationIndex(ctx)
			if err != nil {
				logger.L().Warn(ctx, "Expiration index heal pass failed", zap.Error(err))
			}
			if healed > 0 {
				logger.L().Warn(ctx, "Healed sandboxes missing from expiration index", zap.Int("count", healed))
			}

			report, err := s.healIndexConsistency(ctx, walk)
			if err != nil {
				logger.L().Warn(ctx, "Index consistency heal pass failed", zap.Error(err))
			}
			if report.repairs() > 0 {
				logger.L().Warn(ctx, "Healed sandbox records and index entries",
					zap.Int("readded", report.Readded),
					zap.Int("removed", report.Removed),
					zap.Int("deleted_stale", report.DeletedStale),
				)
			}

			timer.Reset(jitterBackoff(healInterval))
		}
	}
}

// teamWalk is what one walk of the team indexes found beside the records it
// streamed: the entries whose record is absent or that no key can be built
// from, and whether every team was walked.
type teamWalk struct {
	dangling []recordRef
	complete bool
}

// healExpirationIndex scans all stored sandboxes and re-adds expiration index
// members missing for live sandbox keys. Returns the number of healed members
// and what the walk found for healIndexConsistency, which walks nothing
// itself. Per-team failures are isolated by the shared scanner; other teams
// still get healed, and the walk is then incomplete.
func (s *Storage) healExpirationIndex(ctx context.Context) (int, teamWalk, error) {
	healed := 0
	var walk teamWalk
	skipped, err := s.forEachSandboxBatch(ctx, func(_ string, batch []sandboxtypes.Sandbox, absent []recordRef) error {
		walk.dangling = append(walk.dangling, absent...)
		n, err := s.healSandboxBatch(ctx, batch)
		if err != nil {
			return err
		}

		healed += n

		return nil
	})
	if err != nil {
		return healed, walk, err
	}
	walk.complete = skipped == 0

	return healed, walk, nil
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

// healKind names a repair the healer made, the index_heal metric's kind.
type healKind string

const (
	indexHealKindAttr = "kind"
	recordsFormAttr   = "form"

	healReadded      healKind = "readded"       // index entry of a Running record added back
	healRemoved      healKind = "removed"       // index entry without a record removed
	healDeletedStale healKind = "deleted_stale" // stale non-Running record without an entry deleted
)

// healKinds is every kind, in the order the counter and the sweep gauges
// report them.
var healKinds = []healKind{healReadded, healRemoved, healDeletedStale}

// healReport is what one index-consistency sweep repaired and counted. The
// census is complete when every team's index was walked and every sandbox
// with an expiration member was probed.
type healReport struct {
	Readded, Removed, DeletedStale int
	Records                        int
	Complete                       bool
}

func (r *healReport) add(kind healKind) {
	switch kind {
	case healReadded:
		r.Readded++
	case healRemoved:
		r.Removed++
	case healDeletedStale:
		r.DeletedStale++
	}
}

func (r *healReport) repairs() int {
	return r.Readded + r.Removed + r.DeletedStale
}

// healHistory is the reports of the last two sweeps, which the census and
// sweep gauges observe together; previous is nil before the second sweep.
type healHistory struct {
	last     healReport
	previous *healReport
}

// healState is the state the lock-free walks found a sandbox in.
type healState uint8

const (
	unindexedRecord healState = iota + 1 // a record without its index entry
	danglingEntry                        // an index entry without a record
	unusableEntry                        // an index entry no key can be built from
)

// nominee is a sandbox the walks found in a state to repair.
type nominee struct {
	ref   recordRef
	state healState
}

// healIndexConsistency repairs the states an interrupted Add or Remove, or a
// replication loss, leaves between a sandbox's record, its index entry and
// its expiration member, and takes the census. The expiration walk
// here and the team walk behind walk nominate without a lock; every repair
// is made under the sandbox's lock, after the state is read again there.
func (s *Storage) healIndexConsistency(ctx context.Context, walk teamWalk) (report healReport, err error) {
	defer func() {
		history := &healHistory{last: report}
		if prior := s.heals.Load(); prior != nil {
			history.previous = &prior.last
		}
		s.heals.Store(history)
	}()
	now := time.Now()
	report.Complete = walk.complete

	nominees, err := s.walkExpirationMembers(ctx, &report)
	if err != nil {
		report.Complete = false

		return report, err
	}

	// An entry is removed once it has dangled for a grace period; one that
	// stops dangling is forgotten. One no key can be built from goes at
	// once: no writer can hold its lock or write its record.
	since := make(map[recordRef]time.Time, len(walk.dangling))
	for _, ref := range walk.dangling {
		if !validID(ref.SandboxID) {
			nominees = append(nominees, nominee{ref: ref, state: unusableEntry})

			continue
		}
		first, ok := s.danglingSince[ref]
		if !ok {
			first = now
		}
		since[ref] = first
		if now.Sub(first) > healGracePeriod {
			nominees = append(nominees, nominee{ref: ref, state: danglingEntry})
		}
	}
	s.danglingSince = since

	for _, n := range nominees {
		kind, err := s.healNominee(ctx, n, now)
		if err != nil {
			logger.L().Warn(ctx, "Failed to heal sandbox", zap.Error(err), logger.WithTeamID(n.ref.TeamID), logger.WithSandboxID(n.ref.SandboxID))

			continue
		}
		if kind == "" {
			continue
		}
		report.add(kind)
		s.indexHeal.Add(ctx, 1, metric.WithAttributes(attribute.String(indexHealKindAttr, string(kind))))
	}

	return report, nil
}

// walkExpirationMembers probes, for every sandbox with an expiration member,
// the record and the index entry, counts the records into report, and
// returns the sandboxes whose record has no index entry. A member that does
// not parse, or whose probe the server refused, counts nowhere, the latter
// leaving the census incomplete; a sandbox is probed once however many
// executions' members it has.
func (s *Storage) walkExpirationMembers(ctx context.Context, report *healReport) ([]nominee, error) {
	var nominees []nominee
	seen := make(map[recordRef]struct{})
	var refused int
	var firstRefusal error
	var cursor uint64
	for {
		// ZSCAN replies member, score, member, score...
		fields, next, err := s.redisClient.ZScan(ctx, globalExpirationSet, cursor, "", sandboxScanBatchSize).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to scan the expiration index: %w", err)
		}

		refs := make([]recordRef, 0, len(fields)/2)
		for i := 0; i < len(fields); i += 2 {
			teamID, sandboxID, _, ok := parseExpirationMember(fields[i])
			if !ok {
				continue
			}
			ref := recordRef{TeamID: teamID, SandboxID: sandboxID}
			if _, dup := seen[ref]; dup {
				continue
			}
			seen[ref] = struct{}{}
			refs = append(refs, ref)
		}

		probes, err := s.probeRecords(ctx, refs)
		if err != nil {
			return nil, err
		}
		for i, p := range probes {
			if p.err != nil {
				if refused == 0 {
					firstRefusal = p.err
				}
				refused++

				continue
			}
			// A member without a record is the evictor's.
			if !p.exists {
				continue
			}
			report.Records++
			if !p.indexed {
				nominees = append(nominees, nominee{ref: refs[i], state: unindexedRecord})
			}
		}

		cursor = next
		if cursor == 0 {
			break
		}
	}
	if refused > 0 {
		// Once per sweep: a key of another type refuses the same probe every sweep.
		logger.L().Warn(ctx, "Skipped sandboxes whose probes the server refused, census incomplete", zap.Error(firstRefusal), zap.Int("count", refused))
		report.Complete = false
	}

	return nominees, nil
}

// recordProbe is what the walk saw of one sandbox: its record and its index
// entry; or the reply that refused one of its probes.
type recordProbe struct {
	exists, indexed bool
	err             error
}

// probeRecords probes the refs' records and their index entries through one
// pipeline. A probe the server refused, such as SISMEMBER on an index key of
// another type, is returned on its probe for the caller to skip; an error
// that is no reply, the connection, a redirect that ran out, or a hook that
// never ran the commands, ends the walk before the untouched empty replies
// could pass for absent records.
func (s *Storage) probeRecords(ctx context.Context, refs []recordRef) ([]recordProbe, error) {
	if len(refs) == 0 {
		return nil, nil
	}

	pipe := s.redisClient.Pipeline()
	type cmds struct {
		exists  *redis.IntCmd
		indexed *redis.BoolCmd
	}
	all := make([]cmds, len(refs))
	for i, ref := range refs {
		all[i] = cmds{
			exists:  pipe.Exists(ctx, recordKey(ref.SandboxID)),
			indexed: pipe.SIsMember(ctx, GetSandboxStorageTeamIndexKey(ref.TeamID), ref.SandboxID),
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && !refusedProbe(err) {
		recordRedisError(ctx, err)

		return nil, fmt.Errorf("failed to probe sandbox records: %w", err)
	}

	probes := make([]recordProbe, len(refs))
	for i, c := range all {
		if err := cmp.Or(c.exists.Err(), c.indexed.Err()); err != nil {
			recordRedisError(ctx, err)
			if !refusedProbe(err) {
				return nil, fmt.Errorf("failed to probe sandbox records: %w", err)
			}
			probes[i].err = err

			continue
		}
		probes[i] = recordProbe{exists: c.exists.Val() > 0, indexed: c.indexed.Val()}
	}

	return probes, nil
}

// refusedProbe reports whether err is the server's reply refusing one
// command, such as WRONGTYPE, which concerns that command's sandbox alone.
// A redirect or a slot error is a reply too, but concerns the whole walk.
func refusedProbe(err error) bool {
	if _, isReply := errors.AsType[redis.Error](err); !isReply {
		return false
	}
	kind, ok := redisErrorKind(err)

	return ok && kind == errorKindOther
}

// healNominee repairs one nominee under its lock and returns the repair
// made, or "" when none. The state is read again under the lock: the walk's
// read may predate a Remove that completed or a resume of the ID that was
// admitted. An entry no key can be built from is removed without a lock,
// which no writer can hold for its ID. A simulated crash leaves the lock
// held, as every operation's does.
func (s *Storage) healNominee(ctx context.Context, n nominee, now time.Time) (kind healKind, err error) {
	if err := s.fault(faultHealerAfterNominate); err != nil {
		return "", err
	}
	if n.state == unusableEntry {
		return s.removeEntry(ctx, n.ref)
	}

	// Another team's record under the key is nothing of this entry's team
	// to repair, as the guard under the lock finds too.
	if err := s.refuseOtherTeam(ctx, n.ref.TeamID, n.ref.SandboxID); errors.Is(err, sandboxtypes.ErrNotFound) {
		return "", nil
	} else if err != nil {
		return "", err
	}
	lock, err := s.locker.Obtain(ctx, redis_utils.GetLockKey(recordKey(n.ref.SandboxID)), lockTimeout)
	if err != nil {
		return "", fmt.Errorf("failed to obtain lock: %w", err)
	}
	defer releaseLock(ctx, lock.Release, &err)
	if err := s.fault(faultHealerUnderLock); err != nil {
		return "", err
	}

	if n.state != danglingEntry {
		return s.healUnindexed(ctx, lock, n.ref, now)
	}
	// The entry goes while the key still holds no record; a record there,
	// whoever's, is a writer's to judge, as the guard under the lock finds
	// too.
	exists, err := s.redisClient.Exists(ctx, recordKey(n.ref.SandboxID)).Result()
	if err != nil {
		return "", fmt.Errorf("EXISTS: %w", err)
	}
	if exists > 0 {
		return "", nil
	}

	return s.removeEntry(ctx, n.ref)
}

// healUnindexed indexes a Running record again once it is older than the
// grace period, and deletes a non-Running one, with its expiration member,
// once it is stale: its VM is gone, and indexed again it would be listed,
// counted, and returned by a resume as live.
func (s *Storage) healUnindexed(ctx context.Context, lock *storageLock, ref recordRef, now time.Time) (healKind, error) {
	key := recordKey(ref.SandboxID)
	sbx, ok, err := s.readRecordOf(ctx, key, ref)
	if err != nil || !ok {
		return "", err
	}
	teamKey := GetSandboxStorageTeamIndexKey(ref.TeamID)
	indexed, err := s.redisClient.SIsMember(ctx, teamKey, ref.SandboxID).Result()
	if err != nil {
		return "", fmt.Errorf("SISMEMBER: %w", err)
	}
	if indexed {
		return "", nil
	}

	if sbx.State == sandboxtypes.StateRunning {
		if now.Sub(sbx.StartTime) <= healGracePeriod {
			return "", nil
		}
		if err := s.redisClient.SAdd(ctx, teamKey, ref.SandboxID).Err(); err != nil {
			return "", fmt.Errorf("SADD: %w", err)
		}
		// Add lists the team after indexing; the team gauges and the team
		// walk read that list, so the sandbox is counted and walked again.
		if err := s.redisClient.ZAdd(ctx, globalTeamsSet, redis.Z{Score: float64(now.Unix()), Member: ref.TeamID}).Err(); err != nil {
			logger.L().Warn(ctx, "Failed to list the team of a re-indexed sandbox in the global teams index", zap.Error(err), logger.WithTeamID(ref.TeamID))
		}

		return healReadded, nil
	}

	if now.Sub(sbx.EndTime) <= sandboxtypes.StaleCutoff {
		return "", nil
	}
	deleted, _, _, err := parseRemoveReply(RunScript(ctx, s.redisClient, "remove_record", removeSandboxScript, []string{key, goneKeyFor(key), lock.Key(), metaKeyFor(key)}, ref.SandboxID, sbx.ExecutionID, int(goneExecutionsTTL.Seconds()), "", lockValueOf(lock)))
	if err != nil {
		return "", fmt.Errorf("remove_record: %w", err)
	}
	if !deleted {
		return "", nil
	}
	if err := s.redisClient.ZRem(ctx, globalExpirationSet, sandboxExpirationMember(sbx)).Err(); err != nil {
		// The record is gone; ExpiredItems sweeps the member once its score passes.
		logger.L().Warn(ctx, "Failed to remove sandbox from global expiration index", zap.Error(err), logger.WithSandboxID(ref.SandboxID))
	}

	return healDeletedStale, nil
}

// removeEntry removes the sandbox's index entry and reports the repair when
// there was one.
func (s *Storage) removeEntry(ctx context.Context, ref recordRef) (healKind, error) {
	removed, err := s.redisClient.SRem(ctx, GetSandboxStorageTeamIndexKey(ref.TeamID), ref.SandboxID).Result()
	if err != nil {
		return "", fmt.Errorf("SREM: %w", err)
	}
	if removed == 0 {
		return "", nil
	}

	return healRemoved, nil
}

// readRecordOf reads the record under key and reports whether it is ref's:
// an absent record is not, nor one the readers skip, which is logged and
// left alone as they leave it: nothing can be repaired under a key that is
// not the record's own.
func (s *Storage) readRecordOf(ctx context.Context, key string, ref recordRef) (sandboxtypes.Sandbox, bool, error) {
	// Refs come from parseExpirationMember, which admits only UUID teams, so a
	// parse failure here is a programming error worth surfacing.
	team, err := uuid.Parse(ref.TeamID)
	if err != nil {
		return sandboxtypes.Sandbox{}, false, fmt.Errorf("team id %q: %w", ref.TeamID, err)
	}
	data, err := s.redisClient.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return sandboxtypes.Sandbox{}, false, nil
	}
	if err != nil {
		return sandboxtypes.Sandbox{}, false, fmt.Errorf("GET: %w", err)
	}
	sbx, ok := decodeRecord(ctx, readRef{ref: ref, team: team}, data)

	return sbx, ok, nil
}
