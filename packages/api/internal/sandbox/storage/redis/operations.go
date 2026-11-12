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

// addIndexRetries is how many times an index entry is retried after the
// record is written, on top of the client's own retries.
const addIndexRetries = 3

// goneExecutionsTTL is how long an execution removed or replaced under a
// sandbox ID stays refused: longer than any retry or delayed segment of its
// create's write can take to land.
const goneExecutionsTTL = 10 * time.Minute

// releaseLock, deferred with a pointer to the operation's named error
// result, releases the lock once it returns, unless it returned a simulated
// crash, after which it stays held as a crashed replica's would.
func releaseLock(ctx context.Context, release func(context.Context) error, err *error) {
	if crashed(*err) {
		return
	}
	if releaseErr := release(context.WithoutCancel(ctx)); releaseErr != nil {
		logger.L().Error(ctx, "Failed to release lock", zap.Error(releaseErr))
	}
}

// Add stores a sandbox record and indexes it, under the sandbox's lock, so a
// predecessor's operation under it finishes before it writes. The record
// write is the commit point: a write that fails after the client's retries
// may have landed with its reply lost, so Add removes its own execution,
// index entry first, and returns the error; an index entry that fails after
// the record is written is a success with the record unindexed, reachable by
// ID until the healer indexes it.
//
// The write is idempotent per execution: the locks order operations, not a
// command that lands twice (a retried command, a segment delivered after
// later writes), so a record of the same execution already stored is kept
// as it is, and an execution removed or replaced under the ID is refused
// for goneExecutionsTTL.
func (s *Storage) Add(ctx context.Context, sbx sandboxtypes.Sandbox) error {
	return s.add(ctx, sbx, false)
}

// Replace stores sbx over whatever is stored under its ID, this execution's
// record included: a sync or reconcile re-add, never a create's write.
func (s *Storage) Replace(ctx context.Context, sbx sandboxtypes.Sandbox) error {
	return s.add(ctx, sbx, true)
}

func (s *Storage) add(ctx context.Context, sbx sandboxtypes.Sandbox, replace bool) (err error) {
	data, err := json.Marshal(sbx)
	if err != nil {
		return fmt.Errorf("failed to marshal sandbox: %w", err)
	}

	teamID := sbx.TeamID.String()
	key := recordKey(sbx.SandboxID)
	teamKey := GetSandboxStorageTeamIndexKey(teamID)

	lock, err := s.locker.Obtain(ctx, redis_utils.GetLockKey(key), lockTimeout)
	if err != nil {
		return fmt.Errorf("failed to obtain lock: %w", err)
	}
	defer releaseLock(ctx, lock.Release, &err)

	// Index by EndTime first, so an interrupted Add leaks nothing: ExpiredItems
	// sweeps a member whose record never appears. The member is scoped to
	// this execution, so no removal of an older one can unindex it, and a
	// create's write never lowers the score a stored record has reached.
	member := redis.Z{Score: float64(sbx.EndTime.UnixMilli()), Member: sandboxExpirationMember(sbx)}
	err = s.run(faultAddAfterLocks, func() error {
		if replace {
			return s.redisClient.ZAdd(ctx, globalExpirationSet, member).Err()
		}

		return s.redisClient.ZAddNX(ctx, globalExpirationSet, member).Err()
	})
	if err != nil {
		return fmt.Errorf("failed to add sandbox to global expiration index: %w", err)
	}

	// The commit point, the gone set beside the record in its slot; the
	// index entry lives in another slot and follows.
	keys := []string{key, goneKeyFor(key)}
	replaceFlag := "0"
	if replace {
		replaceFlag = "1"
	}
	var written int64
	err = s.run(faultAddBeforeRecordWrite, func() (runErr error) {
		written, runErr = RunScript(ctx, s.redisClient, "add_sandbox", addSandboxScript, keys, data, sbx.SandboxID, sbx.ExecutionID, replaceFlag, int(goneExecutionsTTL.Seconds())).Int64()

		return runErr
	})
	if err == nil {
		err = s.fault(faultAddRecordWrite)
	}
	if crashed(err) {
		return err
	}
	if err != nil {
		// The caller may be gone by now; the cleanup runs regardless, as
		// the lock release does.
		if cleanupErr := s.cleanupAdd(context.WithoutCancel(ctx), sbx); cleanupErr != nil {
			logger.L().Error(ctx, "Failed to clean up an unconfirmed sandbox record write", zap.Error(cleanupErr), logger.WithSandboxID(sbx.SandboxID))
			s.countWriteAnomaly(ctx, writeAnomalyAddWriteUnconfirmed)
		}

		return fmt.Errorf("failed to store sandbox in Redis: %w", err)
	}
	switch written {
	case 3:
		// A held record is its holder's to change.
		return fmt.Errorf("sandbox %q: %w", sbx.SandboxID, sandboxtypes.ErrLifecycleHeld)
	case 0:
		logger.L().Warn(ctx, "Sandbox already stored for this execution, duplicate write kept out", logger.WithSandboxID(sbx.SandboxID), zap.String("execution_id", sbx.ExecutionID))
	case 2:
		// The member written above outlives the refused write, as a member of
		// any execution without a record does: ExpiredItems sweeps it once its
		// score passes, finding no record of its execution.
		logger.L().Warn(ctx, "Sandbox execution removed or replaced meanwhile, late write kept out", logger.WithSandboxID(sbx.SandboxID), zap.String("execution_id", sbx.ExecutionID))
	}

	// The record is committed: the steps that index it and list its team run
	// even when the caller is gone, as the cleanup and the lock release do.
	ctx = context.WithoutCancel(ctx)

	if written == 1 {
		err = s.indexSandbox(ctx, teamKey, sbx.SandboxID)
		if crashed(err) {
			return err
		}
		if err != nil {
			logger.L().Warn(ctx, "Sandbox record written but not indexed, left to the healer", zap.Error(err), logger.WithSandboxID(sbx.SandboxID))
			s.countWriteAnomaly(ctx, writeAnomalyAddUnindexed)
		}
	}

	// Assert the member again after the record's write, in one round trip
	// with the team's entry in the global teams index; neither key is in the
	// script's slot. ExpiredItems removes a member after reading its record
	// absent or another execution's, without a lock; a removal that read the
	// record before the write above can land after the member's write and
	// take it, and this assertion puts it back, due now: the next sweep reads
	// the record and scores the member by it. NX: a score a concurrent writer
	// set stands. A refused write has no record to index. A failure here
	// leaves the record to the healer, which re-indexes a stored record
	// without a member and re-lists its team.
	err = s.run(faultAddAfterIndex, func() error {
		pipe := s.redisClient.Pipeline()
		var asserted *redis.IntCmd
		if written != 2 {
			asserted = pipe.ZAddNX(ctx, globalExpirationSet, redis.Z{Score: float64(time.Now().UnixMilli()), Member: member.Member})
		}
		teamListed := pipe.ZAdd(ctx, globalTeamsSet, redis.Z{Score: float64(time.Now().Unix()), Member: teamID})
		// A pipeline that failed as a whole answered no command: its error
		// is every command's. One that was answered carries each failure on
		// its command.
		if _, execErr := pipe.Exec(ctx); execErr != nil && teamListed.Err() == nil && (asserted == nil || asserted.Err() == nil) {
			return execErr
		}
		if asserted != nil && asserted.Err() != nil {
			logger.L().Warn(ctx, "Failed to assert the sandbox's expiration index member", zap.Error(asserted.Err()), logger.WithSandboxID(sbx.SandboxID))
		}

		return teamListed.Err()
	})
	if crashed(err) {
		return err
	}
	if err != nil {
		logger.L().Warn(ctx, "Failed to list the sandbox's team in the global teams index", zap.Error(err), logger.WithSandboxID(sbx.SandboxID))
	}

	return nil
}

// indexSandbox adds the sandbox's index entry, retrying a failed command
// addIndexRetries times.
func (s *Storage) indexSandbox(ctx context.Context, teamKey, sandboxID string) (err error) {
	for range addIndexRetries + 1 {
		err = s.run(faultAddAfterRecordWrite, func() error {
			return s.redisClient.SAdd(ctx, teamKey, sandboxID).Err()
		})
		if err == nil || crashed(err) {
			return err
		}
	}

	return err
}

// cleanupAdd undoes a record write that failed after the client's retries
// and may have landed, through the remove path: this execution's record and
// index entry go, the entry first, and a record of another execution, a
// stranded predecessor's, is left whole. A delete that exhausts its retries
// is logged as stranded and counted under remove_stranded beside
// add_write_unconfirmed. The guard relies on Add's execution ID being set,
// as every caller sets it: with an empty one the delete would adopt whatever
// execution it finds.
func (s *Storage) cleanupAdd(ctx context.Context, sbx sandboxtypes.Sandbox) error {
	teamID := sbx.TeamID.String()
	_, err := s.removeSandboxRecord(ctx, recordKey(sbx.SandboxID), GetSandboxStorageTeamIndexKey(teamID), nil, sbx.TeamID, sbx.SandboxID, sbx.ExecutionID, "")
	if err != nil && !errors.Is(err, sandboxtypes.ErrExecutionMismatch) {
		return err
	}

	return nil
}

// readRecord reads and decodes the record under key, the tenant guard
// included: an absent record and another team's are both
// sandboxtypes.ErrNotFound, since the key carries no team and the record's
// own is what scopes the read. The bytes read are returned for a
// compare-and-set against them.
func (s *Storage) readRecord(ctx context.Context, key string, teamID uuid.UUID, sandboxID string) ([]byte, sandboxtypes.Sandbox, error) {
	data, err := s.redisClient.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, sandboxtypes.Sandbox{}, fmt.Errorf("sandbox %q: %w", sandboxID, sandboxtypes.ErrNotFound)
	}
	if err != nil {
		return nil, sandboxtypes.Sandbox{}, fmt.Errorf("failed to get sandbox from Redis: %w", err)
	}
	var sbx sandboxtypes.Sandbox
	if err := json.Unmarshal(data, &sbx); err != nil {
		return nil, sandboxtypes.Sandbox{}, fmt.Errorf("failed to unmarshal sandbox: %w", err)
	}
	if sbx.TeamID != teamID {
		return nil, sandboxtypes.Sandbox{}, fmt.Errorf("sandbox %q: %w", sandboxID, sandboxtypes.ErrNotFound)
	}

	return data, sbx, nil
}

// refuseOtherTeam is sandboxtypes.ErrNotFound when the record under the
// sandbox's key names another team, and nil when it is the team's or
// absent. The key carries no team, so this runs before an operation takes
// the lock or follows the transition marker: another team's caller never
// waits on or holds the owner's lock, and never joins the owner's wait.
func (s *Storage) refuseOtherTeam(ctx context.Context, teamID, sandboxID string) error {
	data, err := s.redisClient.Get(ctx, recordKey(sandboxID)).Bytes()
	switch {
	case errors.Is(err, redis.Nil):
		return nil
	case err != nil:
		return fmt.Errorf("failed to probe the record key: %w", err)
	}
	var stored sandboxtypes.Sandbox
	if json.Unmarshal(data, &stored) == nil && stored.TeamID.String() != teamID {
		return fmt.Errorf("sandbox %q: %w", sandboxID, sandboxtypes.ErrNotFound)
	}

	return nil
}

// Get reads a sandbox's record without a lock. A record of another team is
// not found: the key carries no team.
func (s *Storage) Get(ctx context.Context, teamID uuid.UUID, sandboxID string) (sandboxtypes.Sandbox, error) {
	_, sbx, err := s.readRecord(ctx, recordKey(sandboxID), teamID, sandboxID)

	return sbx, err
}

// removeRecordRetries is how many times a record delete is retried after
// its index entry is removed, on top of the client's own retries.
const removeRecordRetries = 3

// Remove deletes a sandbox's record, its team index entry and its expiration
// member, under the sandbox's lock. It removes the record of the expected
// execution, or of any execution when none is expected; a record of another
// execution is left whole, index entry included, and
// sandboxtypes.ErrExecutionMismatch returned. A sandbox already gone is a
// success, as is a record of another team, whose key carries no team.
func (s *Storage) Remove(ctx context.Context, teamID uuid.UUID, sandboxID, expectedExecutionID string) (err error) {
	err = s.refuseOtherTeam(ctx, teamID.String(), sandboxID)
	if errors.Is(err, sandboxtypes.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	lock, err := s.locker.Obtain(ctx, redis_utils.GetLockKey(recordKey(sandboxID)), lockTimeout)
	if err != nil {
		return fmt.Errorf("failed to obtain lock: %w", err)
	}
	defer releaseLock(ctx, lock.Release, &err)

	return s.removeUnderLock(ctx, lock, teamID, sandboxID, expectedExecutionID, "")
}

// removeUnderLock is Remove's removal, run while the caller holds lock, the
// record's, which the record's delete requires to still hold the caller's
// value: the index entry and then the expiration member of the execution
// deleted go with the record. The ZREM is scoped to the execution deleted,
// so a member another execution wrote is never unindexed here. With holdID,
// only a record under that live hold is removed, sandboxtypes.ErrNotHeld
// otherwise; without, a held record is refused with
// sandboxtypes.ErrLifecycleHeld.
func (s *Storage) removeUnderLock(ctx context.Context, lock *storageLock, teamID uuid.UUID, sandboxID, expectedExecutionID, holdID string) (err error) {
	team := teamID.String()
	key := recordKey(sandboxID)
	teamKey := GetSandboxStorageTeamIndexKey(team)

	deleted, err := s.removeSandboxRecord(ctx, key, teamKey, lock, teamID, sandboxID, expectedExecutionID, holdID)
	if err != nil {
		return err
	}
	if deleted == "" {
		// Gone, or another team's record under the key, which is none of
		// this sandbox's.
		return nil
	}

	// Only the deleted execution's member: one left by a record already gone
	// is swept by ExpiredItems once its score passes.
	var sbx sandboxtypes.Sandbox
	decoded := json.Unmarshal([]byte(deleted), &sbx) == nil
	if !decoded || sbx.ExecutionID == "" {
		return nil
	}
	ref := memberRef{member: expirationMember(team, sandboxID, sbx.ExecutionID), teamID: team, sandboxID: sandboxID, executionID: sbx.ExecutionID}
	err = s.run(faultRemoveAfterDelete, func() error {
		return s.redisClient.ZRem(ctx, globalExpirationSet, ref.member).Err()
	})
	if crashed(err) {
		return err
	}
	if err != nil {
		// The removal may have applied without its reply.
		logger.L().Warn(ctx, "Failed to remove sandbox from global expiration index", zap.Error(err), logger.WithSandboxID(sandboxID))
	}
	// A Replace of the deleted execution written past an expired lock can
	// land between the delete and the removal above and lose its member to
	// it: the record is read again.
	s.restoreReinstalled(ctx, []memberRef{ref})

	return nil
}

// lockValueOf is the value a script writing under the record's lock requires
// the lock to hold, none for a caller without a lock.
func lockValueOf(lock *storageLock) string {
	if lock == nil {
		return ""
	}

	return lock.Token() + lock.Metadata()
}

// refusedRemoval is the error of a removal the script refused with the
// record raw left in place: the record is another execution's, held by
// another operation, or, for a holder, not under its hold any more.
func refusedRemoval(raw, sandboxID, expectedExecutionID, holdID string) error {
	var stored sandboxtypes.Sandbox
	decoded := json.Unmarshal([]byte(raw), &stored) == nil
	sameExecution := decoded && (expectedExecutionID == "" || expectedExecutionID == stored.ExecutionID)
	switch {
	case holdID != "" && sameExecution:
		return sandboxtypes.ErrNotHeld
	case sameExecution && stored.Held(time.Now()):
		return sandboxtypes.ErrLifecycleHeld
	}

	return fmt.Errorf("sandbox %q: %w", sandboxID, sandboxtypes.ErrExecutionMismatch)
}

// removeSandboxRecord removes a record, the index entry first, then the
// record, guarded by the execution expected or the one read, and returns
// the JSON deleted, empty when the record was absent or another team's. The
// read comes before the entry goes: a successor admitted while this Remove
// waited for its lock keeps its entry. The script's guard covers a change
// after the read, which only a lock expired under a stalled Remove admits;
// such a successor keeps its record and the healer indexes it again once it
// is older than the grace period. The delete is retried removeRecordRetries
// times; when it keeps failing the record is left stranded for the healer,
// the anomaly counted and the error returned. It runs under lock, nil for
// Add's cleanup, which holds none.
func (s *Storage) removeSandboxRecord(ctx context.Context, key, teamKey string, lock *storageLock, teamID uuid.UUID, sandboxID, expectedExecutionID, holdID string) (string, error) {
	data, err := s.redisClient.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("GET: %w", err)
	}
	var stored sandboxtypes.Sandbox
	decoded := json.Unmarshal(data, &stored) == nil
	switch {
	case decoded && stored.TeamID != teamID:
		return "", nil
	case expectedExecutionID == "" && decoded:
		expectedExecutionID = stored.ExecutionID
	case expectedExecutionID != "" && (!decoded || stored.ExecutionID != expectedExecutionID):
		return "", fmt.Errorf("sandbox %q: %w", sandboxID, sandboxtypes.ErrExecutionMismatch)
	}

	// The index entry goes before the record, in another slot: the same
	// refusals as the script's, read first so no entry goes for a record the
	// script then leaves in place.
	held := stored.Held(time.Now())
	switch {
	case holdID != "" && (!held || stored.LifecycleHold != holdID):
		return "", sandboxtypes.ErrNotHeld
	case holdID == "" && held:
		return "", sandboxtypes.ErrLifecycleHeld
	}

	if err := s.redisClient.SRem(ctx, teamKey, sandboxID).Err(); err != nil {
		return "", fmt.Errorf("SREM: %w", err)
	}

	var deleted, absent bool
	var raw string
	for range removeRecordRetries + 1 {
		err = s.run(faultRemoveAfterSREM, func() (runErr error) {
			deleted, raw, absent, runErr = parseRemoveReply(RunScript(ctx, s.redisClient, "remove_record", removeSandboxScript, []string{key, goneKeyFor(key), redis_utils.GetLockKey(key)}, sandboxID, expectedExecutionID, int(goneExecutionsTTL.Seconds()), holdID, lockValueOf(lock)))

			return runErr
		})
		if err == nil || crashed(err) {
			break
		}
	}
	switch {
	case crashed(err), errors.Is(err, sandboxtypes.ErrLockLost):
		return "", err
	case err != nil:
		// Warn: the caller logs the failed removal.
		logger.L().Warn(ctx, "Sandbox record left stranded, its index entry removed", zap.Error(err), zap.Int("attempts", removeRecordRetries+1), logger.WithSandboxID(sandboxID))
		s.countWriteAnomaly(ctx, writeAnomalyRemoveStranded)

		return "", fmt.Errorf("failed to remove sandbox record: %w", err)
	case absent:
		return "", nil
	case !deleted:
		logger.L().Warn(ctx, "Sandbox record changed under the lock, left in place without its index entry", logger.WithSandboxID(sandboxID))

		return "", refusedRemoval(raw, sandboxID, expectedExecutionID, holdID)
	}

	return raw, nil
}

// TeamItems retrieves sandboxes for a specific team, filtered by states (all
// states when empty).
//
// The team index is read with SSCAN and the records fetched in pipelines of
// at most sandboxScanBatchSize GETs, which caps each pipeline's work and reply
// size regardless of how many sandboxes the team has.
//
// This is not a consistent snapshot, and neither was reading the whole index
// at once: records removed between the index read and the record fetch are
// skipped, and SSCAN may emit a member twice, which scannedItems drops.
// Callers already sort the result and treat it as best effort.
func (s *Storage) TeamItems(ctx context.Context, teamID uuid.UUID, states []sandboxtypes.State) ([]sandboxtypes.Sandbox, error) {
	items := newScannedItems(states)

	err := s.forEachTeamSandboxBatch(ctx, teamID.String(), func(_ string, batch []sandboxtypes.Sandbox, _ []recordRef) error {
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

// Update modifies a sandbox atomically under its lock. A record of another
// team is not found, as in Get, before the lock is taken.
func (s *Storage) Update(ctx context.Context, teamID uuid.UUID, sandboxID string, updateFunc func(sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error)) (_ sandboxtypes.Sandbox, err error) {
	if err := s.refuseOtherTeam(ctx, teamID.String(), sandboxID); err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	key := recordKey(sandboxID)
	lock, err := s.locker.Obtain(ctx, redis_utils.GetLockKey(key), lockTimeout)
	if err != nil {
		return sandboxtypes.Sandbox{}, fmt.Errorf("failed to obtain lock: %w", err)
	}

	defer releaseLock(ctx, lock.Release, &err)

	_, sbx, err := s.readRecord(ctx, key, teamID, sandboxID)
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}

	if sbx.Held(time.Now()) {
		return sandboxtypes.Sandbox{}, sandboxtypes.ErrLifecycleHeld
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
			// Reuse this already-pipelined SCARD as the team-index-size sample
			// instead of a second full SCARD pass over globalTeamsSet.
			s.metrics.teamIndexSize.Record(ctx, count)
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
		written, err = RunScript(ctx, s.redisClient, "fenced_set", fencedSetScript, []string{lock.Key(), key}, lockValueOf(lock), data).Int64()
		if err == nil {
			if written == 1 {
				return nil
			}

			return sandboxtypes.ErrLockLost
		}
	}

	return err
}
