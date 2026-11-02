package redis

import (
	"context"
	"fmt"
	"slices"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// sandboxScanBatchSize bounds per-command work (SSCAN page, GETs per
// pipeline) so teams with many sandboxes can't produce huge single
// commands/replies that stall Redis or explode service memory.
const sandboxScanBatchSize = 256

// sandboxBatchFunc receives one bounded batch of a team's index: the records
// that decode and are the team's, and the entries whose record is absent in
// both forms or that no key can be built from.
type sandboxBatchFunc func(teamID string, batch []sandboxtypes.Sandbox, absent []recordRef) error

// forEachSandboxBatch walks every team's sandbox index and invokes fn with
// bounded batches of decoded records (all states; undecodable records are
// skipped) and of the index entries whose record is absent in both forms or
// that no key can be built from.
//
// Per-team failures — including errors returned by fn — are isolated: the
// failing team is skipped and the remaining teams are still scanned, and the
// number of teams skipped is returned. Only a failure to list the teams
// themselves is an error, because that yields no data at all rather than
// partial data.
//
// Callers must therefore treat a nil error as "best effort complete", not
// "every sandbox was observed", and must not derive fleet-wide conclusions
// from the absence of a record.
func (s *Storage) forEachSandboxBatch(ctx context.Context, fn sandboxBatchFunc) (skipped int, err error) {
	teams, err := s.redisClient.ZRange(ctx, globalTeamsSet, 0, -1).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to list teams from global index: %w", err)
	}

	for _, teamID := range teams {
		if err := s.forEachTeamSandboxBatch(ctx, teamID, fn); err != nil {
			logger.L().Warn(ctx, "Sandbox scan failed for team, skipping it",
				zap.Error(err),
				logger.WithTeamID(teamID),
			)
			skipped++

			continue
		}
	}

	return skipped, nil
}

// forEachTeamSandboxBatch pages through one team's sandbox index with SSCAN
// and feeds fn bounded batches fetched with pipelined GETs.
func (s *Storage) forEachTeamSandboxBatch(ctx context.Context, teamID string, fn sandboxBatchFunc) error {
	var cursor uint64

	for {
		sandboxIDs, next, err := s.redisClient.SScan(ctx, GetSandboxStorageTeamIndexKey(teamID), cursor, "", sandboxScanBatchSize).Result()
		if err != nil {
			return fmt.Errorf("failed to scan team index: %w", err)
		}

		// SSCAN COUNT is a hint, not a cap: split oversized pages so
		// the downstream pipelines stay bounded.
		for start := 0; start < len(sandboxIDs); start += sandboxScanBatchSize {
			end := min(start+sandboxScanBatchSize, len(sandboxIDs))

			batch, absent, err := s.fetchSandboxBatch(ctx, teamID, sandboxIDs[start:end])
			if err != nil {
				return err
			}
			if len(batch) == 0 && len(absent) == 0 {
				continue
			}

			if err := fn(teamID, batch, absent); err != nil {
				return err
			}
		}

		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

// fetchSandboxBatch reads one bounded batch of sandbox records, in both key
// forms, and returns the records found and the refs of the index entries
// whose record is absent in both forms (deleted already, or never written)
// or that no key can be built from, which the healer removes. Unparseable
// records are in neither.
func (s *Storage) fetchSandboxBatch(ctx context.Context, teamID string, sandboxIDs []string) ([]sandboxtypes.Sandbox, []recordRef, error) {
	if len(sandboxIDs) == 0 {
		return nil, nil, nil
	}

	refs := utils.Map(sandboxIDs, func(id string) recordRef { return recordRef{TeamID: teamID, SandboxID: id} })
	found, absent, err := s.fetchRecords(ctx, refs)
	if err != nil {
		return nil, nil, err
	}

	out := make([]sandboxtypes.Sandbox, 0, len(found))
	for _, ref := range refs {
		if sbx, ok := found[ref]; ok {
			out = append(out, sbx)
		} else if validID(teamID) && !validID(ref.SandboxID) {
			absent = append(absent, ref)
		}
	}

	return out, absent, nil
}

// scannedItems accumulates the sandboxes seen across scan batches, keeping one
// copy of each and only those in the wanted states (all states when empty).
//
// SSCAN guarantees every member present for the whole iteration is returned at
// least once — not at most once. A set stored as a hash table can re-emit a
// member on a later page when the table rehashes mid-iteration, which a busy
// team's index does as sandboxes start and stop. Callers read this result as
// the set of records to act on, so the repeats are dropped once here instead
// of being rediscovered by each of them.
type scannedItems struct {
	states []sandboxtypes.State
	seen   map[recordRef]struct{}
	out    []sandboxtypes.Sandbox
}

func newScannedItems(states []sandboxtypes.State) *scannedItems {
	return &scannedItems{states: states, seen: make(map[recordRef]struct{})}
}

func (r *scannedItems) add(batch []sandboxtypes.Sandbox) {
	for _, sbx := range batch {
		if len(r.states) > 0 && !slices.Contains(r.states, sbx.State) {
			continue
		}

		ref := recordRef{TeamID: sbx.TeamID.String(), SandboxID: sbx.SandboxID}
		if _, ok := r.seen[ref]; ok {
			continue
		}

		r.seen[ref] = struct{}{}
		r.out = append(r.out, sbx)
	}
}

// AllRunningItems returns every running sandbox in the store across all teams,
// scanned in bounded batches, once each. Teams whose scan fails are skipped,
// so the result is best effort: a missing sandbox never means "this sandbox
// does not exist" (see forEachSandboxBatch).
func (s *Storage) AllRunningItems(ctx context.Context) ([]sandboxtypes.Sandbox, error) {
	items := newScannedItems([]sandboxtypes.State{sandboxtypes.StateRunning})

	_, err := s.forEachSandboxBatch(ctx, func(_ string, batch []sandboxtypes.Sandbox, _ []recordRef) error {
		items.add(batch)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return items.out, nil
}
