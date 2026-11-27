package templategc

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// PrefixDeleter physically removes every object under a build's storage prefix.
// It is the one side effect of the sweep and is injected so the engine stays
// pure and testable. The production implementation wraps
// storage.DeleteObjectsWithPrefix.
type PrefixDeleter interface {
	DeletePrefix(ctx context.Context, buildID uuid.UUID) error
}

// SweepResult is the outcome of a sweep. The counts reconcile:
//
//	len(approved) == len(Deleted) + len(SkippedChanged) + len(Failed)   (confirmed run)
//
// so "approved but not reclaimed" is always derivable.
type SweepResult struct {
	// Deleted are build prefixes physically removed (or that WOULD be removed in
	// a dry run — see Confirmed).
	Deleted []BuildPrefix
	// SkippedChanged are builds that were in the approved manifest but are no
	// longer fresh deletion candidates: since the manifest was written they
	// became reachable from a live root, or gained a too-new object (possible
	// in-flight upload). This is the TOCTOU guard; these are intentionally kept.
	SkippedChanged []uuid.UUID
	// Failed are builds whose deletion was attempted but errored; their prefix
	// may be partially deleted and is a candidate for the next sweep.
	Failed []SweepFailure
	// ReclaimedBytes is the aggregate size of Deleted prefixes.
	ReclaimedBytes int64
	// Confirmed is false for a dry run (nothing was actually deleted).
	Confirmed bool
}

// SweepFailure records one prefix whose deletion errored.
type SweepFailure struct {
	BuildID uuid.UUID
	Err     string
}

// Sweep deletes the intersection of the operator-approved build IDs and a FRESH
// mark result, re-validating every approved build against ground truth at
// delete time. A build is deleted only if it is STILL a fresh candidate:
//   - not reachable from any live root's header closure (fresh.Keep), and
//   - old enough (not in fresh.TooNew).
//
// Anything approved that is no longer a fresh candidate is SkippedChanged, never
// deleted — this closes the window between the mark run that produced the
// manifest and the sweep that consumes it. Because the fresh Manifest is
// produced by Mark (which fails closed on any unreadable live root), the sweep
// inherits that fail-closed guarantee: if reachability could not be proven,
// fresh.Candidates is empty and nothing is deleted.
//
// When confirm is false the sweep is a dry run: it computes exactly what it
// would delete and returns it with Confirmed=false, calling the deleter zero
// times. A per-build delete error never aborts the others (best-effort).
func Sweep(ctx context.Context, deleter PrefixDeleter, approved []uuid.UUID, fresh *Manifest, confirm bool) (*SweepResult, error) {
	if fresh == nil {
		return nil, fmt.Errorf("sweep: fresh mark manifest is required")
	}

	// Index the fresh candidates by build ID; only these are safe to delete now.
	freshCandidate := make(map[uuid.UUID]BuildPrefix, len(fresh.Candidates))
	for _, c := range fresh.Candidates {
		freshCandidate[c.BuildID] = c
	}

	res := &SweepResult{Confirmed: confirm}

	for _, id := range approved {
		bp, ok := freshCandidate[id]
		if !ok {
			// Approved by the manifest but no longer a fresh candidate: it became
			// reachable or too-new since the mark. Keep it.
			res.SkippedChanged = append(res.SkippedChanged, id)

			continue
		}

		if confirm {
			if err := deleter.DeletePrefix(ctx, id); err != nil {
				res.Failed = append(res.Failed, SweepFailure{BuildID: id, Err: err.Error()})

				continue
			}
		}

		res.Deleted = append(res.Deleted, bp)
		res.ReclaimedBytes += bp.TotalBytes
	}

	return res, nil
}
