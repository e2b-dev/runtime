package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
)

// SetPausedTTL replaces the sandbox's TTL; nil removes it. A running sandbox
// keeps it on its record until the run ends, and its snapshot's bound moves
// to match. A paused one gets it in its snapshot, which restarts its expiry
// clock from now.
func (o *Orchestrator) SetPausedTTL(ctx context.Context, teamID uuid.UUID, sandboxID string, ttl *time.Duration) *api.APIError {
	err := o.setRunningPausedTTL(ctx, teamID, sandboxID, ttl)
	if !errors.Is(err, sandbox.ErrNotFound) {
		return pausedTTLError(sandboxID, err)
	}

	rows, err := o.sqlcDB.StampSnapshotExpiry(ctx, queries.StampSnapshotExpiryParams{
		SandboxID:        sandboxID,
		TeamID:           teamID,
		PausedTtlSeconds: PausedTTLSeconds(ttl),
	})
	if err != nil {
		return &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error when setting the sandbox's paused retention", Err: fmt.Errorf("stamp the paused TTL of %q: %w", sandboxID, err)}
	}
	if rows == 0 {
		return &api.APIError{Code: http.StatusNotFound, ClientMsg: utils.SandboxNotFoundMsg(sandboxID), Err: sandbox.ErrNotFound}
	}
	o.snapshotCache.Invalidate(ctx, sandboxID)

	// A resume that read the snapshot before the write runs with the old TTL
	// and would stamp it back when its run ends.
	if err := o.sandboxStore.WaitForStart(ctx, teamID, sandboxID); err != nil {
		return &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error when setting the sandbox's paused retention", Err: fmt.Errorf("wait for a start of %q: %w", sandboxID, err)}
	}
	err = o.setRunningPausedTTL(ctx, teamID, sandboxID, ttl)
	if errors.Is(err, sandbox.ErrNotFound) {
		return nil
	}

	return pausedTTLError(sandboxID, err)
}

// setRunningPausedTTL refuses a sandbox that is leaving running: its pause or
// kill has already taken the TTL it stamps. The bound its resume wrote is
// moved to the new TTL, as a run end whose stamp fails leaves the sandbox to
// expire at the bound. The bound is written under the record's lock, so a
// pause cannot stamp its expiry first and then have it overwritten.
func (o *Orchestrator) setRunningPausedTTL(ctx context.Context, teamID uuid.UUID, sandboxID string, ttl *time.Duration) error {
	_, err := o.sandboxStore.Update(ctx, teamID, sandboxID, func(sbx sandbox.Sandbox) (sandbox.Sandbox, error) {
		if sbx.State != sandbox.StateRunning {
			return sbx, &sandbox.NotRunningError{SandboxID: sandboxID, State: sbx.State}
		}
		sbx.PausedTTL = ttl
		// A TTL set now reaches the snapshot through the bound, which the
		// run's end must then clear even if the TTL is removed again.
		sbx.PausedTTLStored = sbx.PausedTTLStored || ttl != nil

		// A sandbox that was never paused has no snapshot to bound yet; its
		// first pause stores the TTL from the record.
		if err := o.boundPausedExpiry(ctx, sbx); err != nil && !errors.Is(err, errPausedSnapshotDeleted) {
			return sbx, fmt.Errorf("bound the paused expiry: %w", err)
		}

		return sbx, nil
	})

	return err
}

func pausedTTLError(sandboxID string, err error) *api.APIError {
	var notRunningErr *sandbox.NotRunningError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &notRunningErr):
		return &api.APIError{Code: http.StatusConflict, ClientMsg: utils.SandboxChangingStateMsg(sandboxID, notRunningErr.State), Err: err}
	case errors.Is(err, sandbox.ErrLifecycleHeld):
		return &api.APIError{Code: http.StatusConflict, ClientMsg: utils.SandboxLifecycleHeldMsg(sandboxID), Err: err}
	case errors.Is(err, sandbox.ErrNotFound):
		return &api.APIError{Code: http.StatusNotFound, ClientMsg: utils.SandboxNotFoundMsg(sandboxID), Err: err}
	default:
		return &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error when setting the sandbox's paused retention", Err: fmt.Errorf("set the paused TTL of %q: %w", sandboxID, err)}
	}
}
