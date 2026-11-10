package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

var (
	errMaxInstanceLengthExceeded = errors.New("max instance length exceeded")
	errNodeNotInPool             = errors.New("sandbox node is not in the pool")
)

func (o *Orchestrator) KeepAliveFor(ctx context.Context, teamID uuid.UUID, sandboxID string, duration time.Duration, allowShorter bool) (*sandbox.Sandbox, *api.APIError) {
	now := time.Now()

	updateFunc := func(sbx sandbox.Sandbox) (sandbox.Sandbox, error) {
		if sbx.State != sandbox.StateRunning {
			return sbx, &sandbox.NotRunningError{SandboxID: sandboxID, State: sbx.State}
		}

		// Calculate the maximum TTL that can be set without exceeding the max instance length
		ttl := getMaxAllowedTTL(now, sbx.StartTime, duration, sbx.MaxInstanceLength)
		endTime := now.Add(ttl)

		if time.Since(sbx.StartTime) > sbx.MaxInstanceLength {
			return sbx, errMaxInstanceLengthExceeded
		}

		if !allowShorter && endTime.Before(sbx.EndTime) {
			// If shorter than the current end time, we don't extend, so we can return
			return sbx, nil
		}

		// The store is the expiry clock: refuse to advance it for a node this replica
		// cannot reach. Shortening stays allowed. A map lookup only, as the record
		// lock is held here.
		if endTime.After(sbx.EndTime) && o.GetNode(sbx.ClusterID, sbx.NodeID) == nil {
			return sbx, errNodeNotInPool
		}

		logger.L().Debug(ctx, "sandbox ttl updated", logger.WithSandboxID(sbx.SandboxID), logger.Time("end_time", endTime))
		sbx.EndTime = endTime

		return sbx, nil
	}

	var sbxNotRunningErr *sandbox.NotRunningError
	sbx, err := o.sandboxStore.Update(ctx, teamID, sandboxID, updateFunc)
	if errors.Is(err, errNodeNotInPool) {
		sbx, err = o.retryKeepAliveAfterDiscovery(ctx, teamID, sandboxID, updateFunc)
	}
	if err != nil {
		switch {
		case errors.As(err, &sbxNotRunningErr):
			return nil, &api.APIError{Code: http.StatusConflict, ClientMsg: utils.SandboxChangingStateMsg(sandboxID, sbxNotRunningErr.State), Err: err}
		case errors.Is(err, sandbox.ErrLifecycleHeld):
			return nil, &api.APIError{Code: http.StatusConflict, ClientMsg: utils.SandboxLifecycleHeldMsg(sandboxID), Err: err}
		case errors.Is(err, sandbox.ErrNotFound):
			return nil, &api.APIError{Code: http.StatusNotFound, ClientMsg: utils.SandboxNotFoundMsg(sandboxID), Err: err}
		case errors.Is(err, errMaxInstanceLengthExceeded):
			return nil, &api.APIError{Code: http.StatusBadRequest, ClientMsg: "Max instance length exceeded", Err: err}
		default:
			return nil, &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error when setting sandbox timeout", Err: err}
		}
	}
	err = o.UpdateSandbox(ctx, sbx.TeamID.String(), sandboxID, sbx.EndTime, sbx.ClusterID, sbx.NodeID)
	if err != nil {
		if errors.Is(err, ErrSandboxNotFound) {
			return nil, &api.APIError{Code: http.StatusNotFound, ClientMsg: utils.SandboxNotFoundMsg(sandboxID), Err: err}
		}

		return nil, &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error when setting sandbox timeout", Err: err}
	}

	return &sbx, nil
}

// retryKeepAliveAfterDiscovery runs on-demand discovery outside the record lock
// and retries the update once if the sandbox's node joined the pool.
func (o *Orchestrator) retryKeepAliveAfterDiscovery(ctx context.Context, teamID uuid.UUID, sandboxID string, updateFunc func(sandbox.Sandbox) (sandbox.Sandbox, error)) (sandbox.Sandbox, error) {
	rec, err := o.sandboxStore.Get(ctx, teamID, sandboxID)
	if err != nil {
		return sandbox.Sandbox{}, err
	}

	if o.getOrConnectNode(ctx, rec.ClusterID, rec.NodeID) == nil {
		logger.L().Warn(ctx, "Refusing to extend sandbox timeout, node is not in the pool",
			logger.WithSandboxID(sandboxID),
			logger.WithNodeID(rec.NodeID),
			logger.WithClusterID(rec.ClusterID),
		)

		return sandbox.Sandbox{}, errNodeNotInPool
	}

	return o.sandboxStore.Update(ctx, teamID, sandboxID, updateFunc)
}

// getMaxAllowedTTL calculates the maximum allowed TTL for a sandbox without exceeding its max instance length.
func getMaxAllowedTTL(now time.Time, startTime time.Time, duration, maxInstanceLength time.Duration) time.Duration {
	timeLeft := maxInstanceLength - now.Sub(startTime)
	if timeLeft <= 0 {
		return 0
	}

	return min(timeLeft, duration)
}
