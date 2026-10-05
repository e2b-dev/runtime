package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/pause"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// pauseOrchestrator is the slice of *orchestrator.Orchestrator the pause
// handler consumes — an interface so the load-bearing wiring (the fs-only
// gate's refusal landing BEFORE RemoveSandbox commits) is testable without a
// real orchestrator.
type pauseOrchestrator interface {
	GetSandbox(ctx context.Context, teamID uuid.UUID, sandboxID string) (sandbox.Sandbox, error)
	RemoveSandbox(ctx context.Context, teamID uuid.UUID, sandboxID string, opts sandbox.RemoveOpts) error
}

// pauseBackend returns the pause handler's orchestrator slice, overridable in
// tests via pauseBackendOverride.
func (a *APIStore) pauseBackend() pauseOrchestrator {
	if a.pauseBackendOverride != nil {
		return a.pauseBackendOverride
	}

	return a.orchestrator
}

func (a *APIStore) PostSandboxesSandboxIDPause(c *gin.Context, sandboxID api.SandboxID) {
	ctx := c.Request.Context()
	// Get team from context, use TeamContextKey

	teamID := auth.MustGetTeamID(c)

	var err error
	sandboxID, err = utils.ShortID(sandboxID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sandbox ID")

		return
	}

	span := trace.SpanFromContext(ctx)
	span.SetAttributes(telemetry.WithSandboxID(sandboxID))

	traceID := span.SpanContext().TraceID().String()
	c.Set("traceID", traceID)

	// The request body is optional — existing callers send none. Default to a
	// full memory snapshot; memory:false requests a filesystem-only snapshot.
	// ParseOptionalBody tolerates an absent/empty body and parses a present one
	// regardless of Content-Length (chunked requests report -1 even with a body).
	body, bindErr := ginutils.ParseOptionalBody[api.PostSandboxesSandboxIDPauseJSONRequestBody](ctx, c)
	if bindErr != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", bindErr))

		return
	}
	filesystemOnly := body.Memory != nil && !*body.Memory

	// Version-gate filesystem-only snapshots HERE, before the pause chain
	// commits: RemoveSandbox tears down routing and store state regardless of
	// the orchestrator RPC's outcome, so a refusal any later than this would
	// leave a live VM for the orphan reconciler to kill. Refused here, the
	// sandbox keeps running untouched. The check is EXACT — no flag
	// re-resolution — so for records carrying the orchestrator-resolved
	// version it cannot disagree with the orchestrator's own gate;
	backend := a.pauseBackend()

	pause.LogInitiated(ctx, sandboxID, teamID.String(), pause.ReasonRequest, filesystemOnly)

	err = backend.RemoveSandbox(ctx, teamID, sandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause, FilesystemOnly: filesystemOnly})
	var transErr *sandbox.InvalidStateTransitionError

	switch {
	case err == nil:
		pause.LogSuccess(ctx, sandboxID, teamID.String(), pause.ReasonRequest, filesystemOnly)
	case errors.Is(err, orchestrator.ErrSandboxNotFound):
		apiErr := pauseHandleNotRunningSandbox(ctx, a.snapshotCache, sandboxID, teamID)
		switch apiErr.Code {
		case http.StatusConflict:
			pause.LogSkipped(ctx, sandboxID, teamID.String(), pause.ReasonRequest, pause.SkipReasonAlreadyPaused, filesystemOnly)
		case http.StatusNotFound:
			pause.LogSkipped(ctx, sandboxID, teamID.String(), pause.ReasonRequest, pause.SkipReasonNotFound, filesystemOnly)
		default:
			pause.LogFailure(ctx, sandboxID, teamID.String(), pause.ReasonRequest, filesystemOnly, err)
		}
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)

		return
	case errors.As(err, &transErr):
		pause.LogFailure(ctx, sandboxID, teamID.String(), pause.ReasonRequest, filesystemOnly, err)
		a.sendAPIStoreError(c, http.StatusConflict, fmt.Sprintf("Sandbox '%s' cannot be paused while in '%s' state", sandboxID, transErr.CurrentState))

		return
	// Reached only after the API restored the sandbox, so the retry can succeed.
	case errors.Is(err, orchestrator.PauseQueueExhaustedError{}):
		pause.LogSkipped(ctx, sandboxID, teamID.String(), pause.ReasonRequest, pause.SkipReasonAdmissionRefused, filesystemOnly)
		a.sendAPIStoreError(c, http.StatusServiceUnavailable, fmt.Sprintf("Sandbox '%s' cannot be paused right now because its node is busy, please retry", sandboxID))

		return
	// The sandbox is untouched: another replica, or a retry here, can pause it.
	case errors.Is(err, orchestrator.ErrDraining):
		pause.LogSkipped(ctx, sandboxID, teamID.String(), pause.ReasonRequest, pause.SkipReasonDraining, filesystemOnly)
		a.sendAPIStoreError(c, http.StatusServiceUnavailable, fmt.Sprintf("Sandbox '%s' cannot be paused right now because the server is shutting down, please retry", sandboxID))

		return
	default:
		pause.LogFailure(ctx, sandboxID, teamID.String(), pause.ReasonRequest, filesystemOnly, err)
		telemetry.ReportError(ctx, "error pausing sandbox", err)

		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error pausing sandbox")

		return
	}

	c.Status(http.StatusNoContent)
}

func pauseHandleNotRunningSandbox(ctx context.Context, cache *snapshotcache.SnapshotCache, sandboxID string, teamID uuid.UUID) api.APIError {
	// TODO: ENG-3544 scope GetLastSnapshot query by teamID to avoid post-fetch ownership check.
	snap, err := cache.Get(ctx, sandboxID)
	if err == nil {
		if snap.Snapshot.TeamID != teamID {
			logger.L().Debug(ctx, "Snapshot team mismatch on pause", logger.WithSandboxID(sandboxID), logger.WithTeamID(teamID.String()))

			return api.APIError{
				Code:      http.StatusNotFound,
				ClientMsg: utils.SandboxNotFoundMsg(sandboxID),
			}
		}

		logger.L().Warn(ctx, "Sandbox is already paused", logger.WithSandboxID(sandboxID))

		return api.APIError{
			Code:      http.StatusConflict,
			ClientMsg: fmt.Sprintf("Error pausing sandbox - sandbox '%s' is already paused", sandboxID),
		}
	}

	if errors.Is(err, snapshotcache.ErrSnapshotNotFound) {
		logger.L().Debug(ctx, "Snapshot not found", logger.WithSandboxID(sandboxID))

		return api.APIError{
			Code:      http.StatusNotFound,
			ClientMsg: utils.SandboxNotFoundMsg(sandboxID),
		}
	}

	logger.L().Error(ctx, "Error getting snapshot", zap.Error(err), logger.WithSandboxID(sandboxID))

	return api.APIError{
		Code:      http.StatusInternalServerError,
		ClientMsg: "Error pausing sandbox",
	}
}
