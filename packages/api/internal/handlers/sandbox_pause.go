package handlers

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/pause"
	"github.com/e2b-dev/infra/packages/api/internal/pauselimit"
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
	// By default the node takes a memory snapshot over a guest agent that does
	// not answer and records it; the caller can ask to be refused instead.
	requireEnvdHealthy := body.RequireHealthyGuest != nil && *body.RequireHealthyGuest
	setPauseMode(c, filesystemOnly, requireEnvdHealthy)

	// Version-gate filesystem-only snapshots HERE, before the pause chain
	// commits: RemoveSandbox tears down routing and store state regardless of
	// the orchestrator RPC's outcome, so a refusal any later than this would
	// leave a live VM for the orphan reconciler to kill. Refused here, the
	// sandbox keeps running untouched. The check is EXACT — no flag
	// re-resolution — so for records carrying the orchestrator-resolved
	// version it cannot disagree with the orchestrator's own gate;
	backend := a.pauseBackend()

	pause.LogInitiated(ctx, sandboxID, teamID.String(), pause.ReasonRequest, filesystemOnly)

	err = a.pauseWithinRateLimit(ctx, backend, teamID, sandboxID, sandbox.RemoveOpts{
		Action:             sandbox.StateActionPause,
		FilesystemOnly:     filesystemOnly,
		RequireEnvdHealthy: requireEnvdHealthy,
	})
	var transErr *sandbox.InvalidStateTransitionError
	var limitErr pauseRateLimitedError

	switch {
	case err == nil:
		pause.LogSuccess(ctx, sandboxID, teamID.String(), pause.ReasonRequest, filesystemOnly)
	case errors.As(err, &limitErr):
		pause.LogSkipped(ctx, sandboxID, teamID.String(), pause.ReasonRequest, pause.SkipReasonRateLimited, filesystemOnly)
		retryAfterSecs := max(int(math.Ceil(limitErr.retryAfter.Seconds())), 1)
		c.Header("Retry-After", strconv.Itoa(retryAfterSecs))
		a.sendAPIStoreError(c, http.StatusTooManyRequests, fmt.Sprintf("Sandbox '%s' reached its pause rate limit, please retry in %d seconds", sandboxID, retryAfterSecs))

		return
	case errors.Is(err, orchestrator.ErrExecutionChanged):
		pause.LogSkipped(ctx, sandboxID, teamID.String(), pause.ReasonRequest, pause.SkipReasonStateChanged, filesystemOnly)
		a.sendAPIStoreError(c, http.StatusConflict, fmt.Sprintf("Sandbox '%s' was resumed while it was being paused, please retry", sandboxID))

		return
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
	// Reached for a filesystem-only pause, one that asked for a healthy
	// guest, or a guest that powered off while the probe waited, after the
	// API restored the sandbox; the node left the VM running, or the disk
	// held, so a retry can succeed once the guest agent answers again or
	// as the disk-only capture of the held guest.
	case errors.Is(err, orchestrator.PauseRefusedEnvdUnresponsiveError{}):
		pause.LogSkipped(ctx, sandboxID, teamID.String(), pause.ReasonRequest, pause.SkipReasonEnvdUnresponsive, filesystemOnly)
		a.sendAPIStoreError(c, http.StatusServiceUnavailable, fmt.Sprintf("Sandbox '%s' cannot be paused right now because the sandbox guest agent is not responding, please retry", sandboxID))

		return
	// Reached only after the API restored the sandbox; the checkpoint that
	// refused the pause ends on its own, so a plain retry succeeds.
	case errors.Is(err, orchestrator.PauseRefusedCheckpointInProgressError{}):
		pause.LogSkipped(ctx, sandboxID, teamID.String(), pause.ReasonRequest, pause.SkipReasonCheckpointInProgress, filesystemOnly)
		a.sendAPIStoreError(c, http.StatusServiceUnavailable, fmt.Sprintf("Sandbox '%s' cannot be paused right now because it is being snapshotted, please retry", sandboxID))

		return
	// The API restored the sandbox; the same request will never succeed, a
	// memory pause will. Not 409: the SDKs read every 409 here as already paused.
	case errors.Is(err, orchestrator.PauseUnsupportedError{}):
		pause.LogSkipped(ctx, sandboxID, teamID.String(), pause.ReasonRequest, pause.SkipReasonUnsupported, filesystemOnly)
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Sandbox '%s' cannot be paused filesystem-only: its guest does not support it, pause it with memory instead", sandboxID))

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

// setPauseMode labels the request metric with what the pause asked for,
// before the pause runs so every answer carries it: a filesystem-only
// snapshot, a memory snapshot that requires a healthy guest (strict), or the
// default memory snapshot that proceeds past an unresponsive one.
func setPauseMode(c *gin.Context, filesystemOnly, requireEnvdHealthy bool) {
	mode := "default"
	switch {
	case filesystemOnly:
		mode = "filesystem_only"
	case requireEnvdHealthy:
		mode = "strict"
	}
	c.Set(metricPauseMode, mode)
}

// pauseAttempts bounds the re-checks after a resume replaces the execution a
// pause was pinned to. Each one needs a whole resume to land between the
// lookup and the pause.
const pauseAttempts = 2

// noExecutionID pins a pause to a record that was missing at lookup. No record
// carries it: a record still missing answers as an unpinned pause would, and
// one a resume added since fails the pin.
var noExecutionID = uuid.Nil.String()

type pauseRateLimitedError struct {
	retryAfter time.Duration
}

func (e pauseRateLimitedError) Error() string {
	return fmt.Sprintf("pause rate limit reached, retry in %s", e.retryAfter)
}

// pauseWithinRateLimit checks the rate limit before RemoveSandbox commits the
// pause, so a refusal leaves the sandbox running. The pause is pinned to the
// record the limiter looked at: a request cannot count against, or join, one
// execution and then pause the next one a resume created. A changed execution
// goes through the limiter again. opts carries what the pause asks of the
// node; its pin is set here.
func (a *APIStore) pauseWithinRateLimit(ctx context.Context, backend pauseOrchestrator, teamID uuid.UUID, sandboxID string, opts sandbox.RemoveOpts) error {
	var err error
	for range pauseAttempts {
		check := a.checkPauseRateLimit(ctx, backend, teamID, sandboxID)
		if check.limited {
			return pauseRateLimitedError{retryAfter: check.retryAfter}
		}

		opts.ExpectExecutionID = check.executionID
		err = backend.RemoveSandbox(ctx, teamID, sandboxID, opts)
		if !errors.Is(err, orchestrator.ErrExecutionChanged) {
			return err
		}
	}

	return err
}

type pauseLimitCheck struct {
	// executionID pins the pause; empty when the limiter did not read the record.
	executionID string
	limited     bool
	retryAfter  time.Duration
}

// checkPauseRateLimit counts only a record a new pause can start from: a
// missing record, or one already pausing, is left to RemoveSandbox's own
// answer. Lookup and Redis errors let the pause through.
func (a *APIStore) checkPauseRateLimit(ctx context.Context, backend pauseOrchestrator, teamID uuid.UUID, sandboxID string) pauseLimitCheck {
	if a.pauseLimiter == nil {
		return pauseLimitCheck{}
	}

	limit, ok := a.pauseLimiter.Limit(ctx)
	if !ok {
		return pauseLimitCheck{}
	}

	sbx, err := backend.GetSandbox(ctx, teamID, sandboxID)
	if errors.Is(err, sandbox.ErrNotFound) {
		return pauseLimitCheck{executionID: noExecutionID}
	}
	if err != nil {
		logger.L().Warn(ctx, "Skipping pause rate limit, sandbox lookup failed", zap.Error(err), logger.WithSandboxID(sandboxID))

		return pauseLimitCheck{}
	}

	check := pauseLimitCheck{executionID: sbx.ExecutionID}
	if sbx.State != sandbox.StateRunning && sbx.State != sandbox.StateSnapshotting {
		return check
	}

	result, err := a.pauseLimiter.Allow(ctx, limit, teamID.String(), sandboxID, sbx.ExecutionID)
	if err != nil {
		logger.L().Warn(ctx, "Skipping pause rate limit, limiter unavailable", zap.Error(err), logger.WithSandboxID(sandboxID))

		return check
	}
	check.limited = result.Decision == pauselimit.DecisionLimited
	check.retryAfter = result.RetryAfter

	return check
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
