package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// connectOrchestrator is the slice of *orchestrator.Orchestrator the connect
// handler consults while a transition is in flight.
type connectOrchestrator interface {
	KeepAliveFor(ctx context.Context, teamID uuid.UUID, sandboxID string, duration time.Duration, allowShorter bool) (*sandbox.Sandbox, *api.APIError)
	WaitForStateChange(ctx context.Context, teamID uuid.UUID, sandboxID string) error
}

func (a *APIStore) connectBackend() connectOrchestrator {
	if a.connectBackendOverride != nil {
		return a.connectBackendOverride
	}

	return a.orchestrator
}

func (a *APIStore) PostSandboxesSandboxIDConnect(c *gin.Context, sandboxID api.SandboxID) {
	ctx := c.Request.Context()

	teamInfo := auth.MustGetTeamInfo(c)

	body, err := ginutils.ParseBody[api.PostSandboxesSandboxIDConnectJSONRequestBody](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		telemetry.ReportCriticalError(ctx, "error when parsing request", err)

		return
	}

	timeout := time.Duration(body.Timeout) * time.Second
	if timeout > time.Duration(teamInfo.Limits.MaxLengthHours)*time.Hour {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Timeout cannot be greater than %d hours", teamInfo.Limits.MaxLengthHours))

		return
	}

	a.connectSandbox(c, sandboxID, timeout, body.Memory)
}

// PostV2SandboxesSandboxIDConnect accepts an optional body; an omitted timeout falls back to SandboxTimeoutDefaultV2.
func (a *APIStore) PostV2SandboxesSandboxIDConnect(c *gin.Context, sandboxID api.SandboxID) {
	ctx := c.Request.Context()

	teamInfo := auth.MustGetTeamInfo(c)

	body, err := ginutils.ParseOptionalBody[api.PostV2SandboxesSandboxIDConnectJSONRequestBody](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		telemetry.ReportCriticalError(ctx, "error when parsing request", err)

		return
	}

	timeout, apiErr := validateAndParseTimeoutWithDefault(body.Timeout, teamInfo.Limits.MaxLengthHours, sandbox.SandboxTimeoutDefaultV2)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)

		return
	}

	a.connectSandbox(c, sandboxID, timeout, body.Memory)
}

// connectSandbox extends a running sandbox's TTL or resumes a paused one from its last snapshot.
func (a *APIStore) connectSandbox(c *gin.Context, sandboxID api.SandboxID, timeout time.Duration, memory *bool) {
	ctx := c.Request.Context()

	teamInfo := auth.MustGetTeamInfo(c)

	span := trace.SpanFromContext(ctx)
	traceID := span.SpanContext().TraceID().String()
	c.Set("traceID", traceID)

	telemetry.ReportEvent(ctx, "Parsed body")

	teamID := teamInfo.Team.ID

	sandboxID, err := utils.ShortID(sandboxID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sandbox ID")

		return
	}

	span.SetAttributes(telemetry.WithSandboxID(sandboxID))

	// It could happen that after sandbox transition, it'll be again transitioning, retry up to maxConnectRetries times.
	const maxConnectRetries = 3

	for attempt := range maxConnectRetries {
		sbx, apiErr := a.connectBackend().KeepAliveFor(ctx, teamID, sandboxID, timeout, false)
		if apiErr == nil {
			c.JSON(http.StatusOK, sbx.ToAPISandbox())

			return
		}

		// Sandbox not in store at all → fall through to snapshot resume.
		if errors.Is(apiErr.Err, sandbox.ErrNotFound) {
			break
		}

		// Sandbox exists but isn't running → check which transitional state.
		var notRunningErr *sandbox.NotRunningError
		if !errors.As(apiErr.Err, &notRunningErr) {
			telemetry.ReportErrorByCode(ctx, apiErr.Code, "error keeping sandbox alive", apiErr.Err,
				telemetry.WithSandboxID(sandboxID),
				telemetry.WithTeamID(teamID.String()),
			)
			a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)

			return
		}

		if notRunningErr.State == sandbox.StateKilling {
			a.sendAPIStoreError(c, http.StatusConflict, utils.SandboxChangingStateMsg(sandboxID, notRunningErr.State))

			return
		}

		logger.L().Info(ctx, "Sandbox not running, waiting for state change",
			logger.WithSandboxID(sandboxID),
			zap.String("state", string(notRunningErr.State)),
			zap.Int("attempt", attempt+1),
		)

		err = a.connectBackend().WaitForStateChange(ctx, teamID, sandboxID)
		if err != nil && !errors.Is(err, sandbox.ErrTransitionRestored) {
			telemetry.ReportCriticalError(ctx, "error waiting for sandbox state change", err,
				telemetry.WithSandboxID(sandboxID),
				telemetry.WithTeamID(teamID.String()),
			)
			a.sendAPIStoreError(c, http.StatusInternalServerError,
				"Error waiting for sandbox state change")

			return
		}

		continue
	}

	// TODO: ENG-3544 scope GetLastSnapshot query by teamID to avoid post-fetch ownership check.
	lastSnapshot, err := a.snapshotCache.Get(ctx, sandboxID)
	if err != nil {
		if errors.Is(err, snapshotcache.ErrSnapshotNotFound) {
			logger.L().Debug(ctx, "Snapshot not found", logger.WithSandboxID(sandboxID))
			a.sendAPIStoreError(c, http.StatusNotFound, utils.SandboxNotFoundMsg(sandboxID))

			return
		}

		telemetry.ReportCriticalError(ctx, "Error getting last snapshot", err, telemetry.WithSandboxID(sandboxID), telemetry.WithTeamID(teamID.String()))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting snapshot")

		return
	}

	if lastSnapshot.Snapshot.TeamID != teamID {
		telemetry.ReportError(ctx, fmt.Sprintf("snapshot for sandbox '%s' doesn't belong to team '%s'", sandboxID, teamID.String()), nil)
		a.sendAPIStoreError(c, http.StatusNotFound, utils.SandboxNotFoundMsg(sandboxID))

		return
	}

	// A paused filesystem-only snapshot resumes by cold-booting (reboot) from its
	// rootfs; the orchestrator selects reboot-vs-memory-resume from the snapshot
	// metadata, so the generic resume path below handles it. In-memory state was
	// already discarded at pause time (memory:false), so the reboot is expected —
	// connect is the intended way to bring such a sandbox back. (Auto-resume,
	// which can be triggered by arbitrary traffic, still refuses it.)

	// Pre-flight of the fetcher's authoritative gate so a disabled flag answers
	// 400 even when the start would otherwise join an in-flight one (409).
	if _, apiErr := resolveFilesystemBoot(ctx, a.featureFlags, memory, lastSnapshot.Snapshot); apiErr != nil {
		setMemoryOverrideOutcome(c, memory, apiErr)
		apierrors.SendAPIError(c, apiErr)

		return
	}

	sbxlogger.E(&sbxlogger.SandboxMetadata{
		SandboxID:  sandboxID,
		TemplateID: lastSnapshot.Snapshot.EnvID,
		TeamID:     teamID.String(),
	}).Debug(ctx, "Started resuming sandbox")

	sbx, createErr := a.startSandbox(
		ctx,
		sandboxID,
		timeout,
		teamInfo,
		a.buildResumeSandboxData(sandboxID, nil, memory),
		&c.Request.Header,
		true,
		demandsFilesystemBoot(memory, lastSnapshot.Snapshot),
		nil, // mcp
	)
	setMemoryOverrideOutcome(c, memory, createErr)
	if createErr != nil {
		apierrors.SendAPIError(c, createErr)

		return
	}

	c.JSON(http.StatusCreated, &sbx)
}
