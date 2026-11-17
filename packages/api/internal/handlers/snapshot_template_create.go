package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/middleware/idempotency"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// snapshotOrchestrator is the slice of *orchestrator.Orchestrator the snapshot
// handler consumes. An interface so the load-bearing wiring, both memory:false
// refusals landing before CreateSnapshotTemplate moves the sandbox out of
// Running, is testable without a real orchestrator.
type snapshotOrchestrator interface {
	GetSandbox(ctx context.Context, teamID uuid.UUID, sandboxID string) (sandbox.Sandbox, error)
	EnsureSnapshotSupport(ctx context.Context, sbx sandbox.Sandbox, opts orchestrator.SnapshotTemplateOpts) error
	CreateSnapshotTemplate(ctx context.Context, teamID uuid.UUID, sandboxID string, opts orchestrator.SnapshotTemplateOpts) (orchestrator.SnapshotTemplateResult, error)
}

// snapshotBackend returns the snapshot handler's orchestrator slice,
// overridable in tests via snapshotBackendOverride.
func (a *APIStore) snapshotBackend() snapshotOrchestrator {
	if a.snapshotBackendOverride != nil {
		return a.snapshotBackendOverride
	}

	return a.orchestrator
}

func (a *APIStore) PostSandboxesSandboxIDSnapshots(c *gin.Context, sandboxID api.SandboxID, _ api.PostSandboxesSandboxIDSnapshotsParams) {
	ctx := c.Request.Context()

	teamInfo := auth.MustGetTeamInfo(c)

	teamID := teamInfo.Team.ID

	span := trace.SpanFromContext(ctx)
	traceID := span.SpanContext().TraceID().String()
	c.Set("traceID", traceID)

	telemetry.SetAttributes(ctx,
		telemetry.WithTeamID(teamID.String()),
		telemetry.WithSandboxID(sandboxID),
	)

	var err error
	sandboxID, err = utils.ShortID(sandboxID)
	if err != nil {
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ClientMsg: "Invalid sandbox ID", Retryable: apierrors.NoRetry})

		return
	}

	span.SetAttributes(telemetry.WithSandboxID(sandboxID))

	var body api.PostSandboxesSandboxIDSnapshotsJSONRequestBody
	// ShouldBind, unlike Bind, leaves the response uncommitted so Should-Retry can still be set.
	if err := c.ShouldBind(&body); err != nil {
		telemetry.ReportCriticalError(ctx, "error when parsing request", err)
		apierrors.SendAPIError(c, &apierrors.APIError{
			Code:      http.StatusBadRequest,
			ClientMsg: fmt.Sprintf("Error when parsing request: error when parsing request: %s", err),
			Retryable: apierrors.NoRetry,
		})

		return
	}

	body.Memory = new(sharedUtils.DerefOrDefault(body.Memory, true))
	idempotency.Execute(c, body, func() *api.APIError {
		return a.createSnapshotTemplate(c, sandboxID, body)
	})
}

// createSnapshotTemplate writes the created snapshot or returns the error to send.
func (a *APIStore) createSnapshotTemplate(c *gin.Context, sandboxID api.SandboxID, body api.SandboxSnapshotRequest) *api.APIError {
	ctx := c.Request.Context()
	teamInfo := auth.MustGetTeamInfo(c)
	teamID := teamInfo.Team.ID
	span := trace.SpanFromContext(ctx)

	// Build opts from the optional name; memory defaults to a full snapshot.
	opts := orchestrator.SnapshotTemplateOpts{
		Tag:            id.DefaultTag,
		FilesystemOnly: body.Memory != nil && !*body.Memory,
	}

	// The requested kind, recorded before any refusal: the orchestrator's
	// checkpoint counter sees only requests that reach a node, so this is the
	// one place that counts filesystem-only asks against their 400s and 409s.
	span.SetAttributes(attribute.Bool("snapshot.memory", !opts.FilesystemOnly))

	if body.Name != nil {
		identifier, tag, err := id.ParseName(*body.Name)
		if err != nil {
			return &api.APIError{Code: http.StatusBadRequest, ClientMsg: fmt.Sprintf("Invalid name: %s", err), Retryable: apierrors.NoRetry}
		}

		if err := id.ValidateNamespaceMatchesTeam(identifier, teamInfo.Slug); err != nil {
			return &api.APIError{Code: http.StatusBadRequest, ClientMsg: err.Error(), Retryable: apierrors.NoRetry}
		}

		alias := id.ExtractAlias(identifier)

		if tag != nil {
			tags, err := id.ValidateAndDeduplicateTags([]string{*tag})
			if err != nil {
				return &api.APIError{Code: http.StatusBadRequest, ClientMsg: fmt.Sprintf("Invalid tag: %s", err), Retryable: apierrors.NoRetry}
			}

			opts.Tag = tags[0]
		}

		// Resolve alias using the cache — same pattern as template builds
		aliasInfo, err := a.templateCache.ResolveAlias(ctx, identifier, teamInfo.Slug)
		switch {
		case err == nil && aliasInfo.TeamID == teamID:
			// Alias exists and is owned by this team — reuse the template
			opts.ExistingTemplateID = &aliasInfo.TemplateID
		case err == nil || errors.Is(err, templatecache.ErrTemplateNotFound):
			// Not found, or owned by a different team — will create a new template with this alias
		default:
			// ErrorToAPIError classifies it: CanRetry for a lookup failure, NoRetry otherwise.
			apiErr := templatecache.ErrorToAPIError(err, identifier)
			telemetry.ReportCriticalError(ctx, "error resolving snapshot template alias", apiErr.Err)

			return &api.APIError{Code: apiErr.Code, ClientMsg: apiErr.ClientMsg, Retryable: apiErr.Retryable}
		}

		opts.Alias = &alias
		opts.Namespace = &teamInfo.Slug
	}

	backend := a.snapshotBackend()

	sbx, err := backend.GetSandbox(ctx, teamID, sandboxID)
	if err != nil {
		if errors.Is(err, sandbox.ErrNotFound) {
			return &api.APIError{Code: http.StatusNotFound, ClientMsg: utils.SandboxNotFoundMsg(sandboxID), Retryable: apierrors.NoRetry}
		}
		telemetry.ReportError(ctx, "error getting sandbox for snapshot", err)

		return &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "Failed to get sandbox", Retryable: apierrors.CanRetry}
	}

	if err := sharedUtils.CheckEnvdVersionForSnapshot(sbx.EnvdVersion); err != nil {
		return &api.APIError{Code: http.StatusBadRequest, ClientMsg: err.Error(), Retryable: apierrors.NoRetry}
	}

	// Both memory:false refusals run here, after the sandbox is known to exist
	// and belong to the team, and before it leaves Running: the flag, as the
	// resume path does for its memory:false, then the node's orchestrator release.
	if apiErr := resolveFilesystemOnlySnapshot(ctx, a.featureFlags, body.Memory, teamID.String(), sandboxID); apiErr != nil {
		return apiErr
	}

	if err := backend.EnsureSnapshotSupport(ctx, sbx, opts); err != nil {
		if featErr, ok := errors.AsType[orchestrator.NodeFeatureMissingError](err); ok {
			// The node and the two versions stay out of the client message
			// but must reach the logs: a run of these 409s during a rollout is
			// a list of nodes still behind, or of binaries built without a
			// version string, which report the source default and fail the
			// floor the same way.
			logger.L().Warn(ctx, "Filesystem-only snapshot refused: node below the orchestrator version floor",
				logger.WithSandboxID(sandboxID),
				logger.WithNodeID(featErr.NodeID),
				zap.String("node_version", featErr.NodeVersion),
				zap.String("min_version", featErr.MinVersion),
				zap.String("feature", featErr.Feature),
			)
			span.SetAttributes(
				attribute.String("snapshot.unsupported_node_id", featErr.NodeID),
				attribute.String("snapshot.unsupported_node_version", featErr.NodeVersion),
			)

			return &api.APIError{
				Code:      http.StatusConflict,
				ErrorCode: errCodeFilesystemOnlySnapshotUnsupportedNode,
				ClientMsg: "This sandbox's node cannot make snapshots without memory yet; take a full snapshot, or pause and resume the sandbox and retry",
				Err:       featErr,
				Retryable: apierrors.CanRetry,
			}
		}

		telemetry.ReportError(ctx, "error resolving sandbox node for snapshot", err)

		return &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "Failed to get sandbox", Retryable: apierrors.CanRetry}
	}

	telemetry.ReportEvent(ctx, "Creating snapshot template")

	result, err := backend.CreateSnapshotTemplate(ctx, teamID, sandboxID, opts)
	// The template and its alias are persisted before the node is asked, so
	// the cached miss is stale on a refusal too: a retry must find them.
	if opts.Alias != nil && opts.Namespace != nil && opts.ExistingTemplateID == nil {
		a.templateCache.InvalidateAlias(context.WithoutCancel(ctx), opts.Namespace, *opts.Alias)
	}
	if err != nil {
		if errors.Is(err, sandbox.ErrNotFound) {
			logger.L().Debug(ctx, "Sandbox not found for snapshot", logger.WithSandboxID(sandboxID))

			return &api.APIError{Code: http.StatusNotFound, ClientMsg: utils.SandboxNotFoundMsg(sandboxID), Retryable: apierrors.NoRetry}
		}

		if transErr, ok := errors.AsType[*sandbox.InvalidStateTransitionError](err); ok {
			// Refused before the snapshot writes anything.
			return &api.APIError{Code: http.StatusConflict, ClientMsg: fmt.Sprintf("Sandbox '%s' cannot be snapshotted while in '%s' state", sandboxID, transErr.CurrentState), Retryable: apierrors.CanRetry}
		}

		if errors.Is(err, sandbox.ErrLifecycleHeld) || errors.Is(err, sandbox.ErrLockLost) {
			// Only StartRemoving returns these here, before the snapshot writes anything.
			return &api.APIError{Code: http.StatusConflict, ClientMsg: utils.SandboxLifecycleHeldMsg(sandboxID), Retryable: apierrors.CanRetry}
		}

		if errors.Is(err, orchestrator.FilesystemOnlyDisabledError{}) {
			return filesystemOnlySnapshotDisabledError(sandboxID)
		}

		// The node refusals below follow committed snapshot build and template
		// rows, so they stay replay-only under an idempotency key, while a plain
		// retry without one can succeed. Should-Retry cannot say both, so they
		// stay UndefinedRetry and send no hint.
		// The sandbox is back in Running; the checkpoint that refused this
		// one ends on its own, so a plain retry succeeds.
		if errors.Is(err, orchestrator.PauseRefusedCheckpointInProgressError{}) {
			return &api.APIError{Code: http.StatusServiceUnavailable, ClientMsg: fmt.Sprintf("Sandbox '%s' cannot be snapshotted right now because another snapshot of it is in progress, please retry", sandboxID), Retryable: apierrors.UndefinedRetry}
		}

		// The node left the VM running, so a retry can succeed once the
		// guest agent answers again.
		if errors.Is(err, orchestrator.PauseRefusedEnvdUnresponsiveError{}) {
			return &api.APIError{Code: http.StatusServiceUnavailable, ClientMsg: fmt.Sprintf("Sandbox '%s' cannot be snapshotted right now because the sandbox guest agent is not responding, please retry", sandboxID), Retryable: apierrors.UndefinedRetry}
		}

		// The sandbox is back to Running; the same request will never succeed.
		if errors.Is(err, orchestrator.PauseUnsupportedError{}) {
			return &api.APIError{Code: http.StatusConflict, ClientMsg: fmt.Sprintf("Sandbox '%s' cannot be snapshotted filesystem-only: its guest does not support it, snapshot it with memory instead", sandboxID), Retryable: apierrors.NoRetry}
		}

		if errors.Is(err, orchestrator.PauseQueueExhaustedError{}) {
			return &api.APIError{Code: http.StatusServiceUnavailable, ClientMsg: fmt.Sprintf("Sandbox '%s' cannot be snapshotted right now because its node is busy, please retry", sandboxID), Retryable: apierrors.UndefinedRetry}
		}

		// The sandbox is untouched: another replica, or a retry here, can snapshot it.
		if errors.Is(err, orchestrator.ErrDraining) {
			return &api.APIError{Code: http.StatusServiceUnavailable, ClientMsg: fmt.Sprintf("Sandbox '%s' cannot be snapshotted right now, please retry", sandboxID), Retryable: apierrors.CanRetry}
		}

		telemetry.ReportCriticalError(ctx, "Error creating snapshot template", err, telemetry.WithSandboxID(sandboxID))

		// The checkpoint may have run, so a retry could take a second snapshot.
		return &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error creating snapshot template", Retryable: apierrors.NoRetry}
	}

	a.templateCache.Invalidate(context.WithoutCancel(ctx), result.TemplateID, &opts.Tag)

	// Use namespace/alias when a name was provided, otherwise fall back to the raw template ID
	snapshotID := id.WithTag(result.TemplateID, opts.Tag)
	names := make([]string, 0)
	if opts.Alias != nil && opts.Namespace != nil {
		name := id.WithNamespace(*opts.Namespace, *opts.Alias)
		snapshotID = id.WithTag(name, opts.Tag)
		names = append(names, name)
	}

	c.JSON(http.StatusCreated, api.SnapshotInfo{
		SnapshotID: snapshotID,
		Names:      names,
	})

	return nil
}

const (
	errCodeFilesystemOnlySnapshotDisabled        = "snapshot_filesystem_only_disabled"
	errCodeFilesystemOnlySnapshotUnsupportedNode = "snapshot_filesystem_only_unsupported_node"
)

// resolveFilesystemOnlySnapshot rejects memory:false while the
// filesystem-only checkpoint is disabled for the team. A full snapshot (nil or
// true) consults nothing. The request is never downgraded to a memory
// snapshot: the caller asked for a template that cold-boots.
func resolveFilesystemOnlySnapshot(ctx context.Context, flags featureFlagsClient, memory *bool, teamID, sandboxID string) *api.APIError {
	if memory == nil || *memory {
		return nil
	}

	if !flags.BoolFlag(ctx, featureflags.FilesystemOnlyCheckpointFlag,
		featureflags.TeamContext(teamID),
		featureflags.SandboxContext(sandboxID),
	) {
		return filesystemOnlySnapshotDisabledError(sandboxID)
	}

	return nil
}

func filesystemOnlySnapshotDisabledError(sandboxID string) *api.APIError {
	return &api.APIError{
		Code:      http.StatusBadRequest,
		ErrorCode: errCodeFilesystemOnlySnapshotDisabled,
		ClientMsg: "Snapshots without memory (memory: false) are not enabled for this team; a full snapshot still works",
		Err:       fmt.Errorf("filesystem-only snapshot of sandbox '%s' rejected: feature disabled", sandboxID),
		Retryable: apierrors.NoRetry,
	}
}
