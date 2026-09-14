package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func (a *APIStore) PostSandboxesSandboxIDSnapshots(c *gin.Context, sandboxID api.SandboxID) {
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
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sandbox ID")

		return
	}

	span.SetAttributes(telemetry.WithSandboxID(sandboxID))

	body, err := ginutils.ParseBody[api.PostSandboxesSandboxIDSnapshotsJSONRequestBody](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		return
	}

	// Build opts from the optional name
	opts := orchestrator.SnapshotTemplateOpts{
		Tag: id.DefaultTag,
	}

	if body.Name != nil {
		identifier, tag, err := id.ParseName(*body.Name)
		if err != nil {
			a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Invalid name: %s", err))

			return
		}

		if err := id.ValidateNamespaceMatchesTeam(identifier, teamInfo.Slug); err != nil {
			a.sendAPIStoreError(c, http.StatusBadRequest, err.Error())

			return
		}

		alias := id.ExtractAlias(identifier)

		if tag != nil {
			opts.Tag = *tag
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
			apiErr := templatecache.ErrorToAPIError(err, identifier)
			a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
			telemetry.ReportCriticalError(ctx, "error resolving snapshot template alias", apiErr.Err)

			return
		}

		opts.Alias = &alias
		opts.Namespace = &teamInfo.Slug
	}

	sbx, err := a.orchestrator.GetSandbox(ctx, teamID, sandboxID)
	if err != nil {
		if errors.Is(err, sandbox.ErrNotFound) {
			a.sendAPIStoreError(c, http.StatusNotFound, utils.SandboxNotFoundMsg(sandboxID))
		} else {
			telemetry.ReportError(ctx, "error getting sandbox for snapshot", err)
			a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get sandbox")
		}

		return
	}

	if err := sharedUtils.CheckEnvdVersionForSnapshot(sbx.EnvdVersion); err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, err.Error())

		return
	}

	telemetry.ReportEvent(ctx, "Creating snapshot template")

	result, err := a.orchestrator.CreateSnapshotTemplate(ctx, teamID, sandboxID, opts)
	if err != nil {
		if errors.Is(err, sandbox.ErrNotFound) {
			logger.L().Debug(ctx, "Sandbox not found for snapshot", logger.WithSandboxID(sandboxID))
			a.sendAPIStoreError(c, http.StatusNotFound, utils.SandboxNotFoundMsg(sandboxID))

			return
		}

		if transErr, ok := errors.AsType[*sandbox.InvalidStateTransitionError](err); ok {
			a.sendAPIStoreError(c, http.StatusConflict, fmt.Sprintf("Sandbox '%s' cannot be snapshotted while in '%s' state", sandboxID, transErr.CurrentState))

			return
		}

		if errors.Is(err, orchestrator.PauseQueueExhaustedError{}) {
			a.sendAPIStoreError(c, http.StatusServiceUnavailable, fmt.Sprintf("Sandbox '%s' cannot be snapshotted right now because its node is busy, please retry", sandboxID))

			return
		}

		telemetry.ReportCriticalError(ctx, "Error creating snapshot template", err, telemetry.WithSandboxID(sandboxID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error creating snapshot template")

		return
	}

	// Invalidate cached tombstone if a new alias was created
	if opts.Alias != nil && opts.Namespace != nil && opts.ExistingTemplateID == nil {
		a.templateCache.InvalidateAlias(context.WithoutCancel(ctx), opts.Namespace, *opts.Alias)
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
}
