package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	apiorch "github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// PostSnapshotsSnapshotIDFork creates count new sandboxes from a named
// snapshot template. No running sandbox is required: the snapshot must exist
// as a persistent snapshot template owned by the team. Each fork boots
// independently from the same snapshot and succeeds or fails independently.
func (a *APIStore) PostSnapshotsSnapshotIDFork(c *gin.Context, rawSnapshotID api.SnapshotID) {
	ctx := c.Request.Context()

	teamInfo := auth.MustGetTeamInfo(c)
	teamID := teamInfo.Team.ID

	// snapshotID may arrive URL-encoded when the alias contains a slash.
	snapshotID, err := url.PathUnescape(rawSnapshotID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid snapshot ID")
		return
	}

	span := trace.SpanFromContext(ctx)
	traceID := span.SpanContext().TraceID().String()
	c.Set("traceID", traceID)

	body, err := ginutils.ParseOptionalBody[api.PostSnapshotsSnapshotIDForkJSONRequestBody](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))
		return
	}

	forkTimeout := sandbox.SandboxTimeoutDefault
	if body.Timeout != nil {
		forkTimeout = time.Duration(*body.Timeout) * time.Second
		if forkTimeout > time.Duration(teamInfo.Limits.MaxLengthHours)*time.Hour {
			a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Timeout cannot be greater than %d hours", teamInfo.Limits.MaxLengthHours))
			return
		}
	}

	forkCount := 1
	if body.Count != nil {
		forkCount = int(*body.Count)
	}

	if forkCount < 1 {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Count must be at least 1")
		return
	}

	if forkCount > maxForkCount {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Count cannot be greater than %d", maxForkCount))
		return
	}

	if int64(forkCount) > teamInfo.Limits.SandboxConcurrency {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Count must not exceed the maximum number of concurrent sandboxes (%d)", teamInfo.Limits.SandboxConcurrency))
		return
	}

	// Resolve snapshotID (may be templateID:tag or namespace/alias:tag).
	identifier, tag, err := id.ParseName(snapshotID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Invalid snapshot ID: %s", err))
		return
	}

	if err := id.ValidateNamespaceMatchesTeam(identifier, teamInfo.Slug); err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, err.Error())
		return
	}

	aliasInfo, resolveErr := a.templateCache.ResolveAlias(ctx, identifier, teamInfo.Slug)
	if resolveErr != nil {
		apiErr := templatecache.ErrorToAPIError(resolveErr, identifier)
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		return
	}

	if aliasInfo.TeamID != teamID {
		a.sendAPIStoreError(c, http.StatusNotFound, fmt.Sprintf("Snapshot '%s' not found", snapshotID))
		return
	}

	templateID := aliasInfo.TemplateID
	tagStr := id.DefaultTag
	if tag != nil {
		tagStr = *tag
	}

	// Look up the source sandboxID for this snapshot template.
	snapTemplates, dbErr := a.sqlcDB.ListTeamSnapshotTemplates(ctx, queries.ListTeamSnapshotTemplatesParams{
		TeamID:     teamID,
		EnvID:      &templateID,
		Tag:        &tagStr,
		CursorTime: time.Now().Add(24 * time.Hour),
		CursorID:   maxSnapshotTemplateID,
		PageLimit:  1,
	})
	if dbErr != nil {
		telemetry.ReportCriticalError(ctx, "error looking up snapshot template", dbErr)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error looking up snapshot")
		return
	}

	if len(snapTemplates) == 0 {
		a.sendAPIStoreError(c, http.StatusNotFound, fmt.Sprintf("Snapshot '%s' not found", snapshotID))
		return
	}

	// Fetch build data via templateCache. This path uses env_build_assignments
	// for the persistent template env — it does not depend on the ephemeral
	// snapshot env (source='snapshot') which is soft-deleted when the original
	// sandbox is killed.
	clusterID := clusters.WithClusterFallback(teamInfo.Team.ClusterID)
	env, build, cacheErr := a.templateCache.Get(ctx, templateID, &tagStr, teamID, clusterID)
	if cacheErr != nil {
		visible := aliasInfo.TeamID == teamID
		ref := templatecache.TemplateRef{Identifier: aliasInfo.MatchedIdentifier, Visible: visible}
		apiErr := ref.APIError(cacheErr)
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		return
	}

	if vErr := sharedUtils.CheckEnvdVersionForSnapshot(sharedUtils.DerefOrDefault(build.EnvdVersion, "")); vErr != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, vErr.Error())
		return
	}

	sourceSandboxID := snapTemplates[0].SandboxID
	alias := ""
	if len(env.Names) > 0 {
		alias = env.Names[0]
	}

	sbxlogger.E(&sbxlogger.SandboxMetadata{
		TemplateID: templateID,
		TeamID:     teamID.String(),
	}).Debug(ctx, fmt.Sprintf("Forking %d sandbox(es) from snapshot %s (source sandbox %s)", forkCount, snapshotID, sourceSandboxID))

	// Boot all forks in parallel from the same immutable snapshot checkpoint.
	results := make([]api.SandboxForkResult, forkCount)

	wg := errgroup.Group{}
	for i := range forkCount {
		wg.Go(func() error {
			forkedSandboxID := InstanceIDPrefix + id.Generate()

			getSandboxData := func(_ context.Context) (apiorch.SandboxMetadata, *api.APIError) {
				return apiorch.SandboxMetadata{
					Build:          *build,
					TemplateID:     env.TemplateID,
					BaseTemplateID: env.TemplateID,
					Alias:          alias,
					// SnapshotSandboxID is set so that placement remapping on
					// resume-timeout uses the right sandbox ID.
					SnapshotSandboxID: sourceSandboxID,
				}, nil
			}

			forkedSbx, createErr := a.startSandbox(
				ctx,
				forkedSandboxID,
				forkTimeout,
				teamInfo,
				getSandboxData,
				&c.Request.Header,
				true,  // isResume: restore checkpoint
				false, // demandFilesystemBoot: snapshot forks resume the stored memory image
				nil,   // mcp
			)
			if createErr != nil {
				telemetry.ReportError(ctx, "error creating sandbox from snapshot fork", createErr.Err, telemetry.WithSandboxID(forkedSandboxID))
				results[i] = api.SandboxForkResult{Error: &api.Error{Code: int32(createErr.Code), Message: createErr.ClientMsg}}
				//nolint:nilerr
				return nil
			}

			results[i] = api.SandboxForkResult{Sandbox: forkedSbx}
			return nil
		})
	}
	_ = wg.Wait()

	c.JSON(http.StatusCreated, results)
}
