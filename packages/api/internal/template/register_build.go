package template

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/team"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/api/internal/template")

type RegisterBuildData struct {
	ReplacesTemplateID string
	Public             bool
	ClusterID          uuid.UUID
	TemplateID         api.TemplateID
	UserID             *uuid.UUID
	Team               *types.Team
	Dockerfile         string
	Alias              *string
	Tags               []string
	StartCmd           *string
	ReadyCmd           *string
	CpuCount           *int32
	MemoryMB           *int32
	Version            string

	// The requested minimum free space after build steps, or nil for the team's default.
	// Zero disables requested growth and reaches the row as zero.
	MinFreeDiskMb *int32

	// TODO(ENG-3852): Remove once the template manager resolves the kernel and firecracker versions itself.
	//
	// Deprecated: Template manager should use its own.
	KernelVersion string
	// Deprecated: Template manager should use its own.
	FirecrackerVersion string
}

type RegisterBuildResponse struct {
	Public     bool
	TemplateID string
	BuildID    string
	Aliases    []string
	Names      []string
	Tags       []string
}

func RegisterBuild(
	ctx context.Context,
	templateCache *templatecache.TemplateCache,
	db *sqlcdb.Client,
	data RegisterBuildData,
) (*RegisterBuildResponse, *api.APIError) {
	return telemetry.Observe1(ctx, tracer, "register build", func(ctx context.Context) (*RegisterBuildResponse, *api.APIError) {
		return registerBuild(ctx, templateCache, db, data)
	})
}

func registerBuild(
	ctx context.Context,
	templateCache *templatecache.TemplateCache,
	db *sqlcdb.Client,
	data RegisterBuildData,
) (*RegisterBuildResponse, *api.APIError) {
	// On the span before the free-disk resolution below can return, so a refused
	// registration still says whose request was refused.
	telemetry.SetAttributes(ctx,
		telemetry.WithTeamID(data.Team.ID.String()),
		telemetry.WithTemplateID(data.TemplateID),
	)

	// Ahead of the concurrency check and the transaction, so a target this team
	// may not request is refused as the client's error either way, and the value
	// that reaches the row is the allowance as it stood at registration.
	freeDiskSizeMB, apiError := team.LimitFreeDiskSize(data.Team.Limits, data.MinFreeDiskMb)

	freeDiskAttrs := []attribute.KeyValue{
		attribute.Int64("build.free_disk.default_mb", data.Team.Limits.DefaultFreeDiskSizeMb),
		attribute.Int64("build.free_disk.max_mb", data.Team.Limits.MaxFreeDiskSizeMb),
	}
	if data.MinFreeDiskMb != nil {
		freeDiskAttrs = append(freeDiskAttrs,
			attribute.Int64("build.free_disk.requested_mb", int64(*data.MinFreeDiskMb)))
	}
	telemetry.SetAttributes(ctx, freeDiskAttrs...)

	if apiError != nil {
		telemetry.ReportErrorByCode(ctx, apiError.Code, "free disk space request refused", apiError.Err,
			telemetry.WithTeamID(data.Team.ID.String()))

		return nil, apiError
	}

	telemetry.SetAttributes(ctx, attribute.Int64("build.free_disk.mb", freeDiskSizeMB))

	// Add default tag if no tags are present
	tags := data.Tags
	if len(tags) == 0 {
		tags = []string{id.DefaultTag}
	}

	// This is a simple implementation of concurrency limit
	// It does not guarantee that the limit is not exceeded, but it should be good enough for now (considering overall low number of total builds)
	otherBuildCount, err := db.GetInProgressTemplateBuildsByTeam(ctx, queries.GetInProgressTemplateBuildsByTeamParams{
		TeamID:            data.Team.ID,
		ExcludeTemplateID: data.TemplateID,
		ExcludeTags:       tags,
	})
	if err != nil {
		return nil, &api.APIError{
			Err:       err,
			ClientMsg: fmt.Sprintf("Error when checking concurrent builds: %s", err),
			Code:      http.StatusInternalServerError,
		}
	}

	totalConcurrentTemplateBuilds := data.Team.Limits.BuildConcurrency
	if otherBuildCount >= totalConcurrentTemplateBuilds {
		telemetry.ReportError(ctx, "team has reached max concurrent template builds", nil, telemetry.WithTeamID(data.Team.ID.String()), attribute.Int64("total.concurrent_template_builds", totalConcurrentTemplateBuilds))

		return nil, &api.APIError{
			Code: http.StatusTooManyRequests,
			ClientMsg: fmt.Sprintf(
				"you have reached the maximum number of concurrent template builds (%d). Please wait for existing builds to complete or contact support if you need more concurrent builds.",
				totalConcurrentTemplateBuilds),
			Err: fmt.Errorf("team '%s' has reached the maximum number of concurrent template builds (%d)", data.Team.ID, totalConcurrentTemplateBuilds),
		}
	}

	// Generate a build id for the new build
	buildID, err := uuid.NewRandom()
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error when generating build id", err)

		return nil, &api.APIError{
			Err:       err,
			ClientMsg: "Failed to generate build id",
			Code:      http.StatusInternalServerError,
		}
	}

	telemetry.SetAttributes(ctx,
		attribute.String("env.team.id", data.Team.ID.String()),
		attribute.String("env.team.name", data.Team.Name),
		attribute.String("env.team.tier", data.Team.Tier),
		telemetry.WithBuildID(buildID.String()),
		attribute.String("env.dockerfile", data.Dockerfile),
		attribute.String("env.version", data.Version),
	)

	if data.Alias != nil {
		telemetry.SetAttributes(ctx, attribute.String("env.alias", *data.Alias))
	}
	if len(tags) > 0 {
		telemetry.SetAttributes(ctx, attribute.StringSlice("env.tags", tags))
	}
	if data.StartCmd != nil {
		telemetry.SetAttributes(ctx, attribute.String("env.start_cmd", *data.StartCmd))
	}

	if data.ReadyCmd != nil {
		telemetry.SetAttributes(ctx, attribute.String("env.ready_cmd", *data.ReadyCmd))
	}

	if data.CpuCount != nil {
		telemetry.SetAttributes(ctx, attribute.Int("env.cpu", int(*data.CpuCount)))
	}

	if data.MemoryMB != nil {
		telemetry.SetAttributes(ctx, attribute.Int("env.memory_mb", int(*data.MemoryMB)))
	}

	cpuCount, ramMB, apiError := team.LimitResources(data.Team.Limits, data.CpuCount, data.MemoryMB)
	if apiError != nil {
		return nil, apiError
	}

	// Start a transaction to prevent partial updates
	client, tx, err := db.WithTx(ctx)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error when starting transaction", err)

		return nil, &api.APIError{
			Err:       err,
			ClientMsg: fmt.Sprintf("Error when starting transaction: %s", err),
			Code:      http.StatusInternalServerError,
		}
	}
	defer tx.Rollback(ctx)

	currentCluster, err := client.GetTeamClusterForTemplateBuild(ctx, data.Team.ID)
	if err != nil {
		return nil, &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error when checking the team's cluster", Err: err}
	}
	if clusters.WithClusterFallback(currentCluster) != data.ClusterID {
		return nil, &api.APIError{Code: http.StatusConflict, ClientMsg: "The team's cluster changed; retry the template build request", Err: errors.New("team cluster changed during template registration")}
	}
	if data.ReplacesTemplateID != "" {
		previous, err := client.GetTemplateForAliasRebuild(ctx, queries.GetTemplateForAliasRebuildParams{
			TemplateID: data.ReplacesTemplateID, TeamID: data.Team.ID,
		})
		if err != nil {
			if dberrors.IsNotFoundError(err) {
				return nil, &api.APIError{Code: http.StatusConflict, ClientMsg: "The template changed; retry the template build request", Err: err}
			}

			return nil, &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error when checking the previous template", Err: err}
		}
		data.Public = previous.Public
	}

	var clusterID *uuid.UUID
	if data.ClusterID != consts.LocalClusterID {
		clusterID = &data.ClusterID
	}

	// Create the template / or update the build count. A soft-deleted template
	// is not reactivated: the query returns no row, so the rebuild fails.
	_, err = client.CreateOrUpdateTemplate(ctx, queries.CreateOrUpdateTemplateParams{
		TemplateID: data.TemplateID,
		TeamID:     data.Team.ID,
		CreatedBy:  data.UserID,
		ClusterID:  clusterID,
		Public:     data.Public,
	})
	if err != nil {
		if dberrors.IsNotFoundError(err) {
			return nil, &api.APIError{
				Err:       err,
				ClientMsg: fmt.Sprintf("Template '%s' has been deleted and cannot be rebuilt", data.TemplateID),
				Code:      http.StatusNotFound,
			}
		}

		telemetry.ReportCriticalError(ctx, "error when updating env", err)

		return nil, &api.APIError{
			Err:       err,
			ClientMsg: fmt.Sprintf("Error when updating template: %s", err),
			Code:      http.StatusInternalServerError,
		}
	}
	telemetry.ReportEvent(ctx, "created or update template")

	// Mark the previous not started builds as failed and remove their active-build rows
	err = client.InvalidateUnstartedTemplateBuilds(ctx, queries.InvalidateUnstartedTemplateBuildsParams{
		Reason: dbtypes.BuildReason{
			Message: "The build was canceled because it was superseded by a newer one.",
		},
		TemplateID: data.TemplateID,
		Tags:       tags,
	})
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error when invalidating unstarted builds", err, attribute.StringSlice("tags", tags))

		return nil, &api.APIError{
			Err:       err,
			ClientMsg: fmt.Sprintf("Error when updating template: %s", err),
			Code:      http.StatusInternalServerError,
		}
	}
	telemetry.ReportEvent(ctx, "marked previous builds as failed")

	// Insert the new build
	// TODO(ENG-3469): Switch to dbtypes.BuildStatusPending once all consumers are migrated.
	// kernel_version and firecracker_version are seeded here for backwards
	// compatibility; the template-manager reports the versions it actually
	// used in TemplateBuildMetadata and SetFinished overwrites these rows.
	err = client.CreateTemplateBuild(ctx, queries.CreateTemplateBuildParams{
		BuildID:            buildID,
		Status:             dbtypes.BuildStatusWaiting,
		RamMb:              ramMB,
		Vcpu:               cpuCount,
		KernelVersion:      data.KernelVersion,
		FirecrackerVersion: data.FirecrackerVersion,
		FreeDiskSizeMb:     freeDiskSizeMB,
		StartCmd:           data.StartCmd,
		ReadyCmd:           data.ReadyCmd,
		Dockerfile:         new(data.Dockerfile),
		Version:            new(data.Version),
	})
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error when inserting build", err)

		return nil, &api.APIError{
			Err:       err,
			ClientMsg: fmt.Sprintf("Error when inserting build: %s", err),
			Code:      http.StatusInternalServerError,
		}
	}
	telemetry.ReportEvent(ctx, "inserted new build")

	// Check if the alias is available and claim it
	var aliases, names []string
	if data.Alias != nil {
		// Extract just the alias portion (without namespace) for storage
		// The identifier may be "namespace/alias" or just "alias"
		alias := id.ExtractAlias(*data.Alias)
		aliases = append(aliases, alias)
		names = append(names, id.WithNamespace(data.Team.Slug, alias))

		exists, err := client.CheckAliasConflictsWithTemplateID(ctx, alias)
		if err != nil {
			telemetry.ReportCriticalError(ctx, "error when checking alias", err, attribute.String("alias", alias))

			return nil, &api.APIError{
				Err:       err,
				ClientMsg: fmt.Sprintf("Error when querying alias '%s': %s", alias, err),
				Code:      http.StatusInternalServerError,
			}
		}
		telemetry.ReportEvent(ctx, "checked alias availability")

		if exists {
			err := fmt.Errorf("alias '%s' is already used", alias)

			return nil, &api.APIError{
				Err:       err,
				ClientMsg: fmt.Sprintf("Alias '%s' is already used", alias),
				Code:      http.StatusConflict,
			}
		}

		aliasDB, err := client.CheckAliasExistsInNamespace(ctx, queries.CheckAliasExistsInNamespaceParams{
			Alias:     alias,
			Namespace: &data.Team.Slug,
		})
		switch {
		case err != nil:
			if !dberrors.IsNotFoundError(err) {
				telemetry.ReportCriticalError(ctx, "error when checking alias", err, attribute.String("alias", alias))

				return nil, &api.APIError{
					Err:       err,
					ClientMsg: fmt.Sprintf("Error when querying for alias: %s", err),
					Code:      http.StatusInternalServerError,
				}
			}
			if data.ReplacesTemplateID != "" {
				return nil, &api.APIError{Code: http.StatusConflict, ClientMsg: "The template alias changed; retry the template build request", Err: err}
			}

			aliasKeys, err := client.DeleteOtherTemplateAliases(ctx, data.TemplateID)
			if err != nil {
				telemetry.ReportCriticalError(ctx, "error when deleting template alias", err, attribute.String("alias", alias))

				return nil, &api.APIError{
					Err:       err,
					ClientMsg: fmt.Sprintf("Error when deleting template alias: %s", err),
					Code:      http.StatusInternalServerError,
				}
			}

			templateCache.InvalidateAliasesByTemplateID(context.WithoutCancel(ctx), data.TemplateID, aliasKeys)
			for _, key := range aliasKeys {
				telemetry.ReportEvent(ctx, "deleted old alias", attribute.String("env.alias", key))
			}

			err = client.
				CreateTemplateAlias(ctx, queries.CreateTemplateAliasParams{
					Alias:      alias,
					TemplateID: data.TemplateID,
					Namespace:  &data.Team.Slug,
				})
			if err != nil {
				telemetry.ReportCriticalError(ctx, "error when inserting alias", err, attribute.String("alias", alias))

				return nil, &api.APIError{
					Err:       err,
					ClientMsg: fmt.Sprintf("Error when inserting alias '%s': %s", alias, err),
					Code:      http.StatusInternalServerError,
				}
			}

			// Invalidate any cached tombstone for this alias
			templateCache.InvalidateAlias(context.WithoutCancel(ctx), &data.Team.Slug, alias)

			telemetry.ReportEvent(ctx, "created new alias", attribute.String("env.alias", alias))
		case data.ReplacesTemplateID != "":
			changed, err := client.ReplaceTemplateAlias(ctx, queries.ReplaceTemplateAliasParams{
				Alias: alias, Namespace: &data.Team.Slug, TemplateID: data.TemplateID, PreviousTemplateID: data.ReplacesTemplateID,
			})
			if err != nil {
				return nil, &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error when updating the template alias", Err: err}
			}
			if changed != 1 {
				return nil, &api.APIError{Code: http.StatusConflict, ClientMsg: "The template alias changed; retry the template build request", Err: errors.New("template alias changed during registration")}
			}
		case aliasDB.EnvID != data.TemplateID:
			err := fmt.Errorf("alias '%s' already used", alias)

			return nil, &api.APIError{
				Err:       err,
				ClientMsg: fmt.Sprintf("Alias '%s' already used", alias),
				Code:      http.StatusForbidden,
			}
		}

		telemetry.ReportEvent(ctx, "inserted alias", attribute.String("env.alias", alias))
	}

	for _, tag := range tags {
		// Env is active in this tx (CreateOrUpdateTemplate succeeded above), so
		// the active_envs guard always matches here; rows is ignored.
		_, err = client.CreateTemplateBuildAssignment(ctx, queries.CreateTemplateBuildAssignmentParams{
			TemplateID: data.TemplateID,
			BuildID:    buildID,
			Tag:        tag,
		})
		if err != nil {
			telemetry.ReportCriticalError(ctx, "error when adding tag to build", err, attribute.String("tag", tag))

			return nil, &api.APIError{
				Err:       err,
				ClientMsg: fmt.Sprintf("Error when adding tag '%s' to build: %s", tag, err),
				Code:      http.StatusInternalServerError,
			}
		}
	}

	err = client.CreateActiveTemplateBuild(ctx, queries.CreateActiveTemplateBuildParams{
		BuildID:    buildID,
		TeamID:     data.Team.ID,
		TemplateID: data.TemplateID,
		Tags:       tags,
	})
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error when inserting active build", err)

		return nil, &api.APIError{
			Err:       err,
			ClientMsg: fmt.Sprintf("Error when updating active builds: %s", err),
			Code:      http.StatusInternalServerError,
		}
	}

	// Commit the transaction
	err = tx.Commit(ctx)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error when committing transaction", err)

		return nil, &api.APIError{
			Err:       err,
			ClientMsg: fmt.Sprintf("Error when committing transaction: %s", err),
			Code:      http.StatusInternalServerError,
		}
	}
	telemetry.ReportEvent(ctx, "committed transaction")
	if data.ReplacesTemplateID != "" && data.Alias != nil {
		templateCache.InvalidateAllTags(context.WithoutCancel(ctx), data.ReplacesTemplateID)
		templateCache.InvalidateAlias(context.WithoutCancel(ctx), &data.Team.Slug, id.ExtractAlias(*data.Alias))
	}

	telemetry.SetAttributes(ctx,
		attribute.Int64("build.cpu_count", cpuCount),
		attribute.Int64("build.ram_mb", ramMB),
	)
	telemetry.ReportEvent(ctx, "started updating environment")

	logger.L().Info(ctx, "template build requested", logger.WithTemplateID(data.TemplateID), logger.WithBuildID(buildID.String()))

	return &RegisterBuildResponse{
		Public:     data.Public,
		TemplateID: data.TemplateID,
		BuildID:    buildID.String(),
		Aliases:    aliases,
		Names:      names,
		Tags:       tags,
	}, nil
}
