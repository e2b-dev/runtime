package template

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/constants"
	sqlcdb "github.com/e2b-dev/infra/packages/api/internal/db/client"
	dbtypes "github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/api/internal/team"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/fcversion"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/postgres/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	tracer = otel.Tracer("github.com/e2b-dev/infra/packages/api/internal/template")
	meter  = otel.Meter("github.com/e2b-dev/infra/packages/api/internal/template")

	// maxVcpuCounter counts max CPU resolutions by source and outcome, one per registration.
	maxVcpuCounter = mustCounter("api.template.build.max_vcpu", "Number of template build max CPU resolutions by source and outcome")

	// maxMemoryCounter counts requested memory maximums by outcome, one per registration that names one.
	maxMemoryCounter = mustCounter("api.template.build.max_memory", "Number of template builds requesting a memory maximum, by outcome")
)

func mustCounter(name, description string) metric.Int64Counter {
	counter, err := meter.Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		return nil
	}

	return counter
}

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
	MaxCpuCount        *int32
	MaxMemoryMB        *int32
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
	featureFlags *featureflags.Client,
	data RegisterBuildData,
) (*RegisterBuildResponse, *api.APIError) {
	return telemetry.Observe1(ctx, tracer, "register build", func(ctx context.Context) (*RegisterBuildResponse, *api.APIError) {
		return registerBuild(ctx, templateCache, db, featureFlags, data)
	})
}

func registerBuild(
	ctx context.Context,
	templateCache *templatecache.TemplateCache,
	db *sqlcdb.Client,
	featureFlags *featureflags.Client,
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
		apiError.Retryable = apierrors.NoRetry

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
			Retryable: apierrors.CanRetry,
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
			Err:       fmt.Errorf("team '%s' has reached the maximum number of concurrent template builds (%d)", data.Team.ID, totalConcurrentTemplateBuilds),
			Retryable: apierrors.CanRetry,
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

	if data.MaxCpuCount != nil {
		telemetry.SetAttributes(ctx, attribute.Int("env.max_cpu", int(*data.MaxCpuCount)))
	}

	if data.MaxMemoryMB != nil {
		telemetry.SetAttributes(ctx, attribute.Int("env.max_memory_mb", int(*data.MaxMemoryMB)))
	}

	cpuCount, ramMB, apiError := team.LimitResources(data.Team.Limits, data.CpuCount, data.MemoryMB)
	if apiError != nil {
		apiError.Retryable = apierrors.NoRetry

		return nil, apiError
	}

	maxVcpu, apiError := resolveMaxVcpu(ctx, featureFlags, data.Team.Limits, data.Team.ID, cpuCount, data.MaxCpuCount)
	logger.L().Info(ctx, "template build max cpu resolved",
		logger.WithTeamID(data.Team.ID.String()),
		logger.WithTemplateID(data.TemplateID),
		logger.WithBuildID(buildID.String()),
		zap.Int64("cpu_count", cpuCount),
		zap.Int64("max_cpu_count", utils.DerefOrDefault(maxVcpu.value, 0)),
		zap.String("source", maxVcpu.source),
		zap.String("outcome", maxVcpu.outcome),
		zap.Int("default_max_cpu_flag", maxVcpu.flagValue),
		zap.Int64("team_max_vcpu", data.Team.Limits.MaxVcpu),
	)
	if apiError != nil {
		return nil, apiError
	}

	maxRAMMB, apiError := resolveMaxMemory(ctx, featureFlags, data.Team, data.TemplateID, ramMB, data.MaxMemoryMB)
	if apiError != nil {
		logger.L().Info(ctx, "template build max memory rejected",
			logger.WithTeamID(data.Team.ID.String()),
			logger.WithTemplateID(data.TemplateID),
			logger.WithBuildID(buildID.String()),
			zap.Int64("memory_mb", ramMB),
			zap.Int32p("max_memory_mb", data.MaxMemoryMB),
			zap.Error(apiError.Err),
		)

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
		return nil, &api.APIError{Code: http.StatusConflict, ClientMsg: "The team's cluster changed; retry the template build request", Err: errors.New("team cluster changed during template registration"), Retryable: apierrors.CanRetry}
	}
	if data.ReplacesTemplateID != "" {
		previous, err := client.GetTemplateForAliasRebuild(ctx, queries.GetTemplateForAliasRebuildParams{
			TemplateID: data.ReplacesTemplateID, TeamID: data.Team.ID,
		})
		if err != nil {
			if dberrors.IsNotFoundError(err) {
				return nil, &api.APIError{Code: http.StatusConflict, ClientMsg: "The template changed; retry the template build request", Err: err, Retryable: apierrors.CanRetry}
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
				Retryable: apierrors.NoRetry,
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
	// kernel_version and firecracker_version are seeded here for backwards
	// compatibility; the template-manager reports the versions it actually
	// used in TemplateBuildMetadata and SetFinished overwrites these rows.
	err = client.CreateTemplateBuild(ctx, queries.CreateTemplateBuildParams{
		BuildID:            buildID,
		TemplateID:         data.TemplateID,
		StatusGroup:        dbtypes.BuildStatusGroupPending,
		RamMb:              ramMB,
		Vcpu:               cpuCount,
		MaxVcpu:            maxVcpu.value,
		MaxRamMb:           maxRAMMB,
		StartMemoryMb:      templateStartMemoryMB(ramMB, maxRAMMB),
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
				Retryable: apierrors.NoRetry,
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
				return nil, &api.APIError{Code: http.StatusConflict, ClientMsg: "The template alias changed; retry the template build request", Err: err, Retryable: apierrors.CanRetry}
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
				return nil, &api.APIError{Code: http.StatusConflict, ClientMsg: "The template alias changed; retry the template build request", Err: errors.New("template alias changed during registration"), Retryable: apierrors.CanRetry}
			}
		case aliasDB.EnvID != data.TemplateID:
			err := fmt.Errorf("alias '%s' already used", alias)

			return nil, &api.APIError{
				Err:       err,
				ClientMsg: fmt.Sprintf("Alias '%s' already used", alias),
				Code:      http.StatusForbidden,
				Retryable: apierrors.NoRetry,
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
			// The commit may have reached the database.
			Retryable: apierrors.UndefinedRetry,
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

type maxVcpuResolution struct {
	value *int64
	// source is request, team_default or none.
	source string
	// outcome is accepted, rejected_flag_off, rejected_validation, default_clamped or default_invalid.
	outcome string
	// flagValue is ResizableTemplateDefaultMaxCpuFlag as read, 0 when not read.
	flagValue int
}

// resolveMaxVcpu requires ResizableTemplatesFlag for both explicit and default
// capacity. An explicit value takes precedence; otherwise the team's default is
// clamped to [cpuCount, limits.MaxVcpu]. An invalid default is ignored.
func resolveMaxVcpu(
	ctx context.Context,
	featureFlags *featureflags.Client,
	limits *types.TeamLimits,
	teamID uuid.UUID,
	cpuCount int64,
	requested *int32,
) (resolved maxVcpuResolution, apiErr *api.APIError) {
	defer func() {
		if maxVcpuCounter != nil {
			maxVcpuCounter.Add(ctx, 1, metric.WithAttributes(
				attribute.String("source", resolved.source),
				attribute.String("outcome", resolved.outcome),
			))
		}
	}()

	teamContext := featureflags.TeamContext(teamID.String())
	enabled := featureFlags.BoolFlag(ctx, featureflags.ResizableTemplatesFlag, teamContext)

	if requested != nil {
		if !enabled {
			return maxVcpuResolution{source: "request", outcome: "rejected_flag_off"}, &api.APIError{
				Err:       errors.New("max CPU count requested while resizable templates are off for the team"),
				ClientMsg: "Resizable templates are not enabled for your team",
				Code:      http.StatusBadRequest,
			}
		}

		value, apiErr := team.LimitMaxCPU(limits, cpuCount, *requested)
		if apiErr != nil {
			return maxVcpuResolution{source: "request", outcome: "rejected_validation"}, apiErr
		}

		return maxVcpuResolution{value: &value, source: "request", outcome: "accepted"}, nil
	}

	if !enabled {
		return maxVcpuResolution{source: "none", outcome: "accepted"}, nil
	}

	n := featureFlags.IntFlag(ctx, featureflags.ResizableTemplateDefaultMaxCpuFlag, teamContext)
	if n == 0 {
		return maxVcpuResolution{source: "none", outcome: "accepted", flagValue: n}, nil
	}

	flagValue := int64(n)
	if flagValue < constants.MinTemplateCPU || flagValue > constants.MaxTemplateCPU || (flagValue != 1 && flagValue%2 != 0) {
		return maxVcpuResolution{source: "team_default", outcome: "default_invalid", flagValue: n}, nil
	}

	effective := max(cpuCount, min(flagValue, limits.MaxVcpu))
	// The builder rejects odd counts; cpuCount is 1 or even, so rounding down keeps effective >= cpuCount.
	if effective > 1 && effective%2 != 0 {
		effective--
	}
	// The inherited CPU default can exceed a team's allowance; clamping cannot
	// satisfy both bounds in that case. Ignore an unusable capacity default.
	if _, apiErr := team.LimitMaxCPU(limits, cpuCount, int32(effective)); apiErr != nil {
		return maxVcpuResolution{source: "team_default", outcome: "default_invalid", flagValue: n}, nil
	}
	outcome := "accepted"
	if effective != flagValue {
		outcome = "default_clamped"
	}

	return maxVcpuResolution{value: &effective, source: "team_default", outcome: outcome, flagValue: n}, nil
}

// resolveMaxMemory validates a requested memory maximum. It requires
// ResizableTemplatesFlag and refuses here what the template-manager would fail
// the build for: a size that is not whole slots above boot RAM, a hotplug
// region above template-memory-hotplug-max-ratio times boot RAM (evaluated with
// the template and team contexts; a rule on a context only the template-manager
// carries can still fail the build), or a kernel or Firecracker too old for
// memory hotplug, as the build flags resolve them for the API. A maximum equal
// to boot RAM is no hotplug region and is stored as none.
func resolveMaxMemory(
	ctx context.Context,
	featureFlags *featureflags.Client,
	t *types.Team,
	templateID string,
	ramMB int64,
	requested *int32,
) (value *int64, apiErr *api.APIError) {
	if requested == nil {
		return nil, nil
	}

	outcome := "accepted"
	defer func() {
		if maxMemoryCounter != nil {
			maxMemoryCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
		}
	}()

	teamContext := featureflags.TeamContext(t.ID.String())
	if !featureFlags.BoolFlag(ctx, featureflags.ResizableTemplatesFlag, teamContext) {
		outcome = "rejected_flag_off"

		return nil, &api.APIError{
			Err:       errors.New("max memory requested while resizable templates are off for the team"),
			ClientMsg: "Resizable templates are not enabled for your team",
			Code:      http.StatusBadRequest,
			Retryable: apierrors.NoRetry,
		}
	}

	maxMB, apiErr := team.LimitMaxMemory(t.Limits, ramMB, *requested)
	if apiErr != nil {
		outcome = "rejected_validation"
		apiErr.Retryable = apierrors.NoRetry

		return nil, apiErr
	}

	if maxMB == ramMB {
		outcome = "equal"

		return nil, nil
	}

	ratio := int64(featureFlags.IntFlag(ctx, featureflags.TemplateMemoryHotplugMaxRatioFlag, featureflags.TemplateContext(templateID), teamContext))
	if ratio > 0 && maxMB-ramMB > ratio*ramMB {
		outcome = "rejected_ratio"
		largest := ramMB + ratio*ramMB/constants.MemoryHotplugSlotMiB*constants.MemoryHotplugSlotMiB

		return nil, &api.APIError{
			Err:       fmt.Errorf("hotplug region of %d MiB is above %d times the memory of %d MiB", maxMB-ramMB, ratio, ramMB),
			ClientMsg: fmt.Sprintf("Max memory can be at most %d MiB for a memory of %d MiB", largest, ramMB),
			Code:      http.StatusBadRequest,
			Retryable: apierrors.NoRetry,
		}
	}

	// The versions the template-manager will build with: read, like the ratio, with
	// the template's context, so a per-template rule applies here too.
	templateContext := featureflags.TemplateContext(templateID)
	kernelVersion := featureFlags.StringFlag(ctx, featureflags.BuildKernelVersion, templateContext, teamContext)
	firecrackerVersion := featureFlags.StringFlag(ctx, featureflags.BuildFirecrackerVersion, templateContext, teamContext)
	if kernelTooOldForMemoryHotplug(kernelVersion) {
		outcome = "rejected_kernel"

		return nil, &api.APIError{
			Err:       fmt.Errorf("build kernel %s predates memory hotplug", kernelVersion),
			ClientMsg: "Max memory is not available: the template's kernel does not support memory hotplug",
			Code:      http.StatusBadRequest,
			Retryable: apierrors.NoRetry,
		}
	}
	if info, err := fcversion.New(firecrackerVersion); err == nil && !info.HasMemoryHotplug() {
		outcome = "rejected_firecracker"

		return nil, &api.APIError{
			Err:       fmt.Errorf("build Firecracker %s predates memory hotplug", firecrackerVersion),
			ClientMsg: "Max memory is not available: the template's Firecracker version does not support memory hotplug",
			Code:      http.StatusBadRequest,
			Retryable: apierrors.NoRetry,
		}
	}

	return &maxMB, nil
}

// The oldest guest kernel whose virtio-mem driver works with Firecracker's
// hotplug region on x86, as the template-manager checks it (arm64 needs 5.18;
// the API does not know the build's architecture, so the template-manager
// still refuses 5.16 and 5.17 there).
var (
	minMemoryHotplugKernel = [2]int{5, 16}
	kernelVersionRe        = regexp.MustCompile(`^vmlinux-(\d+)\.(\d+)`)
)

// kernelTooOldForMemoryHotplug reports whether a kernel slot name
// (vmlinux-6.1.158, optionally with a suffix) is older than the minimum. A name
// it cannot read is not refused.
func kernelTooOldForMemoryHotplug(kernelVersion string) bool {
	m := kernelVersionRe.FindStringSubmatch(kernelVersion)
	if m == nil {
		return false
	}
	major, errMajor := strconv.Atoi(m[1])
	minor, errMinor := strconv.Atoi(m[2])
	if errMajor != nil || errMinor != nil {
		return false
	}

	return major < minMemoryHotplugKernel[0] || (major == minMemoryHotplugKernel[0] && minor < minMemoryHotplugKernel[1])
}

// templateStartMemoryMB is the size a template build records for its starts:
// boot RAM, set only on a build with a memory maximum (BuildStartMemoryMB).
func templateStartMemoryMB(ramMB int64, maxRAMMB *int64) *int64 {
	if maxRAMMB == nil {
		return nil
	}

	return &ramMB
}
