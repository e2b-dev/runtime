//go:build linux

package server

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/builderrors"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildlogger"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/oci/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/fcversion"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/templates"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	meter = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/server")

	// buildCmdlineArgs is the engagement signal for the per-team cmdline flag: it counts
	// builds by the parameters they actually applied, so a flag that is set in the
	// feature-flag service but inert in the build shows up as the empty label rather
	// than as silence.
	buildCmdlineArgs = utils.Must(telemetry.GetCounter(meter, telemetry.TemplateBuildCmdlineArgs))
)

func (s *ServerStore) TemplateCreate(ctx context.Context, templateRequest *templatemanager.TemplateCreateRequest) (*emptypb.Empty, error) {
	done := s.info.TrackWork()
	defer done()

	ctx, childSpan := tracer.Start(ctx, "template-create")
	defer childSpan.End()

	cfg := templateRequest.GetTemplate()

	if cfg.GetFromImage() == "" && cfg.GetFromTemplate() == nil {
		return nil, status.Error(codes.InvalidArgument, "template build requires either fromImage or fromTemplate")
	}

	metadata := storage.Paths{
		BuildID: cfg.GetBuildID(),
	}

	// default to scope by template ID
	cacheScope := cfg.GetTemplateID()
	if templateRequest.CacheScope != nil {
		cacheScope = templateRequest.GetCacheScope()
	}

	// Create the auth provider using the factory
	authProvider := auth.NewAuthProvider(cfg.GetFromImageRegistry())

	// TODO: Remove, temporary handling when version is not sent from the API
	version := templateRequest.GetVersion()
	if version == "" {
		version = templates.TemplateV2BetaVersion
	}

	ctx = featureflags.AddToContext(
		ctx,
		featureflags.TemplateContext(cfg.GetTemplateID()),
		featureflags.TeamContext(cfg.GetTeamID()),
	)

	kernelVersion := s.featureFlags.StringFlag(ctx, featureflags.BuildKernelVersion)
	firecrackerVersion := s.featureFlags.StringFlag(ctx, featureflags.BuildFirecrackerVersion)

	// Read once here, at the only place the per-team flag is read, so every boot in this
	// build agrees and what gets recorded is what was actually applied.
	cmdlineArgs, cmdlineErr := fc.ParseCmdlineArgs(s.featureFlags.StringFlag(ctx, featureflags.BuildKernelCmdlineArgs))
	if cmdlineErr == nil {
		cmdlineErr = fc.ValidateCmdlineArgs(cmdlineArgs)
	}
	if cmdlineErr != nil {
		// Fall back to the default command line rather than fail the build: a flag set
		// ahead of a deploy, or left set behind a rollback, should not stop a team
		// shipping templates.
		//
		// s.logger, not s.buildLogger: buildLogger feeds the caller-visible build log
		// stream, and a malformed internal flag is an operator concern the caller never
		// asked about. This is also the only record of what was rejected — the span
		// attribute and the counter both report what was APPLIED.
		s.logger.Warn(ctx, "rejected guest kernel cmdline args, using the default",
			zap.Error(cmdlineErr),
			logger.WithTemplateID(cfg.GetTemplateID()),
			logger.WithBuildID(cfg.GetBuildID()),
		)

		cmdlineArgs = nil
	}

	fcInfo, err := fcversion.New(firecrackerVersion)
	if err != nil {
		return nil, fmt.Errorf("invalid resolved firecracker version %q: %w", firecrackerVersion, err)
	}
	hugePages := fcInfo.HasHugePages()
	freePageReporting := fcInfo.HasFreePageReporting() && s.featureFlags.BoolFlag(ctx, featureflags.FreePageReportingFlag)
	freePageHinting := fcInfo.HasFreePageHinting() && featureflags.IsFreePageHintingEnabled(ctx, s.featureFlags)

	childSpan.SetAttributes(
		telemetry.WithTemplateID(cfg.GetTemplateID()),
		telemetry.WithBuildID(cfg.GetBuildID()),
		telemetry.WithKernelVersion(kernelVersion),
		telemetry.WithFirecrackerVersion(firecrackerVersion),
		attribute.String("env.start_cmd", cfg.GetStartCommand()),
		attribute.Int64("env.memory_mb", int64(cfg.GetMemoryMB())),
		attribute.Int64("env.vcpu_count", int64(cfg.GetVCpuCount())),
		attribute.Bool("env.huge_pages", hugePages),
		attribute.Bool("env.free_page_reporting", freePageReporting),
		attribute.Bool("env.free_page_hinting", freePageHinting),
		attribute.String("env.kernel_cmdline_args", fc.KernelArgs(cmdlineArgs).String()),
	)

	buildCmdlineArgs.Add(ctx, 1, metric.WithAttributes(
		attribute.String("args", fc.KernelArgs(cmdlineArgs).String()),
	))

	freeDiskSizeMB := resolveFreeDiskSizeMB(cfg)

	template := config.TemplateConfig{
		Version:              version,
		TeamID:               cfg.GetTeamID(),
		TemplateID:           cfg.GetTemplateID(),
		CacheScope:           cacheScope,
		VCpuCount:            int64(cfg.GetVCpuCount()),
		MemoryMB:             int64(cfg.GetMemoryMB()),
		StartCmd:             cfg.GetStartCommand(),
		ReadyCmd:             cfg.GetReadyCommand(),
		DiskSizeMB:           int64(cfg.GetDiskSizeMB()),
		FreeDiskSizeMB:       freeDiskSizeMB,
		HugePages:            hugePages,
		FreePageReporting:    freePageReporting,
		FreePageHinting:      freePageHinting,
		FromImage:            cfg.GetFromImage(),
		FromTemplate:         cfg.GetFromTemplate(),
		RegistryAuthProvider: authProvider,
		Force:                cfg.Force,
		Steps:                cfg.GetSteps(),
		KernelVersion:        kernelVersion,
		FirecrackerVersion:   firecrackerVersion,
		CmdlineArgs:          cmdlineArgs,
	}

	logs := buildlogger.NewLogEntryLogger()
	buildInfo, err := s.buildCache.Create(template.TeamID, metadata.BuildID, logs)
	if err != nil {
		return nil, fmt.Errorf("error while creating build cache: %w", err)
	}

	// LogEntryLogger is itself a zapcore.Core that captures every entry into
	// an in-memory slice; tee it with the regular build logger so logs go to
	// both destinations.
	core := zapcore.NewTee(logs, s.buildLogger.Detach(ctx).Core().
		With([]zap.Field{
			{Type: zapcore.StringType, Key: "teamID", String: template.TeamID},
			{Type: zapcore.StringType, Key: "envID", String: cfg.GetTemplateID()},
			{Type: zapcore.StringType, Key: "buildID", String: metadata.BuildID},
		}),
	)

	// Register child work before the foreground request releases its hold.
	buildDone := s.info.TrackWork()
	s.wg.Add(1)
	s.activeBuilds.Add(1)
	go func(ctx context.Context) {
		defer s.wg.Done()
		defer buildDone()
		defer s.activeBuilds.Add(-1)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		ctx, buildSpan := tracer.Start(ctx, "template-background-build", trace.WithAttributes(
			telemetry.WithTemplateID(template.TemplateID),
			telemetry.WithBuildID(metadata.BuildID),
			telemetry.WithTeamID(template.TeamID),
		))
		defer buildSpan.End()

		defer func() {
			if r := recover(); r != nil {
				telemetry.ReportCriticalError(ctx, "recovered from panic in template build handler", nil, attribute.String("panic", fmt.Sprintf("%v", r)), telemetry.WithTemplateID(cfg.GetTemplateID()), telemetry.WithBuildID(cfg.GetBuildID()))
				buildInfo.SetFail(builderrors.UnwrapUserError(nil))
			}
		}()

		// Watch for build cancellation requests
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-buildInfo.Result.Done:
				res, _ := buildInfo.Result.Result()
				if res.Status == templatemanager.TemplateBuildState_Failed {
					cancel()
				}

				return
			}
		}()

		// Queue builds inside the background goroutine so the gRPC handler stays
		// non-blocking and the node does not oversubscribe build resources.
		if s.buildLimitEnabled.Load() {
			if err := s.buildLimiter.Acquire(ctx, 1); err != nil {
				telemetry.ReportEvent(ctx, "build cancelled while queued for the build limiter")
				buildInfo.SetFail(builderrors.UnwrapUserError(err))

				return
			}
			defer s.buildLimiter.Release(1)
		}

		res, err := s.builder.Build(ctx, metadata, template, core)
		_ = core.Sync()
		if err != nil {
			userError := builderrors.UnwrapUserError(err)

			attrs := []attribute.KeyValue{
				telemetry.WithTemplateID(cfg.GetTemplateID()),
				telemetry.WithBuildID(cfg.GetBuildID()),
			}
			if userError.GetMessage() == builderrors.InternalErrorMessage {
				telemetry.ReportCriticalError(ctx, "error while building template", err, attrs...)
			} else {
				telemetry.ReportError(ctx, "error while building template", err, attrs...)
			}

			buildInfo.SetFail(userError)
		} else {
			buildInfo.SetSuccess(&templatemanager.TemplateBuildMetadata{
				RootfsSizeKey:      int32(res.RootfsSizeMB),
				EnvdVersionKey:     res.EnvdVersion,
				KernelVersion:      res.KernelVersion,
				FirecrackerVersion: res.FirecrackerVersion,
				SchedulingMetadata: res.SchedulingMetadata,
			})
			telemetry.ReportEvent(ctx, "Environment built")
		}
	}(context.WithoutCancel(ctx))

	return nil, nil
}

func resolveFreeDiskSizeMB(cfg *templatemanager.TemplateConfig) int64 {
	if cfg.FreeDiskSizeMB != nil {
		return int64(cfg.GetFreeDiskSizeMB())
	}

	return int64(cfg.GetDiskSizeMB())
}
