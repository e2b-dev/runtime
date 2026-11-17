//go:build linux

package server

import (
	"context"
	"errors"
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
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc/cputemplate"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/builderrors"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildlogger"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/oci/auth"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/cache"
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

	// buildCPUTemplate counts builds by the digest of the CPU template applied and by result,
	// so a template the build rejected is not read as a team that has none.
	buildCPUTemplate = utils.Must(telemetry.GetCounter(meter, telemetry.TemplateBuildCPUTemplate))
	buildVcpuSizing  = utils.Must(telemetry.GetCounter(meter, telemetry.TemplateBuildVcpuSizing))
	buildMemSizing   = utils.Must(telemetry.GetCounter(meter, telemetry.TemplateBuildMemorySizing))
)

func (s *ServerStore) TemplateCreate(ctx context.Context, templateRequest *templatemanager.TemplateCreateRequest) (*emptypb.Empty, error) {
	done := s.info.TrackWork()
	defer done()

	ctx, childSpan := tracer.Start(ctx, "template-create")
	defer childSpan.End()

	cfg := templateRequest.GetTemplate()
	childSpan.SetAttributes(telemetry.WithTemplateID(cfg.GetTemplateID()), telemetry.WithBuildID(cfg.GetBuildID()))

	if cfg.GetFromImage() == "" && cfg.GetFromTemplate() == nil {
		return nil, status.Error(codes.InvalidArgument, "template build requires either fromImage or fromTemplate")
	}

	if err := validateMaxVCpuCount(cfg); err != nil {
		// Validation runs before a build sandbox exists, so the template and build
		// IDs are the correlation keys for this rejected request.
		buildVcpuSizing.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "rejected")))
		s.logger.Warn(ctx, "rejected template build VM sizing",
			zap.Error(err), logger.WithTemplateID(cfg.GetTemplateID()), logger.WithBuildID(cfg.GetBuildID()),
			zap.Int32("vcpu_count", cfg.GetVCpuCount()), zap.Int32("max_vcpu_count", cfg.GetMaxVCpuCount()),
		)

		return nil, err
	}
	for _, step := range cfg.GetSteps() {
		if step.FilesHash == nil { //nolint:protogetter // we need the nil check too
			continue
		}

		if err := templates.ValidateFilesHash(step.GetFilesHash()); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	// Validation rejected a present 0, so 0 here means absent.
	vcpuSizing := config.VcpuSizing(int64(cfg.GetVCpuCount()), int64(cfg.GetMaxVCpuCount()))
	buildVcpuSizing.Add(ctx, 1, metric.WithAttributes(attribute.String("result", vcpuSizing)))

	// Every request the vCPU sizing counted is counted once here too, after it, so a bad maximum
	// never hides a vCPU count. The count lands when this RPC decides: its requested sizing once
	// the build is registered and running, else rejected with the reason, internal for any failure
	// no named check caught. A retry of a build already registered is not counted again. Refusals
	// inside the build show on template.build.result instead.
	memResult, memReason, memCount := memorySizingRejected, memoryRejectedInternal, true
	defer func() {
		if memCount {
			s.countMemorySizing(ctx, memResult, memReason)
		}
	}()
	if err := validateMaxMemoryMB(cfg); err != nil {
		memReason = memoryRejectedInvalid
		s.logger.Warn(ctx, "rejected template build memory sizing",
			zap.Error(err), logger.WithTemplateID(cfg.GetTemplateID()), logger.WithBuildID(cfg.GetBuildID()),
			zap.Int32("memory_mb", cfg.GetMemoryMB()), zap.Int32("max_memory_mb", cfg.GetMaxMemoryMB()),
		)

		return nil, err
	}
	memorySizing := config.MemorySizing(int64(cfg.GetMemoryMB()), int64(cfg.GetMaxMemoryMB()))
	memoryRegionMB := config.MemoryHotplugRegionMB(int64(cfg.GetMemoryMB()), int64(cfg.GetMaxMemoryMB()))

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

	// Only a build is capped: a template built under a looser cap keeps resuming and rebooting.
	memoryMaxRatio := int64(s.featureFlags.IntFlag(ctx, featureflags.TemplateMemoryHotplugMaxRatioFlag))
	if err := validateMemoryRatio(cfg, memoryMaxRatio); err != nil {
		memReason = memoryRejectedRatio
		s.logger.Warn(ctx, "rejected template build memory sizing above the hotplug cap",
			zap.Error(err), logger.WithTemplateID(cfg.GetTemplateID()), logger.WithBuildID(cfg.GetBuildID()),
			zap.Int32("memory_mb", cfg.GetMemoryMB()), zap.Int32("max_memory_mb", cfg.GetMaxMemoryMB()),
			zap.Int64("max_ratio", memoryMaxRatio),
		)

		return nil, err
	}

	// Read once here, at the only place the per-team flag is read, so every boot in this
	// build agrees and what gets recorded is what was actually applied.
	cmdlineArgs, cmdlineErr := fc.ParseCmdlineArgs(s.featureFlags.StringFlag(ctx, featureflags.BuildKernelCmdlineArgs))
	if cmdlineErr == nil {
		cmdlineErr = fc.ValidateBuildCmdlineArgs(cmdlineArgs)
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

	// The builder's kernel and Firecracker are the fleet's to fix, not the caller's.
	if err := (fc.Config{KernelVersion: kernelVersion, FirecrackerVersion: firecrackerVersion}).RefuseMemoryHotplug(memoryRegionMB); err != nil {
		memReason = fc.MemoryHotplugRefusalReason(err)
		s.logger.Warn(ctx, "refused a template build with a memory maximum this builder cannot carry", zap.Error(err),
			logger.WithTemplateID(cfg.GetTemplateID()), logger.WithBuildID(cfg.GetBuildID()))

		return nil, status.Error(codes.FailedPrecondition, "this template builder cannot build templates with a maximum memory size yet")
	}

	// Read once per build, so every layer boots with the same template.
	cpuTemplateResult := cpuTemplateNone
	var cpuTemplate *cputemplate.Template
	cpuTemplate, err = cputemplate.Parse([]byte(s.featureFlags.JSONFlag(ctx, featureflags.BuildCPUTemplate).JSONString()))
	if err != nil {
		// A malformed flag fails the build: falling back would silently boot at host frequency.
		// The flag and parse detail stay in the log and the counter, not the client's message.
		s.logger.Error(ctx, "invalid build CPU template flag",
			zap.String("flag", featureflags.BuildCPUTemplate.Key()),
			zap.Error(err),
			logger.WithTeamID(cfg.GetTeamID()),
			logger.WithTemplateID(cfg.GetTemplateID()),
			logger.WithBuildID(cfg.GetBuildID()),
		)
		childSpan.SetAttributes(attribute.String("env.cpu_template_result", cpuTemplateMalformed))
		buildCPUTemplate.Add(ctx, 1, metric.WithAttributes(
			attribute.String("template", ""),
			attribute.String("result", cpuTemplateMalformed),
		))

		return nil, status.Error(codes.Internal, "invalid build configuration, contact support")
	}
	if cpuTemplate != nil {
		// A template this Firecracker version or host architecture cannot apply falls back to
		// none, so one flag value can serve a mixed fleet during rollout. The span and counter
		// mark the build rejected; this log carries the reason.
		if err := cpuTemplate.Validate(fcInfo); err != nil {
			cpuTemplateErr := fmt.Errorf("firecracker %s: %w", firecrackerVersion, err)
			s.logger.Warn(ctx, "rejected build CPU template, using none",
				zap.Error(cpuTemplateErr),
				logger.WithTemplateID(cfg.GetTemplateID()),
				logger.WithBuildID(cfg.GetBuildID()),
			)

			cpuTemplate = nil
			cpuTemplateResult = cpuTemplateRejected
		} else {
			cpuTemplateResult = cpuTemplateApplied
		}
	}

	hugePages := true
	freePageReporting := fcInfo.HasFreePageReporting() && s.featureFlags.BoolFlag(ctx, featureflags.FreePageReportingFlag)
	freePageHinting := fcInfo.HasFreePageHinting() && featureflags.IsFreePageHintingEnabled(ctx, s.featureFlags)

	childSpan.SetAttributes(
		telemetry.WithTemplateID(cfg.GetTemplateID()),
		telemetry.WithBuildID(cfg.GetBuildID()),
		telemetry.WithKernelVersion(kernelVersion),
		telemetry.WithFirecrackerVersion(firecrackerVersion),
		attribute.Int64("env.memory_mb", int64(cfg.GetMemoryMB())),
		attribute.Int64("env.vcpu_count", int64(cfg.GetVCpuCount())),
		attribute.Int64("env.max_memory_mb", int64(cfg.GetMaxMemoryMB())),
		attribute.Bool("env.huge_pages", hugePages),
		attribute.Bool("env.free_page_reporting", freePageReporting),
		attribute.Bool("env.free_page_hinting", freePageHinting),
		attribute.String("env.kernel_cmdline_args", fc.KernelArgs(cmdlineArgs).String()),
		attribute.String("env.cpu_template", cputemplate.AppliedDigest(cpuTemplate)),
		attribute.String("env.cpu_template_result", cpuTemplateResult),
	)

	buildCmdlineArgs.Add(ctx, 1, metric.WithAttributes(
		attribute.String("args", fc.KernelArgs(cmdlineArgs).String()),
	))

	buildCPUTemplate.Add(ctx, 1, metric.WithAttributes(
		attribute.String("template", cputemplate.AppliedDigest(cpuTemplate)),
		attribute.String("result", cpuTemplateResult),
	))

	freeDiskSizeMB := resolveFreeDiskSizeMB(cfg)

	template := config.TemplateConfig{
		Version:              version,
		TeamID:               cfg.GetTeamID(),
		TemplateID:           cfg.GetTemplateID(),
		CacheScope:           cacheScope,
		VCpuCount:            int64(cfg.GetVCpuCount()),
		MaxVCpuCount:         int64(cfg.GetMaxVCpuCount()),
		MemoryMB:             int64(cfg.GetMemoryMB()),
		MaxMemoryMB:          int64(cfg.GetMaxMemoryMB()),
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
		CPUTemplate:          cpuTemplate,
	}

	logs := buildlogger.NewLogEntryLogger()
	buildInfo, err := s.buildCache.Create(template.TeamID, metadata.BuildID, logs)
	if err != nil {
		memCount = !errors.Is(err, cache.ErrBuildExists)

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

	memResult, memReason = memorySizing, ""

	return nil, nil
}

// Results of resolving the build CPU template flag, for the counter's result attribute.
const (
	cpuTemplateApplied  = "applied"
	cpuTemplateRejected = "rejected"
	cpuTemplateNone     = "none"
	// cpuTemplateMalformed is a flag value that does not parse; the build fails.
	cpuTemplateMalformed = "malformed"
)

// validateMaxVCpuCount rejects VM sizes the host cannot boot before any build VM starts.
// Absent keeps the final VM at vCpuCount; an explicit 0 is rejected.
func validateMaxVCpuCount(cfg *templatemanager.TemplateConfig) error {
	if cfg.MaxVCpuCount == nil {
		return nil
	}
	maxVCpus := int64(cfg.GetMaxVCpuCount())
	if maxVCpus < 1 {
		return status.Error(codes.InvalidArgument, "maxVCpuCount must be at least 1 when set")
	}
	if err := fc.ValidateVcpus(int64(cfg.GetVCpuCount()), maxVCpus, "vCpuCount", "maxVCpuCount"); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if !fc.ValidMachineVcpus(int64(cfg.GetVCpuCount())) {
		return status.Errorf(codes.InvalidArgument, "cannot boot an intermediate build VM with %d vcpus on this host", cfg.GetVCpuCount())
	}
	if !fc.ValidMachineVcpus(maxVCpus) {
		return status.Errorf(codes.InvalidArgument, "cannot boot a VM with %d vcpus on this host", maxVCpus)
	}

	return nil
}

const (
	memorySizingRejected      = "rejected"
	memoryRejectedInvalid     = "invalid"
	memoryRejectedFirecracker = "firecracker"
	memoryRejectedKernel      = "kernel"
	memoryRejectedRatio       = "ratio"
	memoryRejectedInternal    = "internal"
)

// countMemorySizing counts one request's memory sizing: absent, equal or resizable, or rejected
// with the reason (invalid, ratio, firecracker, kernel, or internal).
func (s *ServerStore) countMemorySizing(ctx context.Context, result, reason string) {
	attrs := []attribute.KeyValue{attribute.String("result", result)}
	if reason != "" {
		attrs = append(attrs, attribute.String("reason", reason))
	}
	counter := s.memorySizing
	if counter == nil {
		counter = buildMemSizing
	}
	counter.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// validateMemoryRatio rejects a hotplug region above maxRatio times boot RAM (0 is no cap).
func validateMemoryRatio(cfg *templatemanager.TemplateConfig, maxRatio int64) error {
	if cfg.MaxMemoryMB == nil {
		return nil
	}
	if err := fc.ValidateMemoryHotplugRatio(int64(cfg.GetMemoryMB()), int64(cfg.GetMaxMemoryMB()), maxRatio, "maxMemoryMB"); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	return nil
}

// validateMaxMemoryMB rejects memory sizes Firecracker cannot give a hotplug region before any
// build VM starts. Absent keeps the final VM at memoryMB; an explicit value must be at least
// memoryMB, with the region above boot RAM a whole number of slots and at most
// fc.MaxMemoryHotplugMiB.
func validateMaxMemoryMB(cfg *templatemanager.TemplateConfig) error {
	if cfg.MaxMemoryMB == nil {
		return nil
	}
	err := fc.ValidateMemoryHotplug(int64(cfg.GetMemoryMB()), int64(cfg.GetMaxMemoryMB()), "memoryMB", "maxMemoryMB")
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	return nil
}

func resolveFreeDiskSizeMB(cfg *templatemanager.TemplateConfig) int64 {
	if cfg.FreeDiskSizeMB != nil {
		return int64(cfg.GetFreeDiskSizeMB())
	}

	return int64(cfg.GetDiskSizeMB())
}
