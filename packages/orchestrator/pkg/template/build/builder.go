//go:build linux

package build

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/builderrors"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/commands"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/envd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/layer"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases/base"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases/ensurefreedisk"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases/finalize"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases/optimize"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases/steps"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases/user"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/storage/cache"
	"github.com/e2b-dev/infra/packages/shared/pkg/dockerhub"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	orchestratorgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/templates"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build")

type Builder struct {
	logger logger.Logger

	config              cfg.BuilderConfig
	sandboxFactory      *sandbox.Factory
	templateStorage     storage.StorageProvider
	buildStorage        storage.StorageProvider
	dockerhubRepository dockerhub.RemoteRepository
	proxy               *proxy.SandboxProxy
	sandboxes           *sandbox.Map
	templateCache       *sbxtemplate.Cache
	metrics             *metrics.BuildMetrics
	featureFlags        *featureflags.Client
	uploads             *sandbox.Uploads
}

func NewBuilder(
	config cfg.BuilderConfig,
	logger logger.Logger,
	featureFlags *featureflags.Client,
	sandboxFactory *sandbox.Factory,
	templateStorage storage.StorageProvider,
	buildStorage storage.StorageProvider,
	dockerhubRepository dockerhub.RemoteRepository,
	proxy *proxy.SandboxProxy,
	sandboxes *sandbox.Map,
	templateCache *sbxtemplate.Cache,
	buildMetrics *metrics.BuildMetrics,
	uploads *sandbox.Uploads,
) *Builder {
	return &Builder{
		config:              config,
		logger:              logger,
		featureFlags:        featureFlags,
		sandboxFactory:      sandboxFactory,
		templateStorage:     templateStorage,
		buildStorage:        buildStorage,
		dockerhubRepository: dockerhubRepository,
		proxy:               proxy,
		sandboxes:           sandboxes,
		templateCache:       templateCache,
		metrics:             buildMetrics,
		uploads:             uploads,
	}
}

type Result struct {
	EnvdVersion        string
	KernelVersion      string
	FirecrackerVersion string
	RootfsSizeMB       int64
	SchedulingMetadata *orchestratorgrpc.SchedulingMetadata
}

// Build builds the template, uploads it to storage and returns the result metadata.
// It works the following:
// 1. Get docker image from the remote repository
// 2. Inject new file layers with the required setup for hostname, dns, envd service configuration, basic provisioning script that is run before most of VM services
// 3. Extract ext4 filesystem
// 4. Start FC VM with BusyBox init that runs just the provisioning script, wait for exit. This will install systemd, that is later used for proper VM boot.
// 5. Start the FC VM (using systemd) and wait for Envd
// 6. Build the template steps/layers
// 7. Restart the sandbox and run two additional commands:
//   - configuration script (enable swap, create user, change folder permissions, etc.)
//   - start command (if defined), together with the ready command (always with default value if not defined)
//
// 8. Snapshot
// 9. Upload template (and all not yet uploaded layers)
func (b *Builder) Build(ctx context.Context, paths storage.Paths, cfg config.TemplateConfig, logsCore zapcore.Core) (r *Result, e error) {
	// The caller's span (template-background-build for gRPC builds); captured
	// before we open our own so the result attribute lands on both.
	parentSpan := trace.SpanFromContext(ctx)
	ctx, childSpan := tracer.Start(ctx, "build")
	defer childSpan.End()

	// setup launch darkly context
	ctx = featureflags.AddToContext(
		ctx,
		featureflags.TemplateContext(cfg.TemplateID),
		featureflags.TeamContext(cfg.TeamID),
	)

	// Record build duration and result at the end. This defer is registered
	// first so it runs last, after the deferred WrapContextAsUserError below has
	// produced the final error; the metric label and the span attribute are
	// derived from that same final error so they can never disagree.
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		success := e == nil && r != nil
		b.metrics.RecordBuildDuration(ctx, duration, success)

		resultType := ClassifyBuildResult(r, e)
		b.metrics.RecordBuildResult(ctx, cfg.TeamID, resultType)
		if success {
			b.metrics.RecordRootfsSize(ctx, units.MBToBytes(r.RootfsSizeMB))
		}

		// Stamp the classification on the build span and on the caller's span
		// (template-background-build for gRPC builds) so any span in the trace
		// can be filtered by build.result without knowing which span carries an
		// Error status. Child spans keep Error status for every failed
		// operation, including user command failures; this attribute is the
		// only place that says whose fault it was.
		resultAttr := metrics.BuildResultAttribute(resultType)
		childSpan.SetAttributes(resultAttr)
		parentSpan.SetAttributes(resultAttr)
	}()

	cacheScope := cfg.CacheScope

	// Validate template, update force layers if needed
	cfg = forceSteps(cfg)

	l := logger.NewTracedLoggerFromCore(logsCore)
	defer func(ctx context.Context) {
		switch {
		case e != nil:
			l.Error(ctx, fmt.Sprintf("Build failed: %v", builderrors.UnwrapUserError(e).GetMessage()))
		default:
			l.Info(ctx, fmt.Sprintf("Build finished, took %s",
				time.Since(startTime).Truncate(time.Second).String()))
		}
	}(ctx)

	defer func() {
		if r := recover(); r != nil {
			telemetry.ReportCriticalError(ctx, "recovered from panic in template build", nil, attribute.String("panic", fmt.Sprintf("%v", r)), telemetry.WithTemplateID(cfg.TemplateID), telemetry.WithBuildID(paths.BuildID))
			e = errors.New("fatal error occurred during template build, please contact us")
		}
	}()

	// Wrap context as a user error if no user error already exists
	defer func() {
		if ctx.Err() != nil {
			childSpan.AddEvent("build context done", trace.WithAttributes(
				attribute.String("context.err", ctx.Err().Error()),
			))
			e = errors.Join(e, ctx.Err())
		}

		e = builderrors.WrapContextAsUserError(ctx, e)
		if e != nil {
			childSpan.RecordError(e, trace.WithAttributes(
				telemetry.WithTemplateID(cfg.TemplateID),
				telemetry.WithBuildID(paths.BuildID),
				telemetry.WithTeamID(cfg.TeamID),
			))
			childSpan.SetStatus(codes.Error, e.Error())
		}
	}()

	l.Info(ctx, fmt.Sprintf("Building template %s/%s", cfg.TemplateID, paths.BuildID))

	defer func(ctx context.Context) {
		if e == nil {
			return
		}

		// Remove build files if build fails
		removeErr := b.templateStorage.DeleteObjectsWithPrefix(ctx, paths.BuildID)
		if removeErr != nil {
			e = errors.Join(e, fmt.Errorf("error removing build files: %w", removeErr))
		}
	}(context.WithoutCancel(ctx))

	// The envd baked into this build is selected per build by the
	// build-envd-version flag (default: the node-local promoted binary),
	// exactly like BuildKernelVersion / BuildFirecrackerVersion — a version
	// roll is a flag change, never a process restart or node change. The ctx
	// carries the template/team LD context, so cohort canaries work. The
	// resolved path is threaded to every consumer through the BuildContext's
	// BuilderConfig copy.
	envdTarget := b.featureFlags.StringFlag(ctx, featureflags.BuildEnvdVersion)
	hostEnvdPath, err := envd.ResolveBuildBinary(ctx, envdTarget, b.config.HostEnvdPath)
	if err != nil {
		return nil, fmt.Errorf("error resolving envd binary for build: %w", err)
	}

	envdVersion, err := envd.GetEnvdVersion(ctx, hostEnvdPath)
	if err != nil {
		return nil, fmt.Errorf("error getting envd version: %w", err)
	}

	uploadErrGroup := &errgroup.Group{}
	defer func() {
		// Wait for all template layers to be uploaded even if the build fails
		_, waitSpan := tracer.Start(ctx, "wait-for-template-layer-uploads", trace.WithAttributes(
			telemetry.WithTemplateID(cfg.TemplateID),
			telemetry.WithBuildID(paths.BuildID),
			telemetry.WithTeamID(cfg.TeamID),
		))
		defer waitSpan.End()

		err := uploadErrGroup.Wait()
		if err != nil {
			waitSpan.RecordError(err)
			waitSpan.SetStatus(codes.Error, err.Error())
			b.logger.Error(ctx, "template layer upload wait failed",
				logger.WithTemplateID(cfg.TemplateID),
				logger.WithBuildID(paths.BuildID),
				logger.WithTeamID(cfg.TeamID),
				zap.Error(err),
			)
			e = errors.Join(e, fmt.Errorf("error uploading template layers: %w", err))
		}

		if ctx.Err() != nil {
			waitSpan.AddEvent("build context done while waiting for uploads", trace.WithAttributes(
				attribute.String("context.err", ctx.Err().Error()),
			))
		}
	}()

	builderConfig := b.config
	builderConfig.HostEnvdPath = hostEnvdPath

	buildContext := buildcontext.BuildContext{
		BuilderConfig:  builderConfig,
		Config:         cfg,
		Template:       paths,
		UploadErrGroup: uploadErrGroup,
		EnvdVersion:    envdVersion,
		CacheScope:     cacheScope,
		Version:        cfg.Version,
		Rootfs:         resolveRootfsOptions(ctx, b.featureFlags, cfg),
	}

	return runBuild(ctx, l, buildContext, b)
}

// resolveRootfsOptions evaluates, once per build, the flags that shape the
// baked rootfs files; buildcontext.RootfsOptions says why once. It uses the
// template and team contexts, as the provision version does.
func resolveRootfsOptions(ctx context.Context, featureFlags *featureflags.Client, cfg config.TemplateConfig) buildcontext.RootfsOptions {
	return buildcontext.RootfsOptions{
		EnvdMemoryProtection: featureFlags.BoolFlag(
			ctx,
			featureflags.BuildEnvdMemoryProtection,
			featureflags.TemplateContext(cfg.TemplateID),
			featureflags.TeamContext(cfg.TeamID),
		),
	}
}

func (b *Builder) useNFSCache(ctx context.Context) (string, bool) {
	flag := b.featureFlags.BoolFlag(ctx, featureflags.UseNFSCacheForBuildingTemplatesFlag)

	if flag && b.config.SharedChunkCacheDir == "" {
		logger.L().Warn(ctx, "NFSCache feature flag is enabled but cache path is not set")

		return "", false
	}

	return b.config.SharedChunkCacheDir, flag
}

func runBuild(
	ctx context.Context,
	userLogger logger.Logger,
	bc buildcontext.BuildContext,
	builder *Builder,
) (*Result, error) {
	ctx, span := tracer.Start(ctx, "run build")
	defer span.End()

	templateStorage := builder.templateStorage
	if path, ok := builder.useNFSCache(ctx); ok {
		templateStorage = storage.WrapInNFSCache(ctx, path, templateStorage, builder.featureFlags)
		span.SetAttributes(attribute.Bool("use_cache", true))
	} else {
		span.SetAttributes(attribute.Bool("use_cache", false))
	}

	index := cache.NewHashIndex(bc.CacheScope, builder.buildStorage, templateStorage)

	layerExecutor := layer.NewLayerExecutor(
		bc,
		builder.logger,
		builder.templateCache,
		builder.proxy,
		builder.sandboxes,
		templateStorage,
		builder.buildStorage,
		index,
		builder.uploads,
		builder.config.StorageConfig.CompressConfig,
		builder.featureFlags,
	)

	baseBuilder := base.New(
		bc,
		builder.featureFlags,
		builder.logger,
		builder.proxy,
		templateStorage,
		builder.dockerhubRepository,
		layerExecutor,
		index,
		builder.metrics,
		builder.sandboxFactory,
		builder.sandboxes,
	)

	commandExecutor := commands.NewCommandExecutor(
		bc,
		builder.buildStorage,
		builder.proxy,
	)

	userBuilder := user.New(
		bc,
		builder.sandboxFactory,
		builder.logger,
		builder.proxy,
		layerExecutor,
		commandExecutor,
		index,
		builder.metrics,
		builder.featureFlags,
		config.TemplateDefaultUser,
		bc.Config.Force,
	)

	stepBuilders := steps.CreateStepPhases(
		bc,
		builder.sandboxFactory,
		builder.logger,
		builder.proxy,
		layerExecutor,
		commandExecutor,
		index,
		builder.metrics,
		builder.featureFlags,
	)

	postProcessingBuilder := finalize.New(
		bc,
		builder.sandboxFactory,
		templateStorage,
		builder.proxy,
		layerExecutor,
		builder.featureFlags,
		builder.logger,
	)

	optimizeBuilder := optimize.New(
		bc,
		builder.sandboxFactory,
		builder.templateStorage,
		builder.templateCache,
		builder.proxy,
		layerExecutor,
		builder.sandboxes,
		builder.logger,
	)

	// Construct the phases/steps to run
	builders := []phases.BuilderPhase{
		baseBuilder,
	}
	// Default user is only set for version TemplateDefaultUserVersion
	ok, err := utils.IsGTEVersion(bc.Version, templates.TemplateV2ReleaseVersion)
	if err != nil {
		return nil, fmt.Errorf("error checking build version: %w", err)
	}
	if ok {
		builders = append(builders, userBuilder)
	}
	builders = append(builders, stepBuilders...)
	// Grow the quiescent rootfs before finalize cold-boots it.
	if builder.featureFlags.BoolFlag(ctx, featureflags.BuildEnsureFreeDiskSpace) {
		builders = append(builders, ensurefreedisk.New(
			bc,
			builder.sandboxFactory,
			layerExecutor,
			builder.templateCache,
			index,
		))
	}
	builders = append(builders, postProcessingBuilder)
	builders = append(builders, optimizeBuilder)

	lastLayerResult, err := phases.Run(ctx, builder.logger, userLogger, bc, builder.metrics, builders)
	if err != nil {
		return nil, err
	}

	// Ensure the base layer is uploaded before getting the rootfs size
	err = bc.UploadErrGroup.Wait()
	if err != nil {
		return nil, fmt.Errorf("error waiting for layers upload: %w", err)
	}

	// Get the base rootfs size from the template files
	// This is the size of the rootfs after provisioning and before building the layers
	// (as they don't change the rootfs size)
	rootfsSize, err := getRootfsSize(ctx, builder.templateStorage, storage.Paths{BuildID: lastLayerResult.Metadata.Template.BuildID})
	if err != nil {
		return nil, fmt.Errorf("error getting rootfs size: %w", err)
	}
	logger.L().Info(ctx, "rootfs size", zap.Uint64("size", rootfsSize))

	return &Result{
		EnvdVersion:        bc.EnvdVersion,
		KernelVersion:      bc.Config.KernelVersion,
		FirecrackerVersion: bc.Config.FirecrackerVersion,
		RootfsSizeMB:       units.BytesToMB(int64(rootfsSize)),
		SchedulingMetadata: templateSchedulingMetadata(ctx, builder.templateCache, lastLayerResult.Metadata.Template.BuildID),
	}, nil
}

func templateSchedulingMetadata(ctx context.Context, cache *sbxtemplate.Cache, buildID string) *orchestratorgrpc.SchedulingMetadata {
	// Use GetTemplate (not GetCachedTemplate): the optimize phase invalidates
	// the final build from the cache, so re-fetch to resolve its headers.
	t, err := cache.GetTemplate(ctx, buildID, false, false)
	if err != nil {
		return nil
	}
	provider, ok := t.(interface {
		SchedulingMetadata(ctx context.Context) *orchestratorgrpc.SchedulingMetadata
	})
	if !ok {
		return nil
	}

	return provider.SchedulingMetadata(ctx)
}

// forceSteps sets force for all steps after the first encounter.
func forceSteps(template config.TemplateConfig) config.TemplateConfig {
	shouldRebuild := template.Force != nil && *template.Force
	for _, step := range template.Steps {
		// Force rebuild if the step has a Force flag set to true
		if step.Force != nil && step.GetForce() {
			shouldRebuild = true
		}

		if !shouldRebuild {
			continue
		}

		force := true
		step.Force = &force
	}

	return template
}

func getRootfsSize(
	ctx context.Context,
	s storage.StorageProvider,
	paths storage.Paths,
) (uint64, error) {
	obj, err := s.OpenBlob(ctx, paths.RootfsHeader())
	if err != nil {
		return 0, fmt.Errorf("error opening rootfs header object: %w", err)
	}

	h, err := header.Deserialize(ctx, obj)
	if err != nil {
		return 0, fmt.Errorf("error deserializing rootfs header: %w", err)
	}

	return h.Metadata.Size, nil
}
