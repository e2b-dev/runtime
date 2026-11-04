//go:build linux

package steps

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/commands"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/layer"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/sandboxtools"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/storage/cache"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

const layerTimeout = time.Hour

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases/steps")

type StepBuilder struct {
	buildcontext.BuildContext

	stepNumber int
	step       *templatemanager.TemplateStep

	logger              logger.Logger
	defaultLoggingLevel zapcore.Level
	proxy               *proxy.SandboxProxy

	sandboxFactory  *sandbox.Factory
	layerExecutor   *layer.LayerExecutor
	commandExecutor *commands.CommandExecutor
	index           cache.Index
	metrics         *metrics.BuildMetrics
	featureFlags    *featureflags.Client
}

func New(
	buildContext buildcontext.BuildContext,
	sandboxFactory *sandbox.Factory,
	logger logger.Logger,
	proxy *proxy.SandboxProxy,
	layerExecutor *layer.LayerExecutor,
	commandExecutor *commands.CommandExecutor,
	index cache.Index,
	metrics *metrics.BuildMetrics,
	featureFlags *featureflags.Client,
	step *templatemanager.TemplateStep,
	stepNumber int,
	defaultLoggingLevel zapcore.Level,
) *StepBuilder {
	return &StepBuilder{
		BuildContext: buildContext,

		stepNumber: stepNumber,
		step:       step,

		logger:              logger,
		defaultLoggingLevel: defaultLoggingLevel,
		proxy:               proxy,

		sandboxFactory:  sandboxFactory,
		layerExecutor:   layerExecutor,
		commandExecutor: commandExecutor,
		index:           index,
		metrics:         metrics,
		featureFlags:    featureFlags,
	}
}

func (sb *StepBuilder) Prefix() string {
	return fmt.Sprintf("builder %d/%d", sb.stepNumber, len(sb.Config.Steps))
}

func (sb *StepBuilder) String(context.Context) (string, error) {
	return fmt.Sprintf("%s %s", strings.ToUpper(sb.step.GetType()), strings.Join(sb.step.GetArgs(), " ")), nil
}

func (sb *StepBuilder) Metadata() phases.PhaseMeta {
	return phases.PhaseMeta{
		Phase:      metrics.PhaseSteps,
		StepType:   sb.step.GetType(),
		StepNumber: &sb.stepNumber,
	}
}

func (sb *StepBuilder) Layer(
	ctx context.Context,
	sourceLayer phases.LayerResult,
	hash string,
) (phases.LayerResult, error) {
	ctx, span := tracer.Start(ctx, "compute step", trace.WithAttributes(
		attribute.Int("step", sb.stepNumber),
		attribute.String("type", sb.step.GetType()),
		attribute.String("hash", hash),
	))
	defer span.End()

	forceBuild := sb.step.Force != nil && sb.step.GetForce()
	if !forceBuild {
		m, err := sb.index.LayerMetaFromHash(ctx, hash)
		if err != nil {
			sb.logger.Info(ctx, "layer not found in cache, building new step layer", zap.Error(err), zap.String("hash", hash))
		} else {
			// Check if the layer is cached
			meta, err := sb.index.Cached(ctx, m.Template.BuildID)
			if err == nil {
				return phases.LayerResult{
					Metadata: meta,
					Cached:   true,
					Hash:     hash,
				}, nil
			}

			logger.L().Info(ctx, "layer not cached, building new layer", zap.Error(err), zap.String("hash", hash))
		}
	}

	finalMetadata := sourceLayer.Metadata
	// A new step boots at the online count, even when the cached source was
	// snapshotted from a larger VM. Only finalize needs the final VM capacity.
	// Pause records the stored size; this keeps the in-memory layer result in step.
	finalMetadata.VcpuCount = sb.Config.VCpuCount
	finalMetadata.Template = metadata.TemplateMetadata{
		BuildID:            uuid.NewString(),
		KernelVersion:      sb.Config.KernelVersion,
		FirecrackerVersion: sb.Config.FirecrackerVersion,
	}

	return phases.LayerResult{
		Metadata: finalMetadata,
		Cached:   false,
		Hash:     hash,
	}, nil
}

func (sb *StepBuilder) Build(
	ctx context.Context,
	userLogger logger.Logger,
	prefix string,
	sourceLayer phases.LayerResult,
	currentLayer phases.LayerResult,
) (phases.LayerResult, error) {
	ctx, span := tracer.Start(ctx, "build step", trace.WithAttributes(
		attribute.Int("step", sb.stepNumber),
		attribute.String("type", sb.step.GetType()),
		attribute.String("hash", currentLayer.Hash),
	))
	defer span.End()

	step := sb.step

	sbxConfig := sandbox.NewConfig(sandbox.Config{
		Vcpu:              sb.Config.VCpuCount,
		RamMB:             sb.Config.MemoryMB,
		HugePages:         sb.Config.HugePages,
		FreePageReporting: sb.Config.FreePageReporting,
		FreePageHinting:   sb.Config.FreePageHinting,

		Envd: sandbox.EnvdMetadata{
			Version: sb.EnvdVersion,
		},

		FirecrackerConfig: sb.Config.FirecrackerConfig(),
	})

	guestTemplate := sb.Config.GuestTemplate()

	// First not cached layer is create (to change CPU, Memory, etc), subsequent are layers are resumes.
	// A guest profile that always resumes takes the parent's machine (adoptParentMachine).
	var sandboxCreator layer.SandboxCreator
	switch {
	case guestTemplate.AlwaysResume:
		sandboxCreator = layer.NewResumeSandbox(sbxConfig, sb.sandboxFactory, layerTimeout)
	case sourceLayer.Cached:
		sandboxCreator = layer.NewCreateSandbox(
			sbxConfig,
			sb.sandboxFactory,
			layerTimeout,
			append(
				layer.ReservedBlocksOptions(ctx, sb.featureFlags, sb.Config.RootfsBlockSize()),
				layer.BootDiagnosticsOptions(ctx, sb.featureFlags)...,
			)...,
		)
	default:
		sandboxCreator = layer.NewResumeSandbox(sbxConfig, sb.sandboxFactory, layerTimeout)
	}

	actionExecutor := layer.NewFunctionAction(func(ctx context.Context, sbx *sandbox.Sandbox, meta metadata.Template) (metadata.Template, error) {
		cmdMeta, err := sb.commandExecutor.Execute(
			ctx,
			userLogger,
			sb.defaultLoggingLevel,
			sbx,
			prefix,
			step,
			meta.Context,
		)
		if err != nil {
			return metadata.Template{}, phases.NewPhaseBuildError(sb.Metadata(), err)
		}

		err = sandboxtools.SyncChangesToDiskWith(
			ctx,
			sb.proxy,
			sbx.Runtime.SandboxID,
			guestTemplate.SyncCommand,
		)
		if err != nil {
			return metadata.Template{}, fmt.Errorf("error running sync command: %w", err)
		}

		meta.Context = cmdMeta

		return meta, nil
	})

	templateProvider := layer.NewCacheSourceTemplateProvider(sourceLayer.Metadata.Template.BuildID)

	meta, err := sb.layerExecutor.BuildLayer(
		ctx,
		userLogger,
		layer.LayerBuildCommand{
			SourceTemplate: templateProvider,
			CurrentLayer:   currentLayer.Metadata,
			Hash:           currentLayer.Hash,
			UpdateEnvd:     sourceLayer.Cached && !guestTemplate.KeepGuestEnvd,
			SandboxCreator: sandboxCreator,
			ActionExecutor: actionExecutor,
			BuildOrigin:    storage.ObjectOriginTemplateBuildCache,
		},
	)
	if err != nil {
		return phases.LayerResult{}, fmt.Errorf("error building step %d: %w", sb.stepNumber, err)
	}

	return phases.LayerResult{
		Metadata: meta,
		Cached:   false,
		Hash:     currentLayer.Hash,
	}, nil
}
