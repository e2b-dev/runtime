//go:build linux

package base

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/filesystem"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/oci"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/layer"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/sandboxtools"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/storage/cache"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/dockerhub"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

const (
	rootfsBuildFileName = "rootfs.filesystem.build"

	baseLayerTimeout = 10 * time.Minute

	defaultUser = "root"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases/base")

type BaseBuilder struct {
	buildcontext.BuildContext

	logger logger.Logger
	proxy  *proxy.SandboxProxy

	sandboxFactory      *sandbox.Factory
	templateStorage     storage.StorageProvider
	dockerhubRepository dockerhub.RemoteRepository
	featureFlags        *featureflags.Client
	sandboxes           *sandbox.Map

	layerExecutor *layer.LayerExecutor
	index         cache.Index
	metrics       *metrics.BuildMetrics
}

func New(
	buildContext buildcontext.BuildContext,
	featureFlags *featureflags.Client,
	logger logger.Logger,
	proxy *proxy.SandboxProxy,
	templateStorage storage.StorageProvider,
	dockerhubRepository dockerhub.RemoteRepository,
	layerExecutor *layer.LayerExecutor,
	index cache.Index,
	metrics *metrics.BuildMetrics,
	sandboxFactory *sandbox.Factory,
	sandboxes *sandbox.Map,
) *BaseBuilder {
	return &BaseBuilder{
		BuildContext: buildContext,

		logger: logger,
		proxy:  proxy,

		templateStorage:     templateStorage,
		dockerhubRepository: dockerhubRepository,
		sandboxFactory:      sandboxFactory,
		featureFlags:        featureFlags,
		sandboxes:           sandboxes,

		layerExecutor: layerExecutor,
		index:         index,
		metrics:       metrics,
	}
}

func (bb *BaseBuilder) Prefix() string {
	return "base"
}

func (bb *BaseBuilder) String(_ context.Context) (string, error) {
	switch {
	case bb.Config.FromTemplate != nil:
		return "FROM TEMPLATE " + bb.Config.FromTemplate.GetAlias(), nil
	case bb.Config.FromImage != "":
		return "FROM " + bb.Config.FromImage, nil
	default:
		return "", errors.New("build has no base image or template")
	}
}

func (bb *BaseBuilder) Metadata() phases.PhaseMeta {
	return phases.PhaseMeta{
		Phase:    metrics.PhaseBase,
		StepType: "base",
	}
}

func (bb *BaseBuilder) Build(
	ctx context.Context,
	userLogger logger.Logger,
	_ string,
	_ phases.LayerResult,
	currentLayer phases.LayerResult,
) (phases.LayerResult, error) {
	ctx, span := tracer.Start(ctx, "build base", trace.WithAttributes(
		attribute.String("hash", currentLayer.Hash),
	))
	defer span.End()

	baseMetadata, err := bb.buildLayerFromOCI(
		ctx,
		userLogger,
		currentLayer.Metadata,
		currentLayer.Hash,
	)
	if err != nil {
		return phases.LayerResult{}, err
	}

	return phases.LayerResult{
		Metadata: baseMetadata,
		Cached:   false,
		Hash:     currentLayer.Hash,
	}, nil
}

func (bb *BaseBuilder) buildLayerFromOCI(
	ctx context.Context,
	userLogger logger.Logger,
	baseMetadata metadata.Template,
	hash string,
) (metadata.Template, error) {
	templateBuildDir := filepath.Join(bb.BuilderConfig.TemplatesDir, baseMetadata.Template.BuildID)
	err := os.MkdirAll(templateBuildDir, 0o777)
	if err != nil {
		return metadata.Template{}, fmt.Errorf("error creating template build directory: %w", err)
	}
	defer func() {
		err := os.RemoveAll(templateBuildDir)
		if err != nil {
			bb.logger.Error(ctx, "Error while removing template build directory", zap.Error(err))
		}
	}()

	// Created here to be able to pass it to CreateSandbox for populating COW cache
	rootfsPath := filepath.Join(templateBuildDir, rootfsBuildFileName)

	rootfs, memfile, envsImg, err := constructLayerFilesFromOCI(ctx, userLogger, bb.BuildContext, bb.Metadata(), baseMetadata.Template.BuildID, bb.dockerhubRepository, bb.featureFlags, rootfsPath)
	if err != nil {
		return metadata.Template{}, fmt.Errorf("error building environment: %w", err)
	}

	cachePaths, err := storage.Paths{BuildID: baseMetadata.Template.BuildID}.Cache(bb.BuildContext.BuilderConfig.StorageConfig)
	if err != nil {
		err = errors.Join(err, rootfs.Close(), memfile.Close())

		return metadata.Template{}, fmt.Errorf("error creating template files: %w", err)
	}
	localTemplate := sbxtemplate.NewLocalTemplate(cachePaths, rootfs, memfile)
	defer localTemplate.Close(ctx)

	// Env variables from the Docker image
	baseMetadata.Context.EnvVars = oci.ParseEnvs(envsImg.Env)

	// Provision sandbox with systemd and other vital parts
	userLogger.Info(ctx, "Provisioning sandbox template")

	// Allow sandbox internet access during provisioning (nil network = no restrictions).
	baseSbxConfig := sandbox.NewConfig(sandbox.Config{
		Vcpu:              bb.Config.VCpuCount,
		RamMB:             bb.Config.MemoryMB,
		HugePages:         bb.Config.HugePages,
		FreePageReporting: bb.Config.FreePageReporting,
		FreePageHinting:   bb.Config.FreePageHinting,

		Envd: sandbox.EnvdMetadata{
			Version: bb.EnvdVersion,
		},

		FirecrackerConfig: fc.Config{
			KernelVersion:      bb.Config.KernelVersion,
			FirecrackerVersion: bb.Config.FirecrackerVersion,
		},
	})
	err = bb.provisionSandbox(
		ctx,
		userLogger,
		baseSbxConfig,
		sandbox.RuntimeMetadata{
			TemplateID:  bb.Config.TemplateID,
			SandboxID:   config.InstanceBuildPrefix + id.Generate(),
			ExecutionID: uuid.NewString(),
			TeamID:      bb.Config.TeamID,
			BuildID:     bb.Template.BuildID,
			SandboxType: sandbox.SandboxTypeBuild,
		},
		localTemplate,
		rootfsPath,
		provisionLogPrefix,
	)
	if err != nil {
		return metadata.Template{}, fmt.Errorf("error provisioning sandbox: %w", err)
	}

	// Check the rootfs filesystem corruption
	ext4Check, err := filesystem.CheckIntegrity(ctx, rootfsPath, true)
	if err != nil {
		logger.L().Error(ctx, "provisioned filesystem ext4 integrity",
			zap.String("result", ext4Check),
			zap.Error(err),
		)

		return metadata.Template{}, fmt.Errorf("error checking provisioned filesystem integrity: %w", err)
	}
	logger.L().Debug(ctx, "provisioned filesystem ext4 integrity",
		zap.String("result", ext4Check),
	)

	err = bb.enlargeDiskAfterProvisioning(ctx, bb.Config, rootfs)
	if err != nil {
		return metadata.Template{}, fmt.Errorf("error enlarging disk after provisioning: %w", err)
	}

	// Create sandbox for building template
	userLogger.Debug(ctx, "Creating base sandbox template layer")

	sandboxOptions := []layer.CreateSandboxOption{
		layer.WithRootfsCachePath(rootfsPath),
	}
	sandboxOptions = append(sandboxOptions, layer.ReservedBlocksOptions(ctx, bb.featureFlags, bb.Config.RootfsBlockSize())...)

	sandboxCreator := layer.NewCreateSandbox(
		baseSbxConfig,
		bb.sandboxFactory,
		baseLayerTimeout,
		sandboxOptions...,
	)

	actionExecutor := layer.NewFunctionAction(func(ctx context.Context, sbx *sandbox.Sandbox, meta metadata.Template) (metadata.Template, error) {
		err = sandboxtools.SyncChangesToDisk(
			ctx,
			bb.proxy,
			sbx.Runtime.SandboxID,
		)
		if err != nil {
			return metadata.Template{}, fmt.Errorf("error running sync command: %w", err)
		}

		return meta, nil
	})

	templateProvider := layer.NewDirectSourceTemplateProvider(localTemplate)

	baseLayer, err := bb.layerExecutor.BuildLayer(
		ctx,
		userLogger,
		layer.LayerBuildCommand{
			SourceTemplate: templateProvider,
			CurrentLayer:   baseMetadata,
			Hash:           hash,
			UpdateEnvd:     false,
			SandboxCreator: sandboxCreator,
			ActionExecutor: actionExecutor,
			BuildOrigin:    storage.ObjectOriginTemplateBuildCache,
		},
	)
	if err != nil {
		return metadata.Template{}, fmt.Errorf("error building base layer: %w", err)
	}

	return baseLayer, nil
}

func (bb *BaseBuilder) Layer(
	ctx context.Context,
	_ phases.LayerResult,
	hash string,
) (phases.LayerResult, error) {
	ctx, span := tracer.Start(ctx, "compute base", trace.WithAttributes(
		attribute.String("hash", hash),
	))
	defer span.End()

	switch {
	case bb.Config.FromTemplate != nil:
		sourceMeta := metadata.FromTemplate{
			Alias:   bb.Config.FromTemplate.GetAlias(),
			BuildID: bb.Config.FromTemplate.GetBuildID(),
		}

		// If the template is built from another template, use its metadata
		tm, err := bb.index.Cached(ctx, bb.Config.FromTemplate.GetBuildID())
		if err != nil {
			if errors.Is(err, storage.ErrObjectNotExist) {
				return phases.LayerResult{}, phases.NewPhaseBuildError(bb.Metadata(), errors.New("error getting base template, you may need to rebuild it first"))
			}

			return phases.LayerResult{}, fmt.Errorf("error getting base template: %w", err)
		}

		// From template is always cached, never needs to be built
		return phases.LayerResult{
			Metadata: tm.BasedOn(sourceMeta),
			Hash:     hash,
			Cached:   true,
		}, nil
	default:
		cmdMeta := metadata.Context{
			User:    defaultUser,
			WorkDir: nil,
			EnvVars: make(map[string]string),
		}

		meta := metadata.Template{
			Version: metadata.CurrentVersion,
			Template: metadata.TemplateMetadata{
				BuildID:            uuid.New().String(),
				KernelVersion:      bb.Config.KernelVersion,
				FirecrackerVersion: bb.Config.FirecrackerVersion,
			},
			Context:      cmdMeta,
			FromImage:    &bb.Config.FromImage,
			FromTemplate: nil,
			Start:        nil,
		}

		notCachedResult := phases.LayerResult{
			Metadata: meta,
			Cached:   false,
			Hash:     hash,
		}

		// Invalidate base cache
		if bb.Config.Force != nil && *bb.Config.Force {
			return notCachedResult, nil
		}

		bm, err := bb.index.LayerMetaFromHash(ctx, hash)
		if err != nil {
			bb.logger.Info(ctx, "base layer not found in cache, building new base layer", zap.Error(err), zap.String("hash", hash))

			return notCachedResult, nil
		}

		meta, err = bb.index.Cached(ctx, bm.Template.BuildID)
		if err != nil {
			logger.L().Info(ctx, "base layer metadata not found in cache, building new base layer", zap.Error(err), zap.String("hash", hash))

			return notCachedResult, nil
		}

		return phases.LayerResult{
			Metadata: meta,
			Cached:   true,
			Hash:     hash,
		}, nil
	}
}
