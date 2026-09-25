//go:build linux

package finalize

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/layer"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/sandboxtools"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/storage/cache"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/templates"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var finalizeTimeout = configurationTimeout + readyCommandTimeout + 5*time.Minute

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases/finalize")

type PostProcessingBuilder struct {
	buildcontext.BuildContext

	sandboxFactory  *sandbox.Factory
	templateStorage storage.StorageProvider
	proxy           *proxy.SandboxProxy

	layerExecutor *layer.LayerExecutor
	featureFlags  *featureflags.Client

	logger logger.Logger
}

func New(
	buildContext buildcontext.BuildContext,
	sandboxFactory *sandbox.Factory,
	templateStorage storage.StorageProvider,
	proxy *proxy.SandboxProxy,
	layerExecutor *layer.LayerExecutor,
	featureFlags *featureflags.Client,
	logger logger.Logger,
) *PostProcessingBuilder {
	return &PostProcessingBuilder{
		BuildContext: buildContext,

		sandboxFactory:  sandboxFactory,
		templateStorage: templateStorage,
		proxy:           proxy,

		layerExecutor: layerExecutor,
		featureFlags:  featureFlags,

		logger: logger,
	}
}

func (ppb *PostProcessingBuilder) Prefix() string {
	return "finalize"
}

func (ppb *PostProcessingBuilder) String(context.Context) (string, error) {
	return "Finalizing template build", nil
}

func (ppb *PostProcessingBuilder) Metadata() phases.PhaseMeta {
	return phases.PhaseMeta{
		Phase:    metrics.PhaseFinalize,
		StepType: "finalize",
	}
}

func (ppb *PostProcessingBuilder) Hash(_ context.Context, sourceLayer phases.LayerResult) (string, error) {
	// Include the configure script and the cert-bundle command so changes to
	// either invalidate the cached finalize layer, mirroring how the base layer
	// hashes provision.sh.
	return cache.HashKeys(sourceLayer.Hash, "config-run-cmd", configureScriptFile, packCertBundleCmd), nil
}

func (ppb *PostProcessingBuilder) Layer(
	_ context.Context,
	sourceLayer phases.LayerResult,
	hash string,
) (phases.LayerResult, error) {
	result := sourceLayer.Metadata

	// If the start/ready commands are set,
	// use them instead of start metadata from the template it is built from.
	if ppb.Config.StartCmd != "" || ppb.Config.ReadyCmd != "" {
		result.Start = &metadata.Start{
			StartCmd: ppb.Config.StartCmd,
			ReadyCmd: ppb.Config.ReadyCmd,
			Context:  result.Context,
		}
	}

	// The final template is the one from the configuration
	result.Template = metadata.TemplateMetadata{
		BuildID:            ppb.Template.BuildID,
		KernelVersion:      ppb.Config.KernelVersion,
		FirecrackerVersion: ppb.Config.FirecrackerVersion,
	}

	// Stamped from the same config as the kernel version above, because it describes
	// the same thing: how this template's kernel was booted. From here the
	// copy-constructors carry it, so every later pause of this lineage keeps it and a
	// filesystem-only cold boot can re-apply it.
	//
	// Assigned unconditionally, including the empty case. result starts as the source
	// layer's metadata, so leaving the field alone would let a value from elsewhere
	// stand as this build's — recording arguments its kernel never booted with.
	result.CmdlineArgs = nil
	if len(ppb.Config.CmdlineArgs) > 0 {
		result.CmdlineArgs = maps.Clone(ppb.Config.CmdlineArgs)
	}

	return phases.LayerResult{
		Metadata: result,
		Cached:   false,
		Hash:     hash,
	}, nil
}

// Build runs post-processing actions in the sandbox
func (ppb *PostProcessingBuilder) Build(
	ctx context.Context,
	userLogger logger.Logger,
	_ string,
	sourceLayer phases.LayerResult,
	currentLayer phases.LayerResult,
) (phases.LayerResult, error) {
	ctx, span := tracer.Start(ctx, "build final", trace.WithAttributes(
		attribute.String("hash", currentLayer.Hash),
	))
	defer span.End()

	defaultUser := new(currentLayer.Metadata.Context.User)
	defaultWorkdir := currentLayer.Metadata.Context.WorkDir

	ok, err := utils.IsGTEVersion(ppb.Version, templates.TemplateV2ReleaseVersion)
	if err != nil {
		return phases.LayerResult{}, fmt.Errorf("error checking build version: %w", err)
	}
	if !ok {
		// For older builds, always use the template default user
		// and do not set a default workdir (defaults to the user homedir).
		defaultUser = new(consts.TemplateDefaultUser)
		defaultWorkdir = nil
	}

	// Configure sandbox for final layer
	sbxConfig := sandbox.NewConfig(sandbox.Config{
		Vcpu:              ppb.Config.VCpuCount,
		RamMB:             ppb.Config.MemoryMB,
		HugePages:         ppb.Config.HugePages,
		FreePageReporting: ppb.Config.FreePageReporting,
		FreePageHinting:   ppb.Config.FreePageHinting,

		Envd: sandbox.EnvdMetadata{
			Version:        ppb.EnvdVersion,
			DefaultUser:    defaultUser,
			DefaultWorkdir: defaultWorkdir,
		},

		FirecrackerConfig: fc.Config{
			KernelVersion:      ppb.Config.KernelVersion,
			FirecrackerVersion: ppb.Config.FirecrackerVersion,
		},
	})

	// Select the IO Engine to use for the rootfs drive
	ioEngine := ppb.featureFlags.StringFlag(
		ctx,
		featureflags.BuildIoEngine,
	)

	span.SetAttributes(attribute.String("io_engine", ioEngine))
	ppb.logger.Debug(ctx, "using io engine", zap.String("io_engine", ioEngine))

	// Collect sandbox creation options
	sandboxOptions := []layer.CreateSandboxOption{
		layer.WithIoEngine(ioEngine),
	}
	if sourceLayer.Cached {
		sandboxOptions = append(sandboxOptions, layer.ReservedBlocksOptions(ctx, ppb.featureFlags, ppb.Config.RootfsBlockSize())...)
	}

	// Always restart the sandbox for the final layer to properly wire the rootfs path for the final template
	sandboxCreator := layer.NewCreateSandbox(
		sbxConfig,
		ppb.sandboxFactory,
		finalizeTimeout,
		sandboxOptions...,
	)

	actionExecutor := layer.NewFunctionAction(ppb.postProcessingFn(userLogger))

	templateProvider := layer.NewCacheSourceTemplateProvider(sourceLayer.Metadata.Template.BuildID)

	finalLayer, err := ppb.layerExecutor.BuildLayer(
		ctx,
		userLogger,
		layer.LayerBuildCommand{
			SourceTemplate: templateProvider,
			CurrentLayer:   currentLayer.Metadata,
			Hash:           currentLayer.Hash,
			UpdateEnvd:     sourceLayer.Cached,
			SandboxCreator: sandboxCreator,
			ActionExecutor: actionExecutor,
			BuildOrigin:    storage.ObjectOriginTemplateBuild,
		},
	)
	if err != nil {
		return phases.LayerResult{}, fmt.Errorf("error running start and ready commands in sandbox: %w", err)
	}

	return phases.LayerResult{
		Metadata: finalLayer,
		Cached:   false,
		Hash:     currentLayer.Hash,
	}, nil
}

func (ppb *PostProcessingBuilder) postProcessingFn(userLogger logger.Logger) layer.FunctionActionFn {
	return func(ctx context.Context, sbx *sandbox.Sandbox, meta metadata.Template) (cm metadata.Template, e error) {
		ctx, span := tracer.Start(ctx, "run postprocessing")
		defer span.End()

		defer func() {
			if e != nil {
				return
			}

			// Pack the CA cert bundle as the build's final guest step — after the
			// configuration script, start_cmd, and ready_cmd have all run — so the
			// tar that envd.service seeds the boot-time cert tmpfs from reflects the
			// final trust store. Runs before the sync below so it is flushed to disk.
			if err := packCertBundle(ctx, userLogger, ppb.proxy, sbx.Runtime.SandboxID); err != nil {
				// The gate inside packCertBundleCmd fails on user-image state;
				// unwrapped errors surface as internal and page ops.
				e = phases.NewPhaseBuildError(ppb.Metadata(), err)

				return
			}

			// Ensure all changes are synchronized to disk so the sandbox can be restarted
			err := sandboxtools.SyncChangesToDisk(
				ctx,
				ppb.proxy,
				sbx.Runtime.SandboxID,
			)
			if err != nil {
				e = fmt.Errorf("error running sync command: %w", err)

				return
			}
		}()

		// Run configuration script
		configCtx, configCancel := context.WithTimeout(ctx, configurationTimeout)
		defer configCancel()
		err := runConfiguration(
			configCtx,
			userLogger,
			ppb.BuildContext,
			ppb.proxy,
			sbx.Runtime.SandboxID,
		)
		if err != nil {
			return metadata.Template{}, phases.NewPhaseBuildError(ppb.Metadata(), fmt.Errorf("configuration script failed: %w", err))
		}

		if meta.Start == nil {
			return meta, nil
		}

		// Start command
		commandsCtx, commandsCancel := context.WithCancel(ctx)
		defer commandsCancel()

		var startCmdRun errgroup.Group
		startCmdConfirm := make(chan struct{})
		if meta.Start.StartCmd != "" {
			userLogger.Info(ctx, fmt.Sprintf("Running start command: %s", meta.Start.StartCmd))
			startCmdRun.Go(func() error {
				err := sandboxtools.RunCommandWithConfirmation(
					commandsCtx,
					ppb.proxy,
					userLogger,
					zapcore.InfoLevel,
					"start",
					sbx.Runtime.SandboxID,
					meta.Start.StartCmd,
					meta.Start.Context,
					startCmdConfirm,
				)
				// If the ctx is canceled, the ready command succeeded and no start command await is necessary.
				if err != nil && !errors.Is(err, context.Canceled) {
					// Cancel the ready command context, so the ready command does not wait anymore if an error occurs.
					commandsCancel()

					return fmt.Errorf("error running start command: %w", err)
				}

				return nil
			})
		} else {
			// If no start command is defined, we still need to confirm that the start command has started.
			close(startCmdConfirm)
		}

		// Ready command
		readyCmd := meta.Start.ReadyCmd
		if readyCmd == "" {
			if meta.Start.StartCmd == "" {
				readyCmd = "sleep 0"
			} else {
				readyCmd = GetDefaultReadyCommand(ppb.Config.StartCmdTimeout)
			}
		}
		err = ppb.runReadyCommand(
			commandsCtx,
			userLogger,
			sbx.Runtime.SandboxID,
			readyCmd,
			meta.Start.Context,
		)
		if err != nil {
			return metadata.Template{}, phases.NewPhaseBuildError(ppb.Metadata(), fmt.Errorf("ready command failed: %w", err))
		}

		// Wait for the start command to start executing.
		select {
		case <-ctx.Done():
			return metadata.Template{}, phases.NewPhaseBuildError(ppb.Metadata(), fmt.Errorf("waiting for start command failed: %w", commandsCtx.Err()))
		case <-startCmdConfirm:
		}
		// Cancel the start command context (it's running in the background anyway).
		// If it has already finished, check the error.
		commandsCancel()
		err = startCmdRun.Wait()
		if err != nil {
			return metadata.Template{}, phases.NewPhaseBuildError(ppb.Metadata(), fmt.Errorf("start command failed: %w", err))
		}

		return meta, nil
	}
}
