//go:build linux

package layer

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/envd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/sandboxtools"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/storage/cache"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/layer")

type LayerExecutor struct {
	buildcontext.BuildContext

	logger logger.Logger

	templateCache   *sbxtemplate.Cache
	proxy           *proxy.SandboxProxy
	sandboxes       *sandbox.Map
	templateStorage storage.StorageProvider
	buildStorage    storage.StorageProvider
	index           cache.Index
	uploads         *sandbox.Uploads
	compressConfig  storage.CompressConfig
	ff              *featureflags.Client
}

func NewLayerExecutor(
	buildContext buildcontext.BuildContext,
	logger logger.Logger,
	templateCache *sbxtemplate.Cache,
	proxy *proxy.SandboxProxy,
	sandboxes *sandbox.Map,
	templateStorage storage.StorageProvider,
	buildStorage storage.StorageProvider,
	index cache.Index,
	uploads *sandbox.Uploads,
	compressConfig storage.CompressConfig,
	ff *featureflags.Client,
) *LayerExecutor {
	return &LayerExecutor{
		BuildContext: buildContext,

		logger: logger,

		templateCache:   templateCache,
		proxy:           proxy,
		sandboxes:       sandboxes,
		templateStorage: templateStorage,
		buildStorage:    buildStorage,
		index:           index,
		uploads:         uploads,
		compressConfig:  compressConfig,
		ff:              ff,
	}
}

// BuildLayer orchestrates the layer building process
func (lb *LayerExecutor) BuildLayer(
	ctx context.Context,
	userLogger logger.Logger,
	cmd LayerBuildCommand,
) (metadata.Template, error) {
	ctx, childSpan := tracer.Start(ctx, "run-in-sandbox")
	defer childSpan.End()

	localTemplate, releaseTemplate, err := cmd.SourceTemplate.Get(ctx, lb.templateCache)
	// Deferred first so it runs last, after the sandbox built on the template
	// has been closed.
	defer releaseTemplate()
	if err != nil {
		return metadata.Template{}, fmt.Errorf("get template snapshot: %w", err)
	}

	// Create or resume sandbox
	sbx, err := cmd.SandboxCreator.Sandbox(ctx, lb, localTemplate)
	if err != nil {
		return metadata.Template{}, err
	}
	defer sbx.Close(ctx)

	defer func() {
		closeErr := lb.proxy.RemoveFromPool(sbx.LifecycleID)
		if closeErr != nil {
			// Errors here will be from forcefully closing the connections, so we can ignore them—they will at worst timeout on their own.
			lb.logger.Warn(ctx, "errors when manually closing connections to sandbox", zap.Error(closeErr))
		} else {
			lb.logger.Debug(
				ctx,
				"removed proxy from pool",
				logger.WithSandboxID(sbx.Runtime.SandboxID),
				logger.WithExecutionID(sbx.Runtime.ExecutionID),
			)
		}
	}()

	// Update envd binary to the latest version
	if cmd.UpdateEnvd {
		err = lb.updateEnvdInSandbox(ctx, userLogger, sbx)
		if err != nil {
			lb.logger.Error(
				ctx,
				"error updating envd",
				logger.WithSandboxID(sbx.Runtime.SandboxID),
				logger.WithExecutionID(sbx.Runtime.ExecutionID),
				zap.Error(err),
			)

			return metadata.Template{}, fmt.Errorf("update envd: %w", err)
		}
	}

	// Execute the action using the executor
	meta, err := cmd.ActionExecutor.Execute(ctx, sbx, cmd.CurrentLayer)
	if err != nil {
		lb.logger.Error(
			ctx,
			"error executing action",
			logger.WithSandboxID(sbx.Runtime.SandboxID),
			logger.WithExecutionID(sbx.Runtime.ExecutionID),
			zap.Error(err),
		)

		return metadata.Template{}, err
	}

	// Prepare metadata
	meta = meta.NewVersionTemplate(metadata.TemplateMetadata{
		BuildID:            cmd.CurrentLayer.Template.BuildID,
		KernelVersion:      sbx.Config.FirecrackerConfig.KernelVersion,
		FirecrackerVersion: sbx.Config.FirecrackerConfig.FirecrackerVersion,
	})
	err = lb.PauseAndUpload(
		ctx,
		userLogger,
		sbx,
		cmd.Hash,
		meta,
		cmd.BuildOrigin,
	)
	if err != nil {
		return metadata.Template{}, fmt.Errorf("pause and upload: %w", err)
	}

	return meta, nil
}

// updateEnvdInSandbox updates the envd binary in the sandbox to the latest version.
func (lb *LayerExecutor) updateEnvdInSandbox(
	ctx context.Context,
	userLogger logger.Logger,
	sbx *sandbox.Sandbox,
) error {
	ctx, childSpan := tracer.Start(ctx, "update-envd")
	defer childSpan.End()

	envdVersion, err := envd.GetEnvdVersion(ctx, lb.BuilderConfig.HostEnvdPath)
	if err != nil {
		return fmt.Errorf("error getting envd version: %w", err)
	}
	userLogger.Debug(ctx, fmt.Sprintf("Updating envd to version v%s", envdVersion))

	// Step 1: Copy the updated envd binary from host to /tmp in sandbox
	tmpEnvdPath := "/tmp/envd_updated"
	err = sandboxtools.CopyFile(
		ctx,
		lb.proxy,
		sbx.Runtime.SandboxID,
		"root",
		lb.BuilderConfig.HostEnvdPath,
		tmpEnvdPath,
	)
	if err != nil {
		return fmt.Errorf("failed to copy envd binary to sandbox: %w", err)
	}

	// Step 2: Replace the binary
	replaceEnvdCmd := fmt.Sprintf(`
		# Replace the binary and set permissions
		chmod +x %s
		mv -f %s %s
	`, tmpEnvdPath, tmpEnvdPath, storage.GuestEnvdPath)

	err = sandboxtools.RunCommandWithLogger(
		ctx,
		lb.proxy,
		userLogger,
		zap.DebugLevel,
		"update-envd-replace",
		sbx.Runtime.SandboxID,
		replaceEnvdCmd,
		metadata.Context{User: "root"},
	)
	if err != nil {
		return fmt.Errorf("failed to replace envd binary: %w", err)
	}

	// Step 3: Restart the envd service. The error is ignored because the
	// restart kills the very envd this command runs through — the connection
	// loss is expected. systemctl hands the restart to PID1, which survives
	// that. On OpenRC there is no PID1 handoff: rc-service would run in this
	// very shell and die with envd between its stop and start halves
	// (observed on Alpine — envd never came back), so instead envd is simply
	// killed and supervise-daemon's respawn starts the replaced binary; the
	// immediate death also means the post-update wait below can never mistake
	// the old instance for the new one.
	_ = sandboxtools.RunCommand(
		ctx,
		lb.proxy,
		sbx.Runtime.SandboxID,
		`if command -v systemctl >/dev/null 2>&1; then
    systemctl restart envd
else
    kill -TERM "$(pidof envd)"
fi`,
		metadata.Context{User: "root"},
	)

	// Remove the proxy client to prevent reuse of broken connection, because we restarted envd server inside of the sandbox.
	// This might not be necessary if we don't use keepalives for the proxy.
	err = lb.proxy.RemoveFromPool(sbx.LifecycleID)
	if err != nil {
		// Errors here will be from forcefully closing the connections, so we can ignore them—they will at worst timeout on their own.
		lb.logger.Warn(ctx, "errors when manually closing connections to sandbox after restarting envd", zap.Error(err))
	}

	lb.logger.Debug(
		ctx,
		"removed proxy from pool after restarting envd",
		logger.WithSandboxID(sbx.Runtime.SandboxID),
		logger.WithExecutionID(sbx.Runtime.ExecutionID),
	)

	// Step 4: Wait for envd to initialize
	err = sbx.WaitForEnvd(
		ctx,
		sandbox.StartTypeCreate,
		waitEnvdTimeout,
	)
	if err != nil {
		return fmt.Errorf("failed to wait for envd initialization after update: %w", err)
	}

	return nil
}

func (lb *LayerExecutor) PauseAndUpload(
	ctx context.Context,
	userLogger logger.Logger,
	sbx *sandbox.Sandbox,
	hash string,
	meta metadata.Template,
	buildOrigin storage.ObjectOrigin,
) (e error) {
	ctx, childSpan := tracer.Start(ctx, "pause-and-upload")
	defer childSpan.End()

	// snapshot is automatically cleared by the templateCache eviction
	snapshot, err := sbx.Pause(
		ctx,
		meta,
		sandbox.SnapshotUseCaseBuild,
	)
	if err != nil {
		return fmt.Errorf("error processing vm: %w", err)
	}

	return lb.UploadSnapshot(ctx, userLogger, snapshot, hash, meta, buildOrigin)
}

// UploadSnapshot caches and uploads a snapshot produced without a running VM.
func (lb *LayerExecutor) UploadSnapshot(
	ctx context.Context,
	userLogger logger.Logger,
	snapshot *sandbox.Snapshot,
	hash string,
	meta metadata.Template,
	buildOrigin storage.ObjectOrigin,
) error {
	userLogger.Debug(ctx, fmt.Sprintf("Adding layer to cache: %s", meta.Template.BuildID))

	// Add snapshot to template cache so it can be used immediately
	finishUpload, err := lb.templateCache.AddSnapshot(
		context.WithoutCancel(ctx),
		meta.Template.BuildID,
		sbxtemplate.SnapshotLineage{Origin: buildOrigin},
		snapshot.MemorySnapshot.DiffHeader,
		snapshot.RootfsDiffHeader,
		snapshot.Snapfile,
		snapshot.Metafile,
		snapshot.MemorySnapshot.Diff,
		snapshot.RootfsDiff,
		snapshot.MemorySnapshot.ProvisionalDiffHeader,
		snapshot.MemorySnapshot.ProvisionalDiff,
		snapshot.MemorySnapshot.ProvisionalCreatedAt,
		snapshot.MemorySnapshot.ProvisionalSwapDone,
	)
	if err != nil {
		err = errors.Join(err, snapshot.Close(context.WithoutCancel(ctx)))

		return fmt.Errorf("error adding snapshot to template cache: %w", err)
	}

	// Upload snapshot async, it's added to the template cache immediately
	userLogger.Debug(ctx, fmt.Sprintf("Uploading layer: %s", meta.Template.BuildID))

	objectMetadata := lb.BuildContext.Config.ObjectMetadata(buildOrigin)

	upload, err := sandbox.NewUpload(ctx, lb.uploads, snapshot, lb.templateStorage, lb.compressConfig, lb.ff, storage.UseCaseBuild, objectMetadata, finishUpload)
	if err != nil {
		return fmt.Errorf("register upload: %w", err)
	}

	lb.UploadErrGroup.Go(func() (uploadErr error) {
		ctx := context.WithoutCancel(ctx)
		ctx, span := tracer.Start(ctx, "upload snapshot", trace.WithAttributes(
			telemetry.WithTemplateID(lb.Config.TemplateID),
			telemetry.WithBuildID(lb.Template.BuildID),
			telemetry.WithTeamID(lb.Config.TeamID),
			attribute.String("layer.build_id", meta.Template.BuildID),
			attribute.String("layer.hash", hash),
		))
		defer func() {
			if uploadErr != nil {
				span.RecordError(uploadErr)
				span.SetStatus(codes.Error, uploadErr.Error())
				lb.logger.Error(ctx, "template layer snapshot upload failed",
					logger.WithTemplateID(lb.Config.TemplateID),
					logger.WithBuildID(lb.Template.BuildID),
					logger.WithTeamID(lb.Config.TeamID),
					zap.String("layer_build_id", meta.Template.BuildID),
					zap.String("layer_hash", hash),
					zap.Error(uploadErr),
				)
			}

			span.End()
		}()

		// Signal even on error so child layers waiting on this build can abort.
		defer func() { endLayerUpload(ctx, upload, uploadErr) }()

		if err := upload.Run(ctx); err != nil {
			return fmt.Errorf("error uploading snapshot: %w", err)
		}

		// Publish the recipe hash only after every artifact has been uploaded. A
		// cache lookup can then resolve the hash to this build ID without observing
		// a snapshot whose header, metadata, or data is still missing.
		if err := lb.index.SaveLayerMeta(ctx, hash, cache.LayerMetadata{
			Template: cache.Template{
				BuildID: meta.Template.BuildID,
			},
		}); err != nil {
			// Since the data should be basically identical, this is safe to skip.
			if !errors.Is(err, storage.ErrObjectRateLimited) {
				return fmt.Errorf("error saving UUID to hash mapping: %w", err)
			}

			logger.L().Warn(ctx, "rate limited writing layer metadata to object, skipping",
				logger.WithBuildID(meta.Template.BuildID),
				zap.String("hash", hash),
				zap.Error(err),
			)
		}

		userLogger.Debug(ctx, fmt.Sprintf("Saved: %s", meta.Template.BuildID))

		return nil
	})

	return nil
}

// endLayerUpload ends a build layer's upload, returning its template cache pin.
// A failed layer fails its build, so nothing will read it: it is abandoned
// rather than finished.
func endLayerUpload(ctx context.Context, upload *sandbox.Upload, uploadErr error) {
	if uploadErr != nil {
		upload.Abandon(ctx, uploadErr)

		return
	}
	upload.Finish(ctx, nil)
}
