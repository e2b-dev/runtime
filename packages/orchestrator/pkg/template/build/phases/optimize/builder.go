//go:build linux

package optimize

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/layer"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/storage/cache"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

var prefetchTimeout = 5 * time.Minute

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases/optimize")

// prefetchIterations is the number of sandbox runs to perform for prefetch collection.
// Only pages that appear in ALL runs are included in the final prefetch mapping.
const prefetchIterations = 2

// OptimizeBuilder resumes the template, waits for envd, and captures the dirty blocks
// to store as prefetch mapping in the template metadata.
type OptimizeBuilder struct {
	buildcontext.BuildContext

	layerExecutor   *layer.LayerExecutor
	sandboxFactory  *sandbox.Factory
	templateStorage storage.StorageProvider
	templateCache   *sbxtemplate.Cache
	proxy           *proxy.SandboxProxy
	sandboxes       *sandbox.Map

	logger logger.Logger
}

func New(
	buildContext buildcontext.BuildContext,
	sandboxFactory *sandbox.Factory,
	templateStorage storage.StorageProvider,
	templateCache *sbxtemplate.Cache,
	proxy *proxy.SandboxProxy,
	layerExecutor *layer.LayerExecutor,
	sandboxes *sandbox.Map,
	logger logger.Logger,
) *OptimizeBuilder {
	return &OptimizeBuilder{
		BuildContext: buildContext,

		sandboxFactory:  sandboxFactory,
		templateStorage: templateStorage,
		templateCache:   templateCache,
		proxy:           proxy,
		layerExecutor:   layerExecutor,
		sandboxes:       sandboxes,

		logger: logger,
	}
}

func (pb *OptimizeBuilder) Prefix() string {
	return "optimize"
}

func (pb *OptimizeBuilder) String(context.Context) (string, error) {
	return "Optimizing template", nil
}

func (pb *OptimizeBuilder) Metadata() phases.PhaseMeta {
	return phases.PhaseMeta{
		Phase:    metrics.PhaseOptimize,
		StepType: "optimize",
	}
}

func (pb *OptimizeBuilder) Hash(_ context.Context, sourceLayer phases.LayerResult) (string, error) {
	// The optimize phase hash is based on the source layer hash
	// This ensures it runs after finalize but doesn't create a new cacheable layer
	return cache.HashKeys(sourceLayer.Hash, "optimize"), nil
}

func (pb *OptimizeBuilder) Layer(
	_ context.Context,
	sourceLayer phases.LayerResult,
	hash string,
) (phases.LayerResult, error) {
	// The optimize phase doesn't create a new layer, it only updates metadata
	// Return the source layer with the new hash, marked as not cached
	return phases.LayerResult{
		Metadata: sourceLayer.Metadata,
		Cached:   false,
		Hash:     hash,
	}, nil
}

// Build runs the optimize phase which:
// 1. Resumes the template from the finalize snapshot
// 2. Waits for envd to respond
// 3. Captures the dirty blocks from uffd
// 4. Updates the metadata with the prefetch mapping
// 5. Uploads the updated metadata
func (pb *OptimizeBuilder) Build(
	ctx context.Context,
	_ logger.Logger,
	_ string,
	sourceLayer phases.LayerResult,
	currentLayer phases.LayerResult,
) (phases.LayerResult, error) {
	ctx, span := tracer.Start(ctx, "build prefetch")
	defer span.End()

	pb.logger.Info(ctx, "Collecting prefetch mapping from template resume")

	// Get the template from the finalize phase
	// isSnapshot=false, isBuilding=true since we're in a build phase
	localTemplate, err := pb.templateCache.GetTemplate(ctx, sourceLayer.Metadata.Template.BuildID, false, true)
	if err != nil {
		return phases.LayerResult{}, fmt.Errorf("failed to get template from cache: %w", err)
	}

	// Resume the sandbox from the finalize snapshot
	memoryPrefetchMapping, err := pb.collectMemoryPrefetchMapping(ctx, localTemplate)
	if err != nil {
		// Log but don't fail the build - prefetch is an optimization
		pb.logger.Warn(ctx, "failed to collect prefetch mapping, continuing without prefetch",
			zap.Error(err),
		)

		return phases.LayerResult{
			Metadata: sourceLayer.Metadata,
			Cached:   false,
			Hash:     currentLayer.Hash,
		}, nil
	}

	// Update metadata with prefetch mapping
	updatedMetadata := sourceLayer.Metadata.WithPrefetch(&metadata.Prefetch{
		Memory: memoryPrefetchMapping,
	})

	// Wait for the finalize phase's async uploads (snapshot data and the
	// pre-prefetch metadata) before re-uploading metadata with the prefetch
	// mapping. Finalize uploads via the build's UploadErrGroup and returns
	// immediately, so without this wait the goroutine's metadata upload and
	// updateMetadata below race on the same metadata object for this build —
	// the later writer wins, and an interleaving that finishes the finalize
	// upload last clobbers the prefetch mapping. Waiting is also the earliest
	// point where re-publishing makes sense: a failed finalize upload means
	// the remote build is incomplete and the mapping would dangle.
	if err := pb.finalizeUploadsSettled(); err != nil {
		pb.logger.Warn(ctx, "finalize upload failed; skipping prefetch metadata publish", zap.Error(err))

		return phases.LayerResult{
			Metadata: sourceLayer.Metadata,
			Cached:   false,
			Hash:     currentLayer.Hash,
		}, nil
	}

	// Upload the updated metadata
	err = pb.updateMetadata(ctx, updatedMetadata)
	if err != nil {
		pb.logger.Warn(ctx, "failed to upload prefetch metadata, continuing without prefetch",
			zap.Error(err),
		)

		return phases.LayerResult{
			Metadata: sourceLayer.Metadata,
			Cached:   false,
			Hash:     currentLayer.Hash,
		}, nil
	}

	blockCount := 0
	if memoryPrefetchMapping != nil {
		blockCount = memoryPrefetchMapping.Count()
	}

	pb.logger.Info(ctx, "Collected prefetch mapping with memory blocks", zap.Int("block_count", blockCount))

	return phases.LayerResult{
		Metadata: updatedMetadata,
		Cached:   false,
		Hash:     currentLayer.Hash,
	}, nil
}

func (pb *OptimizeBuilder) collectMemoryPrefetchMapping(
	ctx context.Context,
	localTemplate sbxtemplate.Template,
) (*metadata.MemoryPrefetchMapping, error) {
	ctx, span := tracer.Start(ctx, "collect prefetch-mapping")
	defer span.End()

	// Configure sandbox for prefetch collection
	sbxConfig := sandbox.NewConfig(sandbox.Config{
		Vcpu:      pb.Config.VCpuCount,
		RamMB:     pb.Config.MemoryMB,
		HugePages: pb.Config.HugePages,

		Envd: sandbox.EnvdMetadata{
			Version: pb.EnvdVersion,
		},

		FirecrackerConfig: fc.Config{
			KernelVersion:      pb.Config.KernelVersion,
			FirecrackerVersion: pb.Config.FirecrackerVersion,
		},
	})

	// Create sandbox creator for resuming
	sandboxCreator := layer.NewResumeSandbox(sbxConfig, pb.sandboxFactory, prefetchTimeout)

	// Run sandbox multiple times and collect prefetch data
	var allPrefetchData []block.PrefetchData
	for i := range prefetchIterations {
		pb.logger.Debug(ctx, fmt.Sprintf("starting sandbox run %d/%d for prefetch collection", i+1, prefetchIterations))
		prefetchData, err := pb.runSandboxAndCollectPrefetch(ctx, sandboxCreator, localTemplate)
		if err != nil {
			return nil, fmt.Errorf("failed to collect prefetch data from run %d: %w", i+1, err)
		}
		allPrefetchData = append(allPrefetchData, prefetchData)
	}

	// Compute intersection with average order across all runs
	commonEntries := computeCommonPrefetchEntries(allPrefetchData)

	if len(commonEntries) == 0 {
		pb.logger.Debug(ctx, "no common blocks found for prefetch mapping")

		return nil, nil
	}

	span.SetAttributes(
		attribute.Int64("prefetch_iterations", int64(prefetchIterations)),
		attribute.Int64("prefetch_blocks_common", int64(len(commonEntries))),
		attribute.Int64("block_size", allPrefetchData[0].BlockSize),
	)

	return metadata.PrefetchEntriesToMapping(commonEntries, allPrefetchData[0].BlockSize), nil
}

// runSandboxAndCollectPrefetch runs a sandbox and collects the prefetch data.
func (pb *OptimizeBuilder) runSandboxAndCollectPrefetch(
	ctx context.Context,
	sandboxCreator *layer.ResumeSandbox,
	localTemplate sbxtemplate.Template,
) (block.PrefetchData, error) {
	sbx, err := sandboxCreator.Sandbox(ctx, pb.layerExecutor, localTemplate)
	if err != nil {
		return block.PrefetchData{}, fmt.Errorf("failed to resume sandbox: %w", err)
	}
	defer sbx.Close(ctx)

	prefetchData, err := sbx.MemoryPrefetchData(ctx)
	if err != nil {
		return block.PrefetchData{}, fmt.Errorf("failed to get prefetch data: %w", err)
	}

	return prefetchData, nil
}

// finalizeUploadsSettled waits for the finalize phase's async uploads and
// reports whether they all succeeded. The build's UploadErrGroup carries the
// finalize goroutine (snapshot data + the pre-prefetch metadata upload); the
// builder's own Wait for the same group runs only after every phase, so
// without this gate the optimize phase's metadata re-upload can interleave
// with — and be clobbered by — the finalize upload writing the same object.
// A non-nil error means the finalize upload failed; the same error
// resurfaces at the builder-level wait, so callers treat it as a soft skip.
func (pb *OptimizeBuilder) finalizeUploadsSettled() error {
	if pb.UploadErrGroup == nil {
		return nil
	}

	return pb.UploadErrGroup.Wait()
}

// updateMetadata updates the template metadata in storages.
func (pb *OptimizeBuilder) updateMetadata(ctx context.Context, t metadata.Template) error {
	err := metadata.UploadMetadata(ctx, pb.templateStorage, t, pb.BuildContext.Config.ObjectMetadata(storage.ObjectOriginTemplateBuild))
	if err != nil {
		return err
	}

	// Invalidate local cache to force refetch with updated metadata
	pb.templateCache.Invalidate(t.Template.BuildID)

	return nil
}
