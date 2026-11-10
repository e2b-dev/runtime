//go:build linux

package phases

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

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases")

type PhaseMeta struct {
	Phase      metrics.Phase
	StepType   string
	StepNumber *int
}

type BuilderPhase interface {
	Prefix() string
	String(ctx context.Context) (string, error)
	Metadata() PhaseMeta

	Hash(ctx context.Context, sourceLayer LayerResult) (string, error)
	Layer(ctx context.Context, sourceLayer LayerResult, hash string) (LayerResult, error)
	Build(ctx context.Context, userLogger logger.Logger, prefix string, sourceLayer LayerResult, currentLayer LayerResult) (LayerResult, error)
}

type LayerResult struct {
	Metadata metadata.Template
	Cached   bool
	Hash     string
}

func layerInfo(
	cached bool,
	prefix string,
	text string,
	hash string,
) string {
	cachedPrefix := ""
	if cached {
		cachedPrefix = "CACHED "
	}

	return fmt.Sprintf("%s[%s] %s [%s]", cachedPrefix, prefix, text, hash)
}

func Run(
	ctx context.Context,
	logger logger.Logger,
	userLogger logger.Logger,
	bc buildcontext.BuildContext,
	metrics *metrics.BuildMetrics,
	builders []BuilderPhase,
) (LayerResult, error) {
	ctx, span := tracer.Start(ctx, "run phases", trace.WithAttributes(
		attribute.Int("builders", len(builders)),
		telemetry.WithBuildID(bc.Template.BuildID),
		telemetry.WithTemplateID(bc.Config.TemplateID),
	))
	defer span.End()

	sourceLayer := LayerResult{}

	for _, builder := range builders {
		res, err := runPhase(ctx, logger, userLogger, metrics, bc.Config.TeamID, builder, sourceLayer)
		if err != nil {
			return LayerResult{}, err
		}

		sourceLayer = res
	}

	return sourceLayer, nil
}

// runPhase executes a single build phase (hash, cache lookup, and build) within
// its own span so every phase, including cache hits, produces one span.
func runPhase(
	ctx context.Context,
	logger logger.Logger,
	userLogger logger.Logger,
	metrics *metrics.BuildMetrics,
	teamID string,
	builder BuilderPhase,
	sourceLayer LayerResult,
) (_ LayerResult, e error) {
	meta := builder.Metadata()

	ctx, span := tracer.Start(ctx, string(meta.Phase), trace.WithAttributes(
		attribute.String("phase", string(meta.Phase)),
		attribute.String("step_type", meta.StepType),
		attribute.String("step", stepString(meta)),
	))
	if meta.StepNumber != nil {
		span.SetAttributes(attribute.Int("step_number", *meta.StepNumber))
	}
	defer span.End()
	defer func() {
		if e != nil {
			span.RecordError(e)
			span.SetStatus(codes.Error, e.Error())
		}
	}()

	loggerFields := []zap.Field{
		zap.String("phase", string(meta.Phase)),
		zap.String("step_type", meta.StepType),
		zap.Intp("step_number", meta.StepNumber),
		zap.String("step", stepString(meta)),
	}

	logger.Debug(ctx, "running builder phase", loggerFields...)
	stepUserLogger := userLogger.With(loggerFields...)

	phaseStartTime := time.Now()
	hash, err := builder.Hash(ctx, sourceLayer)
	if err != nil {
		return LayerResult{}, fmt.Errorf("getting hash: %w", err)
	}

	currentLayer, err := builder.Layer(ctx, sourceLayer, hash)
	if err != nil {
		return LayerResult{}, fmt.Errorf("getting layer: %w", err)
	}
	metrics.RecordCacheResult(ctx, teamID, meta.Phase, meta.StepType, currentLayer.Cached)
	span.SetAttributes(attribute.Bool("cached", currentLayer.Cached))

	prefix := builder.Prefix()
	source, err := builder.String(ctx)
	if err != nil {
		return LayerResult{}, fmt.Errorf("getting source: %w", err)
	}
	stepUserLogger.Info(ctx, layerInfo(currentLayer.Cached, prefix, source, currentLayer.Hash))

	if currentLayer.Cached {
		phaseDuration := time.Since(phaseStartTime)
		metrics.RecordPhaseDuration(ctx, teamID, phaseDuration, meta.Phase, meta.StepType, true)

		return currentLayer, nil
	}

	err = validateLayer(currentLayer)
	if err != nil {
		return LayerResult{}, fmt.Errorf("validating layer: %w", err)
	}

	res, err := builder.Build(ctx, stepUserLogger, prefix, sourceLayer, currentLayer)
	// Record phase duration
	phaseDuration := time.Since(phaseStartTime)
	metrics.RecordPhaseDuration(ctx, teamID, phaseDuration, meta.Phase, meta.StepType, false)

	if err != nil {
		return LayerResult{}, err
	}

	return res, nil
}

func validateLayer(
	layer LayerResult,
) (err error) {
	if layer.Hash == "" {
		err = errors.Join(err, errors.New("layer hash is empty"))
	}

	return errors.Join(err, validateMetadata(layer.Metadata))
}

func validateMetadata(
	meta metadata.Template,
) (err error) {
	return errors.Join(
		validateTemplate(meta.Template),
		validateContext(meta.Context),
	)
}

func validateTemplate(
	metadata metadata.TemplateMetadata,
) (err error) {
	if metadata.BuildID == "" {
		err = errors.Join(err, errors.New("template build ID is empty"))
	}
	if metadata.KernelVersion == "" {
		err = errors.Join(err, errors.New("template kernel version is empty"))
	}
	if metadata.FirecrackerVersion == "" {
		err = errors.Join(err, errors.New("template firecracker version is empty"))
	}

	return err
}

func validateContext(
	context metadata.Context,
) (err error) {
	if context.User == "" {
		err = errors.Join(err, errors.New("context user is empty"))
	}
	if context.WorkDir != nil && *context.WorkDir == "" {
		err = errors.Join(err, errors.New("context working dir is empty"))
	}

	return err
}
