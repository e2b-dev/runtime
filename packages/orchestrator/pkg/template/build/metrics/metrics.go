package metrics

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// Phase represents a build phase
type Phase string

const (
	PhaseBase       Phase = "base"
	PhaseSteps      Phase = "steps"
	PhaseResizeDisk Phase = "resize-disk"
	PhaseFinalize   Phase = "finalize"
	PhaseOptimize   Phase = "optimize"
)

// BuildResultType represents the type of build result
type BuildResultType string

const (
	BuildResultSuccess       BuildResultType = "success"
	BuildResultUserError     BuildResultType = "user_error"
	BuildResultInternalError BuildResultType = "internal_error"
)

// BuildResultAttributeKey is the span attribute that carries the same value as
// the `result` label on template_build_result_total, so a trace can be filtered
// by classification (e.g. TraceQL `{ span.build.result = "internal_error" }`)
// without relying on which span happened to get an Error status.
const BuildResultAttributeKey = attribute.Key("build.result")

// BuildResultAttribute returns the build.result span attribute for resultType.
func BuildResultAttribute(resultType BuildResultType) attribute.KeyValue {
	return BuildResultAttributeKey.String(string(resultType))
}

// BuildMetrics contains all metrics related to template building
type BuildMetrics struct {
	// Duration histograms
	BuildDurationHistogram      metric.Int64Histogram
	BuildPhaseDurationHistogram metric.Int64Histogram
	BuildStepDurationHistogram  metric.Int64Histogram

	// Result counters
	BuildResultCounter      metric.Int64Counter
	BuildCacheResultCounter metric.Int64Counter

	// Resource histograms
	BuildRootfsSizeHistogram metric.Int64Histogram
}

// NewBuildMetrics creates a new BuildMetrics instance
func NewBuildMetrics(meterProvider metric.MeterProvider) (*BuildMetrics, error) {
	meter := meterProvider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/metrics")

	buildDurationHistogram, err := telemetry.GetHistogram(meter, telemetry.BuildDurationHistogramName)
	if err != nil {
		return nil, fmt.Errorf("failed to create build duration histogram: %w", err)
	}

	buildPhaseDurationHistogram, err := telemetry.GetHistogram(meter, telemetry.BuildPhaseDurationHistogramName)
	if err != nil {
		return nil, fmt.Errorf("failed to create build phase duration histogram: %w", err)
	}

	buildStepDurationHistogram, err := telemetry.GetHistogram(meter, telemetry.BuildStepDurationHistogramName)
	if err != nil {
		return nil, fmt.Errorf("failed to create build step duration histogram: %w", err)
	}

	buildResultCounter, err := telemetry.GetCounter(meter, telemetry.BuildResultCounterName)
	if err != nil {
		return nil, fmt.Errorf("failed to create build result counter: %w", err)
	}

	buildCacheResultCounter, err := telemetry.GetCounter(meter, telemetry.BuildCacheResultCounterName)
	if err != nil {
		return nil, fmt.Errorf("failed to create build cache result counter: %w", err)
	}

	buildRootfsSizeHistogram, err := telemetry.GetHistogram(meter, telemetry.BuildRootfsSizeHistogramName)
	if err != nil {
		return nil, fmt.Errorf("failed to create build rootfs size histogram: %w", err)
	}

	return &BuildMetrics{
		BuildDurationHistogram:      buildDurationHistogram,
		BuildPhaseDurationHistogram: buildPhaseDurationHistogram,
		BuildStepDurationHistogram:  buildStepDurationHistogram,
		BuildResultCounter:          buildResultCounter,
		BuildCacheResultCounter:     buildCacheResultCounter,
		BuildRootfsSizeHistogram:    buildRootfsSizeHistogram,
	}, nil
}

// RecordBuildDuration records the total build duration
func (m *BuildMetrics) RecordBuildDuration(ctx context.Context, teamID string, duration time.Duration, success bool) {
	attrs := []attribute.KeyValue{
		attribute.Bool("success", success),
		telemetry.WithTeamID(teamID),
	}
	m.BuildDurationHistogram.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
}

// RecordPhaseDuration records the duration of a build phase
func (m *BuildMetrics) RecordPhaseDuration(ctx context.Context, teamID string, duration time.Duration, phase Phase, stepType string, cached bool) {
	attrs := []attribute.KeyValue{
		attribute.String("phase", string(phase)),
		attribute.String("step_type", stepType),
		attribute.Bool("cached", cached),
		telemetry.WithTeamID(teamID),
	}
	m.BuildPhaseDurationHistogram.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
}

// RecordBuildResult records the result of a build (success, user_error, or internal_error),
// the vCPU sizing it requested and whether it built a resizable template.
func (m *BuildMetrics) RecordBuildResult(ctx context.Context, teamID string, resultType BuildResultType, vcpuSizing, built string) {
	attrs := []attribute.KeyValue{
		attribute.String("result", string(resultType)),
		attribute.String("vcpu_sizing", vcpuSizing),
		attribute.String("built", built),
		telemetry.WithTeamID(teamID),
	}
	m.BuildResultCounter.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordCacheResult records the result of a cache lookup (hit or miss)
func (m *BuildMetrics) RecordCacheResult(ctx context.Context, teamID string, phase Phase, stepType string, hit bool) {
	attrs := []attribute.KeyValue{
		attribute.String("phase", string(phase)),
		attribute.String("step_type", stepType),
		attribute.Bool("hit", hit),
		telemetry.WithTeamID(teamID),
	}
	m.BuildCacheResultCounter.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordRootfsSize records the rootfs size
func (m *BuildMetrics) RecordRootfsSize(ctx context.Context, teamID string, size int64) {
	m.BuildRootfsSizeHistogram.Record(ctx, size, metric.WithAttributes(telemetry.WithTeamID(teamID)))
}
