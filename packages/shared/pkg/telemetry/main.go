package telemetry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	noopMetric "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	noopTrace "go.opentelemetry.io/otel/trace/noop"
)

const metricExportPeriod = 15 * time.Second

type Client struct {
	MetricExporter  sdkmetric.Exporter
	MeterProvider   metric.MeterProvider
	shutdownMeters  func(ctx context.Context) error
	SpanExporter    sdktrace.SpanExporter
	TracerProvider  trace.TracerProvider
	TracePropagator propagation.TextMapPropagator
	LogsProvider    LogProvider
}

// histogramAggregation exports every histogram as base-2 exponential, whose
// buckets adapt to the data instead of the SDK default boundaries that stop at
// 10s. It also means metric.WithExplicitBucketBoundaries on an instrument is
// discarded — that advice only survives when the reader default is an
// explicit-bucket aggregation. Override per metric with a View, not with
// boundaries.
//
// Sizing from https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/sdk.md#base2-exponential-bucket-histogram-aggregation
func histogramAggregation(kind sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	if kind == sdkmetric.InstrumentKindHistogram {
		return sdkmetric.AggregationBase2ExponentialHistogram{
			MaxSize:  160,
			MaxScale: 20,
			NoMinMax: false,
		}
	}

	return sdkmetric.DefaultAggregationSelector(kind)
}

// New creates a telemetry client that exports traces, metrics, and logs via gRPC.
// Telemetry is enabled when the OTEL_COLLECTOR_GRPC_ENDPOINT environment variable is set
// (e.g. "localhost:4317"). When unset, a noop client is returned with zero overhead.
func New(ctx context.Context, nodeID, serviceName, serviceCommit, serviceVersion, serviceInstanceID string, additional ...attribute.KeyValue) (*Client, error) {
	if otelCollectorGRPCEndpoint == "" {
		return NewNoopClient(), nil
	}

	// Setup metrics
	metricsExporter, err := NewMeterExporter(ctx, otlpmetricgrpc.WithAggregationSelector(histogramAggregation))
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics exporter: %w", err)
	}

	res, err := GetResource(ctx, nodeID, serviceName, serviceCommit, serviceVersion, serviceInstanceID, additional...)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	meterProvider, err := NewMeterProvider(metricsExporter, metricExportPeriod, res)
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics provider: %w", err)
	}
	otel.SetMeterProvider(meterProvider)

	// Setup logging
	logProvider, err := NewLogProvider(ctx, res)
	if err != nil {
		return nil, fmt.Errorf("failed to create log provider: %w", err)
	}

	// Setup tracing
	spanExporter, err := NewSpanExporter(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create span exporter: %w", err)
	}

	tracerProvider := NewTracerProvider(spanExporter, res)
	otel.SetTracerProvider(tracerProvider)

	// There's probably not a reason why not to set the trace propagator globally, it's used in SDKs
	propagator := NewTextPropagator()
	otel.SetTextMapPropagator(propagator)

	return &Client{
		MetricExporter:  metricsExporter,
		MeterProvider:   meterProvider,
		shutdownMeters:  meterProvider.Shutdown,
		SpanExporter:    spanExporter,
		TracerProvider:  tracerProvider,
		TracePropagator: propagator,
		LogsProvider:    logProvider,
	}, nil
}

// NewAnonymous creates a telemetry client for tools and CLI commands that don't
// have build-time injected metadata (commitSHA, version, nodeID).
// serviceName is the primary identifier used for filtering traces and metrics
// in observability tools (e.g. Grafana). The remaining resource attributes
// are filled with sensible defaults (hostname, "unknown" commit, "dev" version).
func NewAnonymous(ctx context.Context, serviceName string) (*Client, error) {
	nodeID, _ := os.Hostname()
	if nodeID == "" {
		nodeID = "unknown"
	}

	return New(ctx, nodeID, serviceName, "unknown", "dev", uuid.NewString())
}

func (t *Client) Shutdown(ctx context.Context) error {
	var errs []error

	// The provider's shutdown cancels an export already in flight, then collects
	// and exports once more under ctx before closing the exporter. ForceFlush
	// exports on the reader's own context instead, and the exporter's Shutdown
	// waits for that export, so neither would be bounded by ctx.
	if err := t.shutdownMeters(ctx); err != nil {
		errs = append(errs, err)
	}
	if t.SpanExporter != nil {
		if err := t.SpanExporter.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if t.LogsProvider != nil {
		if err := t.LogsProvider.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func NewNoopClient() *Client {
	return &Client{
		MetricExporter:  &noopMetricExporter{},
		MeterProvider:   noopMetric.MeterProvider{},
		shutdownMeters:  func(context.Context) error { return nil },
		SpanExporter:    &noopSpanExporter{},
		TracerProvider:  noopTrace.NewTracerProvider(),
		TracePropagator: propagation.NewCompositeTextMapPropagator(),
		LogsProvider:    NewNoopLogProvider(),
	}
}
