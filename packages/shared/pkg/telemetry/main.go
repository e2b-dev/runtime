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
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

// lowMemoryTemporality is the OTLP "lowmemory" preference: synchronous
// counters and histograms export deltas, everything else stays cumulative.
// Asynchronous instruments already drop attribute sets a callback stops
// observing, so only the synchronous kinds need delta to forget idle ones.
func lowMemoryTemporality(kind sdkmetric.InstrumentKind) metricdata.Temporality {
	switch kind {
	case sdkmetric.InstrumentKindCounter, sdkmetric.InstrumentKindHistogram:
		return metricdata.DeltaTemporality
	default:
		return metricdata.CumulativeTemporality
	}
}

// lowMemoryCardinalityLimit lifts the attribute-set cap for the kinds
// lowMemoryTemporality exports as deltas: the SDK forgets their sets after
// every export, so the cap would only fold live series into the overflow set.
// The cumulative kinds keep the provider's limit as their guard.
func lowMemoryCardinalityLimit(kind sdkmetric.InstrumentKind) (int, bool) {
	if lowMemoryTemporality(kind) == metricdata.DeltaTemporality {
		return 0, false
	}

	return 0, true
}

type options struct {
	resourceAttributes []attribute.KeyValue
	deltaTemporality   bool
}

type Option func(*options)

// WithResourceAttributes adds attributes to the telemetry resource.
func WithResourceAttributes(attrs ...attribute.KeyValue) Option {
	return func(o *options) {
		o.resourceAttributes = append(o.resourceAttributes, attrs...)
	}
}

// WithDeltaTemporality exports synchronous counters and histograms as deltas
// and lifts the SDK's cardinality limit for those kinds only. Under cumulative
// temporality the SDK keeps every attribute set until the process exits, so a
// per-team attribute would exhaust the limit and fold new series into one
// overflow series without labels; under delta it holds only the sets recorded
// in the current export interval. The collector receiving these metrics must
// convert them back to cumulative (deltatocumulative) and must be the only
// collector receiving them, so every point of a series reaches the same one.
// A delta export that fails past the exporter's retries is lost for good,
// where a cumulative series caught up on the next success: an outage of the
// node-local collector undercounts these metrics for its duration.
func WithDeltaTemporality() Option {
	return func(o *options) {
		o.deltaTemporality = true
	}
}

// New creates a telemetry client that exports traces, metrics, and logs via gRPC.
// Telemetry is enabled when the OTEL_COLLECTOR_GRPC_ENDPOINT environment variable is set
// (e.g. "localhost:4317"). When unset, a noop client is returned with zero overhead.
func New(ctx context.Context, nodeID, serviceName, serviceCommit, serviceVersion, serviceInstanceID string, additional ...attribute.KeyValue) (*Client, error) {
	return NewWithOptions(ctx, nodeID, serviceName, serviceCommit, serviceVersion, serviceInstanceID, WithResourceAttributes(additional...))
}

// NewWithOptions is New with options; see New.
func NewWithOptions(ctx context.Context, nodeID, serviceName, serviceCommit, serviceVersion, serviceInstanceID string, opts ...Option) (*Client, error) {
	if otelCollectorGRPCEndpoint == "" {
		return NewNoopClient(), nil
	}

	var o options
	for _, opt := range opts {
		opt(&o)
	}

	// Setup metrics
	exporterOpts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithAggregationSelector(histogramAggregation)}
	var readerOpts []sdkmetric.PeriodicReaderOption
	if o.deltaTemporality {
		exporterOpts = append(exporterOpts, otlpmetricgrpc.WithTemporalitySelector(lowMemoryTemporality))
		readerOpts = append(readerOpts, sdkmetric.WithCardinalityLimitSelector(lowMemoryCardinalityLimit))
	}

	metricsExporter, err := NewMeterExporter(ctx, exporterOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics exporter: %w", err)
	}

	res, err := GetResource(ctx, nodeID, serviceName, serviceCommit, serviceVersion, serviceInstanceID, o.resourceAttributes...)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	meterProvider, err := newMeterProvider(metricsExporter, metricExportPeriod, res, readerOpts, nil)
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
