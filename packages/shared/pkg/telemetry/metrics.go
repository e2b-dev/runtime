package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

// Keep this aligned with the collector's max_recv_msg_size_mib setting.
const otelCollectorMaxRequestSize = 100 << 20

type noopMetricExporter struct{}

func (noopMetricExporter) Temporality(sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}

func (noopMetricExporter) Aggregation(sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.AggregationDrop{}
}

func (noopMetricExporter) Export(context.Context, *metricdata.ResourceMetrics) error {
	return nil
}

func (noopMetricExporter) ForceFlush(context.Context) error {
	return nil
}

func (noopMetricExporter) Shutdown(context.Context) error {
	return nil
}

func NewMeterExporter(ctx context.Context, extraOption ...otlpmetricgrpc.Option) (sdkmetric.Exporter, error) {
	opts := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithEndpoint(otelCollectorGRPCEndpoint),
		otlpmetricgrpc.WithMaxRequestSize(otelCollectorMaxRequestSize),
	}
	opts = append(opts, extraOption...)

	metricExporter, metricErr := otlpmetricgrpc.New(
		ctx,
		opts...,
	)
	if metricErr != nil {
		return nil, fmt.Errorf("failed to create metric exporter: %w", metricErr)
	}

	return metricExporter, nil
}

// snapshotBytesView routes the per-snapshot byte histograms to a base-2
// exponential aggregation. The SDK's default explicit buckets max out at
// 10_000 (tuned for ms), which collapses byte values into +Inf and makes
// percentile queries unusable for the large byte ranges these metrics
// cover.
var snapshotBytesView = sdkmetric.NewView(
	sdkmetric.Instrument{
		Kind: sdkmetric.InstrumentKindHistogram,
		Name: "orchestrator.sandbox.snapshot.*",
		Unit: "{By}",
	},
	sdkmetric.Stream{
		Aggregation: sdkmetric.AggregationBase2ExponentialHistogram{
			MaxSize:  160,
			MaxScale: 20,
		},
	},
)

var uploadBytesView = sdkmetric.NewView(
	sdkmetric.Instrument{
		Kind: sdkmetric.InstrumentKindHistogram,
		Name: "orchestrator.sandbox.upload.*",
		Unit: "{By}",
	},
	sdkmetric.Stream{
		Aggregation: sdkmetric.AggregationBase2ExponentialHistogram{
			MaxSize:  160,
			MaxScale: 20,
		},
	},
)

func NewMeterProvider(metricsExporter sdkmetric.Exporter, metricExportPeriod time.Duration, res *resource.Resource, extraOption ...sdkmetric.Option) (*sdkmetric.MeterProvider, error) {
	return newMeterProvider(metricsExporter, metricExportPeriod, res, nil, extraOption)
}

// newMeterProvider is NewMeterProvider with options for the periodic reader
// it creates.
func newMeterProvider(metricsExporter sdkmetric.Exporter, metricExportPeriod time.Duration, res *resource.Resource, readerOptions []sdkmetric.PeriodicReaderOption, extraOption []sdkmetric.Option) (*sdkmetric.MeterProvider, error) {
	opts := []sdkmetric.Option{
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(
				metricsExporter,
				append([]sdkmetric.PeriodicReaderOption{sdkmetric.WithInterval(metricExportPeriod)}, readerOptions...)...,
			),
		),
		// Disable exemplars: they count 1:1 against the Mimir tenant items/s
		// limit and we don't query them in any dashboard. Callers can still
		// override this via extraOption since later options take precedence.
		sdkmetric.WithExemplarFilter(exemplar.AlwaysOffFilter),
	}

	if res != nil {
		opts = append(opts, sdkmetric.WithResource(res))
	}

	opts = append(opts, extraOption...)
	opts = append(opts, sdkmetric.WithView(snapshotBytesView, uploadBytesView))
	// Export these as explicit histograms so collectors can forward them to
	// Prometheus without adding them to an exponential-conversion allowlist.
	for _, name := range []HistogramType{NetworkSlotReturnDurationName, NetworkEgressRetirementDurationName} {
		opts = append(opts, sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Name: string(name), Kind: sdkmetric.InstrumentKindHistogram},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: []float64{0, 1, 10, 100, 500, 1000, 3000, 5000, 10000, 30000, 60000, 120000, 300000, 600000},
			}},
		)))
	}
	// Same reason, with sub-millisecond edges first: a healthy punch takes a few
	// µs and a contended one tens of µs, so those land in separate buckets
	// below the second-scale stalls the rest of the edges are for.
	opts = append(opts, sdkmetric.WithView(sdkmetric.NewView(
		sdkmetric.Instrument{Name: string(OrchestratorBlockCachePunchDuration), Kind: sdkmetric.InstrumentKindHistogram},
		sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
			Boundaries: []float64{0.01, 0.05, 0.25, 1, 10, 100, 500, 1000, 3000, 5000, 10000, 30000, 60000, 120000, 300000, 600000},
		}},
	)))
	// Same reason, with its own buckets: a hit's age is bounded by the lag
	// flag, a read's duration by the 70 s request timeout, and the flag is
	// tuned in the second or two the 250 ms steps resolve.
	opts = append(opts, sdkmetric.WithView(sdkmetric.NewView(
		sdkmetric.Instrument{Name: string(ApiSandboxListAge), Kind: sdkmetric.InstrumentKindHistogram},
		sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
			Boundaries: []float64{1, 10, 50, 100, 250, 500, 750, 1000, 1250, 1500, 1750, 2000, 2250, 2500, 2750, 3000, 4000, 5000, 10000, 30000, 70000},
		}},
	)))

	return sdkmetric.NewMeterProvider(opts...), nil
}
