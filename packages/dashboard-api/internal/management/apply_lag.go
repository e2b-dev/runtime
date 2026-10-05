package management

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	applyLagMetric      = "dashboard-api.management.apply_lag"
	originUnknownMetric = "dashboard-api.management.origin_unknown"
	projectionAttr      = "projection"
	instrumentationName = "github.com/e2b-dev/infra/packages/dashboard-api/internal/management"
)

type projectionKind string

const (
	projectionProjectLimits projectionKind = "project_limits"
	projectionProjectBlocks projectionKind = "project_blocks"
)

type applyLag struct {
	lag     metric.Float64Histogram
	unknown metric.Int64Counter
}

func newApplyLag(provider metric.MeterProvider) applyLag {
	meter := provider.Meter(instrumentationName)

	return applyLag{
		lag: utils.Must(meter.Float64Histogram(applyLagMetric,
			metric.WithDescription("Time from the caller deciding a projection revision to this service storing it"),
			metric.WithUnit("s"),
		)),
		unknown: utils.Must(meter.Int64Counter(originUnknownMetric,
			metric.WithDescription("Projection revisions stored without decided_at, so their apply lag is unknown"),
			metric.WithUnit("{revision}"),
		)),
	}
}

// A decided_at ahead of this host's clock records zero rather than a negative lag.
func (a applyLag) stored(ctx context.Context, projection projectionKind, decidedAt time.Time) {
	attributes := metric.WithAttributes(attribute.String(projectionAttr, string(projection)))
	if decidedAt.IsZero() {
		a.unknown.Add(ctx, 1, attributes)

		return
	}
	a.lag.Record(ctx, max(time.Since(decidedAt), 0).Seconds(), attributes)
}

func decidedAtParam(decidedAt time.Time) *time.Time {
	if decidedAt.IsZero() {
		return nil
	}

	return &decidedAt
}
