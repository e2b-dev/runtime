package proxy

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	parkingOutcomeServed          = "served"
	parkingOutcomeBudgetExhausted = "budget_exhausted"
	parkingOutcomeCanceled        = "canceled"
	parkingOutcomeError           = "error"
)

// parkingLot bounds how many callers wait on retried resumes at once; a caller
// is admitted only after its flight has failed retryably.
type parkingLot struct {
	active   atomic.Int64
	rejected metric.Int64Counter
	wait     metric.Int64Histogram
}

func newParkingLot(meter metric.Meter) (*parkingLot, error) {
	lot := &parkingLot{}

	_, err := telemetry.GetObservableUpDownCounter(
		meter, telemetry.ClientProxyResumeParkingActiveName, func(_ context.Context, observer metric.Int64Observer) error {
			observer.Observe(lot.active.Load())

			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error registering client proxy parking active metric (%s): %w", telemetry.ClientProxyResumeParkingActiveName, err)
	}

	lot.rejected, err = telemetry.GetCounter(meter, telemetry.ClientProxyResumeParkingRejectedCounterName)
	if err != nil {
		return nil, fmt.Errorf("error registering client proxy parking rejected metric (%s): %w", telemetry.ClientProxyResumeParkingRejectedCounterName, err)
	}

	lot.wait, err = telemetry.GetHistogram(meter, telemetry.ClientProxyResumeParkingWaitDurationName)
	if err != nil {
		return nil, fmt.Errorf("error registering client proxy parking wait metric (%s): %w", telemetry.ClientProxyResumeParkingWaitDurationName, err)
	}

	return lot, nil
}

func (l *parkingLot) tryEnter(size int) bool {
	for {
		active := l.active.Load()
		if active >= int64(size) {
			return false
		}

		if l.active.CompareAndSwap(active, active+1) {
			return true
		}
	}
}

func (l *parkingLot) leave(ctx context.Context, parkedAt time.Time, outcome string) {
	l.active.Add(-1)
	l.wait.Record(ctx, time.Since(parkedAt).Milliseconds(), metric.WithAttributes(attribute.String("outcome", outcome)))
}

func (l *parkingLot) reject(ctx context.Context) {
	l.rejected.Add(ctx, 1)
}

func parkingOutcome(err error) string {
	var exhausted *resumeBudgetExhaustedError

	switch {
	case err == nil:
		return parkingOutcomeServed
	case errors.Is(err, errFlightClosed):
		return parkingOutcomeCanceled
	case errors.As(err, &exhausted):
		return parkingOutcomeBudgetExhausted
	default:
		return parkingOutcomeError
	}
}
