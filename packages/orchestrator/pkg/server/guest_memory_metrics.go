//go:build linux

package server

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// guestMemorySource is the part of a sandbox the guest memory gauge reads.
type guestMemorySource interface {
	MemoryPageStates() ([block.NumStates]int64, bool)
}

var pageStateAttrs = func() (attrs [block.NumStates]metric.MeasurementOption) {
	for state := range block.NumStates {
		attrs[state] = telemetry.PrecomputeAttrs(attribute.String("page_state", block.State(state).String()))
	}

	return attrs
}()

// registerGuestMemoryGauge observes, on every collection, the guest memory of
// the sandboxes running at that moment, summed by page state. It is a node
// total: a series per sandbox would not survive the cardinality.
func registerGuestMemoryGauge[S guestMemorySource](meter metric.Meter, sandboxes func() map[string]S) error {
	gauge, err := telemetry.GetGaugeInt(meter, telemetry.OrchestratorMemoryGuestGaugeName)
	if err != nil {
		return fmt.Errorf("failed to create guest memory gauge: %w", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, obs metric.Observer) error {
		var guest [block.NumStates]int64
		for _, sbx := range sandboxes() {
			if states, ok := sbx.MemoryPageStates(); ok {
				for state, bytes := range states {
					guest[state] += bytes
				}
			}
		}

		for state, bytes := range guest {
			obs.ObserveInt64(gauge, bytes, pageStateAttrs[state])
		}

		return nil
	}, gauge)
	if err != nil {
		return fmt.Errorf("failed to register guest memory gauge: %w", err)
	}

	return nil
}
