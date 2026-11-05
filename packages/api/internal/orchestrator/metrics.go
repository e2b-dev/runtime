package orchestrator

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (o *Orchestrator) setupMetrics(meterProvider metric.MeterProvider) error {
	meter := meterProvider.Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator")
	gauge, err := telemetry.GetGaugeInt(meter, telemetry.ApiOrchestratorCountMeterName)
	if err != nil {
		return fmt.Errorf("failed to create orchestrators gauge: %w", err)
	}

	_, err = telemetry.GetObservableCounter(meter, telemetry.ApiOrchestratorSbxCreateSuccess, func(_ context.Context, observer metric.Int64Observer) error {
		for _, node := range o.nodes.Items() {
			observer.Observe(int64(node.PlacementMetrics.SuccessCount()), metric.WithAttributes(
				attribute.String("node.id", node.ID),
			))
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create sandbox create success counter: %w", err)
	}

	_, err = telemetry.GetObservableCounter(meter, telemetry.ApiOrchestratorSbxCreateFailure, func(_ context.Context, observer metric.Int64Observer) error {
		for _, node := range o.nodes.Items() {
			observer.Observe(int64(node.PlacementMetrics.FailsCount()), metric.WithAttributes(
				attribute.String("node.id", node.ID),
			))
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create sandbox create failure counter: %w", err)
	}

	noCeilingGauge, err := telemetry.GetGaugeInt(meter, telemetry.ApiOrchestratorNodesWithoutHugepageCeiling)
	if err != nil {
		return fmt.Errorf("failed to create nodes without hugepage ceiling gauge: %w", err)
	}

	o.metricsRegistration, err = meter.RegisterCallback(
		func(_ context.Context, obs metric.Observer) error {
			nodes := o.nodes.Items()
			for _, node := range nodes {
				obs.ObserveInt64(gauge, 1, metric.WithAttributes(
					attribute.String("status", string(node.Status())),
					attribute.String("node.id", node.ID),
					attribute.String("backend", node.Backend),
				))
			}

			for reason, count := range nodesWithoutHugepageCeiling(nodes) {
				obs.ObserveInt64(noCeilingGauge, count, metric.WithAttributes(
					attribute.String("reason", reason),
				))
			}

			return nil
		}, gauge, noCeilingGauge)
	if err != nil {
		return fmt.Errorf("failed to register orchestrators gauge: %w", err)
	}

	if o.createdSandboxesCounter, err = telemetry.GetCounter(meter, telemetry.ApiOrchestratorCreatedSandboxes); err != nil {
		return fmt.Errorf("failed to create sandboxes counter: %w", err)
	}

	if o.resumeOriginNodeRemapCounter, err = telemetry.GetCounter(meter, telemetry.ApiOrchestratorResumeOriginNodeRemap); err != nil {
		return fmt.Errorf("failed to create resume origin node remap counter: %w", err)
	}

	if o.pauseRefusalRestoreCounter, err = telemetry.GetCounter(meter, telemetry.ApiOrchestratorPauseRefusalRestore); err != nil {
		return fmt.Errorf("failed to create pause refusal restore counter: %w", err)
	}

	if o.networkUpdateUnstoredCounter, err = telemetry.GetCounter(meter, telemetry.ApiOrchestratorNetworkUpdateUnstored); err != nil {
		return fmt.Errorf("failed to create network update unstored counter: %w", err)
	}

	if o.noCeilingPlacementsCounter, err = telemetry.GetCounter(meter, telemetry.ApiOrchestratorPlacementsWithoutHugepageCeiling); err != nil {
		return fmt.Errorf("failed to create placements without hugepage ceiling counter: %w", err)
	}

	if o.migrationOutcomesCounter, err = telemetry.GetCounter(meter, telemetry.ApiOrchestratorMigrationOutcomes); err != nil {
		return fmt.Errorf("failed to create sandbox migration outcomes counter: %w", err)
	}

	if o.listReadsCounter, err = telemetry.GetCounter(meter, telemetry.ApiSandboxListReads); err != nil {
		return fmt.Errorf("failed to create sandbox list reads counter: %w", err)
	}

	if o.listAgeHistogram, err = telemetry.GetHistogram(meter, telemetry.ApiSandboxListAge); err != nil {
		return fmt.Errorf("failed to create sandbox list age histogram: %w", err)
	}

	if o.featureUnsupportedCounter, err = telemetry.GetCounter(meter, telemetry.ApiOrchestratorSandboxFeatureUnsupported); err != nil {
		return fmt.Errorf("failed to create sandbox feature unsupported counter: %w", err)
	}

	// Observable gauge that reads sandbox counts from Redis on each collection interval.
	// This replaces the old UpDownCounter which drifted across multiple API instances.
	sandboxCountGauge, err := telemetry.GetGaugeInt(meter, telemetry.SandboxCountGaugeName)
	if err != nil {
		return fmt.Errorf("failed to create sandbox count gauge: %w", err)
	}

	o.sandboxCountGaugeRegistration, err = meter.RegisterCallback(
		func(ctx context.Context, obs metric.Observer) error {
			teams, err := o.sandboxStore.TeamsWithSandboxes(ctx)
			if err != nil {
				return fmt.Errorf("failed to get teams with sandboxes: %w", err)
			}

			for teamID, count := range teams {
				obs.ObserveInt64(sandboxCountGauge, count, metric.WithAttributes(
					telemetry.WithTeamID(teamID.String()),
				))
			}

			return nil
		},
		sandboxCountGauge,
	)
	if err != nil {
		return fmt.Errorf("failed to register sandbox count gauge: %w", err)
	}

	return nil
}

// nodesWithoutHugepageCeiling counts the ready nodes the hugepage placement
// limit cannot bound, by reason; a reason with no nodes counts 0 so its
// series stays present.
func nodesWithoutHugepageCeiling(nodes map[string]*nodemanager.Node) map[string]int64 {
	counts := map[string]int64{
		placement.NoHugepageCapacity: 0,
		placement.NoHugepagePageSize: 0,
	}

	for _, node := range nodes {
		if node.Status() != api.NodeStatusReady {
			continue
		}

		if reason := placement.MissingHugepageCeiling(node.Metrics()); reason != "" {
			counts[reason]++
		}
	}

	return counts
}
