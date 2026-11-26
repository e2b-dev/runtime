package placement

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// sbxRequestWithBuild is testSbxRequest with a build_id, the field the
// affinity path keys off.
func sbxRequestWithBuild(id, buildID string) *orchestrator.SandboxCreateRequest {
	req := testSbxRequest(id)
	req.Sandbox.BuildId = buildID

	return req
}

// placeOn forces placement onto node, so an observability test controls which
// node takes the sandbox without depending on the sampler.
func placeOn(node *nodemanager.Node) stubAlgorithm {
	return stubAlgorithm{choose: func(map[string]struct{}) (*nodemanager.Node, error) {
		return node, nil
	}}
}

// swapAffinityMeters points both affinity instruments at a fresh ManualReader
// for the duration of a test and returns it, restoring the originals after.
func swapAffinityMeters(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement")

	counter, err := telemetry.GetCounter(meter, telemetry.ApiOrchestratorTemplateAffinityPlacements)
	require.NoError(t, err)
	hist, err := telemetry.GetHistogram(meter, telemetry.ApiOrchestratorTemplateAffinitySiblings)
	require.NoError(t, err)

	prevCounter, prevHist := templateAffinityPlacements, templateAffinitySiblings
	templateAffinityPlacements = counter
	templateAffinitySiblings = hist
	t.Cleanup(func() {
		templateAffinityPlacements = prevCounter
		templateAffinitySiblings = prevHist
	})

	return reader
}

// collectAffinity reads the effect counter and the siblings histogram back out
// of reader: the counter as effect to count, the histogram as its single
// recorded sum and point count.
func collectAffinity(t *testing.T, reader *sdkmetric.ManualReader) (byEffect map[string]int64, histSum int64, histCount uint64) {
	t.Helper()

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &collected))

	byEffect = map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range data.DataPoints {
					effect, _ := p.Attributes.Value("effect")
					byEffect[effect.AsString()] += p.Value
				}
			case metricdata.Histogram[int64]:
				for _, p := range data.DataPoints {
					histSum += p.Sum
					histCount += p.Count
				}
			}
		}
	}

	return byEffect, histSum, histCount
}

// A placement onto a node already running same-build siblings is recorded
// colocated, and the siblings histogram carries the count the node held.
func TestRecordTemplateAffinity_Colocated(t *testing.T) {
	reader := swapAffinityMeters(t)

	node := nodemanager.NewTestNode("node", api.NodeStatusReady, 0, 64, emptyHugePool())
	// Two same-build siblings already in flight on the node.
	for _, id := range []string{"sib-1", "sib-2"} {
		node.PlacementMetrics.StartPlacingIf(id, nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512, HugePages: true, BuildID: "build-hot"}, func() bool { return true })
	}
	nodes := []*nodemanager.Node{node}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, err := placeSandbox(ctx, placeOn(node), nodes, nil, sbxRequestWithBuild("sbx", "build-hot"),
		Requirements{}, false, nil, refusalBackoff{}, time.Time{})
	require.NoError(t, err)

	byEffect, histSum, histCount := collectAffinity(t, reader)
	assert.Equal(t, int64(1), byEffect["colocated"])
	assert.Equal(t, int64(0), byEffect["spread"])
	assert.Equal(t, uint64(1), histCount, "one placement recorded")
	assert.Equal(t, int64(2), histSum, "the two in-flight siblings the node held")
}

// A placement onto a node with no same-build sibling is recorded spread, with
// a siblings count of zero.
func TestRecordTemplateAffinity_Spread(t *testing.T) {
	reader := swapAffinityMeters(t)

	node := nodemanager.NewTestNode("node", api.NodeStatusReady, 0, 64, emptyHugePool())
	// A different build in flight must not count as a sibling.
	node.PlacementMetrics.StartPlacingIf("other", nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512, HugePages: true, BuildID: "build-other"}, func() bool { return true })
	nodes := []*nodemanager.Node{node}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, err := placeSandbox(ctx, placeOn(node), nodes, nil, sbxRequestWithBuild("sbx", "build-hot"),
		Requirements{}, false, nil, refusalBackoff{}, time.Time{})
	require.NoError(t, err)

	byEffect, histSum, histCount := collectAffinity(t, reader)
	assert.Equal(t, int64(1), byEffect["spread"])
	assert.Equal(t, int64(0), byEffect["colocated"])
	assert.Equal(t, uint64(1), histCount)
	assert.Equal(t, int64(0), histSum)
}

// A create without a build_id is not same-template fan-out: it records no
// affinity series at all, so the metric is not diluted by placements that
// sharing cannot help.
func TestRecordTemplateAffinity_NoBuildIDSkipped(t *testing.T) {
	reader := swapAffinityMeters(t)

	node := nodemanager.NewTestNode("node", api.NodeStatusReady, 0, 64, emptyHugePool())
	nodes := []*nodemanager.Node{node}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, err := placeSandbox(ctx, placeOn(node), nodes, nil, testSbxRequest("sbx"),
		Requirements{}, false, nil, refusalBackoff{}, time.Time{})
	require.NoError(t, err)

	byEffect, _, histCount := collectAffinity(t, reader)
	assert.Empty(t, byEffect, "no effect counted for a build-less create")
	assert.Zero(t, histCount, "no siblings recorded for a build-less create")
}
