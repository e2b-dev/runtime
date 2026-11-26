package placement

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
)

// spreadStats is what one simulated fleet run reports: how widely each build's
// sandboxes landed, and how often a sandbox joined a same-build sibling.
type spreadStats struct {
	// avgNodesPerBuild is the mean number of distinct nodes a build's sandboxes
	// spread across. Lower is tighter colocation; the floor is set by how many
	// nodes a build's sandboxes must occupy to fit.
	avgNodesPerBuild float64
	// colocationRate is the fraction of placements that landed on a node
	// already running an in-flight same-build sibling: the share that a shared
	// read-only base (pmem+DAX) could back without a fresh private copy.
	colocationRate float64
}

// simulateSpread fills a fleet with several templates competing for the same
// nodes, placing through the real chooseNode (best-of-K sampling) so the
// affinity discount is exercised exactly as production would. rand is seeded
// per run, so a given seed and weight reproduce byte for byte.
func simulateSpread(t *testing.T, weight float64, seed int64) spreadStats {
	t.Helper()
	rand.Seed(seed)

	config := BestOfKConfig{R: 4, K: 3, Alpha: 0.5, ScoreHugepages: true, TemplateAffinityWeight: weight, TemplateAffinityCap: 8}
	algo := NewBestOfK(config).(*BestOfK)

	const numNodes = 12
	const numBuilds = 4
	const perBuild = 9

	nodes := make([]*nodemanager.Node, numNodes)
	nodeIndex := map[string]int{}
	for i := range nodes {
		nodes[i] = nodemanager.NewTestNode(fmt.Sprintf("n%02d", i), api.NodeStatusReady, 0, 64, emptyHugePool())
		nodeIndex[nodes[i].ID] = i
	}

	perNodeBuild := make([]map[string]int, numNodes)
	for i := range perNodeBuild {
		perNodeBuild[i] = map[string]int{}
	}

	// Interleave builds round-robin so each placement decides against a fleet
	// that is partly filled by every template, not one template at a time.
	order := make([]string, 0, numBuilds*perBuild)
	for r := 0; r < perBuild; r++ {
		for b := 0; b < numBuilds; b++ {
			order = append(order, fmt.Sprintf("build-%d", b))
		}
	}

	colocated := 0
	for k, build := range order {
		res := nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512, HugePages: true, BuildID: build}
		chosen, err := algo.chooseNode(context.Background(), nodes, nil, res, Requirements{}, false, nil)
		require.NoError(t, err)
		require.NotNil(t, chosen)

		idx := nodeIndex[chosen.ID]
		if perNodeBuild[idx][build] > 0 {
			colocated++
		}
		perNodeBuild[idx][build]++
		chosen.PlacementMetrics.StartPlacingIf(fmt.Sprintf("s%d", k), res, func() bool { return true })
	}

	nodesPerBuild := map[string]map[int]struct{}{}
	for i := range perNodeBuild {
		for b := range perNodeBuild[i] {
			if nodesPerBuild[b] == nil {
				nodesPerBuild[b] = map[int]struct{}{}
			}
			nodesPerBuild[b][i] = struct{}{}
		}
	}
	sumSpread := 0.0
	for _, s := range nodesPerBuild {
		sumSpread += float64(len(s))
	}

	return spreadStats{
		avgNodesPerBuild: sumSpread / float64(len(nodesPerBuild)),
		colocationRate:   float64(colocated) / float64(len(order)),
	}
}

func meanSpread(t *testing.T, weight float64, seeds []int64) spreadStats {
	t.Helper()
	var sumNodes, sumColo float64
	for _, s := range seeds {
		st := simulateSpread(t, weight, s)
		sumNodes += st.avgNodesPerBuild
		sumColo += st.colocationRate
	}
	n := float64(len(seeds))
	return spreadStats{avgNodesPerBuild: sumNodes / n, colocationRate: sumColo / n}
}

// TestTemplateAffinity_TightensColocation is the PR's evidence that the
// discount does what #3824/#3825 need: on a fleet where several templates
// compete for the same nodes, turning the affinity weight on packs each
// template's sandboxes onto fewer nodes and raises how often a sandbox lands
// beside a same-build sibling — the sibling a shared read-only base
// (pmem+DAX) would collapse to one copy. Averaged over many seeds so the
// comparison does not ride on one sampling draw.
func TestTemplateAffinity_TightensColocation(t *testing.T) {
	seeds := make([]int64, 24)
	for i := range seeds {
		seeds[i] = int64(i + 1)
	}

	off := meanSpread(t, 0.0, seeds)
	on := meanSpread(t, 0.6, seeds)

	t.Logf("weight 0.0: nodes_per_build=%.2f colocation=%.1f%%", off.avgNodesPerBuild, 100*off.colocationRate)
	t.Logf("weight 0.6: nodes_per_build=%.2f colocation=%.1f%%", on.avgNodesPerBuild, 100*on.colocationRate)

	assert.Less(t, on.avgNodesPerBuild, off.avgNodesPerBuild,
		"affinity should spread each build across fewer nodes")
	assert.Greater(t, on.colocationRate, off.colocationRate,
		"affinity should raise the same-build colocation rate")
}

// TestTemplateAffinity_WeightRaisesColocation shows the weight is a usable
// knob: every positive setting colocates more than the off baseline, and the
// effect grows overall as the weight climbs. It is deliberately not asserted
// strictly monotone point to point — the discount interacts with best-of-K
// sampling, so an individual step can wobble while the trend holds. What an
// operator can rely on is the direction, which is what this pins.
func TestTemplateAffinity_WeightRaisesColocation(t *testing.T) {
	seeds := make([]int64, 24)
	for i := range seeds {
		seeds[i] = int64(i + 1)
	}

	baseline := meanSpread(t, 0.0, seeds).colocationRate
	t.Logf("weight 0.0 (baseline): colocation=%.1f%%", 100*baseline)

	for _, w := range []float64{0.2, 0.4, 0.6} {
		rate := meanSpread(t, w, seeds).colocationRate
		t.Logf("weight %.1f: colocation=%.1f%%", w, 100*rate)
		assert.Greater(t, rate, baseline, "every positive weight should colocate more than the off baseline")
	}

	// The strongest mid setting should lift colocation by a clear margin, not
	// a sampling-noise sliver, over off.
	strong := meanSpread(t, 0.6, seeds).colocationRate
	assert.Greater(t, strong, baseline+0.05, "a strong weight should raise colocation by a clear margin")
}
