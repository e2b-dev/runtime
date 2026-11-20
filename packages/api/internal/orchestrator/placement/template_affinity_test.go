package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
)

func TestApplyTemplateAffinity(t *testing.T) {
	t.Parallel()

	const load = 0.4

	tests := []struct {
		name      string
		weight    float64
		cap       int
		sameBuild int
		want      float64
	}{
		{name: "weight zero is a no-op", weight: 0, cap: 8, sameBuild: 5, want: load},
		{name: "no siblings is a no-op", weight: 0.1, cap: 8, sameBuild: 0, want: load},
		{name: "non-positive cap is a no-op", weight: 0.1, cap: 0, sameBuild: 5, want: load},
		{name: "one sibling discounts by weight/cap", weight: 0.2, cap: 8, sameBuild: 1, want: load * (1 - 0.2*1.0/8.0)},
		{name: "siblings at cap discount by full weight", weight: 0.2, cap: 8, sameBuild: 8, want: load * (1 - 0.2)},
		{name: "siblings past cap do not discount further", weight: 0.2, cap: 8, sameBuild: 100, want: load * (1 - 0.2)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			config := BestOfKConfig{TemplateAffinityWeight: tt.weight, TemplateAffinityCap: tt.cap}
			got := applyTemplateAffinity(load, tt.sameBuild, config)
			assert.InDelta(t, tt.want, got, 1e-9)
		})
	}
}

// A discounted score must never drop below zero or reorder past a genuinely
// empty node: the discount is a fraction of a non-negative load.
func TestApplyTemplateAffinity_NeverNegative(t *testing.T) {
	t.Parallel()
	config := BestOfKConfig{TemplateAffinityWeight: 0.99, TemplateAffinityCap: 4}
	got := applyTemplateAffinity(0.001, 4, config)
	assert.GreaterOrEqual(t, got, 0.0)
}

func TestSameBuildInProgress(t *testing.T) {
	t.Parallel()

	node := nodemanager.NewTestNode("n", api.NodeStatusReady, 2, 8)
	node.PlacementMetrics.StartPlacingIf("a", nodemanager.SandboxResources{BuildID: "build-1"}, func() bool { return true })
	node.PlacementMetrics.StartPlacingIf("b", nodemanager.SandboxResources{BuildID: "build-1"}, func() bool { return true })
	node.PlacementMetrics.StartPlacingIf("c", nodemanager.SandboxResources{BuildID: "build-2"}, func() bool { return true })
	node.PlacementMetrics.StartPlacingIf("d", nodemanager.SandboxResources{BuildID: ""}, func() bool { return true })

	assert.Equal(t, 2, node.PlacementMetrics.SameBuildInProgress("build-1"))
	assert.Equal(t, 1, node.PlacementMetrics.SameBuildInProgress("build-2"))
	assert.Equal(t, 0, node.PlacementMetrics.SameBuildInProgress("build-missing"))
	assert.Equal(t, 0, node.PlacementMetrics.SameBuildInProgress(""), "empty build counts nothing")
}

// With affinity on, a node already running same-build siblings scores lower
// (better) than an identical node with none, while a weight of 0 ties them.
func TestScore_TemplateAffinityPrefersSameBuild(t *testing.T) {
	t.Parallel()

	resources := nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512, BuildID: "build-1"}

	newNode := func(id string) *nodemanager.Node {
		n := nodemanager.NewTestNode(id, api.NodeStatusReady, 2, 8, emptyHugePool())
		n.PlacementMetrics.StartPlacingIf(id+"-sib", nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512, HugePages: true, BuildID: "build-1"}, func() bool { return true })
		return n
	}

	withSibling := newNode("with")
	bare := nodemanager.NewTestNode("bare", api.NodeStatusReady, 2, 8, emptyHugePool())
	// Match the sibling's load on the bare node with a different build, so only
	// the build identity differs between the two candidates.
	bare.PlacementMetrics.StartPlacingIf("bare-other", nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512, HugePages: true, BuildID: "other"}, func() bool { return true })

	off := BestOfKConfig{R: 4, K: 3, Alpha: 0.5, ScoreHugepages: true, TemplateAffinityWeight: 0, TemplateAffinityCap: 8}
	on := off
	on.TemplateAffinityWeight = 0.2

	algo := NewBestOfK(off).(*BestOfK)

	offWith := algo.Score(withSibling, resources, off)
	offBare := algo.Score(bare, resources, off)
	require.InDelta(t, offBare, offWith, 1e-9, "weight 0 ties identical loads")

	onWith := algo.Score(withSibling, resources, on)
	onBare := algo.Score(bare, resources, on)
	assert.Less(t, onWith, onBare, "affinity makes the same-build node score better")
	assert.InDelta(t, offBare, onBare, 1e-9, "a node with no same-build sibling is unchanged by affinity")
}
