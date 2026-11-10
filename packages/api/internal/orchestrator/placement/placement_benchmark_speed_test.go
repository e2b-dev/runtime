package placement

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
)

func BenchmarkChooseNode(b *testing.B) {
	ctx := b.Context()

	tests := []struct {
		name   string
		newAlg func() Algorithm // factory in case the alg holds state
	}{
		{
			name: "BestOfK_K3",
			newAlg: func() Algorithm {
				return NewBestOfK(DefaultBestOfKConfig())
			},
		},
	}

	resources := nodemanager.SandboxResources{CPUs: 2, MiBMemory: 512}
	sizes := []int{10, 100, 1000}

	// Deterministic randomness for reproducibility
	rng := rand.New(rand.NewSource(1))

	for _, tc := range tests {
		for _, n := range sizes {
			b.Run(fmt.Sprintf("%s/nodes=%d", tc.name, n), func(b *testing.B) {
				// Build input once per sub-benchmark
				nodes := make([]*nodemanager.Node, n)
				for i := range n {
					nodes[i] = nodemanager.NewTestNode(
						fmt.Sprintf("node-%d", i),
						api.NodeStatusReady,
						int64(rng.Intn(80)), // 0–80% CPU usage
						16,
					)
				}
				exclude := make(map[string]struct{})

				alg := tc.newAlg() // create once unless it mutates per call

				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					_, _ = alg.chooseNode(ctx, nodes, exclude, resources, Requirements{}, false, nil)
				}
			})
		}
	}
}
