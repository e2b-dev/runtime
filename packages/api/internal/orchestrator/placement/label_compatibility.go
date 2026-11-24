package placement

import (
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
)

// NodeSatisfiesLabels checks if a single node is compatible with the build labels.
// Returns true if:
// - Build has no labels (empty slice)
// - Node has all required labels
func NodeSatisfiesLabels(node *nodemanager.Node, requiredLabels []string) bool {
	nodeLabels := node.Labels()

	for _, req := range requiredLabels {
		if _, ok := nodeLabels[req]; !ok {
			return false
		}
	}

	return true
}
