package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
)

func TestNodeSatisfiesLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		nodeLabels     []string
		requiredLabels []string
		expected       bool
	}{
		{
			name:           "both empty - default pool match",
			nodeLabels:     nil,
			requiredLabels: nil,
			expected:       true,
		},
		{
			name:           "both empty slices - default pool match",
			nodeLabels:     []string{},
			requiredLabels: []string{},
			expected:       true,
		},
		{
			name:           "node labeled, sandbox unlabeled - accept (dedicated are protected higher up)",
			nodeLabels:     []string{"gpu-support"},
			requiredLabels: nil,
			expected:       true,
		},
		{
			name:           "node labeled, sandbox empty slice - accept (dedicated are protected higher up)",
			nodeLabels:     []string{"gpu-support"},
			requiredLabels: []string{},
			expected:       true,
		},
		{
			name:           "sandbox requires labels, node unlabeled - reject",
			nodeLabels:     nil,
			requiredLabels: []string{"gpu-support"},
			expected:       false,
		},
		{
			name:           "sandbox requires labels, node empty slice - reject",
			nodeLabels:     []string{},
			requiredLabels: []string{"gpu-support"},
			expected:       false,
		},
		{
			name:           "exact match single label",
			nodeLabels:     []string{"gpu-support"},
			requiredLabels: []string{"gpu-support"},
			expected:       true,
		},
		{
			name:           "subset match - node has superset",
			nodeLabels:     []string{"gpu-support", "gpu-type-h100"},
			requiredLabels: []string{"gpu-support"},
			expected:       true,
		},
		{
			name:           "exact match multiple labels",
			nodeLabels:     []string{"gpu-support", "gpu-type-h100"},
			requiredLabels: []string{"gpu-support", "gpu-type-h100"},
			expected:       true,
		},
		{
			name:           "required label missing from node",
			nodeLabels:     []string{"gpu-support"},
			requiredLabels: []string{"gpu-support", "gpu-type-h100"},
			expected:       false,
		},
		{
			name:           "completely disjoint labels",
			nodeLabels:     []string{"region-us"},
			requiredLabels: []string{"gpu-support"},
			expected:       false,
		},
		{
			name:           "default labeled node matches default requirement",
			nodeLabels:     []string{"default"},
			requiredLabels: []string{"default"},
			expected:       true,
		},
		{
			name:           "default labeled node rejects unlabeled sandbox",
			nodeLabels:     []string{"default"},
			requiredLabels: nil,
			expected:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var opts []nodemanager.TestOptions
			if tt.nodeLabels != nil {
				opts = append(opts, nodemanager.WithLabels(tt.nodeLabels))
			}

			node := nodemanager.NewTestNode("test-node", api.NodeStatusReady, 0, 4, opts...)

			result := NodeSatisfiesLabels(node, tt.requiredLabels)
			assert.Equal(t, tt.expected, result, "nodeLabels=%v requiredLabels=%v", tt.nodeLabels, tt.requiredLabels)
		})
	}
}
