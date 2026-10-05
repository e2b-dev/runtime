package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
)

func isolationHosts(ids ...string) map[string]struct{} {
	hosts := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		hosts[id] = struct{}{}
	}

	return hosts
}

func isolationNode(id string) *nodemanager.Node {
	return nodemanager.NewTestNode(id, api.NodeStatusReady, 0, 8)
}

func TestHostIsolationAllows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		hosts      map[string]struct{}
		originHost string
		nodeID     string
		expected   bool
	}{
		{
			name:       "partition off - isolated node accepted",
			hosts:      nil,
			originHost: "fenced-1",
			nodeID:     "fenced-1",
			expected:   true,
		},
		{
			name:       "partition off - any node accepted",
			hosts:      nil,
			originHost: "",
			nodeID:     "open-1",
			expected:   true,
		},
		{
			name:       "isolated origin - isolated node accepted",
			hosts:      isolationHosts("fenced-1", "fenced-2"),
			originHost: "fenced-1",
			nodeID:     "fenced-2",
			expected:   true,
		},
		{
			name:       "isolated origin - open node rejected",
			hosts:      isolationHosts("fenced-1", "fenced-2"),
			originHost: "fenced-1",
			nodeID:     "open-1",
			expected:   false,
		},
		{
			name:       "open origin - isolated node rejected",
			hosts:      isolationHosts("fenced-1"),
			originHost: "open-1",
			nodeID:     "fenced-1",
			expected:   false,
		},
		{
			name:       "open origin - open node accepted",
			hosts:      isolationHosts("fenced-1"),
			originHost: "open-1",
			nodeID:     "open-2",
			expected:   true,
		},
		{
			name:       "unrecorded origin - treated as open, isolated node rejected",
			hosts:      isolationHosts("fenced-1"),
			originHost: "",
			nodeID:     "fenced-1",
			expected:   false,
		},
		{
			name:       "unrecorded origin - treated as open, open node accepted",
			hosts:      isolationHosts("fenced-1"),
			originHost: "",
			nodeID:     "open-1",
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			isolation := HostIsolation{Hosts: tt.hosts, OriginHost: tt.originHost}
			assert.Equal(t, tt.expected, isolation.Allows(isolationNode(tt.nodeID)))
		})
	}
}

// The set is a policy, not a roster: a host listed but absent from the cluster
// must not conjure a candidate, and the result is the intersection of the two.
func TestHostIsolationFilterNodesIntersectsTheLiveFleet(t *testing.T) {
	t.Parallel()

	nodes := []*nodemanager.Node{isolationNode("fenced-1"), isolationNode("open-1")}
	isolation := HostIsolation{
		Hosts:      isolationHosts("fenced-1", "fenced-offline"),
		OriginHost: "fenced-1",
	}

	filtered := isolation.FilterNodes(nodes)
	require.Len(t, filtered, 1)
	assert.Equal(t, "fenced-1", filtered[0].ID)
}

func TestHostIsolationFilterNodesExcludesIsolatedHostsForOpenOrigins(t *testing.T) {
	t.Parallel()

	nodes := []*nodemanager.Node{
		isolationNode("fenced-1"),
		isolationNode("open-1"),
		isolationNode("fenced-2"),
		isolationNode("open-2"),
	}
	isolation := HostIsolation{
		Hosts:      isolationHosts("fenced-1", "fenced-2"),
		OriginHost: "open-1",
	}

	filtered := isolation.FilterNodes(nodes)
	require.Len(t, filtered, 2)
	assert.Equal(t, "open-1", filtered[0].ID)
	assert.Equal(t, "open-2", filtered[1].ID)
}

// An isolated origin with every fenced host offline must starve rather than
// fall back to the open side.
func TestHostIsolationFilterNodesStarvesRatherThanFallingBack(t *testing.T) {
	t.Parallel()

	nodes := []*nodemanager.Node{isolationNode("open-1"), isolationNode("open-2")}
	isolation := HostIsolation{
		Hosts:      isolationHosts("fenced-1"),
		OriginHost: "fenced-1",
	}

	assert.Empty(t, isolation.FilterNodes(nodes))
}

func TestHostIsolationFilterNodesPassesEverythingWhenDisabled(t *testing.T) {
	t.Parallel()

	nodes := []*nodemanager.Node{isolationNode("open-1"), isolationNode("open-2")}
	isolation := HostIsolation{OriginHost: "open-1"}

	assert.False(t, isolation.Enabled())
	assert.Equal(t, nodes, isolation.FilterNodes(nodes))
}
