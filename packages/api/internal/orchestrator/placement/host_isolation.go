package placement

import (
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
)

// HostIsolation cuts a cluster in two: the hosts named in Hosts, and every
// other host. A sandbox is confined to the side its artifact came from. A
// template built on an isolated host, or a snapshot taken on one, may run only
// on isolated hosts; anything built or snapshotted elsewhere is kept off them.
//
// Hosts is a scheduling policy, not a view of the fleet. It says nothing about
// which of those hosts are registered or healthy, so the candidate set is
// always the intersection of the partition side with the nodes placement was
// handed — an isolated host that is offline simply never comes up.
//
// The constraint is applied where the candidate list is assembled rather than
// inside the placement algorithm: it is set membership rather than a score, and
// the same rule has to cover the resume affinity node, which never reaches the
// algorithm at all.
type HostIsolation struct {
	// Hosts is the set of cluster host (node) IDs on the isolated side. Empty
	// turns the partition off, and every node is then allowed.
	Hosts map[string]struct{}

	// OriginHost is the cluster host the sandbox's artifact was produced on:
	// the builder node for a template build, the node a snapshot was taken on
	// for a resume. Empty means unrecorded — builds predating the column, for
	// instance — which puts the sandbox on the non-isolated side.
	OriginHost string
}

// Enabled reports whether the partition constrains anything.
func (h HostIsolation) Enabled() bool {
	return len(h.Hosts) > 0
}

// IsolatedOrigin reports whether the artifact came off an isolated host, and so
// whether the sandbox is confined to them or excluded from them.
func (h HostIsolation) IsolatedOrigin() bool {
	_, ok := h.Hosts[h.OriginHost]

	return ok
}

// Allows reports whether node sits on the origin's side of the partition.
func (h HostIsolation) Allows(node *nodemanager.Node) bool {
	if !h.Enabled() {
		return true
	}

	_, isolatedNode := h.Hosts[node.ID]

	return isolatedNode == h.IsolatedOrigin()
}

// FilterNodes returns the nodes on the origin's side of the partition,
// preserving order. It returns nodes untouched when the partition is off.
func (h HostIsolation) FilterNodes(nodes []*nodemanager.Node) []*nodemanager.Node {
	if !h.Enabled() {
		return nodes
	}

	allowed := make([]*nodemanager.Node, 0, len(nodes))
	for _, node := range nodes {
		if h.Allows(node) {
			allowed = append(allowed, node)
		}
	}

	return allowed
}
