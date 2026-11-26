package orchestrator

import (
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

// NewTestOrchestrator holds only the given nodes, for handler tests that need
// no other part of the orchestrator.
func NewTestOrchestrator(nodes ...*nodemanager.Node) *Orchestrator {
	o := &Orchestrator{nodes: smap.New[*nodemanager.Node]()}
	for _, n := range nodes {
		o.nodes.Insert(o.scopedNodeID(n.ClusterID, n.ID), n)
	}

	return o
}

// NewTestOrchestratorWithStore holds only the given sandbox store, for handler
// tests that read or claim sandboxes.
func NewTestOrchestratorWithStore(store *sandbox.Store) *Orchestrator {
	return &Orchestrator{nodes: smap.New[*nodemanager.Node](), sandboxStore: store}
}
