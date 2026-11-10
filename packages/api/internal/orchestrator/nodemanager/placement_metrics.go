package nodemanager

import (
	"sync"
	"sync/atomic"

	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

type SandboxResources struct {
	CPUs      int64
	MiBMemory int64
	// HugePages reports whether the sandbox's RAM comes from the node's
	// hugepage pool rather than ordinary pages.
	HugePages bool
}

type PlacementMetrics struct {
	sandboxesInProgress *smap.Map[SandboxResources]

	// startMu makes StartPlacingIf's check and insert one step per node.
	startMu sync.Mutex

	createSuccess atomic.Uint64
	createFails   atomic.Uint64
}

func (p *PlacementMetrics) Success(sandboxID string) {
	p.createSuccess.Add(1)
	p.removeSandbox(sandboxID)
}

func (p *PlacementMetrics) Skip(sandboxID string) {
	p.removeSandbox(sandboxID)
}

func (p *PlacementMetrics) Fail(sandboxID string) {
	p.createFails.Add(1)
	p.removeSandbox(sandboxID)
}

func (p *PlacementMetrics) SuccessCount() uint64 {
	return p.createSuccess.Load()
}

func (p *PlacementMetrics) FailsCount() uint64 {
	return p.createFails.Load()
}

func (p *PlacementMetrics) InProgress() map[string]SandboxResources {
	return p.sandboxesInProgress.Items()
}

func (p *PlacementMetrics) InProgressCount() uint32 {
	return uint32(p.sandboxesInProgress.Count())
}

// StartPlacingIf records the placement only if fits holds, checking and
// inserting under one lock so concurrent placements cannot all pass a check
// that counts in-flight placements. fits must not call StartPlacingIf.
func (p *PlacementMetrics) StartPlacingIf(sandboxID string, resources SandboxResources, fits func() bool) bool {
	p.startMu.Lock()
	defer p.startMu.Unlock()

	if !fits() {
		return false
	}

	p.sandboxesInProgress.Insert(sandboxID, resources)

	return true
}

func (p *PlacementMetrics) removeSandbox(sandboxID string) {
	p.sandboxesInProgress.Remove(sandboxID)
}
