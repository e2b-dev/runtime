package placement

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
)

// BestOfKConfig holds the configuration parameters for the placement algorithm
type BestOfKConfig struct {
	// R is the cluster-wide max CPU over-commit ratio
	R float64
	// Alpha is the weight for CPU and memory usage in the score calculation
	Alpha float64
	// K is the number of candidate nodes sampled per placement ("power of K choices")
	K int
	// ScoreHugepages includes hugepage-pool load in the score (max of CPU and memory).
	ScoreHugepages bool
}

// DefaultBestOfKConfig returns the default placement configuration
func DefaultBestOfKConfig() BestOfKConfig {
	return BestOfKConfig{
		R:              4,
		K:              3,
		Alpha:          0.5,
		ScoreHugepages: true,
	}
}

// Score calculates the placement score for this node. CPU load always
// contributes. When config.ScoreHugepages is set, the score is the higher
// of CPU and hugepage-memory load, so the tighter resource decides.
func (b *BestOfK) Score(node *nodemanager.Node, resources nodemanager.SandboxResources, config BestOfKConfig) float64 {
	metrics := node.Metrics()

	// Get locally recorded resources that haven't been reported yet.
	pendingCPUs := int64(0)
	pendingMiB := int64(0)
	for _, res := range node.PlacementMetrics.InProgress() {
		pendingCPUs += res.CPUs
		if res.HugePages {
			pendingMiB += res.MiBMemory
		}
	}

	// to avoid division by zero
	cpuCount := float64(metrics.CpuCount)
	if cpuCount == 0 {
		return math.MaxFloat64
	}

	// Combine allocated resources with in-progress allocations
	reserved := metrics.CpuAllocated + uint32(pendingCPUs)

	// CpuPercent is whole-host utilization, so convert it to busy cores.
	busyCPUs := float64(metrics.CpuPercent) / 100 * cpuCount

	totalCapacity := config.R * cpuCount

	cpuRequested := float64(resources.CPUs)

	cpuScore := (cpuRequested + float64(reserved) + config.Alpha*busyCPUs) / totalCapacity

	if !config.ScoreHugepages {
		return cpuScore
	}

	return max(cpuScore, memoryScore(metrics, resources, pendingMiB, config))
}

// memoryScore is the CPU score's shape over the hugepage pool, where sandbox
// memory lives: committed pages (used + reserved + in-flight + requested) plus
// Alpha-weighted pages actually faulted in, over the pool. Commitment is the
// kernel counters, not MemoryAllocatedBytes, so a sandbox on ordinary pages
// does not fill the pool. No over-commit ratio; the pool is physical. A node
// reporting no pool scores 0.5, so it does not look free next to a reporter.
func memoryScore(metrics nodemanager.Metrics, resources nodemanager.SandboxResources, pendingMiB int64, config BestOfKConfig) float64 {
	pageBytes := float64(metrics.HugePageSizeBytes)
	poolBytes := float64(scoredPoolPages(metrics)) * pageBytes
	if poolBytes == 0 {
		return 0.5
	}

	requested := float64(units.MBToBytes(resources.MiBMemory))
	committed := float64(metrics.HugePagesUsed+metrics.HugePagesReserved)*pageBytes + float64(units.MBToBytes(pendingMiB))
	used := float64(metrics.HugePagesUsed) * pageBytes

	return (requested + committed + config.Alpha*used) / poolBytes
}

// scoredPoolPages is the pool the memory score divides by: the base pool,
// allocated at boot. HugePages_Total is the base pool plus the surplus pages
// borrowed right now, so it grows with the load once a node passes its base
// pool and every such node reads about full; a fixed base keeps the score
// climbing past 1 as a node borrows. A pool reported as all overcommit (base
// 0) is scored against its ceiling. An orchestrator that does not report the
// base falls back to HugePages_Total, the old divisor, even when it reports
// the ceiling: scoring it against the ceiling would make it look emptier than
// its upgraded peers for the whole rollout.
func scoredPoolPages(metrics nodemanager.Metrics) uint64 {
	switch {
	case metrics.HugePagesBaseSet && metrics.HugePagesBase > 0:
		return metrics.HugePagesBase
	case metrics.HugePagesBaseSet && metrics.HugePagesCapacity > 0:
		return metrics.HugePagesCapacity
	default:
		return metrics.HugePagesTotal
	}
}

// BestOfK implements the fit-score-place algorithm
type BestOfK struct {
	config BestOfKConfig
	mu     sync.RWMutex
}

var _ Algorithm = &BestOfK{}

// NewBestOfK creates a new placement algorithm with the given config
func NewBestOfK(config BestOfKConfig) Algorithm {
	return &BestOfK{
		config: config,
	}
}

func (b *BestOfK) getConfig() BestOfKConfig {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.config
}

// UpdateConfig updates the BestOfK algorithm configuration
func (b *BestOfK) UpdateConfig(config BestOfKConfig) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.config = config
}

// chooseNode selects the best node for placing a VM with the given quota
func (b *BestOfK) chooseNode(_ context.Context, nodes []*nodemanager.Node, excludedNodes map[string]struct{}, resources nodemanager.SandboxResources, requirements Requirements, filterByLabels bool, requiredLabels []string) (bestNode *nodemanager.Node, err error) {
	// Fix the config, we want to dynamically update it
	config := b.getConfig()

	// Filter eligible nodes
	candidates, poolsFull := b.sample(nodes, config, excludedNodes, resources, requirements, filterByLabels, requiredLabels)

	// Find the best node among candidates
	bestScore := math.MaxFloat64

	for _, node := range candidates {
		// Calculate score
		score := b.Score(node, resources, config)

		if score < bestScore {
			bestNode = node
			bestScore = score
		}
	}

	if bestNode == nil {
		// Full pools, or an unavailable node that would fit, are a shortage that
		// clears, not a mismatch.
		if poolsFull || unavailableNodeWouldFit(nodes, excludedNodes, requirements, filterByLabels, requiredLabels) {
			return nil, NoNodesAvailableError{}
		}

		return nil, FailedToPlaceSandboxError{
			filterByLabels: filterByLabels,
			requiredLabels: requiredLabels,
			cpu:            requirements.CPU,
			features:       requirements.Features,
		}
	}

	return bestNode, nil
}

type FailedToPlaceSandboxError struct {
	filterByLabels bool
	requiredLabels []string
	cpu            CPURequirement
	features       FeatureRequirement
}

var _ error = FailedToPlaceSandboxError{}

func (e FailedToPlaceSandboxError) Error() string {
	message := fmt.Sprintf("no node available with required metadata: machine=%v", e.cpu.Build)

	if e.cpu.PinnedModel != "" {
		message += fmt.Sprintf(", cpu_model_pinned=%s", e.cpu.PinnedModel)
	}

	if e.filterByLabels {
		message += fmt.Sprintf(", labels=%v", e.requiredLabels)
	}

	if e.features.MinVersion() != "" {
		message += fmt.Sprintf(", features=%v, min_orchestrator_version=%s", e.features.FeatureNames(), e.features.MinVersion())
	}

	return message
}

// unavailableNodeWouldFit reports whether a node that cannot take new sandboxes
// right now would otherwise satisfy the requirements.
func unavailableNodeWouldFit(nodes []*nodemanager.Node, excludedNodes map[string]struct{}, requirements Requirements, filterByLabels bool, requiredLabels []string) bool {
	for _, n := range nodes {
		if _, ok := excludedNodes[n.ID]; ok || n.CanAcceptNewRequests() {
			continue
		}

		if NodeSatisfiesCPU(n, requirements.CPU) && NodeSatisfiesFeatures(n, requirements.Features) && (!filterByLabels || isNodeLabelsCompatible(n, requiredLabels)) {
			return true
		}
	}

	return false
}

// sample returns up to k items chosen uniformly from those passing ok, and
// whether any node passed every check but memory.
func (b *BestOfK) sample(items []*nodemanager.Node, config BestOfKConfig, excludedNodes map[string]struct{}, resources nodemanager.SandboxResources, requirements Requirements, filterByLabels bool, requiredLabels []string) (candidates []*nodemanager.Node, poolsFull bool) {
	if config.K <= 0 || len(items) == 0 {
		return nil, false
	}

	indices := make([]int, len(items))
	for i := range indices {
		indices[i] = i
	}

	candidates = make([]*nodemanager.Node, 0, config.K)
	remaining := len(indices) // active pool is indices[:remaining]

	for len(candidates) < config.K && remaining > 0 {
		// pick from the active pool
		j := rand.Intn(remaining)
		pick := indices[j]

		// remove j from pool
		indices[j], indices[remaining-1] = indices[remaining-1], indices[j]
		remaining--

		n := items[pick]

		// Excluded filter
		if _, ok := excludedNodes[n.ID]; ok {
			continue
		}

		// If the node can't take new sandboxes, skip it
		if !n.CanAcceptNewRequests() {
			continue
		}

		// Skip if node is not CPU compatible
		if !NodeSatisfiesCPU(n, requirements.CPU) {
			continue
		}

		// Skip if the node's orchestrator predates a requested feature
		if !NodeSatisfiesFeatures(n, requirements.Features) {
			continue
		}

		// Skip if node doesn't have the required labels
		if filterByLabels && !isNodeLabelsCompatible(n, requiredLabels) {
			continue
		}

		if !NodeSatisfiesMemory(n, resources, requirements.Memory) {
			poolsFull = true

			continue
		}

		candidates = append(candidates, n)
	}

	return candidates, poolsFull
}
