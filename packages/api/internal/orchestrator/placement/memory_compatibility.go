package placement

import (
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
)

// MemoryRequirement bounds how much of a node's hugepage pool placement may
// commit.
type MemoryRequirement struct {
	// Limit is the share of the pool ceiling committed pages may reach (of
	// the node's memory outside the pool, used memory may reach, for a
	// sandbox without hugepages); 0 turns the check off.
	Limit float64
	// MaxOvercommit is how many times the base pool (the node's total memory,
	// for a sandbox without hugepages) the template RAM of the node's
	// sandboxes (allocated, in flight, requested) may reach; 0 turns the
	// check off.
	MaxOvercommit float64
}

// NodeSatisfiesMemory reports whether node can take resources with its
// committed hugepages (used, reserved, in flight, requested) staying within
// memory.Limit of the pool ceiling, and the template RAM of its sandboxes
// within memory.MaxOvercommit of its base pool; a sandbox without hugepages
// is measured against the node's total memory instead (for the limit, the
// part outside the pool), its used memory standing in for committed pages.
// HugePages_Total is no ceiling: it is the base pool plus only the surplus
// already borrowed, so a node on surplus would always look full against it.
func NodeSatisfiesMemory(node *nodemanager.Node, resources nodemanager.SandboxResources, memory MemoryRequirement) bool {
	ok, _ := MemoryAdmission(node, resources, memory)

	return ok
}

// MemoryAdmission is NodeSatisfiesMemory with the reason the node passed
// unbounded: non-empty when the hugepage limit applies to the request but the
// node's metrics report no ceiling, so the check could not run; "" otherwise.
func MemoryAdmission(node *nodemanager.Node, resources nodemanager.SandboxResources, memory MemoryRequirement) (bool, string) {
	checkLimit := memory.Limit > 0
	checkOvercommit := memory.MaxOvercommit > 0
	if !checkLimit && !checkOvercommit {
		return true, ""
	}

	// In flight before metrics: a placement finishing in between moves from
	// the first to the second, so it is counted at least once. Under
	// StartPlacingIf's lock, CompletePlacement cannot run in between, so it is
	// counted exactly once.
	inFlight := node.PlacementMetrics.InProgress()
	metrics := node.Metrics()

	ok, missing := true, ""
	switch {
	case checkLimit && resources.HugePages:
		ok, missing = withinCommitLimit(metrics, inFlight, resources, memory.Limit)
	case checkLimit:
		ok = withinUsedMemoryLimit(metrics, inFlight, resources, memory.Limit)
	}
	if ok && checkOvercommit {
		ok = withinMaxOvercommit(metrics, inFlight, resources, memory.MaxOvercommit)
	}
	if !ok {
		return false, ""
	}

	return true, missing
}

func withinCommitLimit(metrics nodemanager.Metrics, inFlight map[string]nodemanager.SandboxResources, resources nodemanager.SandboxResources, limit float64) (bool, string) {
	if missing := MissingHugepageCeiling(metrics); missing != "" {
		return true, missing
	}

	var pendingMiB int64
	for _, res := range inFlight {
		if res.HugePages {
			pendingMiB += res.MiBMemory
		}
	}

	committed := metrics.HugePagesUsed + metrics.HugePagesReserved +
		nodemanager.HugePagesFor(pendingMiB, metrics.HugePageSizeBytes) +
		nodemanager.HugePagesFor(resources.MiBMemory, metrics.HugePageSizeBytes)

	return float64(committed) <= limit*float64(metrics.HugePagesCapacity), ""
}

// withinUsedMemoryLimit measures the memory outside the hugepage pool, which
// is all ordinary pages can get. MemoryUsedBytes is total less MemAvailable,
// so it counts every page in the pool (HugePages_Total, free ones included),
// and the pool comes off both sides. Hugepages committed past HugePages_Total,
// by placements in flight or not yet synced, are surplus the kernel takes out
// of ordinary memory when it reserves them, so they count as used.
func withinUsedMemoryLimit(metrics nodemanager.Metrics, inFlight map[string]nodemanager.SandboxResources, resources nodemanager.SandboxResources, limit float64) bool {
	pendingMiB := resources.MiBMemory
	var pendingHugePageMiB int64
	for _, res := range inFlight {
		if res.HugePages {
			pendingHugePageMiB += res.MiBMemory
		} else {
			pendingMiB += res.MiBMemory
		}
	}

	pageBytes := metrics.HugePageSizeBytes
	poolBytes := metrics.HugePagesTotal * pageBytes
	committedPages := metrics.HugePagesUsed + metrics.HugePagesReserved + nodemanager.HugePagesFor(pendingHugePageMiB, pageBytes)
	borrowedBytes := (committedPages - min(committedPages, metrics.HugePagesTotal)) * pageBytes

	used := float64(metrics.MemoryUsedBytes-min(metrics.MemoryUsedBytes, poolBytes)) +
		float64(borrowedBytes) + float64(units.MBToBytes(pendingMiB))
	available := float64(metrics.MemoryTotalBytes) - float64(poolBytes)

	return used <= limit*available
}

// withinMaxOvercommit counts every sandbox's RAM, on hugepages or not, against
// either denominator: the node reports one allocated total.
func withinMaxOvercommit(metrics nodemanager.Metrics, inFlight map[string]nodemanager.SandboxResources, resources nodemanager.SandboxResources, maxOvercommit float64) bool {
	pendingMiB := resources.MiBMemory
	for _, res := range inFlight {
		pendingMiB += res.MiBMemory
	}

	allocated := float64(metrics.MemoryAllocatedBytes) + float64(units.MBToBytes(pendingMiB))

	return allocated <= maxOvercommit*float64(overcommitCapacityBytes(metrics, resources.HugePages))
}

// overcommitCapacityBytes is the memory MaxOvercommit multiplies: the
// hugepage pool for a sandbox on hugepages, the node's total memory for one
// on ordinary pages.
func overcommitCapacityBytes(metrics nodemanager.Metrics, hugePages bool) uint64 {
	if !hugePages {
		return metrics.MemoryTotalBytes
	}

	return overcommitPoolPages(metrics) * metrics.HugePageSizeBytes
}

// The reasons MissingHugepageCeiling exempts a node from the limit.
const (
	// NoHugepageCapacity: the orchestrator reports no pool ceiling, as builds
	// predating the report do.
	NoHugepageCapacity = "no_capacity"
	// NoHugepagePageSize: the orchestrator reports no page size.
	NoHugepagePageSize = "no_page_size"
)

// MissingHugepageCeiling names why NodeSatisfiesMemory cannot bound a node
// reporting metrics, or returns "" when the limit applies to it.
func MissingHugepageCeiling(metrics nodemanager.Metrics) string {
	switch {
	case metrics.HugePagesCapacity == 0:
		return NoHugepageCapacity
	case metrics.HugePageSizeBytes == 0:
		return NoHugepagePageSize
	default:
		return ""
	}
}

// overcommitPoolPages is the pool MaxOvercommit multiplies, the same one the
// memory score divides by: the base pool, or the ceiling for a pool that is
// all overcommit. Unlike the score it has no HugePages_Total fallback, which
// grows as the node borrows and would loosen the cap exactly when it binds.
func overcommitPoolPages(metrics nodemanager.Metrics) uint64 {
	if metrics.HugePagesBase > 0 {
		return metrics.HugePagesBase
	}

	return metrics.HugePagesCapacity
}
