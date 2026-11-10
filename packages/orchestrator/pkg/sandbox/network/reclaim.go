//go:build linux

package network

import (
	"context"
	"fmt"
)

// ReclaimLeakedSlots tears down network namespaces and slots left over from a
// previous orchestrator run. It discovers leaked slots by scanning netnsDir:
// the netns entry is a reliable record of every leaked slot, because it is
// created before any host-side networking in CreateNetwork and removed last in
// RemoveNetwork (which preserves it as a rediscovery anchor if host-side
// teardown fails). It returns the number of slots removed and any failures.
func ReclaimLeakedSlots(ctx context.Context, netnsDir string, config Config, egressProxy EgressProxy) (int, []error) {
	var failures []error

	slots, err := ListSlotNamespaces(netnsDir)
	if err != nil {
		failures = append(failures, err)
	}

	reclaimed := 0
	for _, idx := range slots {
		slot, err := NewSlot(fmt.Sprintf("startup-reclaim-%d", idx), idx, config, egressProxy)
		if err != nil {
			failures = append(failures, err)

			continue
		}

		if err := slot.RemoveNetwork(ctx); err != nil {
			failures = append(failures, fmt.Errorf("failed to remove network slot %d: %w", idx, err))

			continue
		}

		reclaimed++
	}

	return reclaimed, failures
}
