package nodemanager

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/api/internal/api"
)

// Placements racing on one node must not all pass a check that counts the
// placements already in flight.
func TestStartPlacingIf_ConcurrentPlacementsStopAtTheLimit(t *testing.T) {
	t.Parallel()

	node := NewTestNode("node", api.NodeStatusReady, 0, 8)
	const room = 3

	var placed atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			<-start
			ok := node.PlacementMetrics.StartPlacingIf(fmt.Sprintf("sbx-%d", i), SandboxResources{MiBMemory: 512, HugePages: true}, func() bool {
				inFlight := node.PlacementMetrics.InProgressCount()
				time.Sleep(time.Millisecond) // widen the window between reading and inserting

				return inFlight < room
			})
			if ok {
				placed.Add(1)
			}
		})
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int32(room), placed.Load())
	assert.Equal(t, uint32(room), node.PlacementMetrics.InProgressCount())
}

func TestStartPlacingIf_RecordsNothingWhenItDoesNotFit(t *testing.T) {
	t.Parallel()

	node := NewTestNode("node", api.NodeStatusReady, 0, 8)

	assert.False(t, node.PlacementMetrics.StartPlacingIf("sbx", SandboxResources{MiBMemory: 512}, func() bool { return false }))
	assert.Zero(t, node.PlacementMetrics.InProgressCount())
}
