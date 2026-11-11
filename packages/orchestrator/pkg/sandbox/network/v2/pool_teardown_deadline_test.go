//go:build linux

package v2

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
)

// A pool too large to clear in time must not use up the rest of a shutdown:
// Close stops tearing pooled slots down at its teardown deadline and reports
// the ones it left. A retained slot is torn down whatever the time.
func TestV2Pool_CloseLeavesSlotsPastItsTeardownDeadline(t *testing.T) {
	t.Parallel()

	for i, tc := range []struct {
		name     string
		deadline time.Duration
		tornDown int64
	}{
		{"passes during the drain", 3500 * time.Millisecond, 4},
		{"already passed", -time.Second, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				// Each teardown takes one second on the test clock.
				storage := &failingStorage{releaseFn: func(*network.Slot) error {
					time.Sleep(time.Second)

					return nil
				}}
				// Nine fixed indexes per case, at the top of the slot range: these
				// slots never get a network, and reserved indexes are not handed back.
				idx := network.GetVrtSlotsSize() - 9*i
				slot := func() *network.Slot {
					idx--

					return makeTestSlot(t, idx+1)
				}
				pool := NewV2Pool(storage, testConfig(), nil, nil)
				pool.retainedSlots = map[*network.Slot]struct{}{slot(): {}}
				for range 2 {
					pool.newSlots <- slot()
				}
				close(pool.newSlots)
				for range 6 {
					pool.reusedSlots <- slot()
				}

				err := pool.Close(network.WithTeardownDeadline(t.Context(), time.Now().Add(tc.deadline)))

				require.ErrorIs(t, err, network.ErrSlotsLeft)
				require.Equal(t, tc.tornDown, storage.released.Load())
			})
		})
	}
}
