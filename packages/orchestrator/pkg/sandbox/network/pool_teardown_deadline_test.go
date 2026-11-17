//go:build linux

package network

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

// A pool too large to clear in time must not use up the rest of a shutdown:
// Close stops tearing pooled slots down at its teardown deadline and reports
// the ones it left. A retained slot is torn down whatever the time.
func TestPool_CloseLeavesSlotsPastItsTeardownDeadline(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
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
				storage := &fakeStorage{releaseFn: func(*Slot) error {
					time.Sleep(time.Second)

					return nil
				}}
				pool := NewPool(3, 6, storage, Config{})
				pool.retainedSlots = map[*Slot]struct{}{newTestSlot(1): {}}
				for i := range 2 {
					pool.newSlots <- newTestSlot(i + 2)
				}
				close(pool.newSlots)
				for i := range 6 {
					pool.reusedSlots <- newTestSlot(i + 4)
				}

				err := pool.Close(WithTeardownDeadline(t.Context(), time.Now().Add(tc.deadline)))

				require.ErrorIs(t, err, ErrSlotsLeft)
				require.Equal(t, tc.tornDown, storage.released.Load())
			})
		})
	}
}
