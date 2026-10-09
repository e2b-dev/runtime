//go:build linux

package v2

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
)

func TestV2Pool_TakePrefersReusedSlot(t *testing.T) {
	t.Parallel()

	pool := NewV2Pool(&failingStorage{}, testConfig(), nil, nil)
	reused := &network.Slot{Idx: 1}
	pool.reusedSlots <- reused
	reusableSlotsAvailableCounter.Add(t.Context(), 1)
	pool.newSlots <- &network.Slot{Idx: 2}
	newSlotsAvailableCounter.Add(t.Context(), 1)

	got, err := pool.take(t.Context())
	require.NoError(t, err)
	require.Same(t, reused, got)
	assert.Len(t, pool.newSlots, 1)
}

func TestV2Pool_TakeReceivesSlotReturnedWhileWaiting(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		pool := NewV2Pool(&failingStorage{}, testConfig(), nil, nil)
		type result struct {
			slot *network.Slot
			err  error
		}
		done := make(chan result, 1)
		go func() {
			slot, err := pool.take(t.Context())
			done <- result{slot, err}
		}()
		// Both queues are empty, so take is past its fast path and waiting.
		synctest.Wait()

		returned := &network.Slot{Idx: 3}
		pool.reusedSlots <- returned
		reusableSlotsAvailableCounter.Add(t.Context(), 1)
		synctest.Wait()

		select {
		case got := <-done:
			require.NoError(t, got.err)
			require.Same(t, returned, got.slot)
		default:
			t.Fatal("a waiting take must receive a slot returned to the reuse queue")
		}
	})
}

func TestV2Pool_TakeReturnsErrClosedWhenNewSlotsAreClosed(t *testing.T) {
	t.Parallel()

	pool := NewV2Pool(&failingStorage{}, testConfig(), nil, nil)
	close(pool.newSlots)

	got, err := pool.take(t.Context())
	require.Nil(t, got)
	require.ErrorIs(t, err, network.ErrClosed)
}
