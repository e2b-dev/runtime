package orchestrator

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A zero-value Orchestrator is enough: GoTrackedTail/TrackWork/Drain only touch
// drainMu, draining, work, and outstandingWork, all usable at their zero value.

// TestGoTrackedTail_DrainWaitsForTail proves a tail started before the drain is
// registered with TrackWork, so Drain blocks until it finishes.
func TestGoTrackedTail_DrainWaitsForTail(t *testing.T) {
	t.Parallel()

	var o Orchestrator
	var done atomic.Bool
	release := make(chan struct{})

	o.GoTrackedTail(func() {
		<-release
		done.Store(true)
	})

	// Outstanding work is visible synchronously (TrackWork.Add ran before return).
	require.Equal(t, int64(1), o.OutstandingWork(), "tail must be tracked before drain")

	drainReturned := make(chan error, 1)
	go func() {
		drainReturned <- o.Drain(context.Background())
	}()

	// Drain must still be blocked while the tail is in flight.
	select {
	case <-drainReturned:
		t.Fatal("Drain returned before the tracked tail finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-drainReturned:
		require.NoError(t, err)
		assert.True(t, done.Load(), "tail should have completed")
	case <-time.After(2 * time.Second):
		t.Fatal("Drain did not return after the tail finished")
	}
}

// TestGoTrackedTail_RunsUntrackedDuringDrain proves that once the drain has
// started (TrackWork refuses new counts) a tail still runs — fire-and-forget,
// never dropped — and never panics by adding to the WaitGroup after Wait.
func TestGoTrackedTail_RunsUntrackedDuringDrain(t *testing.T) {
	t.Parallel()

	var o Orchestrator

	// Drain with nothing in flight returns immediately and flips draining=true.
	require.NoError(t, o.Drain(context.Background()))

	ran := make(chan struct{})
	assert.NotPanics(t, func() {
		o.GoTrackedTail(func() { close(ran) })
	})

	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("tail started during drain never ran")
	}
}
