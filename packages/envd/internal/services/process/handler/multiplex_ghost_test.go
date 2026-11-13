package handler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression tests for #3292: a ghost subscriber — one whose client TCP
// connection died after a sandbox resume so its consumer goroutine is wedged
// inside stream.Send — never reads its channel AND never calls its cancel func.
//
// This is the case the existing TestMultiplexedChannel_AbandonedConsumer* tests
// do NOT cover: those model a consumer that stops reading but then calls
// cancel(), which closes done and lets the fan-out's `case <-s.done` unblock.
// A real ghost never returns from stream.Send, so cancel never runs. On an
// unbuffered implementation the fan-out loop would then block on both
// `case s.ch <- v` (nobody reading) and `case <-s.done` (never cancelled) for
// the full TCP retransmit timeout (~15 min at tcp_retries2=15), stalling every
// other subscriber. The buffered channel plus non-blocking overflow drop is
// what prevents that; these tests fail (hang to timeout) without it.

// forkGhost registers a subscriber whose consumer never reads and never cancels.
// It returns nothing on purpose: the ghost channel is deliberately leaked to the
// fan-out so only the overflow-drop path can reclaim it.
func forkGhost[T any](m *MultiplexedChannel[T]) {
	_, _ = m.Fork()
}

// TestMultiplexedChannel_GhostNeverReadingDoesNotWedgeFanOut is the core
// regression. A ghost is present that never reads and never cancels. We push
// subscriberBufSize+10 values; every send must stay deliverable and the one
// healthy subscriber must receive all of them well within the timeout. Without
// the overflow drop, the fan-out wedges on the ghost once its buffer fills and
// the healthy subscriber stops receiving.
func TestMultiplexedChannel_GhostNeverReadingDoesNotWedgeFanOut(t *testing.T) {
	t.Parallel()

	m := NewMultiplexedChannel[int](1)
	t.Cleanup(func() { close(m.Source) })

	healthy, cancelHealthy := m.Fork()
	t.Cleanup(cancelHealthy)

	healthyGot := make(chan int, subscriberBufSize+32)
	go func() {
		for v := range healthy {
			healthyGot <- v
		}
	}()

	// A ghost that will never read its channel and never cancel.
	forkGhost(m)

	const total = subscriberBufSize + 10

	for i := 1; i <= total; i++ {
		require.Truef(t,
			sendOrTimeout(t, m.Source, i, multiplexTestTimeout),
			"send %d must not be back-pressured by a ghost subscriber", i,
		)
	}

	// The healthy subscriber must observe every value in order.
	for i := 1; i <= total; i++ {
		got, ok := recvOrTimeout(t, healthyGot, multiplexTestTimeout)
		require.Truef(t, ok, "healthy subscriber should receive value %d, not stall behind the ghost", i)
		assert.Equalf(t, i, got, "healthy subscriber received out-of-order value")
	}
}

// TestMultiplexedChannel_GhostChannelClosedAfterOverflow proves the overflow
// path closes the ghost's channel, so a consumer goroutine ranging over it
// exits promptly instead of leaking. We take the ghost channel ourselves but
// never drain it during the flood, forcing its buffer to fill and overflow.
func TestMultiplexedChannel_GhostChannelClosedAfterOverflow(t *testing.T) {
	t.Parallel()

	m := NewMultiplexedChannel[int](1)
	t.Cleanup(func() { close(m.Source) })

	healthy, cancelHealthy := m.Fork()
	t.Cleanup(cancelHealthy)
	go func() {
		for range healthy { //nolint:revive // drain to keep the healthy path moving
		}
	}()

	ghost, _ := m.Fork()

	// Flood past the buffer without reading the ghost, triggering overflow.
	const total = subscriberBufSize + 5
	for i := 1; i <= total; i++ {
		require.Truef(t,
			sendOrTimeout(t, m.Source, i, multiplexTestTimeout),
			"send %d should pass; the ghost must not back-pressure the producer", i,
		)
	}

	// A consumer ranging over the ghost channel must terminate: the channel
	// holds its buffered values and is then closed by the overflow drop.
	drained := make(chan struct{})
	go func() {
		for range ghost { //nolint:revive // we only care that the range terminates
		}
		close(drained)
	}()

	select {
	case <-drained:
	case <-time.After(multiplexTestTimeout):
		t.Fatal("ghost channel was not closed after overflow; a ranging consumer would leak")
	}
}

// TestMultiplexedChannel_MultipleHealthyUnaffectedByOneGhost shows that with
// several healthy subscribers and one ghost, every healthy subscriber receives
// the full stream; only the ghost is dropped.
func TestMultiplexedChannel_MultipleHealthyUnaffectedByOneGhost(t *testing.T) {
	t.Parallel()

	m := NewMultiplexedChannel[int](1)
	t.Cleanup(func() { close(m.Source) })

	const healthyCount = 3
	collectors := make([]chan int, healthyCount)
	for i := range collectors {
		ch, cancel := m.Fork()
		t.Cleanup(cancel)

		collectors[i] = make(chan int, subscriberBufSize+32)
		go func(src <-chan int, sink chan int) {
			for v := range src {
				sink <- v
			}
		}(ch, collectors[i])
	}

	// One ghost among the healthy subscribers.
	forkGhost(m)

	const total = subscriberBufSize + 10
	for i := 1; i <= total; i++ {
		require.Truef(t,
			sendOrTimeout(t, m.Source, i, multiplexTestTimeout),
			"send %d must not be back-pressured by the ghost", i,
		)
	}

	for c, sink := range collectors {
		for i := 1; i <= total; i++ {
			got, ok := recvOrTimeout(t, sink, multiplexTestTimeout)
			require.Truef(t, ok, "healthy subscriber %d should receive value %d", c, i)
			assert.Equalf(t, i, got, "healthy subscriber %d received out-of-order value", c)
		}
	}
}
