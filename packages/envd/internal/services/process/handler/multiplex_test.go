package handler

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for MultiplexedChannel fan-out, covering:
//  1. Normal fan-out semantics.
//  2. Regression: a ghost subscriber (stuck consumer that has NOT called
//     cancel) must not block the fan-out loop or starve healthy subscribers.
//  3. A temporarily slow but live subscriber must NOT be cancelled on buffer
//     overflow; it stays subscribed and continues to receive future events.

const multiplexTestTimeout = 500 * time.Millisecond

// recvOrTimeout reads one value from ch or returns ok=false after timeout.
func recvOrTimeout[T any](t *testing.T, ch <-chan T, timeout time.Duration) (T, bool) {
	t.Helper()

	select {
	case v, ok := <-ch:
		return v, ok
	case <-time.After(timeout):
		var zero T

		return zero, false
	}
}

// sendOrTimeout pushes v into ch or returns false after timeout.
func sendOrTimeout[T any](t *testing.T, ch chan<- T, v T, timeout time.Duration) bool {
	t.Helper()

	select {
	case ch <- v:
		return true
	case <-time.After(timeout):
		return false
	}
}

func TestMultiplexedChannel_BasicFanOut(t *testing.T) {
	t.Parallel()

	m := NewMultiplexedChannel[int](1)

	consA, cancelA := m.Fork()
	consB, cancelB := m.Fork()
	t.Cleanup(cancelA)
	t.Cleanup(cancelB)

	var wg sync.WaitGroup
	wg.Add(2)

	gotA := make([]int, 0, 3)
	gotB := make([]int, 0, 3)

	go func() {
		defer wg.Done()
		for v := range consA {
			gotA = append(gotA, v)
		}
	}()

	go func() {
		defer wg.Done()
		for v := range consB {
			gotB = append(gotB, v)
		}
	}()

	for _, v := range []int{1, 2, 3} {
		require.True(t,
			sendOrTimeout(t, m.Source, v, multiplexTestTimeout),
			"basic fan-out should not block when subscribers drain",
		)
	}

	close(m.Source)
	wg.Wait()

	assert.Equal(t, []int{1, 2, 3}, gotA)
	assert.Equal(t, []int{1, 2, 3}, gotB)
}

// TestGhostSubscriberDoesNotWedgeFanOut is the core regression test for
// issue #3292. It models the exact production scenario:
//
//  1. A healthy subscriber actively drains its channel.
//  2. A ghost subscriber exists whose consumer goroutine is stuck (simulated
//     by simply never reading from the channel) and has NOT called cancel —
//     mirroring the real case where stream.Send to a dead peer blocks for the
//     full TCP retransmit timeout (~924 s with tcp_retries2=15).
//
// After the fix, the fan-out must skip the ghost's full buffer non-blockingly
// and continue delivering events to the healthy subscriber without any stall.
func TestGhostSubscriberDoesNotWedgeFanOut(t *testing.T) {
	t.Parallel()

	m := NewMultiplexedChannel[int](0)
	defer close(m.Source)

	// Healthy subscriber: goroutine actively drains.
	healthy, cancelHealthy := m.Fork()
	t.Cleanup(cancelHealthy)

	healthyGot := make(chan int, subscriberBufSize+10)
	go func() {
		for v := range healthy {
			healthyGot <- v
		}
	}()

	// Ghost subscriber: channel is never read and cancel is never called,
	// simulating a consumer goroutine stuck inside stream.Send.
	_, _ = m.Fork()
	// Deliberately do not read from ghost's channel and do not call cancel.

	// Flood enough events to fill the ghost's buffer and keep going.
	// After subscriberBufSize events the ghost's buffer is full; from that
	// point every subsequent event takes the default: branch (non-blocking
	// skip) so the fan-out must never stall.
	const events = subscriberBufSize + 10

	for i := range events {
		ok := sendOrTimeout(t, m.Source, i, multiplexTestTimeout)
		require.Truef(t, ok,
			"fan-out blocked on ghost subscriber at event %d; "+
				"healthy subscriber should not be starved", i)
	}

	// Healthy subscriber must have received all events (it has its own buffer).
	for i := range events {
		v, ok := recvOrTimeout(t, healthyGot, multiplexTestTimeout)
		require.Truef(t, ok, "healthy subscriber did not receive event %d", i)
		assert.Equal(t, i, v)
	}
}

// TestSlowLiveSubscriberIsNotCancelledOnOverflow verifies that a subscriber
// whose buffer is temporarily full is NOT cancelled or disconnected. Once it
// drains its buffer it must continue to receive subsequent events normally,
// preserving HasSubscribers() == true throughout.
func TestSlowLiveSubscriberIsNotCancelledOnOverflow(t *testing.T) {
	t.Parallel()

	m := NewMultiplexedChannel[int](0)
	defer close(m.Source)

	slow, cancelSlow := m.Fork()
	t.Cleanup(cancelSlow)

	// Fill the slow subscriber's buffer without draining it.
	for i := range subscriberBufSize {
		require.True(t, sendOrTimeout(t, m.Source, i, multiplexTestTimeout),
			"pre-fill send %d timed out", i)
	}

	// One more event: this hits default: in the fan-out (buffer full).
	// The subscriber must NOT be cancelled.
	require.True(t, sendOrTimeout(t, m.Source, -1, multiplexTestTimeout),
		"overflow event timed out")

	// The subscriber must still be alive (not cancelled).
	assert.True(t, m.HasSubscribers(),
		"slow subscriber was cancelled on overflow; HasSubscribers must stay true "+
			"to avoid silently truncating subsequent process output")

	// Drain the buffer: subscriber catches up.
	got := 0
	for range subscriberBufSize {
		_, ok := recvOrTimeout(t, slow, multiplexTestTimeout)
		require.True(t, ok, "unexpected channel close while draining")
		got++
	}
	assert.Equal(t, subscriberBufSize, got)

	// After draining, the subscriber must receive new events normally.
	require.True(t, sendOrTimeout(t, m.Source, 999, multiplexTestTimeout),
		"post-drain send timed out")
	v, ok := recvOrTimeout(t, slow, multiplexTestTimeout)
	require.True(t, ok, "slow subscriber did not receive post-drain event")
	assert.Equal(t, 999, v)
}

// TestMultiplexedChannel_AbandonedConsumerDoesNotWedgeFanOut verifies that a
// consumer that reads one value and then explicitly cancels does not block the
// producer or starve other subscribers.
func TestMultiplexedChannel_AbandonedConsumerDoesNotWedgeFanOut(t *testing.T) {
	t.Parallel()

	m := NewMultiplexedChannel[int](1)
	t.Cleanup(func() { close(m.Source) })

	abandoned, cancelAbandoned := m.Fork()

	// Consumer reads one value then exits, modelling a disconnected client.
	abandonReader := make(chan struct{})
	go func() {
		<-abandoned
		close(abandonReader)
	}()

	require.True(t,
		sendOrTimeout(t, m.Source, 1, multiplexTestTimeout),
		"first send should be deliverable",
	)

	select {
	case <-abandonReader:
	case <-time.After(multiplexTestTimeout):
		t.Fatal("abandoned consumer should have read its single value")
	}

	// Simulate the handler's deferred cancel after return.
	cancelDone := make(chan struct{})
	go func() {
		cancelAbandoned()
		close(cancelDone)
	}()
	select {
	case <-cancelDone:
	case <-time.After(multiplexTestTimeout):
		t.Fatal("cancel func did not return promptly; fan-out is wedged")
	}

	// Producer should still make progress through Source.
	for i := 2; i <= 8; i++ {
		require.Truef(t,
			sendOrTimeout(t, m.Source, i, multiplexTestTimeout),
			"send %d should not be back-pressured by an abandoned consumer", i,
		)
	}
}

// TestMultiplexedChannel_AbandonedConsumerDoesNotStarveOthers checks that
// a cancelled subscriber does not prevent other live subscribers from receiving.
func TestMultiplexedChannel_AbandonedConsumerDoesNotStarveOthers(t *testing.T) {
	t.Parallel()

	m := NewMultiplexedChannel[int](1)
	t.Cleanup(func() { close(m.Source) })

	healthy, cancelHealthy := m.Fork()
	t.Cleanup(cancelHealthy)

	healthyReceived := make(chan int, 16)
	go func() {
		for v := range healthy {
			healthyReceived <- v
		}
	}()

	abandoned, cancelAbandoned := m.Fork()
	go func() {
		<-abandoned
	}()

	require.True(t,
		sendOrTimeout(t, m.Source, 1, multiplexTestTimeout),
		"first send should pass while both consumers are active",
	)
	got, ok := recvOrTimeout(t, healthyReceived, multiplexTestTimeout)
	require.True(t, ok, "healthy subscriber should receive value 1")
	assert.Equal(t, 1, got)

	// Abandon and cancel the second subscriber.
	cancelAbandoned()

	// Healthy subscriber must keep receiving every subsequent value.
	for i := 2; i <= 6; i++ {
		require.Truef(t,
			sendOrTimeout(t, m.Source, i, multiplexTestTimeout),
			"send %d should not be back-pressured", i,
		)
		got, ok = recvOrTimeout(t, healthyReceived, multiplexTestTimeout)
		require.Truef(t, ok, "healthy subscriber should still receive value %d", i)
		assert.Equalf(t, i, got, "healthy subscriber received wrong value")
	}
}

// cancel must be idempotent and non-blocking even under producer load.
func TestMultiplexedChannel_CancelIsIdempotentAndPrompt(t *testing.T) {
	t.Parallel()

	m := NewMultiplexedChannel[int](0)

	_, cancel := m.Fork()

	// Concurrently push values without anyone draining the consumer chan.
	stop := make(chan struct{})
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for {
			select {
			case <-stop:
				return
			case m.Source <- 0:
			}
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-producerDone
		close(m.Source)
	})

	// Give the fan-out a chance to enter a per-subscriber select.
	time.Sleep(20 * time.Millisecond)

	cancelDone := make(chan struct{})
	go func() {
		cancel()
		cancel() // idempotent
		cancel()
		close(cancelDone)
	}()
	select {
	case <-cancelDone:
	case <-time.After(multiplexTestTimeout):
		t.Fatal("cancel func did not return promptly under producer load")
	}
}

// Closing Source must close all live subscriber channels.
func TestMultiplexedChannel_SourceCloseClosesLiveSubscribers(t *testing.T) {
	t.Parallel()

	m := NewMultiplexedChannel[int](1)

	cons, cancel := m.Fork()
	t.Cleanup(cancel)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range cons { //nolint:revive // drain until closed
		}
	}()

	require.True(t,
		sendOrTimeout(t, m.Source, 1, multiplexTestTimeout),
		"send should succeed",
	)

	close(m.Source)

	select {
	case <-done:
	case <-time.After(multiplexTestTimeout):
		t.Fatal("consumer's `for v := range cons` did not terminate after " +
			"Source close")
	}
}

// Fork after Source close must return a pre-closed channel.
func TestMultiplexedChannel_ForkAfterSourceCloseReturnsClosedChan(t *testing.T) {
	t.Parallel()

	m := NewMultiplexedChannel[int](0)
	close(m.Source)

	// Wait for the fan-out goroutine to observe Source close.
	deadline := time.Now().Add(multiplexTestTimeout)
	for !m.exited.Load() {
		if time.Now().After(deadline) {
			t.Fatal("fan-out did not mark itself exited after Source close")
		}
		time.Sleep(time.Millisecond)
	}

	cons, cancel := m.Fork()
	cancel() // must not panic

	_, ok := recvOrTimeout(t, cons, multiplexTestTimeout)
	assert.False(t, ok, "Fork after shutdown must return a pre-closed channel")
}

// Goroutine count must return to baseline after cancelled subscribers settle.
func TestMultiplexedChannel_NoGoroutineLeakOnAbandon(t *testing.T) { //nolint:paralleltest // relies on a stable goroutine count
	const wedges = 16

	time.Sleep(50 * time.Millisecond)
	runtime.GC() //nolint:revive // intentional: settle goroutines before measuring baseline
	before := runtime.NumGoroutine()

	for range wedges {
		m := NewMultiplexedChannel[int](1)
		_, cancel := m.Fork()
		// Park one value so fan-out has work, then cancel mid-iteration.
		m.Source <- 0
		cancel()
		close(m.Source)
	}

	// Allow scheduled goroutines to finish.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC() //nolint:revive // intentional: help goroutines finalize
		runtime.Gosched()
		time.Sleep(20 * time.Millisecond)
		if runtime.NumGoroutine() <= before+2 {
			break
		}
	}

	after := runtime.NumGoroutine()
	leaked := after - before
	// Small slack for runtime bookkeeping; the old bug leaked >= wedges.
	assert.LessOrEqualf(t, leaked, 2,
		"goroutine count grew by %d after %d cancelled wedges; "+
			"before=%d after=%d (expected ~0)", leaked, wedges, before, after,
	)
}

// TestMultipleHealthySubscribersWithOneGhost ensures that N healthy
// subscribers all receive all events even when one ghost is present.
func TestMultipleHealthySubscribersWithOneGhost(t *testing.T) {
	t.Parallel()

	const numHealthy = 3
	const events = subscriberBufSize + 5

	m := NewMultiplexedChannel[int](0)
	defer close(m.Source)

	// Ghost: never reads, never cancels.
	_, _ = m.Fork()

	// Healthy subscribers.
	type result struct {
		mu  sync.Mutex
		got []int
	}
	results := make([]*result, numHealthy)
	for i := range numHealthy {
		ch, cancel := m.Fork()
		t.Cleanup(cancel)
		r := &result{}
		results[i] = r
		go func(c <-chan int, res *result) {
			for v := range c {
				res.mu.Lock()
				res.got = append(res.got, v)
				res.mu.Unlock()
			}
		}(ch, r)
	}

	for i := range events {
		require.Truef(t,
			sendOrTimeout(t, m.Source, i, multiplexTestTimeout),
			"send %d blocked despite ghost subscriber being non-blocking", i,
		)
	}

	// Give goroutines time to drain their buffered channels.
	time.Sleep(50 * time.Millisecond)

	for i, r := range results {
		r.mu.Lock()
		got := len(r.got)
		r.mu.Unlock()
		assert.Equalf(t, events, got,
			"healthy subscriber %d received %d events, expected %d", i, got, events)
	}
}
