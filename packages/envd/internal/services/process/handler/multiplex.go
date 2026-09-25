package handler

import (
	"slices"
	"sync"
	"sync/atomic"
)

// subscriberBufSize is the capacity of each subscriber's channel. A buffer
// this deep lets a temporarily slow consumer absorb bursts without blocking
// the fan-out loop. When the buffer is full the fan-out drops that event for
// that subscriber (non-blockingly) rather than stalling all other consumers.
//
// 256 gives a slow-but-live client roughly 256 ms of headroom at a process
// emission rate of ~1000 events/s before it starts losing events; a ghost
// subscriber (consumer goroutine stuck inside stream.Send to a dead peer)
// never blocks the fan-out loop regardless of how long TCP takes to time out.
const subscriberBufSize = 256

// MultiplexedChannel fans out values written to Source to every subscriber
// obtained via Fork. Each subscriber channel is buffered; when a subscriber's
// buffer is full the fan-out skips that one event for that subscriber instead
// of blocking, ensuring that a stuck or slow consumer cannot wedge the loop
// or starve the others.
type MultiplexedChannel[T any] struct {
	Source chan T

	mu       sync.RWMutex
	channels []*subscriber[T]
	exited   atomic.Bool
}

type subscriber[T any] struct {
	ch   chan T
	done chan struct{}
	once sync.Once
}

// cancel marks the subscriber as gone. Idempotent and non-blocking.
func (s *subscriber[T]) cancel() {
	s.once.Do(func() {
		close(s.done)
	})
}

// isCancelled reports whether cancel has been called.
func (s *subscriber[T]) isCancelled() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

func NewMultiplexedChannel[T any](buffer int) *MultiplexedChannel[T] {
	c := &MultiplexedChannel[T]{
		Source: make(chan T, buffer),
	}

	go c.run()

	return c
}

// run is the fan-out loop. It delivers each Source value to every live
// subscriber and closes all consumer channels when Source is closed.
func (m *MultiplexedChannel[T]) run() {
	for v := range m.Source {
		m.mu.RLock()
		subs := m.channels
		m.mu.RUnlock()

		for _, s := range subs {
			// Skip already-cancelled subscribers.
			if s.isCancelled() {
				continue
			}

			select {
			case s.ch <- v:
			case <-s.done:
			default:
				// The subscriber's buffer is full. Skip this event for this
				// subscriber rather than blocking the fan-out loop.
				//
				// A legitimately slow client will drain its buffer as soon as it
				// catches up, and will resume receiving future events normally;
				// only the events dropped during the burst are lost. A ghost
				// subscriber (consumer goroutine stuck inside stream.Send to a
				// dead peer) will have its buffer permanently full and every
				// event skipped until the underlying TCP timeout eventually
				// closes the connection and the consumer goroutine exits.
				//
				// In either case the subscriber is NOT cancelled here. Cancelling
				// on the first overflow would disconnect a temporarily slow but
				// still-alive client, causing HasSubscribers() to return false
				// and silently truncating subsequent process output.
			}
		}
	}

	m.exited.Store(true)

	// Close all remaining consumer channels so `for range` loops exit.
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, s := range m.channels {
		s.cancel()
		close(s.ch)
	}
	m.channels = nil
}

// HasSubscribers reports whether any non-cancelled subscriber exists.
func (m *MultiplexedChannel[T]) HasSubscribers() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, s := range m.channels {
		if !s.isCancelled() {
			return true
		}
	}

	return false
}

// Fork registers a new subscriber and returns its channel plus a cancel func.
// If Source is already closed it returns a pre-closed channel and a no-op cancel.
func (m *MultiplexedChannel[T]) Fork() (<-chan T, func()) {
	if m.exited.Load() {
		ch := make(chan T)
		close(ch)

		return ch, func() {}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Re-check under lock in case run() finished between the fast path and here.
	if m.exited.Load() {
		ch := make(chan T)
		close(ch)

		return ch, func() {}
	}

	s := &subscriber[T]{
		ch:   make(chan T, subscriberBufSize),
		done: make(chan struct{}),
	}

	m.channels = append(m.channels, s)

	return s.ch, func() {
		m.remove(s)
	}
}

// remove unsubscribes s. Safe to call multiple times.
func (m *MultiplexedChannel[T]) remove(s *subscriber[T]) {
	// Cancel before locking so an in-flight fan-out send can unblock.
	s.cancel()

	m.mu.Lock()
	defer m.mu.Unlock()

	for i, sub := range m.channels {
		if sub == s {
			// New backing array so run()'s concurrent iteration is safe.
			m.channels = slices.Concat(m.channels[:i], m.channels[i+1:])

			return
		}
	}
}
