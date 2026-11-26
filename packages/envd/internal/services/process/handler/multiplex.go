package handler

import (
	"slices"
	"sync"
	"sync/atomic"
)

// MultiplexedChannel fans out values written to Source to every subscriber
// obtained via Fork. Each subscriber send is guarded by a done channel so
// a cancelled consumer can never wedge the fan-out loop.
type MultiplexedChannel[T any] struct {
	Source chan T

	mu       sync.RWMutex
	channels []*subscriber[T]
	exited   atomic.Bool
}

// subscriberBufSize is how many fan-out events a subscriber channel buffers
// before the consumer is treated as a ghost. It absorbs normal bursts past a
// briefly-slow consumer; a consumer that falls this far behind (e.g. one whose
// client TCP connection died after a resume and is stuck in stream.Send) is
// dropped so it cannot wedge the fan-out for everyone else.
const subscriberBufSize = 64

type subscriber[T any] struct {
	ch   chan T
	done chan struct{}
	once sync.Once
	// chOnce guards closing ch; separate from once (which guards done) so cancel
	// and closeCh are independent and each idempotent.
	chOnce sync.Once
}

// cancel marks the subscriber as gone. Idempotent and non-blocking.
func (s *subscriber[T]) cancel() {
	s.once.Do(func() {
		close(s.done)
	})
}

// closeCh closes the consumer channel so a ranging consumer exits. Idempotent.
func (s *subscriber[T]) closeCh() {
	s.chOnce.Do(func() {
		close(s.ch)
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
				// Buffer full and the consumer is not draining it — a ghost
				// (e.g. a dead client TCP connection stuck in stream.Send).
				// Drop it: cancel so later iterations skip it, and close its
				// channel so its consumer goroutine exits. Healthy subscribers
				// are untouched; their sends buffered or completed above.
				s.cancel()
				s.closeCh()
			}
		}
	}

	m.exited.Store(true)

	// Close all remaining consumer channels so `for range` loops exit.
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, s := range m.channels {
		s.cancel()
		s.closeCh()
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
