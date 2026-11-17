package redis

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"
)

// subscriptionManager maintains a Redis PubSub connection and
// fans out storage notifications to registered in-process waiters.
type subscriptionManager struct {
	mu      sync.RWMutex
	waiters map[string]map[chan struct{}]struct{} // routingKey → registered waiters

	redisClient redis.UniversalClient
	channel     string
	stop        chan struct{}
	once        sync.Once
}

func newSubscriptionManager(redisClient redis.UniversalClient, channel string) *subscriptionManager {
	return &subscriptionManager{
		waiters:     make(map[string]map[chan struct{}]struct{}),
		redisClient: redisClient,
		channel:     channel,
		stop:        make(chan struct{}),
	}
}

// start subscribes to the global PubSub channel and dispatches signals
// to registered waiters. It blocks until the context is cancelled or
// close is called. It is intended to be called in a goroutine.
func (m *subscriptionManager) start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Cancel the context when close is called.
	go func() {
		select {
		case <-m.stop:
			cancel()
		case <-ctx.Done():
		}
	}()

	ps := m.redisClient.Subscribe(ctx, m.channel)
	defer ps.Close()

	ch := ps.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			m.dispatch(msg.Payload)
		}
	}
}

// subscribe registers a waiter for the given routingKey.
// The returned channel receives a signal when a matching PubSub message
// arrives for that transition. The caller MUST invoke the returned cleanup
// function when done to avoid a memory leak.
func (m *subscriptionManager) subscribe(routingKey string) (<-chan struct{}, func()) {
	channel := make(chan struct{}, 1) // buffered so dispatch never blocks

	m.mu.Lock()
	if m.waiters[routingKey] == nil {
		m.waiters[routingKey] = make(map[chan struct{}]struct{})
	}
	m.waiters[routingKey][channel] = struct{}{}
	m.mu.Unlock()

	cleanup := func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		delete(m.waiters[routingKey], channel)
		if len(m.waiters[routingKey]) == 0 {
			delete(m.waiters, routingKey)
		}
	}

	return channel, cleanup
}

// dispatch signals all waiters registered for the given routing key.
func (m *subscriptionManager) dispatch(routingKey string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for waiter := range m.waiters[routingKey] {
		select {
		case waiter <- struct{}{}:
		default:
			// Waiter already has a pending signal; skip.
		}
	}
}

// close shuts down the subscription manager and its Redis PubSub connection.
func (m *subscriptionManager) close() {
	m.once.Do(func() {
		close(m.stop)
	})
}
