package redis

import (
	"context"
)

// Notifier is the public seam onto the shared storage pub/sub infrastructure.
type Notifier struct {
	sub *subscriptionManager
	pub *publisher
}

// Subscribe registers interest in routingKey. The returned channel is
// signaled (non-blocking, drop-on-full) whenever a matching message
// arrives on the shared notify channel. The caller MUST invoke cleanup
// when done to avoid a memory leak.
func (n *Notifier) Subscribe(routingKey string) (<-chan struct{}, func()) {
	return n.sub.subscribe(routingKey)
}

// Publish enqueues routingKey for asynchronous PUBLISH on the shared
// notify channel. Every consumer should use a fallback ticker.
func (n *Notifier) Publish(ctx context.Context, routingKey string) {
	n.pub.Publish(ctx, routingKey)
}

// Notifier returns the cross-package pub/sub seam
func (s *Storage) Notifier() *Notifier {
	return &Notifier{sub: s.subManager, pub: s.publisher}
}
